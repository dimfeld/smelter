use std::borrow::Cow;
use std::fmt::Debug;
use std::sync::Arc;

use crate::{
    scheduler::{SchedulerBehavior, SlowTaskBehavior},
    spawn::{SpawnedTask, Spawner, TaskError},
    task_status::{StatusCollector, StatusUpdateInput},
    TaskDefWithOutput, TaskInfo, TaskType,
};
use error_stack::{IntoReport, IntoReportCompat, Report, ResultExt};
use tokio::sync::Semaphore;
#[derive(Debug)]
struct TaskTrackingInfo<INPUT: Debug> {
    input: INPUT,
    try_num: usize,
}

pub struct JobManager<TASKTYPE: TaskType, SPAWNER: Spawner> {
    spawner: Arc<SPAWNER>,
    task_type: TASKTYPE,
    scheduler: SchedulerBehavior,
    global_semaphore: Option<Arc<Semaphore>>,
}

impl<TASKTYPE: TaskType, SPAWNER: Spawner> JobManager<TASKTYPE, SPAWNER> {
    pub fn new(
        task_type: TASKTYPE,
        spawner: Arc<SPAWNER>,
        scheduler: SchedulerBehavior,
        global_semaphore: Option<Arc<Semaphore>>,
    ) -> Self {
        Self {
            spawner,
            task_type,
            scheduler,
            global_semaphore,
        }
    }

    pub async fn run(
        &self,
        status_collector: StatusCollector,
        task_def: TASKTYPE::TaskDef,
    ) -> Result<Vec<String>, Report<TaskError>> {
        let map_tasks = self
            .task_type
            .create_map_tasks(&task_def)
            .await
            .into_report()
            .change_context(TaskError::TaskGenerationFailed)?;

        let map_results = self
            .run_tasks_stage("map".to_string(), status_collector.clone(), map_tasks)
            .await?;

        let first_reducer_tasks = self
            .task_type
            .create_top_level_reducers(&task_def, &map_results)
            .await
            .into_report()
            .change_context(TaskError::TaskGenerationFailed)?;

        if first_reducer_tasks.is_empty() {
            return Ok(map_results
                .into_iter()
                .map(|task| task.output_location)
                .collect());
        }

        let mut reducer_results = self
            .run_tasks_stage(
                "reducer_0".to_string(),
                status_collector.clone(),
                first_reducer_tasks,
            )
            .await?
            .into_iter()
            .map(|task| TaskDefWithOutput {
                task_def: task.task_def.into(),
                output_location: task.output_location,
            })
            .collect::<Vec<_>>();

        let mut reducer_index = 1;
        loop {
            let reducer_tasks = self
                .task_type
                .create_intermediate_reducers(&task_def, &reducer_results)
                .await
                .into_report()
                .change_context(TaskError::TaskGenerationFailed)?;
            if reducer_tasks.is_empty() {
                break;
            }

            reducer_results = self
                .run_tasks_stage(
                    format!("reducer_{reducer_index}"),
                    status_collector.clone(),
                    reducer_tasks,
                )
                .await?;
            reducer_index += 1;
        }

        Ok(reducer_results
            .into_iter()
            .map(|task| task.output_location)
            .collect())
    }

    async fn run_tasks_stage<DEF: TaskInfo + Send>(
        &self,
        stage_name: String,
        status_collector: StatusCollector,
        inputs: Vec<DEF>,
    ) -> Result<Vec<TaskDefWithOutput<DEF>>, Report<TaskError>> {
        let max_concurrent_tasks = self.scheduler.max_concurrent_tasks.unwrap_or(usize::MAX);
        let total_num_tasks = inputs.len();
        let mut unfinished = inputs
            .into_iter()
            .map(|input| Some(TaskTrackingInfo { input, try_num: 0 }))
            .collect::<Vec<_>>();
        let mut output_list = Vec::with_capacity(unfinished.len());

        // when the number of in progress tasks drops below this number, retry all the remaining
        // tasks.
        let retry_all_at = match self.scheduler.slow_task_behavior {
            SlowTaskBehavior::Wait => 0,
            SlowTaskBehavior::RerunLastPercent(n) => total_num_tasks * n / 100,
            SlowTaskBehavior::RerunLastN(n) => std::cmp::min(n, total_num_tasks / 2),
        };

        let mut failed = false;
        let (results_tx, results_rx) = flume::unbounded();

        let job_semaphore = Semaphore::new(max_concurrent_tasks);
        let syncs = Arc::new(TaskSyncs {
            job_semaphore,
            global_semaphore: self.global_semaphore.clone(),
            results_tx,
        });

        let spawn_task = |i: usize, task: &TaskTrackingInfo<DEF>| {
            let input = task
                .input
                .serialize_input()
                .into_report()
                .change_context(TaskError::TaskGenerationFailed);

            let input = match input {
                Ok(p) => p,
                Err(e) => {
                    status_collector.add(stage_name.clone(), i, StatusUpdateInput::Failed(e));
                    return false;
                }
            };

            let spawn_name = task.input.spawn_name();
            let local_id = format!("{stage_name}:{i:05}:{try_num:02}", try_num = task.try_num);
            let payload = TaskPayload {
                input,
                spawn_name,
                local_id,
                stage_name: stage_name.clone(),
                try_num: task.try_num,
                status_collector: status_collector.clone(),
                spawner: self.spawner.clone(),
            };

            tokio::task::spawn(run_one_task(i, syncs.clone(), payload));

            true
        };

        for (i, task) in unfinished.iter_mut().enumerate() {
            let task = task.as_mut().expect("task was None right away");
            let succeeded = spawn_task(i, task);
            if !succeeded {
                failed = true;
                break;
            }
        }

        let mut performed_tail_retry = false;

        while !failed && output_list.len() < total_num_tasks {
            if let Ok((task_index, result)) = results_rx.recv_async().await {
                match result {
                    Ok(output) => {
                        if let Some(task_info) = unfinished[task_index].take() {
                            output_list.push(TaskDefWithOutput {
                                task_def: task_info.input,
                                output_location: output.output_location,
                            });

                            if !performed_tail_retry
                                && total_num_tasks - output_list.len() < retry_all_at
                            {
                                performed_tail_retry = true;

                                // Re-enqueue all unfinished tasks. Some serverless platforms
                                // can have very high tail latency, so this gets
                                // around that issue. Since this isn't an error-based retry, we
                                // don't increment the retry count.
                                for (i, task) in unfinished.iter().enumerate() {
                                    if let Some(task) = task.as_ref() {
                                        status_collector.add(
                                            stage_name.clone(),
                                            i,
                                            StatusUpdateInput::Retry((
                                                i,
                                                Report::new(TaskError::TailRetry),
                                            )),
                                        );

                                        spawn_task(i, task);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if let Some(task_info) = unfinished[task_index].as_mut() {
                            if e.current_context().retryable()
                                && task_info.try_num <= self.scheduler.max_retries
                            {
                                status_collector.add(
                                    stage_name.clone(),
                                    task_index,
                                    StatusUpdateInput::Retry((task_info.try_num, e)),
                                );
                                task_info.try_num += 1;
                                spawn_task(task_index, task_info);
                            } else {
                                status_collector.add(
                                    stage_name.clone(),
                                    task_index,
                                    StatusUpdateInput::Failed(e),
                                );
                                failed = true;
                            }
                        }
                    }
                }
            }
        }

        syncs.job_semaphore.close();

        if failed {
            Err(TaskError::Failed(false)).into_report()
        } else {
            Ok(output_list)
        }
    }
}

impl<TASKTYPE: TaskType, SPAWNER: Spawner> Drop for JobManager<TASKTYPE, SPAWNER> {
    fn drop(&mut self) {
        if let Some(sem) = self.global_semaphore.as_ref() {
            sem.close();
        }
    }
}

struct TaskOutput {
    output_location: String,
}

struct TaskPayload<SPAWNER: Spawner> {
    input: Vec<u8>,
    stage_name: String,
    spawn_name: Cow<'static, str>,
    local_id: String,
    try_num: usize,
    status_collector: StatusCollector,
    spawner: Arc<SPAWNER>,
}

struct TaskSyncs {
    results_tx: flume::Sender<(usize, Result<TaskOutput, Report<TaskError>>)>,
    global_semaphore: Option<Arc<Semaphore>>,
    job_semaphore: Semaphore,
}

async fn run_one_task<SPAWNER: Spawner>(
    task_index: usize,
    syncs: Arc<TaskSyncs>,
    payload: TaskPayload<SPAWNER>,
) {
    let TaskSyncs {
        results_tx,
        global_semaphore,
        job_semaphore,
    } = syncs.as_ref();

    let job_acquired = job_semaphore.acquire().await;
    if job_acquired.is_err() {
        // The semaphore was closed which means that the whole job has already exited before we could run.
        return;
    }

    let global_acquired = match global_semaphore.as_ref() {
        Some(semaphore) => semaphore.acquire().await.map(Some),
        None => Ok(None),
    };
    if global_acquired.is_err() {
        // The entire job system is shutting down.
        return;
    }

    let result = run_one_task_internal(task_index, payload).await;
    results_tx.send((task_index, result)).ok();
}

async fn run_one_task_internal<SPAWNER: Spawner>(
    task_index: usize,
    payload: TaskPayload<SPAWNER>,
) -> Result<TaskOutput, Report<TaskError>> {
    let TaskPayload {
        input,
        stage_name,
        spawn_name,
        local_id,
        try_num,
        status_collector,
        spawner,
    } = payload;

    let mut task = spawner.spawn(local_id, spawn_name, input).await?;
    let runtime_id = task.runtime_id().await?;
    status_collector.add(
        stage_name,
        task_index,
        StatusUpdateInput::Spawned(runtime_id.clone()),
    );

    task.wait()
        .await
        .attach_printable_lazy(|| format!("Job {task_index} try {try_num}"))
        .attach_printable_lazy(|| format!("Runtime ID {runtime_id}"))?;

    let output = TaskOutput {
        output_location: task.output_location(),
    };

    Ok(output)
}
