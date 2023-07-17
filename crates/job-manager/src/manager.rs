mod run_subtask;
#[cfg(test)]
mod tests;

use std::fmt::Debug;
use std::sync::Arc;

use crate::{
    scheduler::{SchedulerBehavior, SlowTaskBehavior},
    spawn::{Spawner, TaskError},
    task_status::{StatusCollector, StatusUpdateInput},
    TaskDefWithOutput, TaskInfo, TaskType,
};
use error_stack::{IntoReport, IntoReportCompat, Report, ResultExt};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::sync::Semaphore;

use self::run_subtask::{SubtaskPayload, SubtaskSyncs};

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
        let mut stage_tasks = self
            .task_type
            .create_initial_subtasks(&task_def)
            .await
            .into_report()
            .change_context(TaskError::TaskGenerationFailed)?;

        let mut stage_index = 0;
        loop {
            let stage_results = self
                .run_tasks_stage(stage_index, status_collector.clone(), stage_tasks)
                .await?;

            stage_tasks = self
                .task_type
                .create_subtasks_from_result(&task_def, stage_index, &stage_results)
                .await
                .into_report()
                .change_context(TaskError::TaskGenerationFailed)?;

            if stage_tasks.is_empty() {
                return Ok(stage_results
                    .into_iter()
                    .map(|task| task.output_location)
                    .collect());
            }

            stage_index += 1;
        }
    }

    async fn run_tasks_stage<DEF: TaskInfo + Send>(
        &self,
        stage_index: usize,
        status_collector: StatusCollector,
        inputs: Vec<DEF>,
    ) -> Result<Vec<TaskDefWithOutput<DEF>>, Report<TaskError>> {
        let max_concurrent_tasks = self
            .scheduler
            .max_concurrent_tasks
            .unwrap_or(i32::MAX as usize);
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

        // We never transmit anything on cancel_tx, but let it drop at the end of the function to
        // cancel any tasks still running.
        let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(());
        let mut failed = false;

        let job_semaphore = Semaphore::new(max_concurrent_tasks);
        let syncs = Arc::new(SubtaskSyncs {
            job_semaphore,
            global_semaphore: self.global_semaphore.clone(),
            cancel: cancel_rx,
        });

        let mut running_tasks = FuturesUnordered::new();
        let spawn_task = |i: usize,
                          task: &TaskTrackingInfo<DEF>,
                          futures: &mut FuturesUnordered<_>,
                          failed: &mut bool| {
            let input = task
                .input
                .serialize_input()
                .into_report()
                .change_context(TaskError::TaskGenerationFailed);

            let input = match input {
                Ok(p) => p,
                Err(e) => {
                    status_collector.add(stage_index, i, StatusUpdateInput::Failed(e));
                    *failed = true;
                    return;
                }
            };

            let spawn_name = task.input.spawn_name();
            let local_id = format!(
                "{stage_index:03}:{i:05}:{try_num:02}",
                try_num = task.try_num
            );
            let payload = SubtaskPayload {
                input,
                spawn_name,
                local_id,
                stage_index,
                try_num: task.try_num,
                status_collector: status_collector.clone(),
                spawner: self.spawner.clone(),
            };

            let new_task = tokio::task::spawn(run_subtask::run_subtask(i, syncs.clone(), payload))
                .map(move |join_handle| (i, join_handle));
            futures.push(new_task);
        };

        for (i, task) in unfinished.iter_mut().enumerate() {
            let task = task.as_mut().expect("task was None right away");
            spawn_task(i, task, &mut running_tasks, &mut failed);
            if failed {
                break;
            }
        }

        let mut performed_tail_retry = false;

        while !failed && output_list.len() < total_num_tasks {
            while let Some((task_index, result)) = running_tasks.next().await {
                match result {
                    // The semaphore closed so the task did not run. This means that the whole
                    // system is shutting down, so don't worry about it here.
                    Ok(None) => {}
                    Ok(Some(Ok(output))) => {
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
                                            stage_index,
                                            i,
                                            StatusUpdateInput::Retry((
                                                i,
                                                Report::new(TaskError::TailRetry),
                                            )),
                                        );

                                        spawn_task(i, task, &mut running_tasks, &mut failed);
                                    }
                                }
                            }
                        }
                    }
                    // Task finished with an error.
                    Ok(Some(Err(e))) => {
                        if let Some(task_info) = unfinished[task_index].as_mut() {
                            if e.current_context().retryable()
                                && task_info.try_num <= self.scheduler.max_retries
                            {
                                status_collector.add(
                                    stage_index,
                                    task_index,
                                    StatusUpdateInput::Retry((task_info.try_num, e)),
                                );
                                task_info.try_num += 1;
                                spawn_task(task_index, task_info, &mut running_tasks, &mut failed);
                            } else {
                                status_collector.add(
                                    stage_index,
                                    task_index,
                                    StatusUpdateInput::Failed(e),
                                );
                                failed = true;
                            }
                        }
                    }
                    // Task panicked. We can't really decipher the error so always consider it
                    // retryable.
                    Err(e) => {
                        if let Some(task_info) = unfinished[task_index].as_mut() {
                            let e = Report::new(e).change_context(TaskError::Failed(true));
                            if task_info.try_num <= self.scheduler.max_retries {
                                status_collector.add(
                                    stage_index,
                                    task_index,
                                    StatusUpdateInput::Retry((task_info.try_num, e)),
                                );
                                task_info.try_num += 1;
                                spawn_task(task_index, task_info, &mut running_tasks, &mut failed);
                            } else {
                                status_collector.add(
                                    stage_index,
                                    task_index,
                                    StatusUpdateInput::Failed(e),
                                );
                                failed = true;
                            }
                        }
                    }
                }

                if self
                    .global_semaphore
                    .as_ref()
                    .map(|s| s.is_closed())
                    .unwrap_or(false)
                {
                    // The system is shutting down.
                    break;
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
