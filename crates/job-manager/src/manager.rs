mod run_subtask;
#[cfg(test)]
mod tests;

use std::{fmt::Debug, marker::PhantomData, ops::Deref, sync::Arc};

use ahash::HashMap;
use error_stack::{IntoReport, IntoReportCompat, Report, ResultExt};
use flume::{Receiver, Sender};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::{sync::Semaphore, task::JoinHandle};
use tracing::instrument;

use self::run_subtask::{SubtaskPayload, SubtaskSyncs};
use crate::{
    scheduler::{SchedulerBehavior, SlowTaskBehavior},
    spawn::{Spawner, TaskError},
    task_status::{StatusCollector, StatusUpdateInput},
    Job, SubTask, TaskDefWithOutput,
};

#[derive(Debug, Copy, Clone)]
pub struct SubtaskId {
    pub stage: u16,
    pub task: u32,
    pub try_num: u16,
}

impl std::fmt::Display for SubtaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:03}-{:05}-{:02}", self.stage, self.task, self.try_num)
    }
}

#[derive(Debug)]
struct TaskTrackingInfo<INPUT: Debug> {
    input: INPUT,
    try_num: usize,
}

pub struct JobManager {
    scheduler: SchedulerBehavior,
    global_semaphore: Option<Arc<Semaphore>>,
    status_collector: StatusCollector,
}

impl JobManager {
    pub fn new(
        scheduler: SchedulerBehavior,
        status_collector: StatusCollector,
        global_semaphore: Option<Arc<Semaphore>>,
    ) -> Self {
        Self {
            scheduler,
            status_collector,
            global_semaphore,
        }
    }

    pub fn new_job(&self, scheduler: SchedulerBehavior) -> Job {
        Job::new(self, scheduler)
    }
}

pub struct Job {
    scheduler: SchedulerBehavior,
    job_semaphore: Arc<Semaphore>,
    global_semaphore: Option<Arc<Semaphore>>,
    status_collector: StatusCollector,
    num_stages: usize,
}

impl Job {
    fn new(manager: &JobManager, scheduler: SchedulerBehavior) -> Job {
        let max_concurrent_tasks = scheduler.max_concurrent_tasks.unwrap_or(i32::MAX as usize);
        let job_semaphore = Arc::new(Semaphore::new(max_concurrent_tasks));

        Job {
            scheduler,
            job_semaphore,
            global_semaphore: manager.global_semaphore.clone(),
            status_collector: manager.status_collector.clone(),
            num_stages: 0,
        }
    }

    #[instrument(skip(self))]
    pub async fn add_stage<SUBTASK: SubTask>(&mut self) -> JobStage<SUBTASK> {
        let stage_index = self.num_stages;
        self.num_stages += 1;

        // TODO update some internal state so we can detect when the stage is done.
        JobStage::new(stage_index, self)
    }
}

struct JobStageData<SUBTASK: SubTask> {
    pub subtask_result: Receiver<TaskDefWithOutput<SUBTASK>>,
    stage_index: usize,
    new_task_tx: Option<Sender<SUBTASK>>,
    stage_task: JoinHandle<Result<(), Report<TaskError>>>,
}

#[derive(Clone)]
pub struct JobStage<SUBTASK: SubTask>(Arc<JobStageData<SUBTASK>>);

impl<SUBTASK: SubTask> JobStage<SUBTASK> {
    fn new(stage_index: usize, job: &Job) -> JobStage<SUBTASK> {
        let (subtask_result_tx, subtask_result_rx) = flume::bounded(20);
        let (new_task_tx, new_task_rx) = flume::bounded(20);

        let stage_task = tokio::task::spawn(run_tasks_stage(
            stage_index,
            new_task_rx,
            subtask_result_tx,
            job.scheduler,
            job.status_collector.clone(),
            job.job_semaphore.clone(),
            job.global_semaphore.clone(),
        ));

        let data = JobStageData {
            stage_index,
            subtask_result: subtask_result_rx,
            new_task_tx: Some(new_task_tx),
            stage_task,
        };

        JobStage(Arc::new(data))
    }
}

impl<SUBTASK: SubTask> JobStageData<SUBTASK> {
    pub async fn add_subtask(&mut self, task: SUBTASK) {
        let Some(new_task_tx) = self.new_task_tx.as_ref() else {
            panic!("Tried to add new subtask after it had closed");
        };

        new_task_tx.send_async(task).await.ok();
    }

    pub fn finish(&mut self) {
        // TODO Return a future that resolves when the task is all done.
        self.new_task_tx.take();
    }

    pub fn is_finished(&self) -> bool {
        self.subtask_result.is_disconnected()
    }
}

async fn run_tasks_stage<SUBTASK: SubTask>(
    stage_index: usize,
    new_task_rx: Receiver<SUBTASK>,
    subtask_result_tx: Sender<TaskDefWithOutput<SUBTASK>>,
    scheduler: SchedulerBehavior,
    status_collector: StatusCollector,
    job_semaphore: Arc<Semaphore>,
    global_semaphore: Option<Arc<Semaphore>>,
) -> Result<(), Report<TaskError>> {
    let mut total_num_tasks = 0;
    let mut unfinished = HashMap::default();

    // when the number of in progress tasks drops below this number, retry all the remaining
    // tasks.
    let retry_all_at = match scheduler.slow_task_behavior {
        SlowTaskBehavior::Wait => 0,
        SlowTaskBehavior::RerunLastPercent(n) => total_num_tasks * n / 100,
        SlowTaskBehavior::RerunLastN(n) => std::cmp::min(n, total_num_tasks / 2),
    };

    // We never transmit anything on cancel_tx, but let it drop at the end of the function to
    // cancel any tasks still running.
    let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(());
    let mut failed = false;

    let syncs = Arc::new(SubtaskSyncs {
        job_semaphore: job_semaphore.clone(),
        global_semaphore: global_semaphore.clone(),
        cancel: cancel_rx,
    });

    let mut running_tasks = FuturesUnordered::new();
    let spawn_task = |i: usize,
                      try_num: u16,
                      task: SUBTASK,
                      futures: &mut FuturesUnordered<_>,
                      failed: &mut bool| {
        let task_id = SubtaskId {
            stage: stage_index as u16,
            task: i as u32,
            try_num,
        };

        let payload = SubtaskPayload {
            input: task.clone(),
            task_id,
            status_collector: status_collector.clone(),
        };

        let current_span = tracing::Span::current();
        let new_task = tokio::task::spawn(run_subtask::run_subtask(
            current_span,
            syncs.clone(),
            payload,
        ))
        .map(move |join_handle| (i, join_handle));
        futures.push(new_task);
    };

    let mut performed_tail_retry = false;

    let mut retry_or_fail =
        |failed: &mut bool,
         futures: &mut FuturesUnordered<_>,
         e: Report<TaskError>,
         task_index: usize,
         task_info: &mut TaskTrackingInfo<SUBTASK>| {
            let task_id = SubtaskId {
                stage: stage_index as u16,
                task: task_index as u32,
                try_num: task_info.try_num as u16,
            };

            if e.current_context().retryable() && task_info.try_num < scheduler.max_retries {
                status_collector.add(task_id, StatusUpdateInput::Retry(e));
                task_info.try_num += 1;
                spawn_task(
                    task_index,
                    task_info.try_num as u16,
                    task_info.input.clone(),
                    futures,
                    failed,
                );
            } else {
                status_collector.add(task_id, StatusUpdateInput::Failed(e));
                *failed = true;
            }
        };

    // TODO detect this so we know when to start the tail retry
    let mut no_more_new_tasks = false;
    while !failed && (!unfinished.is_empty() || !new_task_rx.is_disconnected()) {
        tokio::select! {
            new_task = new_task_rx.recv_async(), if !new_task_rx.is_disconnected() || !no_more_new_tasks => {
                match new_task {
                    Ok(new_task) => {
                        let input = new_task.clone();
                        let new_task = unfinished.insert (total_num_tasks, TaskTrackingInfo{
                            try_num: 0,
                            input: new_task,
                        });
                        spawn_task(total_num_tasks, 0, input, &mut running_tasks, &mut failed);
                        total_num_tasks += 1;
                    }
                    Err(_) => no_more_new_tasks = true,
                }
            }

            Some((task_index, result)) = running_tasks.next(), if !running_tasks.is_empty() => {
                // let result = match result {
                //     Ok(Some(Ok(output))) => {
                //         let output = TASKTYPE::read_task_response(&output.output)
                //             .into_report()
                //             .change_context(TaskError::Failed(true));
                //         Ok(Some(output))
                //     }
                //     _ => result,
                // };

                match result {
                    // The semaphore closed so the task did not run. This means that the whole
                    // system is shutting down, so don't worry about it here.
                    Ok(None) => {}
                    Ok(Some(Ok(output))) => {
                        if let Some(mut task_info) = unfinished[task_index].take() {
                            let output = SUBTASK::read_task_response(output.output);
                            match output {
                                Err(e) => {
                                    retry_or_fail(
                                        &mut failed,
                                        &mut running_tasks,
                                        Report::new(e).change_context(TaskError::Failed(true)),
                                        task_index,
                                        &mut task_info,
                                    );
                                    // Deserialization failed, so put it back.
                                    unfinished[task_index] = Some(task_info);
                                }
                                Ok(output) => {
                                    output_list.push(TaskDefWithOutput {
                                        task_def: task_info.input,
                                        output,
                                    });

                                    if !performed_tail_retry
                                        && total_num_tasks - output_list.len() <= retry_all_at
                                    {
                                        performed_tail_retry = true;

                                        // Re-enqueue all unfinished tasks. Some serverless platforms
                                        // can have very high tail latency, so this gets around that issue.
                                        for (i, task) in unfinished.iter_mut().enumerate() {
                                            if let Some(task) = task.as_mut() {
                                                self.status_collector.add(
                                                    SubtaskId {
                                                        stage: self.stage_index as u16,
                                                        task: i as u32,
                                                        try_num: task.try_num as u16,
                                                    },
                                                    StatusUpdateInput::Retry(Report::new(
                                                        TaskError::TailRetry,
                                                    )),
                                                );

                                                task.try_num += 1;
                                                spawn_task(i, task, &mut running_tasks, &mut failed);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // Task finished with an error.
                    Ok(Some(Err(e))) => {
                        if let Some(task_info) = unfinished[task_index].as_mut() {
                            retry_or_fail(&mut failed, &mut running_tasks, e, task_index, task_info);
                        }
                    }
                    // Task panicked. We can't really decipher the error so always consider it
                    // retryable.
                    Err(e) => {
                        if let Some(task_info) = unfinished[task_index].as_mut() {
                            let e = Report::new(e).change_context(TaskError::Failed(true));
                            retry_or_fail(&mut failed, &mut running_tasks, e, task_index, task_info);
                        }
                    }
                }
            }
        }

        if global_semaphore
            .as_ref()
            .map(|s| s.is_closed())
            .unwrap_or(false)
        {
            // The system is shutting down.
            break;
        }
    }

    if failed {
        Err(TaskError::Failed(false)).into_report()
    } else {
        Ok(output_list)
    }
}
