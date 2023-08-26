use std::{fmt::Debug, sync::Arc};

use ahash::HashMap;
use error_stack::Report;
use flume::{Receiver, Sender};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt, TryStreamExt};
use tokio::sync::Semaphore;
use tracing::{event, instrument, Level, Span};

use crate::{
    manager::SubtaskId,
    run_subtask::{run_subtask, SubtaskPayload, SubtaskSyncs},
    spawn::TaskError,
    SchedulerBehavior, SlowTaskBehavior, StatusCollector, StatusUpdateData, SubTask,
    TaskDefWithOutput,
};

/// Information to run a task as well as which retry we're on.
#[derive(Debug)]
struct TaskTrackingInfo<INPUT: Debug> {
    input: INPUT,
    try_num: usize,
}

/// A channel that can send new tasks into a job stage. This type can be cheaply Cloned to use it from multiple places. Say,
/// consuming finished tasks from the stage and adding new jobs to it concurrently from different
/// places. Drop this object or call [JobStageTaskSender::finish] to signify that no more tasks will be added to the stage.
#[derive(Clone)]
pub struct JobStageTaskSender<SUBTASK: SubTask> {
    /// The channel on which new subtasks are sent.
    pub(crate) tx: Sender<SUBTASK>,
    /// The stage index.
    pub(crate) stage: usize,
}

impl<SUBTASK: SubTask> Debug for JobStageTaskSender<SUBTASK> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobStageTaskSender")
            .field("stage", &self.stage)
            .finish()
    }
}

impl<SUBTASK: SubTask> JobStageTaskSender<SUBTASK> {
    /// Add a subtask to the stage.
    #[instrument(level = "debug")]
    pub async fn add_subtask(&self, task: SUBTASK) {
        self.tx.send_async(task).await.ok();
    }

    /// Add multiple subtasks to the stage.
    #[instrument(level = "debug")]
    pub async fn extend(&mut self, tasks: impl IntoIterator<Item = SUBTASK> + Debug) {
        for task in tasks {
            self.add_subtask(task).await;
        }
    }

    /// Finish sending tasks. This is equivalent to just dropping the sender.
    pub fn finish(self) {}
}

/// Receive the results of finished tasks for a stage.
pub struct JobStageResultReceiver<SUBTASK: SubTask> {
    /// The channel on which task results are sent.
    pub(crate) rx: Receiver<Result<TaskDefWithOutput<SUBTASK>, Report<TaskError>>>,
    /// The stage index.
    pub(crate) stage: usize,
}

impl<SUBTASK: SubTask> Debug for JobStageResultReceiver<SUBTASK> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobStageResultReceiver")
            .field("stage", &self.stage)
            .finish()
    }
}

impl<SUBTASK: SubTask> JobStageResultReceiver<SUBTASK> {
    /// Return true if all the jobs in this stage have finished, and there will be no more jobs.
    pub fn is_finished(&self) -> bool {
        self.rx.is_disconnected() && self.rx.is_empty()
    }

    /// Wait for all the tasks in the stage to finish and return their results. Note that this will
    /// not return until the stage's [JobStageTaskSender] is dropped.
    #[instrument(level = "debug")]
    pub async fn collect(self) -> Result<Vec<TaskDefWithOutput<SUBTASK>>, Report<TaskError>> {
        self.rx.into_stream().try_collect().await
    }
}

/// when the number of in progress tasks drops below this number, retry all the remaining
/// tasks.
fn ready_for_tail_retry(
    scheduler: &SchedulerBehavior,
    remaining_tasks: usize,
    num_tasks: usize,
) -> bool {
    match scheduler.slow_task_behavior {
        SlowTaskBehavior::Wait => false,
        SlowTaskBehavior::RerunLastPercent(n) => num_tasks * n / 100 < remaining_tasks,
        SlowTaskBehavior::RerunLastN(n) => remaining_tasks <= n,
    }
}

pub(crate) struct StageArgs<SUBTASK: SubTask> {
    pub stage_index: usize,
    pub parent_span: Span,
    pub new_task_rx: Receiver<SUBTASK>,
    pub subtask_result_tx: Sender<Result<TaskDefWithOutput<SUBTASK>, Report<TaskError>>>,
    pub scheduler: SchedulerBehavior,
    pub status_collector: StatusCollector,
    pub job_semaphore: Arc<Semaphore>,
    pub global_semaphore: Option<Arc<Semaphore>>,
}

#[instrument(
    level = Level::DEBUG,
    parent = &args.parent_span,
    fields(
        stage_index = %args.stage_index
    ),
    skip(args)
)]
pub(crate) async fn run_tasks_stage<SUBTASK: SubTask>(
    args: StageArgs<SUBTASK>,
) -> Result<(), Report<TaskError>> {
    let StageArgs {
        stage_index,
        new_task_rx,
        subtask_result_tx,
        scheduler,
        status_collector,
        job_semaphore,
        global_semaphore,
        ..
    } = args;

    event!(Level::DEBUG, "Adding stage {}", stage_index);
    let mut total_num_tasks = 0;
    let mut unfinished = HashMap::default();

    // We never transmit anything on cancel_tx, but let it drop at the end of the function to
    // cancel any tasks still running.
    let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(());

    let syncs = Arc::new(SubtaskSyncs {
        job_semaphore: job_semaphore.clone(),
        global_semaphore: global_semaphore.clone(),
        cancel: cancel_rx,
    });

    let mut running_tasks = FuturesUnordered::new();
    let spawn_task = |i: usize, try_num: u16, task: SUBTASK, futures: &mut FuturesUnordered<_>| {
        let task_id = SubtaskId {
            stage: stage_index as u16,
            task: i as u32,
            try_num,
        };

        let payload = SubtaskPayload {
            input: task,
            task_id,
            status_collector: status_collector.clone(),
        };

        let current_span = tracing::Span::current();
        let new_task = tokio::task::spawn(run_subtask(current_span, syncs.clone(), payload))
            .map(move |join_handle| (i, join_handle));
        futures.push(new_task);
    };

    let mut performed_tail_retry = false;
    let mut no_more_new_tasks = false;

    let retry_or_fail = |futures: &mut FuturesUnordered<_>,
                         e: Report<TaskError>,
                         task_index: usize,
                         task_info: &mut TaskTrackingInfo<SUBTASK>| {
        let task_id = SubtaskId {
            stage: stage_index as u16,
            task: task_index as u32,
            try_num: task_info.try_num as u16,
        };

        if e.current_context().retryable() && task_info.try_num < scheduler.max_retries {
            status_collector.add(task_id, StatusUpdateData::Retry(format!("{e:?}")));
            task_info.try_num += 1;
            spawn_task(
                task_index,
                task_info.try_num as u16,
                task_info.input.clone(),
                futures,
            );
            Ok(())
        } else {
            status_collector.add(task_id, StatusUpdateData::Failed(format!("{e:?}")));
            Err(e)
        }
    };

    while !unfinished.is_empty() || !no_more_new_tasks {
        tokio::select! {
            new_task = new_task_rx.recv_async(), if !no_more_new_tasks => {
                match new_task {
                    Ok(new_task) => {
                        event!(Level::DEBUG, %stage_index, ?new_task, "received task");
                        unfinished.insert (total_num_tasks, TaskTrackingInfo{
                            try_num: 0,
                            input: new_task.clone(),
                        });
                        spawn_task(total_num_tasks, 0, new_task, &mut running_tasks);
                        total_num_tasks += 1;
                    }
                    Err(_) => {
                        event!(Level::DEBUG, %stage_index, "tasks finished");
                        no_more_new_tasks = true;
                    }
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
                    Ok(None) => {
                        // The semaphore closed so the task did not run. This means that the whole
                        // system is shutting down, so don't worry about it here.
                    }
                    Ok(Some(Ok(output))) => {
                        if let Some(mut task_info) = unfinished.remove(&task_index) {
                            let output = SUBTASK::read_task_response(output.output);
                            match output {
                                Err(e) => {
                                    let e = Report::new(e).change_context(TaskError::Failed(true));
                                    retry_or_fail(
                                        &mut running_tasks,
                                        e,
                                        task_index,
                                        &mut task_info,
                                    )?;

                                    // Deserialization failed, so put it back.
                                    unfinished.insert(task_index, task_info);
                                }
                                Ok(output) => {
                                    let output = TaskDefWithOutput {
                                        task_def: task_info.input,
                                        output,
                                    };
                                    subtask_result_tx.send_async(Ok(output)).await.ok();

                                    if !performed_tail_retry && no_more_new_tasks
                                        && ready_for_tail_retry(&scheduler, unfinished.len(), total_num_tasks)
                                    {
                                        event!(Level::INFO, "starting tail retry");
                                        performed_tail_retry = true;

                                        // Re-enqueue all unfinished tasks. Some serverless platforms
                                        // can have very high tail latency, so this gets around that issue.
                                        for (i, task) in unfinished.iter_mut() {
                                            status_collector.add(
                                                SubtaskId {
                                                    stage: stage_index as u16,
                                                    task: *i as u32,
                                                    try_num: task.try_num as u16,
                                                },
                                                StatusUpdateData::Retry(format!("{:?}", Report::new(
                                                    TaskError::TailRetry,
                                                ))),
                                            );

                                            task.try_num += 1;
                                            spawn_task(*i, task.try_num as u16, task.input.clone(), &mut running_tasks);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // Task finished with an error.
                    Ok(Some(Err(e))) => {
                        if let Some(task_info) = unfinished.get_mut(&task_index) {
                            retry_or_fail(&mut running_tasks, e, task_index, task_info)?;
                        }
                    }
                    // Task panicked. We can't really decipher the error so always consider it
                    // retryable.
                    Err(e) => {
                        if let Some(task_info) = unfinished.get_mut(&task_index) {
                            let e = Report::new(e).change_context(TaskError::Failed(true));
                            retry_or_fail(&mut running_tasks, e, task_index, task_info)?;
                        }
                    }
                }
            }
        }

        if global_semaphore
            .as_ref()
            .map(|s| s.is_closed())
            .unwrap_or_else(|| job_semaphore.is_closed())
        {
            // This job or the system is shutting down.
            event!(Level::DEBUG, "semaphores closed");
            break;
        }
    }

    Ok(())
}