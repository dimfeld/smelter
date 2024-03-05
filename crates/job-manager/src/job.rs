use std::sync::Arc;

use error_stack::{Report, ResultExt};
use flume::{Receiver, Sender};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::{
    sync::Semaphore,
    task::{JoinError, JoinHandle},
};
use tracing::{event, instrument, Level, Span};
use uuid::Uuid;

use crate::{
    spawn::TaskError, JobManager, JobStageResultReceiver, JobStageTaskSender, SchedulerBehavior,
    StageError, StageRunner, StatusSender, SubTask,
};

type StageTaskHandle = JoinHandle<Result<(), Report<TaskError>>>;

#[derive(Debug, Clone, Copy)]
pub(crate) enum CancelState {
    Idle,
    Started,
    Finished,
}

/// A job in the system.
pub struct Job {
    pub(crate) id: Uuid,
    scheduler: SchedulerBehavior,
    job_semaphore: Arc<Semaphore>,
    global_semaphore: Option<Arc<Semaphore>>,
    status_sender: StatusSender,
    stage_task_tx: Sender<(usize, StageTaskHandle)>,
    job_cancel: Arc<tokio::sync::Notify>,
    tasks_cancel_rx: tokio::sync::watch::Receiver<CancelState>,
    done_adding: tokio::sync::watch::Sender<()>,
    num_stages: usize,
    stage_monitor_task: Option<JoinHandle<Result<bool, Report<StageError>>>>,
}

impl Job {
    /// Create a new job. This is internal and called from the job manager.
    pub(crate) fn new(manager: &JobManager, scheduler: SchedulerBehavior) -> Job {
        let max_concurrent_tasks = scheduler.max_concurrent_tasks.unwrap_or(i32::MAX as usize);
        let job_semaphore = Arc::new(Semaphore::new(max_concurrent_tasks));

        // When we're done adding tasks
        let (done_adding_tx, done_adding_rx) = tokio::sync::watch::channel(());
        // Signal to the subtasks to cancel them
        let (tasks_cancel_tx, tasks_cancel_rx) = tokio::sync::watch::channel(CancelState::Idle);
        // Signal to the task monitor to trigger a cancel
        let job_cancel = Arc::new(tokio::sync::Notify::new());

        let (stage_task_tx, stage_task_rx) = flume::unbounded();
        let stage_monitor_task = tokio::task::spawn(Self::monitor_job_stages(
            stage_task_rx,
            manager.cancel_rx.clone(),
            job_cancel.clone(),
            done_adding_rx,
            tasks_cancel_tx,
            job_semaphore.clone(),
            manager.cancel_timeout.clone(),
        ));

        Job {
            id: Uuid::now_v7(),
            scheduler,
            job_semaphore,
            global_semaphore: manager.global_semaphore.clone(),
            status_sender: manager.status_sender.clone(),
            stage_task_tx,
            job_cancel,
            tasks_cancel_rx,
            done_adding: done_adding_tx,
            stage_monitor_task: Some(stage_monitor_task),
            num_stages: 0,
        }
    }

    /// The unique ID of the job
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Wait for the job to finish and return any errors. This must be used to properly detect
    /// a job failure.
    #[instrument(skip(self))]
    pub async fn wait(&mut self) -> Result<(), Report<StageError>> {
        self.done_adding.send(()).ok();
        let Some(stage_monitor_task) = self.stage_monitor_task.take() else {
            return Ok(());
        };

        match stage_monitor_task.await {
            Ok(Ok(cancelled)) => {
                if cancelled {
                    Err(Report::new(StageError {
                        stage: 0,
                        cancelled: true,
                    }))
                } else {
                    Ok(())
                }
            }
            Ok(Err(e)) => Err(e),
            Err(e) => Err(e).change_context(StageError {
                stage: 0,
                cancelled: false,
            }),
        }
    }

    /// Cancel any pending or running tasks for the job
    pub async fn cancel(&self) {
        if !self.job_semaphore.is_closed() {
            self.job_semaphore.close();
        }
        self.done_adding.send(()).ok();
        self.job_cancel.notify_one();
    }

    /// Create a new stage in the job, which is a set of linked subtasks.
    pub async fn add_stage<SUBTASK: SubTask>(
        &mut self,
    ) -> (JobStageTaskSender<SUBTASK>, JobStageResultReceiver<SUBTASK>) {
        let stage_index = self.num_stages;
        self.num_stages += 1;

        let (subtask_result_tx, subtask_result_rx) = flume::unbounded();
        let (new_task_tx, new_task_rx) = flume::bounded(10);

        let args = StageRunner {
            job_id: self.id,
            stage_index,
            parent_span: Span::current(),
            new_task_rx,
            cancel_rx: self.tasks_cancel_rx.clone(),
            job_cancel_tx: self.job_cancel.clone(),
            subtask_result_tx,
            scheduler: self.scheduler.clone(),
            status_sender: self.status_sender.clone(),
            job_semaphore: self.job_semaphore.clone(),
            global_semaphore: self.global_semaphore.clone(),
        };

        let stage_task = tokio::task::spawn(args.run());
        event!(Level::DEBUG, %stage_index, "Started stage");

        let tx = JobStageTaskSender {
            tx: new_task_tx,
            stage: stage_index,
        };

        let rx = JobStageResultReceiver {
            rx: subtask_result_rx,
            stage: stage_index,
        };

        self.stage_task_tx
            .send_async((stage_index, stage_task))
            .await
            .ok();
        (tx, rx)
    }

    /// Create a new stage and add a fixed set of tasks to it.
    pub async fn add_stage_from_iter<SUBTASK: SubTask>(
        &mut self,
        tasks: impl IntoIterator<Item = SUBTASK>,
    ) -> JobStageResultReceiver<SUBTASK> {
        let (tx, rx) = self.add_stage().await;
        for task in tasks {
            tx.push(task).await;
        }
        rx
    }

    #[instrument(
        level = "debug",
        skip(
            stage_task_rx,
            manager_cancelled,
            job_cancelled,
            done_adding_rx,
            tasks_cancel_tx,
            job_semaphore
        )
    )]
    async fn monitor_job_stages(
        stage_task_rx: Receiver<(usize, StageTaskHandle)>,
        mut manager_cancelled: tokio::sync::watch::Receiver<()>,
        job_cancelled: Arc<tokio::sync::Notify>,
        mut done_adding_rx: tokio::sync::watch::Receiver<()>,
        tasks_cancel_tx: tokio::sync::watch::Sender<CancelState>,
        job_semaphore: Arc<Semaphore>,
        cancel_timeout: std::time::Duration,
    ) -> Result<bool, Report<StageError>> {
        let mut outstanding = FuturesUnordered::new();
        let mut done_adding = false;
        let mut cancelling = false;

        let mut cancel_timeout_time = tokio::time::Instant::now();

        while (!job_semaphore.is_closed() && !done_adding) || !outstanding.is_empty() {
            tokio::select! {
                result = outstanding.next(), if !outstanding.is_empty() => {
                    let Some(result) = result else {
                        continue;
                    };

                    let (index, result): (usize, Result<Result<(), Report<TaskError>>, JoinError>) = result;

                    match result {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => {
                            let result = Err(e).change_context(StageError{ stage: index, cancelled: false });
                            job_semaphore.close();
                            return result;
                        }
                        Err(e) => {
                            // Task panicked
                            job_semaphore.close();
                            return Err(e).change_context(StageError{ stage: index, cancelled: false });
                        }

                    }
                }

                stage = stage_task_rx.recv_async() => {
                    match stage {
                        Ok((index, stage_task)) => {
                            event!(Level::TRACE, %index, "Received stage");
                            let stage_task = stage_task.map(move |result| (index, result));
                            outstanding.push(stage_task);

                        }
                        Err(_) => {
                            event!(Level::TRACE, "No more stages");
                            done_adding = true;
                        }
                    }
                }

                _ = manager_cancelled.changed(), if !cancelling => {
                    event!(Level::DEBUG, "manager cancelled");
                    done_adding = true;
                    cancelling = true;
                    job_semaphore.close();
                    cancel_timeout_time = tokio::time::Instant::now() + cancel_timeout;
                    tasks_cancel_tx.send(CancelState::Started).ok();
                }

                _ = job_cancelled.notified(), if !cancelling => {
                    event!(Level::DEBUG, "job cancelled");
                    done_adding = true;
                    cancelling = true;
                    job_semaphore.close();
                    cancel_timeout_time = tokio::time::Instant::now() + cancel_timeout;
                    tasks_cancel_tx.send(CancelState::Started).ok();
                }

                _ = done_adding_rx.changed() => {
                    done_adding = true;
                }

                _ = tokio::time::sleep_until(cancel_timeout_time), if cancelling => {
                    // Some of the tasks didn't finish cancelling in time
                    event!(Level::DEBUG, "cancel timeout");
                    break;
                }
            }
        }

        if cancelling {
            tasks_cancel_tx.send(CancelState::Finished).ok();
        }

        Ok(cancelling)
    }
}
