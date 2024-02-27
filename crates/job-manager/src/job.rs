use std::sync::Arc;

use error_stack::{Report, ResultExt};
use flume::{Receiver, Sender};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::{
    sync::Semaphore,
    task::{JoinError, JoinHandle},
};
use tracing::{event, instrument, Level, Span};

use crate::{
    spawn::TaskError, JobManager, JobStageResultReceiver, JobStageTaskSender, SchedulerBehavior,
    StageArgs, StageError, StatusCollector, SubTask,
};

type StageTaskHandle = JoinHandle<Result<(), Report<TaskError>>>;

/// A job in the system.
pub struct Job {
    scheduler: SchedulerBehavior,
    job_semaphore: Arc<Semaphore>,
    global_semaphore: Option<Arc<Semaphore>>,
    status_collector: StatusCollector,
    stage_task_tx: Sender<(usize, StageTaskHandle)>,
    done: tokio::sync::watch::Sender<()>,
    num_stages: usize,
    stage_monitor_task: Option<JoinHandle<Result<(), Report<StageError>>>>,
}

impl Job {
    /// Create a new job. This is internal and called from the job manager.
    pub(crate) fn new(manager: &JobManager, scheduler: SchedulerBehavior) -> Job {
        let max_concurrent_tasks = scheduler.max_concurrent_tasks.unwrap_or(i32::MAX as usize);
        let job_semaphore = Arc::new(Semaphore::new(max_concurrent_tasks));

        let (done_tx, done_rx) = tokio::sync::watch::channel(());

        let (stage_task_tx, stage_task_rx) = flume::unbounded();
        let stage_monitor_task = tokio::task::spawn(Self::monitor_job_stages(
            stage_task_rx,
            done_rx,
            job_semaphore.clone(),
        ));

        Job {
            scheduler,
            job_semaphore,
            global_semaphore: manager.global_semaphore.clone(),
            status_collector: manager.status_collector.clone(),
            stage_task_tx,
            done: done_tx,
            stage_monitor_task: Some(stage_monitor_task),
            num_stages: 0,
        }
    }

    /// Wait for the job to finish and return any errors.
    #[instrument(skip(self))]
    pub async fn wait(&mut self) -> Result<(), Report<StageError>> {
        self.done.send(()).ok();
        let Some(stage_monitor_task) = self.stage_monitor_task.take() else {
            return Ok(());
        };

        match stage_monitor_task.await {
            Ok(e) => e,
            Err(e) => Err(e).change_context(StageError(0)),
        }
    }

    /// Create a new stage in the job, which is a set of linked subtasks.
    pub async fn add_stage<SUBTASK: SubTask>(
        &mut self,
    ) -> (JobStageTaskSender<SUBTASK>, JobStageResultReceiver<SUBTASK>) {
        let stage_index = self.num_stages;
        self.num_stages += 1;

        let (subtask_result_tx, subtask_result_rx) = flume::unbounded();
        let (new_task_tx, new_task_rx) = flume::bounded(10);

        let args = StageArgs {
            stage_index,
            parent_span: Span::current(),
            new_task_rx,
            subtask_result_tx,
            scheduler: self.scheduler.clone(),
            status_collector: self.status_collector.clone(),
            job_semaphore: self.job_semaphore.clone(),
            global_semaphore: self.global_semaphore.clone(),
        };

        let stage_task = tokio::task::spawn(crate::stage::run_tasks_stage(args));
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

    #[instrument(level = "debug")]
    async fn monitor_job_stages(
        stage_task_rx: Receiver<(usize, StageTaskHandle)>,
        mut close: tokio::sync::watch::Receiver<()>,
        job_semaphore: Arc<Semaphore>,
    ) -> Result<(), Report<StageError>> {
        let mut outstanding = FuturesUnordered::new();
        let mut done = false;

        while (!job_semaphore.is_closed() && !done) || !outstanding.is_empty() {
            tokio::select! {
                Some(result) = outstanding.next(), if !outstanding.is_empty() => {
                    let (index, result): (usize, Result<Result<(), Report<TaskError>>, JoinError>) = result;

                    match result {
                        Ok(r) => {
                            let result = r.change_context(StageError(index));
                            if result.is_err() {
                                job_semaphore.close();
                                return result;
                            }

                        }
                        Err(e) => {
                            // Task panicked
                            job_semaphore.close();
                            return Err(e).change_context(StageError(index));
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
                            done = true;
                        }
                    }
                }

                _ = close.changed() => {
                    done = true;
                }
            }
        }

        Ok(())
    }
}

impl Drop for Job {
    fn drop(&mut self) {
        self.job_semaphore.close();
    }
}
