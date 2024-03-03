#[cfg(test)]
pub(crate) mod tests;

use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::{scheduler::SchedulerBehavior, Job, StatusSender};

/// The [JobManager] holds state and behavior that is shared between multiple jobs.
pub struct JobManager {
    pub(crate) scheduler: SchedulerBehavior,
    pub(crate) global_semaphore: Option<Arc<Semaphore>>,
    pub(crate) status_sender: StatusSender,
    cancel_tx: tokio::sync::watch::Sender<()>,
    pub(crate) cancel_rx: tokio::sync::watch::Receiver<()>,
}

impl JobManager {
    /// Create a new [JobManager]
    /// * scheduler - How subtasks should be scheduled within a job
    /// * status_collector - A collector of job and subtask status information.
    /// * global_semaphore - If supplied, the `global_semaphore` will limit the number of tasks that are running
    ///   concurrently, across all jobs.
    pub fn new(
        scheduler: SchedulerBehavior,
        status_sender: StatusSender,
        global_semaphore: Option<Arc<Semaphore>>,
    ) -> Self {
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(());
        Self {
            scheduler,
            status_sender,
            global_semaphore,
            cancel_tx,
            cancel_rx,
        }
    }

    /// Create a new job with custom scheduler behavior.
    pub fn new_job_with_scheduler(&self, scheduler: SchedulerBehavior) -> Job {
        Job::new(self, scheduler)
    }

    /// Create a new job.
    pub fn new_job(&self) -> Job {
        self.new_job_with_scheduler(self.scheduler.clone())
    }

    /// Cancel all jobs under this job manager
    pub fn cancel(&self) {
        self.cancel_tx.send(()).ok();
    }
}
