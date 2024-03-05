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
    pub(crate) cancel_timeout: std::time::Duration,
}

impl JobManager {
    /// Create a new [JobManager]
    /// * scheduler - How subtasks should be scheduled within a job
    /// * status_collector - A collector of job and subtask status information.
    pub fn new(status_sender: StatusSender) -> Self {
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(());
        Self {
            scheduler: SchedulerBehavior::default(),
            status_sender,
            global_semaphore: None,
            cancel_tx,
            cancel_rx,
            cancel_timeout: std::time::Duration::from_secs(86400),
        }
    }

    /// Add a global semaphore to this job manager, to limit the number of jobs run across all
    /// jobs.
    pub fn with_global_semaphore(mut self, global_semaphore: Arc<Semaphore>) -> Self {
        self.global_semaphore = Some(global_semaphore);
        self
    }

    /// Alter the scheduler behavior for the job manager
    pub fn with_scheduler_behavior(mut self, scheduler: SchedulerBehavior) -> Self {
        self.scheduler = scheduler;
        self
    }

    /// Set how long to wait for tasks to cancel when an error occurs.
    pub fn with_cancel_timeout(mut self, cancel_timeout: std::time::Duration) -> Self {
        self.cancel_timeout = cancel_timeout;
        self
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
