#[cfg(test)]
pub(crate) mod tests;

use std::{fmt::Debug, sync::Arc};

use tokio::sync::Semaphore;

use crate::{scheduler::SchedulerBehavior, task_status::StatusCollector, Job};

/// The ID for a subtask, which uniquely identifies it within a [Job].
#[derive(Debug, Copy, Clone)]
pub struct SubtaskId {
    /// Which stage the subtask is running on.
    pub stage: u16,
    /// The index of the task within that stage.
    pub task: u32,
    /// Which retry of this task is being executed.
    pub try_num: u16,
}

impl std::fmt::Display for SubtaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:03}-{:05}-{:02}", self.stage, self.task, self.try_num)
    }
}

/// The [JobManager] holds state and behavior that is shared between multiple jobs.
pub struct JobManager {
    pub(crate) scheduler: SchedulerBehavior,
    pub(crate) global_semaphore: Option<Arc<Semaphore>>,
    pub(crate) status_collector: StatusCollector,
}

impl JobManager {
    /// Create a new [JobManager]
    /// * scheduler - How subtasks should be scheduled within a job
    /// * status_collector - A collector of job and subtask status information.
    /// * global_semaphore - If supplied, the `global_semaphore` will limit the number of jobs that are running
    ///   concurrently.
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

    /// Create a new job with custom scheduler behavior.
    pub fn new_job_with_scheduler(&self, scheduler: SchedulerBehavior) -> Job {
        Job::new(self, scheduler)
    }

    /// Craete a new job.
    pub fn new_job(&self) -> Job {
        self.new_job_with_scheduler(self.scheduler.clone())
    }
}
