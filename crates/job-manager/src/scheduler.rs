/// Customization for the behavior of the job scheduler
#[derive(Debug, Clone)]
pub struct SchedulerBehavior {
    /// The maximum number of tasks to run at once in a job. Leave this as `None` to run
    /// them as fast as possible, leaving the limits to the underlying task runtime.
    /// Cross-job limits can be achieved using by passing `global_semaphore` to
    /// [JobManager::new].
    pub max_concurrent_tasks: Option<usize>,
    /// The maximum number of time to retry a task.
    pub max_retries: usize,
    /// When to eagerly retry the remaining tasks in a job stage. For certain task hosts, such as AWS
    /// Lambda, this can sometimes greatly reduce tail latency.
    pub slow_task_behavior: SlowTaskBehavior,
}

impl Default for SchedulerBehavior {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: Default::default(),
            max_retries: 2,
            slow_task_behavior: Default::default(),
        }
    }
}

/// When to rerun tasks that may be stalled.
#[derive(Debug, Default, Clone)]
pub enum SlowTaskBehavior {
    /// Just wait for slow tasks to finish. This is the default behavior.
    #[default]
    Wait,
    /// Rerun the last N percent of tasks, in case they turned out to be slow.
    RerunLastPercent(usize),
    /// Rerun the last N tasks, in case they turned out to be slow.
    RerunLastN(usize),
}
