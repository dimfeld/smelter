#[derive(Debug, Clone)]
pub struct SchedulerBehavior {
    /// The maximum number of tasks to run at once. Leave this as `None` to run them as fast as
    /// possible, leaving the limits to the underlying task runtime.
    pub max_concurrent_tasks: Option<usize>,
    /// The maximum number of time to retry a task.
    pub max_retries: usize,
    pub slow_task_behavior: SlowTaskBehavior,
}

#[derive(Debug, Clone)]
pub enum SlowTaskBehavior {
    /// Just wait for slow tasks to finish.
    Wait,
    /// Rerun the last N percent of tasks, in case they turned out to be slow.
    RerunLastPercent(usize),
    /// Rerun the last N tasks, in case they turned out to be slow.
    RerunLastN(usize),
}
