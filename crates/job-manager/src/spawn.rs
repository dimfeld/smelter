#[cfg(test)]
pub mod inprocess;

use error_stack::Report;
use smelter_worker::SubtaskId;
use thiserror::Error;

#[derive(Debug)]
pub struct StageError {
    pub stage: usize,
    pub cancelled: bool,
}

impl std::error::Error for StageError {}

impl std::fmt::Display for StageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.stage == 0 {
            write!(f, "Job")?;
        } else {
            write!(f, "Stage {}", self.stage)?;
        }

        if self.cancelled {
            write!(f, " was cancelled")
        } else {
            write!(f, " failed")
        }
    }
}

/// A stringified error copied from a worker's output.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct SerializedWorkerFailure(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskError {
    pub task_id: SubtaskId,
    pub kind: TaskErrorKind,
}

impl std::error::Error for TaskError {}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Task {}: {}", self.task_id, self.kind)
    }
}

impl TaskError {
    pub fn new(task_id: SubtaskId, kind: TaskErrorKind) -> Self {
        Self { task_id, kind }
    }

    pub fn did_not_start(task_id: SubtaskId, retryable: bool) -> Self {
        Self::new(task_id, TaskErrorKind::DidNotStart(retryable))
    }

    pub fn timed_out(task_id: SubtaskId) -> Self {
        Self::new(task_id, TaskErrorKind::TimedOut)
    }

    pub fn lost(task_id: SubtaskId) -> Self {
        Self::new(task_id, TaskErrorKind::Lost)
    }

    pub fn cancelled(task_id: SubtaskId) -> Self {
        Self::new(task_id, TaskErrorKind::Cancelled)
    }

    pub fn failed(task_id: SubtaskId, retryable: bool) -> Self {
        Self::new(task_id, TaskErrorKind::Failed(retryable))
    }

    pub fn task_generation_failed(task_id: SubtaskId) -> Self {
        Self::new(task_id, TaskErrorKind::TaskGenerationFailed)
    }

    pub fn tail_retry(task_id: SubtaskId) -> Self {
        Self::new(task_id, TaskErrorKind::TailRetry)
    }

    pub fn retryable(&self) -> bool {
        self.kind.retryable()
    }
}

/// An error indicating that a task failed in some way.
#[derive(Clone, Error, Debug, PartialEq, Eq)]
pub enum TaskErrorKind {
    /// The spawner failed to start the task.
    #[error("Failed to start")]
    DidNotStart(bool),
    /// The task exceeded its timeout.
    #[error("Timed out")]
    TimedOut,
    /// The task disappeared in such a way that the job manager could not figure out what happened
    /// to it. This might happen, for example, if a serverless job is started but at some point
    /// requests for its status return a "not found" error.
    #[error("Lost by runtime")]
    Lost,
    /// The task was cancelled because the job is finishing early. This usually means that some
    /// other task in the job failed.
    #[error("Cancelled")]
    Cancelled,
    /// The task failed.
    #[error("Encountered an error")]
    Failed(bool),
    /// The setup for the task failed.
    #[error("Failed to generate a subtask")]
    TaskGenerationFailed,
    /// This error indicates that the job is coming near an end, and another copy of this task was
    /// opportunistically spawned, since this one had not finished yet and the [SlowTaskBehavior] condition
    /// was reached.
    #[error("Retrying per tail task policy")]
    TailRetry,
}

impl TaskErrorKind {
    pub fn retryable(&self) -> bool {
        match self {
            Self::DidNotStart(retryable) => *retryable,
            Self::TimedOut => true,
            Self::Lost => true,
            Self::Cancelled => false,
            Self::Failed(retryable) => *retryable,
            Self::TaskGenerationFailed => false,
            Self::TailRetry => false,
        }
    }
}

#[async_trait::async_trait]
pub trait SpawnedTask: Send + Sync + 'static {
    /// The internal ID of the spawned task in the runtime, when accessible.
    async fn runtime_id(&self) -> Result<String, TaskError>;
    /// Return a future that resolves when a task finishes.
    async fn wait(&mut self) -> Result<Vec<u8>, Report<TaskError>>;
    /// Attempt to kill a task before it finishes.
    async fn kill(&mut self) -> Result<(), Report<TaskError>>;

    /// Convert a SpawnedTask into a boxed trait object, to return it to the job system.
    fn into_boxed(self) -> Box<dyn SpawnedTask>
    where
        Self: Sized,
    {
        Box::new(self)
    }
}

/// Convert a [SpawnedTask] inside a [Result] into a boxed trait object.
trait SpawnedTaskResultExt<E> {
    fn into_boxed(self) -> Result<Box<dyn SpawnedTask>, E>
    where
        Self: Sized;
}

impl<ST: SpawnedTask, E> SpawnedTaskResultExt<E> for Result<ST, E> {
    /// Convert a [SpawnedTask] inside a [Result] into a boxed trait object.
    fn into_boxed(self) -> Result<Box<dyn SpawnedTask>, E> {
        self.map(|st| st.into_boxed())
    }
}
