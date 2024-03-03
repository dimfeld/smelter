#[cfg(test)]
pub mod inprocess;

use error_stack::Report;
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

/// An error indicating that a task failed in some way.
#[derive(Clone, Error, Debug, PartialEq, Eq)]
pub enum TaskError {
    /// The spawner failed to start the task.
    #[error("Failed to start")]
    DidNotStart(bool),
    /// The task exceeded its timeout.
    #[error("Task timed out")]
    TimedOut,
    /// The task disappeared in such a way that the job manager could not figure out what happened
    /// to it. This might happen, for example, if a serverless job is started but at some point
    /// requests for its status return a "not found" error.
    #[error("Task was lost by runtime")]
    Lost,
    /// The task was cancelled because the job is finishing early. This usually means that some
    /// other task in the job failed.
    #[error("Task was cancelled")]
    Cancelled,
    /// The task failed.
    #[error("Task encountered an error")]
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

impl TaskError {
    pub fn retryable(&self) -> bool {
        match self {
            Self::DidNotStart(retryable) => *retryable,
            Self::TimedOut => true,
            Self::Lost => true,
            Self::Cancelled => true,
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
