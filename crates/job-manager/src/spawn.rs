pub mod fail_wrapper;
#[cfg(feature = "inprocess")]
pub mod inprocess;

use std::borrow::Cow;

use error_stack::{Report, ResultExt};
use thiserror::Error;

use crate::manager::SubtaskId;

#[async_trait::async_trait]
pub trait Spawner: Send + Sync + 'static {
    type SpawnedTask: SpawnedTask;

    // It would be nice to just pass a dyn Serialize instead, but that makes the trait not
    // object-safe so it's tricky. There's probably some better way to set this up but it's not too
    // important for now.
    /// Spawn a task with the given input. The input is a JSON-serialized version of the task definition.
    async fn spawn(
        &self,
        task_id: SubtaskId,
        task_name: Cow<'static, str>,
        input: Vec<u8>,
    ) -> Result<Self::SpawnedTask, Report<TaskError>>;

    /// Spawn a task, and convert the task payload to JSON.
    async fn spawn_with_json(
        &self,
        task_id: SubtaskId,
        task_name: Cow<'static, str>,
        data: impl serde::Serialize + Send,
    ) -> Result<Self::SpawnedTask, Report<TaskError>> {
        let data = serde_json::to_vec(&data).change_context(TaskError::TaskGenerationFailed)?;
        self.spawn(task_id, task_name, data).await
    }
}

#[derive(Debug, Error)]
#[error("Stage {0} failed")]
pub struct StageError(pub usize);

#[derive(Clone, Error, Debug, PartialEq, Eq)]
pub enum TaskError {
    #[error("Failed to start")]
    DidNotStart(bool),
    #[error("Task timed out")]
    TimedOut,
    #[error("Task was lost by runtime")]
    Lost,
    #[error("Task was cancelled")]
    Cancelled,
    #[error("Task encountered an error")]
    Failed(bool),
    #[error("Failed to generate tasks from query")]
    TaskGenerationFailed,
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
}
