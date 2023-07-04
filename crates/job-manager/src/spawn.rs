#[cfg(feature = "inprocess")]
pub mod inprocess;
pub mod local;

use error_stack::Report;
use thiserror::Error;

#[async_trait::async_trait]
pub trait Spawner {
    type SpawnedTask: SpawnedTask;

    // It would be nice to just pass a dyn Serialize instead, but that makes the trait not
    // object-safe so it's tricky. There's probably some better way to set this up but it's not too
    // important for now.
    /// Spawn a task with the given input. The input is a JSON-serialized version of the task definition.
    async fn spawn(
        &self,
        local_id: &str,
        task_name: &str,
        input: &[u8],
    ) -> Result<Self::SpawnedTask, Report<TaskError>>;
}

#[derive(Error, Debug)]
pub enum TaskError {
    #[error("Failed to start")]
    DidNotStart,
    #[error("Task timed out")]
    TimedOut,
    #[error("Task was lost by runtime")]
    Lost,
    #[error("Task was cancelled")]
    Cancelled,
    #[error("Task encountered an error")]
    Failed,
    #[error("Failed to generate tasks from query")]
    TaskGenerationFailed,
}

#[async_trait::async_trait]
pub trait SpawnedTask {
    /// The internal ID of the spawned task in the runtime.
    async fn runtime_id(&self) -> Result<String, TaskError>;
    /// Check if a task is finished yet.
    async fn check_finished(&mut self) -> Result<bool, Report<TaskError>>;
    /// Return a future that resolves when a task finishes.
    async fn wait(&mut self) -> Result<(), Report<TaskError>>;
    /// Attempt to kill a task before it finishes.
    async fn kill(&mut self) -> Result<(), Report<TaskError>>;
    /// Return the location where the task should have written its output.
    fn output_location(&self) -> String;
}
