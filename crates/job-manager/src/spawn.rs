use thiserror::Error;

#[async_trait::async_trait]
pub trait Spawner {
    type SpawnedTask: SpawnedTask;

    // It would be nice to just pass a dyn Serialize instead, but that makes the trait not
    // object-safe so it's tricky. For now we just assume JSON.
    /// Spawn a task with the given input. The input is a JSON-serialized version of the task definition.
    async fn spawn(&self, input: &[u8]) -> Self::SpawnedTask;
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
}

#[async_trait::async_trait]
pub trait SpawnedTask {
    async fn check_status(&self) -> Result<(), TaskError>;
    async fn wait(&self) -> Result<String, TaskError>;
    fn output_location(&self) -> String;
}
