use std::{borrow::Cow, fmt::Debug};

use error_stack::Report;
use manager::SubtaskId;
use serde::de::DeserializeOwned;
use spawn::{SpawnedTask, TaskError};

pub mod manager;
pub mod scheduler;
pub mod spawn;
pub mod task_status;
#[cfg(test)]
mod test_util;

#[derive(Debug)]
pub struct TaskDefWithOutput<DEF: SubTask> {
    pub task_def: DEF,
    pub output: DEF::Output,
}

pub enum FailureType {
    DoNotRetry,
    RetryNow,
    RetryAfter { ms: usize },
}

#[async_trait::async_trait]
pub trait SubTask: Clone + Debug + Send + Sync + 'static {
    type Output: Debug + DeserializeOwned + Send + 'static;

    /// A name that describes the task.
    fn description(&self) -> Cow<'static, str>;

    /// Start the task with the appropriate arguments.
    async fn spawn(&self, task_id: SubtaskId) -> Result<Box<dyn SpawnedTask>, Report<TaskError>>;

    fn read_task_response(data: Vec<u8>) -> Result<Self::Output, TaskError>;
}
