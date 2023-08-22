use std::{borrow::Cow, fmt::Debug};

use flume::{Receiver, Sender};
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
pub struct TaskDefWithOutput<DEF: TaskInfo> {
    pub task_def: DEF,
    pub output: DEF::Output,
}

pub enum FailureType {
    DoNotRetry,
    RetryNow,
    RetryAfter { ms: usize },
}

#[async_trait::async_trait]
pub trait TaskInfo: Debug + Send {
    type Output: Debug + DeserializeOwned + Send + 'static;

    /// A name that describes the task.
    fn description(&self) -> Cow<'static, str>;

    /// Start the task with the appropriate arguments.
    async fn spawn(&self, task_id: SubtaskId) -> Box<dyn SpawnedTask>;

    fn read_task_response(data: Vec<u8>) -> Result<Self::Output, TaskError>;
}

#[async_trait::async_trait]
pub trait TaskDef: Send + Sync + Debug {
    type SubTaskDef: TaskInfo + Send + Debug;
    type Error: std::error::Error + error_stack::Context + Send + Sync;

    // TODO Change this so that
    /// Given an initial task definition, run the various subtasks and handle their results.
    async fn run(&self) -> Result<(), Self::Error>;
}
