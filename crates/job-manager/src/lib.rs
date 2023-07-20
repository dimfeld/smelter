use std::{borrow::Cow, fmt::Debug};

use serde::de::DeserializeOwned;

pub mod manager;
pub mod scheduler;
pub mod spawn;
pub mod task_status;
#[cfg(test)]
mod test_util;

#[derive(Debug)]
pub struct TaskDefWithOutput<DEF: Send + Debug, OUTPUT: Debug + DeserializeOwned + Send + 'static> {
    task_def: DEF,
    output: OUTPUT,
}

pub enum FailureType {
    DoNotRetry,
    RetryNow,
    RetryAfter { ms: usize },
}

pub trait TaskInfo: Debug {
    /// A name that the spawner can use to run the appropriate task.
    fn spawn_name(&self) -> Cow<'static, str>;

    /// Serialize the input into a format that the worker expects (usually JSON).
    fn serialize_input(&self) -> Result<Vec<u8>, eyre::Report>;
}

#[async_trait::async_trait]
pub trait TaskType: Send + Sync {
    type TaskDef: Send + Debug;
    type SubTaskDef: TaskInfo + Send + Debug;
    type SubtaskOutput: Debug + DeserializeOwned + Send + 'static;
    type Error: std::error::Error + error_stack::Context + Send + Sync;

    /// Given an initial task definition, create a list of subtasks to run.
    async fn create_initial_subtasks(
        &self,
        task_def: &Self::TaskDef,
    ) -> Result<Vec<Self::SubTaskDef>, Self::Error>;

    /// Create reducer tasks to run on the output of a previous stage. If there is nothing left to
    /// do, return an empty Vec.
    async fn create_subtasks_from_result(
        &self,
        task_def: &Self::TaskDef,
        stage_number: usize,
        subtasks: &[TaskDefWithOutput<Self::SubTaskDef, Self::SubtaskOutput>],
    ) -> Result<Vec<Self::SubTaskDef>, Self::Error>;

    fn read_task_response(data: Vec<u8>) -> Result<Self::SubtaskOutput, Self::Error>;
}
