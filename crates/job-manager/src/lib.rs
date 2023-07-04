use std::borrow::Cow;

use error_stack::Report;
use spawn::{Spawner, TaskError};

pub mod manager;
pub mod scheduler;
pub mod spawn;

pub struct TaskDefWithOutput<DEF: Send> {
    task_def: DEF,
    output_location: String,
}

pub enum FailureType {
    DoNotRetry,
    RetryNow,
    RetryAfter { ms: usize },
}

pub trait TaskInfo {
    /// A name that the spawner can use to run the appropriate task.
    fn spawn_name(&self) -> Cow<'static, str>;

    /// Serialize the input into a format that the worker expects (usually JSON).
    fn serialize_input(&self) -> Result<Vec<u8>, eyre::Report>;
}

#[async_trait::async_trait]
pub trait TaskType: Send + Sync {
    type TaskDef: Send;
    type MapTaskDef: TaskInfo + serde::Serialize + Send;
    type TopLevelReducerTaskDef: TaskInfo + serde::Serialize + Send;
    type IntermediateReducerTaskDef: TaskInfo
        + serde::Serialize
        + Send
        + From<Self::TopLevelReducerTaskDef>;
    type Error: std::error::Error + error_stack::Context + Send + Sync;

    /// Given a single task definition, create a list of map tasks to run.
    async fn create_map_tasks(
        &self,
        task_def: &Self::TaskDef,
    ) -> Result<Vec<Self::MapTaskDef>, Self::Error>;

    /// Create reducer tasks to run on the output of the map tasks.
    async fn create_top_level_reducers(
        &self,
        task_def: &Self::TaskDef,
        subtasks: &[TaskDefWithOutput<Self::MapTaskDef>],
    ) -> Result<Vec<Self::TopLevelReducerTaskDef>, Self::Error>;

    /// Create reducers that that can run on the output of previous reducers. Each invocation
    /// of this function receives the list of [IntermediateReducerTaskDef]s for the previous set of reducers.
    /// If there are no further reducers to run, return an empty vector.
    async fn create_intermediate_reducers(
        &self,
        task_def: &Self::TaskDef,
        reducers: &[TaskDefWithOutput<Self::IntermediateReducerTaskDef>],
    ) -> Result<Vec<Self::IntermediateReducerTaskDef>, Self::Error> {
        Ok(Vec::new())
    }

    /// Indicate if a task can be retried after a particular error or not.
    fn can_retry(&self, error: &Self::Error) -> FailureType {
        FailureType::RetryNow
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
