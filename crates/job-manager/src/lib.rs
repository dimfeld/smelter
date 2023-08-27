// #![warn(missing_docs)]
// #![warn(clippy::missing_docs_in_private_items)]

//! Manage and run jobs for Smelter

use std::{borrow::Cow, fmt::Debug};

use error_stack::Report;
pub use manager::SubtaskId;
use serde::de::DeserializeOwned;
pub use spawn::{SpawnedTask, TaskError};

mod job;
mod manager;
mod run_subtask;
mod scheduler;
mod spawn;
mod stage;
mod task_status;
#[cfg(test)]
mod test_util;

pub use job::*;
pub use manager::*;
pub use scheduler::*;
pub use spawn::*;
pub use stage::*;
pub use task_status::*;

/// A task definition, along with the output that resulted from running it.
#[derive(Debug)]
pub struct TaskDefWithOutput<DEF: SubTask> {
    /// The task definition.
    pub task_def: DEF,
    /// The output of running the task.
    pub output: DEF::Output,
}

/// A definition of a subtask.
#[async_trait::async_trait]
pub trait SubTask: Clone + Debug + Send + Sync + 'static {
    /// The type of output produced by this task.
    type Output: Debug + DeserializeOwned + Send + 'static;

    /// A name that describes the task.
    fn description(&self) -> Cow<'static, str>;

    /// Start the task with the appropriate arguments.
    async fn spawn(&self, task_id: SubtaskId) -> Result<Box<dyn SpawnedTask>, Report<TaskError>>;
}
