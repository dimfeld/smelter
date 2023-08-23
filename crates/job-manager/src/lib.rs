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
pub trait SubTask: Debug + Clone + Send {
    type Output: Debug + DeserializeOwned + Send + 'static;

    /// A name that describes the task.
    fn description(&self) -> Cow<'static, str>;

    /// Start the task with the appropriate arguments.
    async fn spawn(&self, task_id: SubtaskId) -> Box<dyn SpawnedTask>;

    fn read_task_response(data: Vec<u8>) -> Result<Self::Output, TaskError>;
}

async fn run_test() {
    let job = Job::new();

    // this returns a JobStage
    let stage_1 = job.add_stage("stage 1");
    stage_1.add_task();

    for finished in stage_1.subtask_result {
        // get jobs until we are done
    }
}
