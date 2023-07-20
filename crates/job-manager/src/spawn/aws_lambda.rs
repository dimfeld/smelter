use std::borrow::Cow;

use async_trait::async_trait;
use error_stack::Report;

use super::{SpawnedTask, Spawner, TaskError};
use crate::manager::SubtaskId;

pub struct LambdaSpawner {
    s3_output_path: String,
}

#[async_trait]
impl Spawner for LambdaSpawner {
    type SpawnedTask = SpawnedLambda;

    async fn spawn(
        &self,
        task_id: SubtaskId,
        task_name: Cow<'static, str>,
        input: Vec<u8>,
    ) -> Result<Self::SpawnedTask, Report<TaskError>> {
        todo!()
    }
}

pub struct SpawnedLambda {}

#[async_trait]
impl SpawnedTask for SpawnedLambda {
    async fn runtime_id(&self) -> Result<String, TaskError> {
        todo!()
    }

    async fn wait(&mut self) -> Result<Vec<u8>, Report<TaskError>> {
        todo!()
    }

    async fn kill(&mut self) -> Result<(), Report<TaskError>> {
        todo!()
    }
}
