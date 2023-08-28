use std::borrow::Cow;

use async_trait::async_trait;
use error_stack::{Report, ResultExt};
use serde::Serialize;

use super::{inprocess::InProcessTaskInfo, Spawner, TaskError};
use crate::SubtaskId;

pub struct FailingSpawner<
    SPAWNER: Spawner,
    FAILFUNC: Fn(InProcessTaskInfo) -> Result<(), TaskError> + Send + Sync + 'static,
> {
    inner: SPAWNER,
    fail_func: FAILFUNC,
}

impl<
        SPAWNER: Spawner,
        FAILFUNC: Fn(InProcessTaskInfo) -> Result<(), TaskError> + Send + Sync + 'static,
    > FailingSpawner<SPAWNER, FAILFUNC>
{
    pub fn new(inner: SPAWNER, fail_func: FAILFUNC) -> Self {
        Self { fail_func, inner }
    }
}

#[async_trait]
impl<
        SPAWNER: Spawner,
        FAILFUNC: Fn(InProcessTaskInfo) -> Result<(), TaskError> + Send + Sync + 'static,
    > Spawner for FailingSpawner<SPAWNER, FAILFUNC>
{
    type SpawnedTask = SPAWNER::SpawnedTask;

    async fn spawn(
        &self,
        task_id: SubtaskId,
        task_name: Cow<'static, str>,
        input: impl Serialize + Send,
    ) -> Result<Self::SpawnedTask, Report<TaskError>> {
        let input_value =
            serde_json::to_vec(&input).change_context(TaskError::TaskGenerationFailed)?;
        let info = InProcessTaskInfo {
            task_name: task_name.to_string(),
            task_id,
            input_value: &input_value,
        };

        (self.fail_func)(info)?;

        self.inner.spawn(task_id, task_name, input).await
    }
}
