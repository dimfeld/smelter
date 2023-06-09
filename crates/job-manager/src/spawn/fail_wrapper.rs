use std::borrow::Cow;

use async_trait::async_trait;
use error_stack::{IntoReport, Report};

use super::{inprocess::InProcessTaskInfo, Spawner, TaskError};

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
        local_id: String,
        task_name: Cow<'static, str>,
        input: Vec<u8>,
    ) -> Result<Self::SpawnedTask, Report<TaskError>> {
        let info = InProcessTaskInfo {
            task_name: task_name.to_string(),
            local_id: local_id.to_string(),
            input_value: &input,
        };

        (self.fail_func)(info).into_report()?;

        self.inner.spawn(local_id, task_name, input).await
    }
}
