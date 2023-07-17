use std::borrow::Cow;

use super::*;
use async_trait::async_trait;
use thiserror::Error;

struct TestTask {
    num_stages: usize,
    tasks_per_stage: usize,
}

struct TestTaskDef {}

#[derive(Debug)]
struct TestSubTaskDef {
    spawn_name: String,
    fail_serialize: bool,
}

#[derive(Error, Debug)]
#[error("A test error")]
struct TestError {}

impl TaskInfo for TestSubTaskDef {
    fn spawn_name(&self) -> std::borrow::Cow<'static, str> {
        Cow::from(self.spawn_name.clone())
    }

    fn serialize_input(&self) -> Result<Vec<u8>, eyre::Report> {
        if self.fail_serialize {
            Err(eyre::eyre!("failed to serialize input"))
        } else {
            Ok(Vec::new())
        }
    }
}

#[async_trait]
impl TaskType for TestTask {
    type TaskDef = TestTaskDef;
    type SubTaskDef = TestSubTaskDef;

    type Error = TestError;

    async fn create_initial_subtasks(
        &self,
        _task_def: &Self::TaskDef,
    ) -> Result<Vec<Self::SubTaskDef>, TestError> {
        let tasks = (0..self.tasks_per_stage)
            .map(|_| TestSubTaskDef {
                spawn_name: "test".to_string(),
                fail_serialize: false,
            })
            .collect();
        Ok(tasks)
    }

    async fn create_subtasks_from_result(
        &self,
        task_def: &Self::TaskDef,
        stage_number: usize,
        _subtasks: &[TaskDefWithOutput<Self::SubTaskDef>],
    ) -> Result<Vec<Self::SubTaskDef>, Self::Error> {
        if stage_number > self.num_stages {
            Ok(Vec::new())
        } else {
            self.create_initial_subtasks(task_def).await
        }
    }
}

mod run_tasks_stage {
    use crate::spawn::inprocess::InProcessSpawner;

    use super::*;

    #[tokio::test]
    async fn normal_run() {
        let task_data = TestTask {
            num_stages: 2,
            tasks_per_stage: 5,
        };

        let spawner = Arc::new(InProcessSpawner::new(|info| async move {
            Ok(format!("result {}", info.local_id))
        }));

        let manager = JobManager::new(
            task_data,
            spawner,
            SchedulerBehavior {
                max_concurrent_tasks: None,
                max_retries: 2,
                slow_task_behavior: SlowTaskBehavior::Wait,
            },
            None,
        );

        let status = StatusCollector::new(10);

        let result = manager.run(status.clone(), TestTaskDef {}).await;
    }

    #[tokio::test]
    #[ignore]
    async fn tail_retry() {}

    #[tokio::test]
    #[ignore]
    async fn retry_failures() {}

    #[tokio::test]
    #[ignore]
    async fn task_payload_serialize_failure() {}

    #[tokio::test]
    #[ignore]
    async fn permanent_failure_task_error() {}

    #[tokio::test]
    #[ignore]
    async fn too_many_retries() {}

    #[tokio::test]
    #[ignore]
    async fn tail_retries() {}
}

mod run {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn single_stage() {}

    #[tokio::test]
    #[ignore]
    async fn multiple_stages() {}
}
