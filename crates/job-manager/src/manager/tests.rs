use std::borrow::Cow;

use super::*;
use async_trait::async_trait;
use thiserror::Error;
use tracing::info;

struct TestTask {
    num_stages: usize,
    tasks_per_stage: usize,
}

#[derive(Debug)]
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
        if stage_number + 1 >= self.num_stages {
            Ok(Vec::new())
        } else {
            self.create_initial_subtasks(task_def).await
        }
    }
}

mod run_tasks_stage {
    use std::time::Duration;

    use crate::{
        spawn::inprocess::InProcessSpawner, task_status::StatusUpdateData,
        test_util::setup_test_tracing,
    };

    use super::*;

    #[tokio::test]
    async fn normal_run() {
        let task_data = TestTask {
            num_stages: 3,
            tasks_per_stage: 5,
        };

        let spawner = Arc::new(InProcessSpawner::new(|info| async move {
            Ok(format!("result {}", info.task_id))
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

        let result = manager
            .run(status.clone(), TestTaskDef {})
            .await
            .expect("Run succeeded");
        assert_eq!(result.len(), 5);
        // println!("{:?}", result);
    }

    #[tokio::test]
    async fn tail_retry() {
        setup_test_tracing();
        let task_data = TestTask {
            num_stages: 1,
            tasks_per_stage: 5,
        };

        let spawner = Arc::new(InProcessSpawner::new(|info| async move {
            info!("Running task {}", info.task_id);
            let sleep_time = if (info.task_id.task == 0 || info.task_id.task == 2)
                && info.task_id.try_num == 0
            {
                10000
            } else {
                10
            };
            tokio::time::sleep(Duration::from_millis(sleep_time)).await;
            info!("Finished task {}", info.task_id);
            Ok(format!("result {}", info.task_id))
        }));

        let manager = JobManager::new(
            task_data,
            spawner,
            SchedulerBehavior {
                max_concurrent_tasks: None,
                max_retries: 2,
                slow_task_behavior: SlowTaskBehavior::RerunLastN(2),
            },
            None,
        );

        let status = StatusCollector::new(10);

        let mut result = manager
            .run(status.clone(), TestTaskDef {})
            .await
            .expect("Run succeeded");
        result.sort();

        assert_eq!(
            result,
            vec![
                "test_000-00000-01".to_string(),
                "test_000-00001-00".to_string(),
                "test_000-00002-01".to_string(),
                "test_000-00003-00".to_string(),
                "test_000-00004-00".to_string(),
            ],
            "Finished tasks should be the tail retry tasks for 0 and 2"
        );

        info!("{:?}", result);
    }

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
    async fn task_panicked() {}

    #[tokio::test]
    async fn max_concurrent_tasks() {
        let task_data = TestTask {
            num_stages: 3,
            tasks_per_stage: 5,
        };

        let spawner = Arc::new(InProcessSpawner::new(|info| async move {
            Ok(format!("result {}", info.task_id))
        }));

        let manager = JobManager::new(
            task_data,
            spawner,
            SchedulerBehavior {
                max_concurrent_tasks: Some(2),
                max_retries: 2,
                slow_task_behavior: SlowTaskBehavior::Wait,
            },
            None,
        );

        let status = StatusCollector::new(10);

        let result = manager
            .run(status.clone(), TestTaskDef {})
            .await
            .expect("Run succeeded");
        assert_eq!(result.len(), 5);

        let mut active = 0;
        let mut max_active = 0;

        let status = status.take().await;
        for item in status {
            match item.data {
                StatusUpdateData::Spawned(_) => {
                    active += 1;
                }
                StatusUpdateData::Success(_) => {
                    active -= 1;
                }
                o => panic!("Unexpected status: {:?}", o),
            }

            max_active = std::cmp::max(max_active, active);
        }

        assert_eq!(max_active, 2, "No more than 2 tasks running at onces");
    }
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
