use std::{borrow::Cow, sync::Arc, time::Duration};

use async_trait::async_trait;
use thiserror::Error;
use tracing::info;

use super::SubtaskId;
use crate::{
    manager::JobManager,
    scheduler::{SchedulerBehavior, SlowTaskBehavior},
    spawn::{fail_wrapper::FailingSpawner, inprocess::InProcessSpawner, TaskError},
    task_status::{StatusCollector, StatusUpdateData},
    TaskDefWithOutput, TaskInfo, TaskType,
};

struct TestTask {
    num_stages: usize,
    tasks_per_stage: usize,
}

#[derive(Debug, Default)]
struct TestTaskDef {
    fail_serialize: Option<SubtaskId>,
}

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

impl TestTask {
    async fn create_subtasks(
        &self,
        task_def: &TestTaskDef,
        stage_num: usize,
    ) -> Result<Vec<TestSubTaskDef>, TestError> {
        let tasks = (0..self.tasks_per_stage)
            .map(|task_index| {
                let fail_serialize = task_def
                    .fail_serialize
                    .map(|id| id.stage == stage_num as u16 && id.task == task_index as u32)
                    .unwrap_or(false);
                TestSubTaskDef {
                    spawn_name: "test".to_string(),
                    fail_serialize,
                }
            })
            .collect();
        Ok(tasks)
    }
}

#[async_trait]
impl TaskType for TestTask {
    type TaskDef = TestTaskDef;
    type SubTaskDef = TestSubTaskDef;
    type SubtaskOutput = String;

    type Error = TestError;

    async fn create_initial_subtasks(
        &self,
        task_def: &Self::TaskDef,
    ) -> Result<Vec<Self::SubTaskDef>, TestError> {
        self.create_subtasks(task_def, 0).await
    }

    async fn create_subtasks_from_result(
        &self,
        task_def: &Self::TaskDef,
        stage_number: usize,
        _subtasks: &[TaskDefWithOutput<Self::SubTaskDef, Self::SubtaskOutput>],
    ) -> Result<Vec<Self::SubTaskDef>, Self::Error> {
        if stage_number + 1 >= self.num_stages {
            Ok(Vec::new())
        } else {
            self.create_subtasks(task_def, stage_number).await
        }
    }

    fn read_task_response(data: Vec<u8>) -> Result<Self::SubtaskOutput, Self::Error> {
        String::from_utf8(data).map_err(|_| TestError {})
    }
}

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
        .run(status.clone(), TestTaskDef::default())
        .await
        .expect("Run succeeded");
    assert_eq!(result.len(), 5);
    // println!("{:?}", result);
}

#[tokio::test]
async fn single_stage() {
    let task_data = TestTask {
        num_stages: 1,
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
        .run(status.clone(), TestTaskDef::default())
        .await
        .expect("Run succeeded")
        .into_iter()
        .map(|result| result.output)
        .collect::<Vec<_>>();
    assert_eq!(
        result,
        vec![
            "result 000-00000-00".to_string(),
            "result 000-00001-00".to_string(),
            "result 000-00002-00".to_string(),
            "result 000-00003-00".to_string(),
            "result 000-00004-00".to_string(),
        ],
    );
}

#[tokio::test]
async fn tail_retry() {
    let task_data = TestTask {
        num_stages: 2,
        tasks_per_stage: 5,
    };

    let spawner = Arc::new(InProcessSpawner::new(|info| async move {
        info!("Running task {}", info.task_id);
        let sleep_time =
            if (info.task_id.task == 0 || info.task_id.task == 2) && info.task_id.try_num == 0 {
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
        .run(status.clone(), TestTaskDef::default())
        .await
        .expect("Run succeeded")
        .into_iter()
        .map(|result| result.output)
        .collect::<Vec<_>>();
    result.sort();

    info!("{:?}", result);
    assert_eq!(
        result,
        vec![
            "result 001-00000-01".to_string(),
            "result 001-00001-00".to_string(),
            "result 001-00002-01".to_string(),
            "result 001-00003-00".to_string(),
            "result 001-00004-00".to_string(),
        ],
        "Finished tasks should be the tail retry tasks for 0 and 2"
    );
}

#[tokio::test]
async fn retry_failures() {
    let task_data = TestTask {
        num_stages: 2,
        tasks_per_stage: 5,
    };

    let spawner = Arc::new(InProcessSpawner::new(|info| async move {
        if (info.task_id.task == 0 || info.task_id.task == 2) && info.task_id.try_num < 2 {
            info!("Failing task {}", info.task_id);
            Err(TaskError::Failed(true))
        } else {
            info!("Working task {}", info.task_id);
            Ok(format!("result {}", info.task_id))
        }
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

    let mut result = manager
        .run(status.clone(), TestTaskDef::default())
        .await
        .expect("Run succeeded")
        .into_iter()
        .map(|result| result.output)
        .collect::<Vec<_>>();
    result.sort();

    info!("{:?}", result);
    assert_eq!(
        result,
        vec![
            "result 001-00000-02".to_string(),
            "result 001-00001-00".to_string(),
            "result 001-00002-02".to_string(),
            "result 001-00003-00".to_string(),
            "result 001-00004-00".to_string(),
        ],
        "Finished tasks should be the retry tasks for 0 and 2"
    );
}

#[tokio::test]
async fn permanent_failure_task_error() {
    let task_data = TestTask {
        num_stages: 2,
        tasks_per_stage: 5,
    };

    let spawner = Arc::new(InProcessSpawner::new(|info| async move {
        if info.task_id.task == 2 {
            info!("Failing task {}", info.task_id);
            Err(TaskError::Failed(false))
        } else {
            info!("Working task {}", info.task_id);
            Ok(format!("result {}", info.task_id))
        }
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
        .run(status.clone(), TestTaskDef::default())
        .await
        .expect_err("Run failed");

    info!("{:?}", result);
    assert_eq!(
        result.current_context(),
        &TaskError::Failed(false),
        "Should finish with failed error"
    );

    let status = status.take().await;
    status
        .iter()
        .find(|item| {
            item.task_id.stage == 0
                && item.task_id.task == 2
                && item.task_id.try_num == 0
                && matches!(item.data, StatusUpdateData::Failed(_))
        })
        .expect("Should find status item for failed try");

    let later_items = status
        .iter()
        .find(|item| item.task_id.stage == 0 && item.task_id.task == 2 && item.task_id.try_num > 0);
    assert!(
        later_items.is_none(),
        "Should not have tried to run task after permanent failure"
    );
}

#[tokio::test]
async fn too_many_retries() {
    let task_data = TestTask {
        num_stages: 2,
        tasks_per_stage: 5,
    };

    let spawner = Arc::new(InProcessSpawner::new(|info| async move {
        if info.task_id.task == 2 {
            info!("Failing task {}", info.task_id);
            Err(TaskError::Failed(true))
        } else {
            info!("Working task {}", info.task_id);
            Ok(format!("result {}", info.task_id))
        }
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
        .run(status.clone(), TestTaskDef::default())
        .await
        .expect_err("Run failed");

    info!("{:?}", result);
    assert_eq!(
        result.current_context(),
        &TaskError::Failed(false),
        "Should finish with failed error"
    );

    let status = status.take().await;
    status
        .into_iter()
        .find(|item| {
            item.task_id.stage == 0
                && item.task_id.task == 2
                && item.task_id.try_num == 2
                && matches!(item.data, StatusUpdateData::Failed(_))
        })
        .expect("Should find status item for failed final try");
}

#[tokio::test]
async fn task_panicked() {
    let spawner = Arc::new(FailingSpawner::new(
        InProcessSpawner::new(|info| async move { Ok(format!("result {}", info.task_id)) }),
        |info| {
            if info.task_id.task == 2 && info.task_id.try_num == 0 {
                panic!("test panic")
            }

            Ok(())
        },
    ));

    let task_data = TestTask {
        num_stages: 2,
        tasks_per_stage: 5,
    };

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

    let mut result = manager
        .run(status.clone(), TestTaskDef::default())
        .await
        .expect("Run succeeded")
        .into_iter()
        .map(|result| result.output)
        .collect::<Vec<_>>();
    result.sort();

    info!("{:?}", result);
    assert_eq!(
        result,
        vec![
            "result 001-00000-00".to_string(),
            "result 001-00001-00".to_string(),
            "result 001-00002-01".to_string(),
            "result 001-00003-00".to_string(),
            "result 001-00004-00".to_string(),
        ],
        "Finished tasks should be the retry task for task 2"
    );
}

#[tokio::test]
async fn task_payload_serialize_failure() {
    let task_data = TestTask {
        num_stages: 2,
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
        .run(
            status.clone(),
            TestTaskDef {
                fail_serialize: Some(SubtaskId {
                    stage: 0,
                    task: 2,
                    try_num: 0,
                }),
            },
        )
        .await
        .expect_err("Run failed");

    info!("{:?}", result);
    assert_eq!(
        result.current_context(),
        &TaskError::Failed(false),
        "Should finish with failed error"
    );

    let status = status.take().await;
    status
        .iter()
        .find(|item| {
            item.task_id.stage == 0
                && item.task_id.task == 2
                && item.task_id.try_num == 0
                && matches!(item.data, StatusUpdateData::Failed(_))
        })
        .expect("Should find status item for failed try");

    let later_items = status
        .iter()
        .find(|item| item.task_id.stage == 0 && item.task_id.task == 2 && item.task_id.try_num > 0);
    assert!(
        later_items.is_none(),
        "Should not have tried to run task after permanent failure"
    );
}

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
        .run(status.clone(), TestTaskDef::default())
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
