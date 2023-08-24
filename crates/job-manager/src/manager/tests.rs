use std::{borrow::Cow, fmt::Debug, sync::Arc, time::Duration};

use async_trait::async_trait;
use error_stack::{IntoReportCompat, Report, ResultExt};
use futures::stream;
use thiserror::Error;
use tracing::info;

use super::{Job, SubtaskId};
use crate::{
    manager::JobManager,
    scheduler::{SchedulerBehavior, SlowTaskBehavior},
    spawn::{
        fail_wrapper::FailingSpawner, inprocess::InProcessSpawner, SpawnedTask, Spawner, TaskError,
    },
    task_status::{StatusCollector, StatusUpdateData},
    SubTask, TaskDefWithOutput,
};

struct TestTask<SPAWNER: Spawner> {
    num_stages: usize,
    tasks_per_stage: usize,
    spawner: Arc<SPAWNER>,
    fail_serialize: Option<SubtaskId>,
}

pub(crate) struct TestSubTaskDef<SPAWNER: Spawner> {
    pub spawn_name: String,
    pub fail_serialize: bool,
    pub spawner: Arc<SPAWNER>,
}

impl<SPAWNER: Spawner> Debug for TestSubTaskDef<SPAWNER> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestSubTaskDef")
            .field("spawn_name", &self.spawn_name)
            .field("fail_serialize", &self.fail_serialize)
            .finish()
    }
}

impl<SPAWNER: Spawner> Clone for TestSubTaskDef<SPAWNER> {
    fn clone(&self) -> Self {
        Self {
            spawn_name: self.spawn_name.clone(),
            fail_serialize: self.fail_serialize.clone(),
            spawner: self.spawner.clone(),
        }
    }
}

#[derive(Error, Debug)]
#[error("A test error")]
struct TestError {}

#[async_trait::async_trait]
impl<SPAWNER: Spawner> SubTask for TestSubTaskDef<SPAWNER> {
    type Output = String;

    fn description(&self) -> std::borrow::Cow<'static, str> {
        Cow::from(self.spawn_name.clone())
    }

    async fn spawn(&self, task_id: SubtaskId) -> Result<Box<dyn SpawnedTask>, Report<TaskError>> {
        if self.fail_serialize {
            Err(eyre::eyre!("failed to serialize input"))
                .into_report()
                .change_context(TaskError::TaskGenerationFailed)?;
        }

        let task = self
            .spawner
            .spawn(task_id, Cow::from(self.spawn_name.clone()), Vec::new())
            .await?;

        Ok(Box::new(task))
    }

    fn read_task_response(data: Vec<u8>) -> Result<Self::Output, TaskError> {
        Ok(String::from_utf8(data).unwrap_or_else(|_| String::new()))
    }
}

impl<SPAWNER: Spawner> TestTask<SPAWNER> {
    async fn run(
        &self,
        manager: &JobManager,
    ) -> Result<Vec<TaskDefWithOutput<TestSubTaskDef<SPAWNER>>>, Report<TaskError>> {
        let mut job = manager.new_job();
        let mut results: Vec<TaskDefWithOutput<TestSubTaskDef<SPAWNER>>> = Vec::new();

        for stage_index in 0..self.num_stages {
            let (stage_tx, stage_rx) = job.add_stage().await;
            for task_index in 0..self.tasks_per_stage {
                let fail_serialize = self
                    .fail_serialize
                    .map(|id| id.stage == stage_index as u16 && id.task == task_index as u32)
                    .unwrap_or(false);

                stage_tx
                    .add_subtask(TestSubTaskDef {
                        spawn_name: format!("test-{stage_index}-{task_index}"),
                        spawner: self.spawner.clone(),
                        fail_serialize,
                    })
                    .await;
            }

            results = stage_rx.collect().await?;
            assert_eq!(results.len(), self.tasks_per_stage);
        }

        job.wait().await.change_context(TaskError::Failed(false))?;

        Ok(results)
    }
}

#[tokio::test]
async fn normal_run() {
    let spawner = Arc::new(InProcessSpawner::new(|info| async move {
        Ok(format!("result {}", info.task_id))
    }));

    let task = TestTask {
        num_stages: 3,
        tasks_per_stage: 5,
        spawner,
        fail_serialize: None,
    };

    let status = StatusCollector::new(10);
    let manager = JobManager::new(
        SchedulerBehavior {
            max_concurrent_tasks: None,
            max_retries: 2,
            slow_task_behavior: SlowTaskBehavior::Wait,
        },
        status.clone(),
        None,
    );

    task.run(&manager).await.expect("Run succeeded");
    // println!("{:?}", result);
}

#[tokio::test]
async fn single_stage() {
    let spawner = Arc::new(InProcessSpawner::new(|info| async move {
        Ok(format!("result {}", info.task_id))
    }));

    let task_data = TestTask {
        num_stages: 1,
        tasks_per_stage: 5,
        spawner,
        fail_serialize: None,
    };

    let status = StatusCollector::new(10);
    let manager = JobManager::new(
        SchedulerBehavior {
            max_concurrent_tasks: None,
            max_retries: 2,
            slow_task_behavior: SlowTaskBehavior::Wait,
        },
        status.clone(),
        None,
    );

    let mut result = task_data
        .run(&manager)
        .await
        .expect("Run succeeded")
        .into_iter()
        .map(|result| result.output)
        .collect::<Vec<_>>();
    result.sort();

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

    let task_data = TestTask {
        num_stages: 2,
        tasks_per_stage: 5,
        spawner,
        fail_serialize: None,
    };

    let status = StatusCollector::new(10);
    let manager = JobManager::new(
        SchedulerBehavior {
            max_concurrent_tasks: None,
            max_retries: 2,
            slow_task_behavior: SlowTaskBehavior::RerunLastN(2),
        },
        status.clone(),
        None,
    );

    let mut result = task_data
        .run(&manager)
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
    let spawner = Arc::new(InProcessSpawner::new(|info| async move {
        if (info.task_id.task == 0 || info.task_id.task == 2) && info.task_id.try_num < 2 {
            info!("Failing task {}", info.task_id);
            Err(TaskError::Failed(true))
        } else {
            info!("Working task {}", info.task_id);
            Ok(format!("result {}", info.task_id))
        }
    }));

    let task_data = TestTask {
        num_stages: 2,
        tasks_per_stage: 5,
        spawner,
        fail_serialize: None,
    };

    let status = StatusCollector::new(10);
    let manager = JobManager::new(
        SchedulerBehavior {
            max_concurrent_tasks: None,
            max_retries: 2,
            slow_task_behavior: SlowTaskBehavior::Wait,
        },
        status.clone(),
        None,
    );

    let mut result = task_data
        .run(&manager)
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
    let spawner = Arc::new(InProcessSpawner::new(|info| async move {
        if info.task_id.task == 2 {
            info!("Failing task {}", info.task_id);
            Err(TaskError::Failed(false))
        } else {
            info!("Working task {}", info.task_id);
            Ok(format!("result {}", info.task_id))
        }
    }));

    let task_data = TestTask {
        num_stages: 2,
        tasks_per_stage: 5,
        spawner,
        fail_serialize: None,
    };

    let status = StatusCollector::new(10);
    let manager = JobManager::new(
        SchedulerBehavior {
            max_concurrent_tasks: None,
            max_retries: 2,
            slow_task_behavior: SlowTaskBehavior::Wait,
        },
        status.clone(),
        None,
    );

    let result = task_data.run(&manager).await.expect_err("Run failed");

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
    let spawner = Arc::new(InProcessSpawner::new(|info| async move {
        if info.task_id.task == 2 {
            info!("Failing task {}", info.task_id);
            Err(TaskError::Failed(true))
        } else {
            info!("Working task {}", info.task_id);
            Ok(format!("result {}", info.task_id))
        }
    }));

    let task_data = TestTask {
        num_stages: 2,
        tasks_per_stage: 5,
        spawner,
        fail_serialize: None,
    };

    let status = StatusCollector::new(10);
    let manager = JobManager::new(
        SchedulerBehavior {
            max_concurrent_tasks: None,
            max_retries: 2,
            slow_task_behavior: SlowTaskBehavior::Wait,
        },
        status.clone(),
        None,
    );

    let result = task_data.run(&manager).await.expect_err("Run failed");

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
        spawner,
        fail_serialize: None,
    };

    let status = StatusCollector::new(10);
    let manager = JobManager::new(
        SchedulerBehavior {
            max_concurrent_tasks: None,
            max_retries: 2,
            slow_task_behavior: SlowTaskBehavior::Wait,
        },
        status.clone(),
        None,
    );

    let mut result = task_data
        .run(&manager)
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
    let spawner = Arc::new(InProcessSpawner::new(|info| async move {
        Ok(format!("result {}", info.task_id))
    }));

    let task_data = TestTask {
        num_stages: 2,
        tasks_per_stage: 5,
        spawner,
        fail_serialize: Some(SubtaskId {
            stage: 0,
            task: 2,
            try_num: 0,
        }),
    };

    let status = StatusCollector::new(10);
    let manager = JobManager::new(
        SchedulerBehavior {
            max_concurrent_tasks: None,
            max_retries: 2,
            slow_task_behavior: SlowTaskBehavior::Wait,
        },
        status.clone(),
        None,
    );

    let result = task_data.run(&manager).await.expect_err("Run failed");

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
    let spawner = Arc::new(InProcessSpawner::new(|info| async move {
        Ok(format!("result {}", info.task_id))
    }));

    let task_data = TestTask {
        num_stages: 3,
        tasks_per_stage: 5,
        spawner,
        fail_serialize: None,
    };

    let status = StatusCollector::new(10);
    let manager = JobManager::new(
        SchedulerBehavior {
            max_concurrent_tasks: Some(2),
            max_retries: 2,
            slow_task_behavior: SlowTaskBehavior::Wait,
        },
        status.clone(),
        None,
    );

    let result = task_data.run(&manager).await.expect("Run succeeded");
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

#[tokio::test]
async fn wait_unordered() {
    let spawner = Arc::new(InProcessSpawner::new(|info| async move {
        let duration = 100 - (info.task_id.task * 10);
        tokio::time::sleep(Duration::from_millis(duration as u64)).await;
        Ok(format!("result {}", info.task_id))
    }));

    let task_data = TestTask {
        num_stages: 1,
        tasks_per_stage: 5,
        spawner,
        fail_serialize: None,
    };

    let status = StatusCollector::new(10);
    let manager = JobManager::new(
        SchedulerBehavior {
            max_concurrent_tasks: None,
            max_retries: 2,
            slow_task_behavior: SlowTaskBehavior::Wait,
        },
        status.clone(),
        None,
    );

    let result = task_data
        .run(&manager)
        .await
        .expect("Run succeeded")
        .into_iter()
        .map(|i| i.output)
        .collect::<Vec<_>>();

    let mut sorted = result.clone();
    sorted.sort();

    assert_eq!(
        sorted,
        vec![
            "result 000-00000-00".to_string(),
            "result 000-00001-00".to_string(),
            "result 000-00002-00".to_string(),
            "result 000-00003-00".to_string(),
            "result 000-00004-00".to_string(),
        ],
    );

    assert_ne!(
        result, sorted,
        "final task result should not correspond to order tasks were run"
    );
}
