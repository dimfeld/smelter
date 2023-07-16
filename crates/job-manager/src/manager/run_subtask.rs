use std::{borrow::Cow, sync::Arc};

use crate::{
    spawn::{SpawnedTask, Spawner, TaskError},
    task_status::{StatusCollector, StatusUpdateInput},
};
use error_stack::{Report, ResultExt};
use tokio::sync::Semaphore;

pub(super) type SubtaskResult = Result<SubtaskOutput, Report<TaskError>>;

#[derive(Debug)]
pub struct SubtaskOutput {
    pub output_location: String,
}

pub(super) struct SubtaskPayload<SPAWNER: Spawner> {
    pub input: Vec<u8>,
    pub stage_index: usize,
    pub spawn_name: Cow<'static, str>,
    pub local_id: String,
    pub try_num: usize,
    pub status_collector: StatusCollector,
    pub spawner: Arc<SPAWNER>,
}

pub(super) struct SubtaskSyncs {
    pub global_semaphore: Option<Arc<Semaphore>>,
    pub job_semaphore: Semaphore,
}

pub(super) async fn run_subtask<SPAWNER: Spawner>(
    task_index: usize,
    syncs: Arc<SubtaskSyncs>,
    payload: SubtaskPayload<SPAWNER>,
) -> Option<SubtaskResult> {
    let SubtaskSyncs {
        global_semaphore,
        job_semaphore,
    } = syncs.as_ref();

    let job_acquired = job_semaphore.acquire().await;
    if job_acquired.is_err() {
        // The semaphore was closed which means that the whole job has already exited before we could run.
        return None;
    }

    let global_acquired = match global_semaphore.as_ref() {
        Some(semaphore) => semaphore.acquire().await.map(Some),
        None => Ok(None),
    };
    if global_acquired.is_err() {
        // The entire job system is shutting down.
        return None;
    }

    let result = run_subtask_internal(task_index, payload).await;
    Some(result)
}

async fn run_subtask_internal<SPAWNER: Spawner>(
    task_index: usize,
    payload: SubtaskPayload<SPAWNER>,
) -> SubtaskResult {
    let SubtaskPayload {
        input,
        stage_index,
        spawn_name,
        local_id,
        try_num,
        status_collector,
        spawner,
    } = payload;

    let mut task = spawner.spawn(local_id, spawn_name, input).await?;
    let runtime_id = task.runtime_id().await?;
    status_collector.add(
        stage_index,
        task_index,
        StatusUpdateInput::Spawned(runtime_id.clone()),
    );

    task.wait()
        .await
        .attach_printable_lazy(|| format!("Job {task_index} try {try_num}"))
        .attach_printable_lazy(|| format!("Runtime ID {runtime_id}"))?;

    let output = SubtaskOutput {
        output_location: task.output_location(),
    };

    Ok(output)
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;
    use crate::spawn::{fail_wrapper::FailingSpawner, inprocess::InProcessSpawner, TaskError};

    fn create_task_input() -> (StatusCollector, Arc<SubtaskSyncs>) {
        let syncs = Arc::new(SubtaskSyncs {
            job_semaphore: Semaphore::new(1),
            global_semaphore: None,
        });

        let status_collector = StatusCollector::new(1);

        (status_collector, syncs)
    }

    #[tokio::test]
    async fn successful_task() {
        let spawner = Arc::new(InProcessSpawner::new(|info| async move {
            Ok(format!("result {}", info.local_id))
        }));

        let (status_collector, syncs) = create_task_input();

        let payload = SubtaskPayload {
            input: Vec::new(),
            stage_index: 0,
            spawn_name: Cow::Borrowed("test"),
            local_id: "the_id".to_string(),
            try_num: 0,
            status_collector: status_collector.clone(),
            spawner,
        };

        let result = run_subtask(0, syncs, payload)
            .await
            .expect("task result should return Some")
            .expect("task result should be Ok");
        assert_eq!(
            result.output_location,
            "test_the_id".to_string(),
            "output location"
        );
    }

    #[tokio::test]
    async fn semaphore_waits() {
        let spawner = Arc::new(InProcessSpawner::new(|info| async move {
            Ok(format!("result {}", info.local_id))
        }));

        let (status_collector, _) = create_task_input();

        let global_semaphore = Arc::new(Semaphore::new(1));
        let job_semaphore = Semaphore::new(1);

        let syncs = Arc::new(SubtaskSyncs {
            global_semaphore: Some(global_semaphore),
            job_semaphore,
        });

        let global_lock = syncs
            .global_semaphore
            .as_ref()
            .unwrap()
            .acquire()
            .await
            .expect("global semaphore lock");
        let job_lock = syncs
            .job_semaphore
            .acquire()
            .await
            .expect("job semaphore lock");

        let payload = SubtaskPayload {
            input: Vec::new(),
            stage_index: 0,
            spawn_name: Cow::Borrowed("test"),
            local_id: "the_id".to_string(),
            try_num: 0,
            status_collector: status_collector.clone(),
            spawner,
        };

        let task = tokio::task::spawn(run_subtask(0, syncs.clone(), payload));

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(!task.is_finished(), "task waits for semaphores");
        assert!(
            status_collector.read().await.is_empty(),
            "job did not spawn yet"
        );

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        drop(job_lock);
        assert!(
            !task.is_finished(),
            "task waits for job semaphore after acquring job semaphore"
        );
        assert!(
            status_collector.read().await.is_empty(),
            "job did not spawn yet"
        );

        drop(global_lock);

        let result = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("task did not time out")
            .expect("task should finish")
            .expect("task result should return Some")
            .expect("task result should be Ok");
        assert_eq!(
            result.output_location,
            "test_the_id".to_string(),
            "output location"
        );
    }

    #[tokio::test]
    async fn global_semaphore_closes() {
        let spawner = Arc::new(InProcessSpawner::new(|info| async move {
            Ok(format!("result {}", info.local_id))
        }));

        let (status_collector, _) = create_task_input();

        let global_semaphore = Arc::new(Semaphore::new(1));
        let job_semaphore = Semaphore::new(1);

        let syncs = Arc::new(SubtaskSyncs {
            global_semaphore: Some(global_semaphore),
            job_semaphore,
        });

        let _global_lock = syncs
            .global_semaphore
            .as_ref()
            .unwrap()
            .acquire()
            .await
            .expect("global semaphore lock");

        let payload = SubtaskPayload {
            input: Vec::new(),
            stage_index: 0,
            spawn_name: Cow::Borrowed("test"),
            local_id: "the_id".to_string(),
            try_num: 0,
            status_collector: status_collector.clone(),
            spawner,
        };

        let task = tokio::task::spawn(run_subtask(0, syncs.clone(), payload));
        syncs.global_semaphore.as_ref().unwrap().close();

        let result = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("task did not time out")
            .expect("task should finish");
        assert!(
            result.is_none(),
            "task should not run because global semaphore closed"
        );
    }

    #[tokio::test]
    async fn job_semaphore_closes() {
        let spawner = Arc::new(InProcessSpawner::new(|info| async move {
            Ok(format!("result {}", info.local_id))
        }));

        let (status_collector, _) = create_task_input();

        let global_semaphore = Arc::new(Semaphore::new(1));
        let job_semaphore = Semaphore::new(1);

        let syncs = Arc::new(SubtaskSyncs {
            global_semaphore: Some(global_semaphore),
            job_semaphore,
        });

        let _job_lock = syncs
            .job_semaphore
            .acquire()
            .await
            .expect("job semaphore lock");

        let payload = SubtaskPayload {
            input: Vec::new(),
            stage_index: 0,
            spawn_name: Cow::Borrowed("test"),
            local_id: "the_id".to_string(),
            try_num: 0,
            status_collector: status_collector.clone(),
            spawner,
        };

        let task = tokio::task::spawn(run_subtask(0, syncs.clone(), payload));
        syncs.job_semaphore.close();

        let result = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("task did not time out")
            .expect("task should finish");
        assert!(
            result.is_none(),
            "task should not run because job semaphore closed"
        );
    }

    #[tokio::test]
    async fn failed_task() {
        let spawner = Arc::new(InProcessSpawner::new(|_| async move {
            Err::<(), _>(TaskError::Failed(false))
        }));
        let (status_collector, syncs) = create_task_input();

        let payload = SubtaskPayload {
            input: Vec::new(),
            stage_index: 0,
            spawn_name: Cow::Borrowed("test"),
            local_id: "the_id".to_string(),
            try_num: 0,
            status_collector: status_collector.clone(),
            spawner,
        };

        let err = run_subtask(0, syncs, payload)
            .await
            .expect("task result should return Some")
            .expect_err("task result should be Err");
        assert_eq!(err.current_context(), &TaskError::Failed(false));
    }

    #[tokio::test]
    async fn failed_to_spawn() {
        let spawner = Arc::new(FailingSpawner::new(
            InProcessSpawner::new(|info| async move { Ok(format!("result {}", info.local_id)) }),
            |_| Err(TaskError::DidNotStart(true)),
        ));

        let (status_collector, syncs) = create_task_input();

        let payload = SubtaskPayload {
            input: Vec::new(),
            stage_index: 0,
            spawn_name: Cow::Borrowed("test"),
            local_id: "the_id".to_string(),
            try_num: 0,
            status_collector: status_collector.clone(),
            spawner,
        };

        let err = run_subtask(0, syncs, payload)
            .await
            .expect("task result should return Some")
            .expect_err("task result should be Err");
        assert_eq!(err.current_context(), &TaskError::DidNotStart(true));
    }
}
