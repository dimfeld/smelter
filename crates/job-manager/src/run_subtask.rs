use std::sync::Arc;

use error_stack::{Report, ResultExt};
use smelter_worker::WorkerResult;
use tokio::sync::Semaphore;
use tracing::{instrument, Level};

use super::SubtaskId;
use crate::{
    spawn::TaskError,
    task_status::{
        StatusCollector, StatusUpdateData, StatusUpdateSpawnedData, StatusUpdateSuccessData,
    },
    SerializedWorkerFailure, SubTask,
};

pub(super) type SubtaskResult<T> = Result<SubtaskOutput<T>, Report<TaskError>>;

#[derive(Debug)]
pub struct SubtaskOutput<T> {
    pub output: T,
}

pub(crate) struct SubtaskPayload<SUBTASK: SubTask> {
    pub input: SUBTASK,
    pub task_id: SubtaskId,
    pub status_collector: StatusCollector,
}

pub(crate) struct SubtaskSyncs {
    pub global_semaphore: Option<Arc<Semaphore>>,
    pub job_semaphore: Arc<Semaphore>,
    pub cancel: tokio::sync::watch::Receiver<()>,
}

#[instrument(level=Level::DEBUG, ret, parent=&parent_span, skip(syncs, parent_span, payload), fields(task_id = ?payload.task_id))]
pub(super) async fn run_subtask<SUBTASK: SubTask>(
    parent_span: tracing::Span,
    syncs: Arc<SubtaskSyncs>,
    payload: SubtaskPayload<SUBTASK>,
) -> Option<SubtaskResult<SUBTASK::Output>> {
    let SubtaskSyncs {
        global_semaphore,
        job_semaphore,
        cancel,
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

    let result = run_subtask_internal(cancel.clone(), payload).await;
    Some(result)
}

#[instrument(level=Level::TRACE, skip(cancel, payload), fields(task_id = %payload.task_id))]
async fn run_subtask_internal<SUBTASK: SubTask>(
    mut cancel: tokio::sync::watch::Receiver<()>,
    payload: SubtaskPayload<SUBTASK>,
) -> SubtaskResult<SUBTASK::Output> {
    let SubtaskPayload {
        input,
        task_id,
        status_collector,
    } = payload;

    let mut task = input.spawn(task_id).await?;
    let runtime_id = task.runtime_id().await?;
    status_collector.add(
        task_id,
        StatusUpdateData::Spawned(StatusUpdateSpawnedData {
            runtime_id: runtime_id.clone(),
        }),
    );

    tokio::select! {
        res = task.wait() => {
            let res = res.attach_printable_lazy(|| format!("Job {task_id} Runtime ID {runtime_id}"))?;

            let output: SUBTASK::Output = WorkerResult::parse(&res)
                .map_err(|e| Report::new(SerializedWorkerFailure(e.error)).change_context(TaskError::Failed(e.retryable)))
                .attach_printable_lazy(|| format!("Job {task_id} Runtime ID {runtime_id}"))?;

            status_collector.add(task_id, StatusUpdateData::Success(
                    StatusUpdateSuccessData {
                        output: res.clone(),
                    }
                ));

            Ok(SubtaskOutput { output })
        }

        _ = cancel.changed() => {
            task.kill().await.ok();
            status_collector.add(task_id, StatusUpdateData::Cancelled);
            Err(Report::new(TaskError::Cancelled))
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;
    use crate::{
        manager::tests::TestSubTaskDef,
        spawn::{fail_wrapper::FailingSpawner, inprocess::InProcessSpawner, TaskError},
        Spawner,
    };

    fn create_task_input<SPAWNER: Spawner>(
        spawner: Arc<SPAWNER>,
    ) -> (
        TestSubTaskDef<SPAWNER>,
        StatusCollector,
        tokio::sync::watch::Sender<()>,
        Arc<SubtaskSyncs>,
    ) {
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(());
        let syncs = Arc::new(SubtaskSyncs {
            job_semaphore: Arc::new(Semaphore::new(1)),
            global_semaphore: None,
            cancel: cancel_rx,
        });

        let status_collector = StatusCollector::new(1);

        let task = TestSubTaskDef {
            spawn_name: "test".to_string(),
            fail_serialize: false,
            spawner,
        };

        (task, status_collector, cancel_tx, syncs)
    }

    #[tokio::test]
    async fn successful_task() {
        let spawner = Arc::new(InProcessSpawner::new(|info| async move {
            Ok(format!("result {}", info.task_id))
        }));

        let (input, status_collector, _cancel_tx, syncs) = create_task_input(spawner);

        let payload = SubtaskPayload {
            input,
            status_collector: status_collector.clone(),
            task_id: SubtaskId {
                stage: 0,
                task: 0,
                try_num: 0,
            },
        };

        let result = run_subtask(tracing::Span::current(), syncs, payload)
            .await
            .expect("task result should return Some")
            .expect("task result should be Ok");

        assert_eq!(result.output, "result 000-00000-00", "output");
    }

    #[tokio::test]
    async fn cancel_task() {
        let spawner = Arc::new(InProcessSpawner::new(|info| async move {
            futures::future::pending::<()>().await;
            Ok::<_, TaskError>(format!("result {}", info.task_id))
        }));

        let (input, status_collector, cancel_tx, syncs) = create_task_input(spawner);

        let payload = SubtaskPayload {
            input,
            task_id: SubtaskId {
                stage: 0,
                task: 0,
                try_num: 0,
            },
            status_collector: status_collector.clone(),
        };

        let task = tokio::task::spawn(run_subtask(tracing::Span::current(), syncs, payload));

        drop(cancel_tx);

        let result = timeout(Duration::from_secs(1), task)
            .await
            .expect("task should finish")
            .expect("task should not panic")
            .expect("task result should return Some")
            .expect_err("task result should be Err");
        assert_eq!(
            result.current_context(),
            &TaskError::Cancelled,
            "task result should be Cancelled"
        );
    }

    #[tokio::test]
    async fn semaphore_waits() {
        let spawner = Arc::new(InProcessSpawner::new(|info| async move {
            Ok(format!("result {}", info.task_id))
        }));

        let (input, status_collector, _cancel_tx, syncs) = create_task_input(spawner);

        let global_semaphore = Arc::new(Semaphore::new(1));
        let job_semaphore = Arc::new(Semaphore::new(1));

        let syncs = Arc::new(SubtaskSyncs {
            global_semaphore: Some(global_semaphore),
            job_semaphore,
            cancel: syncs.cancel.clone(),
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
            input,
            task_id: SubtaskId {
                stage: 0,
                task: 0,
                try_num: 0,
            },
            status_collector: status_collector.clone(),
        };

        let task = tokio::task::spawn(run_subtask(
            tracing::Span::current(),
            syncs.clone(),
            payload,
        ));

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
        assert_eq!(result.output, "result 000-00000-00", "output location");
    }

    #[tokio::test]
    async fn global_semaphore_closes() {
        let spawner = Arc::new(InProcessSpawner::new(|info| async move {
            Ok(format!("result {}", info.task_id))
        }));

        let (input, status_collector, _cancel_tx, syncs) = create_task_input(spawner);

        let global_semaphore = Arc::new(Semaphore::new(1));
        let job_semaphore = Arc::new(Semaphore::new(1));

        let syncs = Arc::new(SubtaskSyncs {
            global_semaphore: Some(global_semaphore),
            job_semaphore,
            cancel: syncs.cancel.clone(),
        });

        let _global_lock = syncs
            .global_semaphore
            .as_ref()
            .unwrap()
            .acquire()
            .await
            .expect("global semaphore lock");

        let payload = SubtaskPayload {
            input,
            task_id: SubtaskId {
                stage: 0,
                task: 0,
                try_num: 0,
            },
            status_collector: status_collector.clone(),
        };

        let task = tokio::task::spawn(run_subtask(
            tracing::Span::current(),
            syncs.clone(),
            payload,
        ));
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
            Ok(format!("result {}", info.task_id))
        }));

        let (input, status_collector, _cancel_tx, syncs) = create_task_input(spawner);

        let global_semaphore = Arc::new(Semaphore::new(1));
        let job_semaphore = Arc::new(Semaphore::new(1));

        let syncs = Arc::new(SubtaskSyncs {
            global_semaphore: Some(global_semaphore),
            job_semaphore,
            cancel: syncs.cancel.clone(),
        });

        let _job_lock = syncs
            .job_semaphore
            .acquire()
            .await
            .expect("job semaphore lock");

        let payload = SubtaskPayload {
            input,
            task_id: SubtaskId {
                stage: 0,
                task: 0,
                try_num: 0,
            },
            status_collector: status_collector.clone(),
        };

        let task = tokio::task::spawn(run_subtask(
            tracing::Span::current(),
            syncs.clone(),
            payload,
        ));
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
    async fn failed_to_spawn() {
        let spawner = Arc::new(FailingSpawner::new(
            InProcessSpawner::new(|info| async move { Ok(format!("result {}", info.task_id)) }),
            |_| Err(TaskError::DidNotStart(true)),
        ));

        let (input, status_collector, _cancel_tx, syncs) = create_task_input(spawner);

        let payload = SubtaskPayload {
            input,
            task_id: SubtaskId {
                stage: 0,
                task: 0,
                try_num: 0,
            },
            status_collector: status_collector.clone(),
        };

        let err = run_subtask(tracing::Span::current(), syncs, payload)
            .await
            .expect("task result should return Some")
            .expect_err("task result should be Err");
        assert_eq!(err.current_context(), &TaskError::DidNotStart(true));
    }
}
