//! Run workers in the same process as the scheduler. This is only really useful for some unit
//! tests.

use std::{borrow::Cow, future::Future};

use ahash::HashMap;
use async_trait::async_trait;
use error_stack::{Report, ResultExt};
use smelter_worker::{WorkerError, WorkerResult};
use tokio::{sync::oneshot, task::JoinHandle};

use super::{SpawnedTask, Spawner, TaskError};
use crate::manager::SubtaskId;
#[cfg(test)]
use crate::test_util::setup_test_tracing;

pub struct InProcessTaskInfo<'a> {
    pub task_name: String,
    pub task_id: SubtaskId,
    pub input_value: &'a [u8],
}

pub struct InProcessSpawner<F, FUNC>
where
    F: Future<Output = Result<String, TaskError>> + 'static,
    FUNC: FnOnce(InProcessTaskInfo) -> F + Send + Sync + Clone + 'static,
{
    task_fn: FUNC,
}

impl<F, FUNC> InProcessSpawner<F, FUNC>
where
    F: Future<Output = Result<String, TaskError>> + 'static,
    FUNC: FnOnce(InProcessTaskInfo) -> F + Send + Sync + Clone + 'static,
{
    pub fn new(task_fn: FUNC) -> Self {
        #[cfg(test)]
        setup_test_tracing();

        Self { task_fn }
    }
}

#[async_trait]
impl<F, FUNC> Spawner for InProcessSpawner<F, FUNC>
where
    F: Future<Output = Result<String, TaskError>> + Send + Sync,
    FUNC: FnOnce(InProcessTaskInfo) -> F + Send + Sync + Clone + 'static,
{
    type SpawnedTask = InProcessSpawnedTask;

    async fn spawn(
        &self,
        task_id: SubtaskId,
        task_name: Cow<'static, str>,
        input: Vec<u8>,
    ) -> Result<Self::SpawnedTask, Report<TaskError>> {
        let task_fn = self.task_fn.clone();
        let input = input.to_vec();
        let task = InProcessSpawnedTask {
            task_id,
            task: Some(tokio::task::spawn(async move {
                let result = (task_fn)(InProcessTaskInfo {
                    task_name: task_name.to_string(),
                    task_id,
                    input_value: &input,
                })
                .await?;

                Ok::<String, TaskError>(result)
            })),
        };

        Ok(task)
    }
}

pub struct InProcessSpawnedTask {
    task_id: SubtaskId,
    task: Option<JoinHandle<Result<String, TaskError>>>,
}

#[async_trait]
impl SpawnedTask for InProcessSpawnedTask {
    async fn runtime_id(&self) -> Result<String, TaskError> {
        Ok(self.task_id.to_string())
    }

    async fn kill(&mut self) -> Result<(), Report<TaskError>> {
        if let Some(task) = self.task.as_ref() {
            task.abort();
        }

        Ok(())
    }

    async fn wait(&mut self) -> Result<Vec<u8>, Report<TaskError>> {
        let Some(task) = self.task.take() else {
            return Ok(Vec::new());
        };

        let result = task.await;
        let result = match &result {
            Ok(Ok(r)) => WorkerResult::Ok(r),
            Ok(Err(e)) => WorkerResult::Err(WorkerError::from_error(e.retryable(), e)),
            Err(e) => WorkerResult::Err(WorkerError::from_error(false, e)),
        };

        let result = serde_json::to_vec(&result).change_context(TaskError::Failed(true))?;

        Ok(result)
    }
}

enum OutputCollectorOp<T: Clone> {
    Read(oneshot::Sender<HashMap<String, T>>),
    Write((String, T)),
    Clear,
}

/// Collect output from spawned inprocess tasks. This uses unwrap and should only be used inside
/// tests. All clones of an OutputCollector will refer to the same internal collection.
#[derive(Clone)]
pub struct OutputCollector<T: Clone + Send + 'static> {
    tx: flume::Sender<OutputCollectorOp<T>>,
}

impl<T: Clone + Send + 'static> OutputCollector<T> {
    pub fn new() -> OutputCollector<T> {
        let (tx, rx) = flume::bounded(50);

        std::thread::spawn(move || Self::manager_task(rx));

        OutputCollector { tx }
    }

    pub fn write(&self, key: String, contents: T) {
        self.tx
            .send(OutputCollectorOp::Write((key, contents)))
            .unwrap();
    }

    pub fn clear(&self) {
        self.tx.send(OutputCollectorOp::Clear).unwrap();
    }

    pub async fn read(&self) -> HashMap<String, T> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(OutputCollectorOp::Read(tx)).unwrap();
        rx.await.unwrap()
    }

    fn manager_task(rx: flume::Receiver<OutputCollectorOp<T>>) {
        let mut contents = HashMap::default();

        while let Ok(op) = rx.recv() {
            match op {
                OutputCollectorOp::Read(tx) => {
                    tx.send(contents.clone()).ok();
                }
                OutputCollectorOp::Write((key, value)) => {
                    contents.insert(key, value);
                }
                OutputCollectorOp::Clear => {
                    contents.clear();
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use futures::{StreamExt, TryStreamExt};

    use super::*;
    use crate::spawn::Spawner;

    #[tokio::test]
    async fn output_collector() {
        let collector = super::OutputCollector::new();
        let cloned = collector.clone();
        collector.write("foo".to_string(), "bar".to_string());
        collector.write("bax".to_string(), "bah".to_string());
        let read = collector.read().await;
        assert_eq!(read.get("foo"), Some(&"bar".to_string()));
        assert_eq!(read.get("bax"), Some(&"bah".to_string()));
        assert_eq!(read.get("boo"), None);

        // Make sure that the cloned version actually references the same hashmap and has all the
        // writes.
        let read_clone = cloned.read().await;
        assert_eq!(read_clone.get("foo"), Some(&"bar".to_string()));
        assert_eq!(read_clone.get("bax"), Some(&"bah".to_string()));
        assert_eq!(read_clone.get("boo"), None);
    }

    #[tokio::test]
    async fn spawner() {
        let spawner = super::InProcessSpawner::new(|info| async move {
            Ok(format!("result {}", info.task_id.task))
        });

        let tasks = futures::stream::iter(1..=3)
            .map(Ok)
            .and_then(|i| {
                spawner.spawn(
                    SubtaskId {
                        stage: 0,
                        task: i,
                        try_num: 0,
                    },
                    "map".into(),
                    vec![],
                )
            })
            .try_collect::<Vec<_>>()
            .await
            .expect("Creating tasks");

        let mut outputs = std::collections::HashMap::new();
        for mut task in tasks {
            let output = String::from_utf8(task.wait().await.expect("Waiting for task"))
                .expect("decoding string");
            outputs.insert(task.task_id.to_string(), output);
        }

        println!("output: {:?}", outputs);
        assert_eq!(
            outputs.get("000-00001-00"),
            Some(&r##"{"type":"ok","data":"result 1"}"##.to_string())
        );
        assert_eq!(
            outputs.get("000-00002-00"),
            Some(&r##"{"type":"ok","data":"result 2"}"##.to_string())
        );
        assert_eq!(
            outputs.get("000-00003-00"),
            Some(&r##"{"type":"ok","data":"result 3"}"##.to_string())
        );
    }
}
