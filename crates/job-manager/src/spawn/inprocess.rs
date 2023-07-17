//! Run workers in the same process as the scheduler. This is only really useful for some unit
//! tests.

use ahash::HashMap;
use async_trait::async_trait;
use error_stack::{IntoReport, Report, ResultExt};
use std::{borrow::Cow, future::Future};
use tokio::{sync::oneshot, task::JoinHandle};

use crate::manager::SubtaskId;
#[cfg(test)]
use crate::test_util::setup_test_tracing;

use super::{SpawnedTask, Spawner, TaskError};

pub struct InProcessTaskInfo<'a> {
    pub task_name: String,
    pub task_id: SubtaskId,
    pub input_value: &'a [u8],
}

pub struct InProcessSpawner<F, FUNC, RESULT>
where
    F: Future<Output = Result<RESULT, TaskError>> + 'static,
    FUNC: FnOnce(InProcessTaskInfo) -> F + Send + Sync + Clone + 'static,
    RESULT: Clone + Send + 'static,
{
    task_fn: FUNC,
    pub output: OutputCollector<RESULT>,
}

impl<F, FUNC, RESULT> InProcessSpawner<F, FUNC, RESULT>
where
    F: Future<Output = Result<RESULT, TaskError>> + 'static,
    FUNC: FnOnce(InProcessTaskInfo) -> F + Send + Sync + Clone + 'static,
    RESULT: Clone + Send + 'static,
{
    pub fn new(task_fn: FUNC) -> Self {
        #[cfg(test)]
        setup_test_tracing();

        Self {
            task_fn,
            output: OutputCollector::new(),
        }
    }
}

#[async_trait]
impl<F, FUNC, RESULT> Spawner for InProcessSpawner<F, FUNC, RESULT>
where
    F: Future<Output = Result<RESULT, TaskError>> + Send + Sync,
    FUNC: FnOnce(InProcessTaskInfo) -> F + Send + Sync + Clone + 'static,
    RESULT: Clone + Send + 'static,
{
    type SpawnedTask = InProcessSpawnedTask;

    async fn spawn(
        &self,
        task_id: SubtaskId,
        task_name: Cow<'static, str>,
        input: Vec<u8>,
    ) -> Result<Self::SpawnedTask, Report<TaskError>> {
        let output_location = format!("{task_name}_{task_id}");
        let output = self.output.clone();
        let task_fn = self.task_fn.clone();
        let input = input.to_vec();
        let task = InProcessSpawnedTask {
            task_id,
            output_location: output_location.clone(),
            task: Some(tokio::task::spawn(async move {
                let result = (task_fn)(InProcessTaskInfo {
                    task_name: task_name.to_string(),
                    task_id,
                    input_value: &input,
                })
                .await?;

                output.write(output_location, result);
                Ok::<(), TaskError>(())
            })),
        };

        Ok(task)
    }
}

pub struct InProcessSpawnedTask {
    task_id: SubtaskId,
    output_location: String,
    task: Option<JoinHandle<Result<(), TaskError>>>,
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

    async fn wait(&mut self) -> Result<(), Report<TaskError>> {
        let Some(task) = self.task.take() else {
            return Ok(());
        };

        let result = task.await;
        let retryable = match &result {
            Ok(Ok(_)) => false,
            Ok(Err(e)) => e.retryable(),
            Err(_) => false,
        };

        result
            .into_report()
            .change_context(TaskError::Failed(retryable))?
            .into_report()?;

        Ok(())
    }

    fn output_location(&self) -> String {
        self.output_location.clone()
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

        for mut task in tasks {
            task.wait().await.expect("Waiting for task");
        }

        let output = spawner.output.read().await;
        assert_eq!(output.len(), 3);
        println!("output: {:?}", output);
        assert_eq!(
            output.get("map_000-00001-00"),
            Some(&"result 1".to_string())
        );
        assert_eq!(
            output.get("map_000-00002-00"),
            Some(&"result 2".to_string())
        );
        assert_eq!(
            output.get("map_000-00003-00"),
            Some(&"result 3".to_string())
        );
    }
}
