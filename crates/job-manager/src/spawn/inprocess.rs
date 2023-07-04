//! Run workers in the same process as the scheduler. This is only really useful for some unit
//! tests.

use ahash::{HashMap, HashMapExt};
use async_trait::async_trait;
use error_stack::{IntoReport, Report, ResultExt};
use std::future::Future;
use tokio::{sync::oneshot, task::JoinHandle};

use super::{SpawnedTask, Spawner, TaskError};

pub struct InProcessTaskInfo {
    pub input_value: Vec<u8>,
}

pub struct InProcessSpawner<F, FUNC, RESULT>
where
    F: Future<Output = Result<RESULT, TaskError>>,
    FUNC: FnOnce(InProcessTaskInfo) -> F + Send + Sync + Clone + 'static,
    RESULT: Clone + Send + 'static,
{
    task_fn: FUNC,
    pub output: OutputCollector<RESULT>,
}

impl<F, FUNC, RESULT> InProcessSpawner<F, FUNC, RESULT>
where
    F: Future<Output = Result<RESULT, TaskError>>,
    FUNC: FnOnce(InProcessTaskInfo) -> F + Send + Sync + Clone + 'static,
    RESULT: Clone + Send + 'static,
{
    pub fn new(task_fn: FUNC) -> Self {
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
        local_id: &str,
        task_name: &str,
        input: &[u8],
    ) -> Result<Self::SpawnedTask, Report<TaskError>> {
        let output_location = format!("{task_name}_{local_id}");
        let output = self.output.clone();
        let task_fn = self.task_fn.clone();
        let input = input.to_vec();
        let task = InProcessSpawnedTask {
            task_id: local_id.to_string(),
            output_location: output_location.clone(),
            task: Some(tokio::task::spawn(async move {
                let result = (task_fn)(InProcessTaskInfo { input_value: input }).await?;

                output.write(output_location, result);
                Ok::<(), TaskError>(())
            })),
        };

        Ok(task)
    }
}

pub struct InProcessSpawnedTask {
    task_id: String,
    output_location: String,
    task: Option<JoinHandle<Result<(), TaskError>>>,
}

#[async_trait]
impl SpawnedTask for InProcessSpawnedTask {
    async fn runtime_id(&self) -> Result<String, TaskError> {
        Ok(self.task_id.clone())
    }

    async fn kill(&mut self) -> Result<(), Report<TaskError>> {
        if let Some(task) = self.task.as_ref() {
            task.abort();
        }

        Ok(())
    }

    async fn check_finished(&mut self) -> Result<bool, Report<TaskError>> {
        Ok(self.task.as_ref().map(|m| m.is_finished()).unwrap_or(true))
    }

    async fn wait(&mut self) -> Result<(), Report<TaskError>> {
        let Some(task) = self.task.take() else {
            return Ok(());
        };

        task.await
            .into_report()
            .change_context(TaskError::Failed)?
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
