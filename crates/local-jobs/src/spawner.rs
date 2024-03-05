//! Run workers as normal processes on the local system. This is generally only useful for
//! development and testing.

use std::{
    borrow::Cow,
    path::PathBuf,
    process::{ExitStatus, Stdio},
};

use error_stack::{Report, ResultExt};
use futures::stream::TryStreamExt;
use rand::Rng;
use serde::Serialize;
use smelter_job_manager::{LogSender, SpawnedTask, TaskError};
use smelter_worker::{SubtaskId, WorkerInputPayload};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio_stream::wrappers::LinesStream;

/// Spawn a local job
#[derive(Default)]
pub struct LocalSpawner {
    /// A temporary directory to use instead of the system default.
    pub tmpdir: Option<PathBuf>,
}

impl LocalSpawner {
    /// Spawn the provided command.
    pub async fn spawn(
        &self,
        task_id: SubtaskId,
        log_collector: Option<LogSender>,
        mut command: tokio::process::Command,
        input: impl Serialize + Send,
    ) -> Result<LocalSpawnedTask, Report<TaskError>> {
        let dir = self
            .tmpdir
            .as_deref()
            .map(Cow::from)
            .unwrap_or_else(|| Cow::from(std::env::temp_dir()));

        let prefix: usize = rand::thread_rng().gen();
        let input_path = dir.join(format!("{prefix}-{task_id}-input.json"));
        let output_path = dir.join(format!("{prefix}-{task_id}-output.json"));

        let mut input_file = tokio::fs::File::create(&input_path)
            .await
            .change_context(TaskError::did_not_start(task_id, false))
            .attach_printable("Failed to create temporary directory for input")?;

        let input = serde_json::to_vec(&WorkerInputPayload::new(task_id, input))
            .change_context(TaskError::task_generation_failed(task_id))?;

        input_file
            .write_all(&input)
            .await
            .change_context(TaskError::did_not_start(task_id, false))
            .attach_printable("Failed to write input definition")?;

        input_file
            .flush()
            .await
            .change_context(TaskError::did_not_start(task_id, false))
            .attach_printable("Failed to write input definition")?;

        if log_collector.is_some() {
            command.stdout(Stdio::piped()).stderr(Stdio::piped());
        }

        let mut child_process = command
            .env("INPUT_FILE", &input_path)
            .env("OUTPUT_FILE", &output_path)
            .spawn()
            .change_context(TaskError::did_not_start(task_id, false))?;

        if let Some(collector) = log_collector {
            {
                let stdout = child_process.stdout.take().unwrap();
                let stdout_reader = tokio::io::BufReader::new(stdout);
                let collector = collector.clone();
                tokio::task::spawn(async move {
                    let mut stdout_lines = LinesStream::new(stdout_reader.lines());

                    while let Ok(Some(line)) = stdout_lines.try_next().await {
                        collector.send(true, line);
                    }
                });
            }

            {
                let stderr = child_process.stderr.take().unwrap();
                let stderr_reader = tokio::io::BufReader::new(stderr);
                tokio::task::spawn(async move {
                    let mut stderr_lines = LinesStream::new(stderr_reader.lines());

                    while let Ok(Some(line)) = stderr_lines.try_next().await {
                        collector.send(false, line);
                    }
                });
            }
        }

        Ok(LocalSpawnedTask {
            task_id,
            input_path,
            output_path,
            child_process,
            persist: false,
        })
    }
}

/// A handled to a task spawned by a [LocalSpawner]
pub struct LocalSpawnedTask {
    task_id: SubtaskId,
    input_path: PathBuf,
    output_path: PathBuf,
    child_process: tokio::process::Child,
    persist: bool,
}

impl LocalSpawnedTask {
    /// If called, the input and output files for the task will not be deleted when the task is
    /// done.
    pub fn persist(&mut self) {
        self.persist = true;
    }

    fn handle_exit_status(&self, status: ExitStatus) -> Result<(), Report<TaskError>> {
        if status.success() {
            Ok(())
        } else {
            let retryable = status.code().unwrap_or(-1) == 2;
            Err(TaskError::failed(self.task_id, retryable)).attach_printable_lazy(|| {
                format!(
                    "Task failed with code {code}",
                    code = status.code().unwrap_or(-1)
                )
            })
        }
    }
}

#[async_trait::async_trait]
impl SpawnedTask for LocalSpawnedTask {
    async fn runtime_id(&self) -> Result<String, TaskError> {
        self.child_process
            .id()
            .map(|id| id.to_string())
            .ok_or(TaskError::lost(self.task_id))
    }

    async fn kill(&mut self) -> Result<(), Report<TaskError>> {
        self.child_process
            .kill()
            .await
            .change_context(TaskError::failed(self.task_id, false))
    }

    async fn wait(&mut self) -> Result<Vec<u8>, Report<TaskError>> {
        let result = self
            .child_process
            .wait()
            .await
            .change_context(TaskError::lost(self.task_id))?;

        self.handle_exit_status(result)?;

        let mut file = tokio::fs::File::open(&self.output_path)
            .await
            .change_context(TaskError::failed(self.task_id, true))
            .attach_printable_lazy(|| self.output_path.display().to_string())?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)
            .await
            .change_context(TaskError::failed(self.task_id, true))?;
        Ok(data)
    }
}

impl Drop for LocalSpawnedTask {
    fn drop(&mut self) {
        let input_path = self.input_path.clone();
        let output_path = self.output_path.clone();
        tokio::spawn(async move {
            tokio::fs::remove_file(input_path).await.ok();
            tokio::fs::remove_file(output_path).await.ok();
        });
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn spawn() {
        let dir = tempfile::TempDir::new().expect("Creating temp dir");
        let spawner = LocalSpawner {
            tmpdir: Some(dir.path().to_path_buf()),
        };
        let task_id = SubtaskId {
            job: Uuid::nil(),
            stage: 0,
            task: 0,
            try_num: 0,
        };

        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c").arg("cat $INPUT_FILE > $OUTPUT_FILE");
        let mut task = spawner
            .spawn(task_id, None, cmd, "test-output")
            .await
            .expect("Spawning task");

        let output = task.wait().await.expect("Waiting for task");
        let output: WorkerInputPayload<String> =
            serde_json::from_slice(&output).expect("Reading output");

        assert_eq!(output.input, "test-output");
    }
}
