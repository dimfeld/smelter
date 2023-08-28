//! Run workers as normal processes on the local system. This is generally only useful for
//! development and testing.

use std::{borrow::Cow, path::PathBuf, process::ExitStatus};

use error_stack::{Report, ResultExt};
use serde::Serialize;
use smelter_job_manager::{SpawnedTask, Spawner, TaskError};
use smelter_worker::{SubtaskId, WorkerInput};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Default)]
pub struct LocalSpawner {
    pub shell: bool,
    pub tmpdir: Option<PathBuf>,
}

impl LocalSpawner {
    pub async fn spawn_command(
        &self,
        task_id: SubtaskId,
        mut command: tokio::process::Command,
        input: impl Serialize + Send,
    ) -> Result<LocalSpawnedTask, Report<TaskError>> {
        let dir = self
            .tmpdir
            .as_deref()
            .map(Cow::from)
            .unwrap_or_else(|| Cow::from(std::env::temp_dir()));

        let input_path = dir.join(format!("{task_id}-input.json"));
        let output_path = dir.join(format!("{task_id}-output.json"));

        let mut input_file = tokio::fs::File::create(&input_path)
            .await
            .change_context(TaskError::DidNotStart(false))
            .attach_printable("Failed to create temporary directory for input")?;

        let input = serde_json::to_vec(&WorkerInput::new(task_id, input))
            .change_context(TaskError::TaskGenerationFailed)?;

        input_file
            .write_all(&input)
            .await
            .change_context(TaskError::DidNotStart(false))
            .attach_printable("Failed to write input definition")?;

        input_file
            .flush()
            .await
            .change_context(TaskError::DidNotStart(false))
            .attach_printable("Failed to write input definition")?;

        let child_process = command
            .env("INPUT_FILE", &input_path)
            .env("OUTPUT_FILE", &output_path)
            .spawn()
            .change_context(TaskError::DidNotStart(false))?;

        Ok(LocalSpawnedTask {
            input_path,
            output_path,
            child_process,
            persist: false,
        })
    }
}

#[async_trait::async_trait]
impl Spawner for LocalSpawner {
    type SpawnedTask = LocalSpawnedTask;

    async fn spawn(
        &self,
        task_id: SubtaskId,
        task_name: Cow<'static, str>,
        input: impl Serialize + Send,
    ) -> Result<Self::SpawnedTask, Report<TaskError>> {
        let (exec_name, args) = if self.shell {
            ("sh", vec!["-c", task_name.as_ref()])
        } else {
            (task_name.as_ref(), vec![])
        };

        let mut command = tokio::process::Command::new(exec_name);
        command.args(args);

        self.spawn_command(task_id, command, input).await
    }
}

pub struct LocalSpawnedTask {
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

    fn handle_exit_status(status: ExitStatus) -> Result<(), Report<TaskError>> {
        if status.success() {
            Ok(())
        } else {
            let retryable = status.code().unwrap_or(-1) == 2;
            Err(TaskError::Failed(retryable)).attach_printable_lazy(|| {
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
            .ok_or(TaskError::Lost)
    }

    async fn kill(&mut self) -> Result<(), Report<TaskError>> {
        self.child_process
            .kill()
            .await
            .change_context(TaskError::Failed(false))
    }

    async fn wait(&mut self) -> Result<Vec<u8>, Report<TaskError>> {
        let result = self
            .child_process
            .wait()
            .await
            .change_context(TaskError::Lost)?;

        Self::handle_exit_status(result)?;

        let mut file = tokio::fs::File::open(&self.output_path)
            .await
            .change_context(TaskError::Failed(true))
            .attach_printable_lazy(|| self.output_path.display().to_string())?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)
            .await
            .change_context(TaskError::Failed(true))?;
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
    use super::*;

    #[tokio::test]
    async fn spawn() {
        let dir = tempfile::TempDir::new().expect("Creating temp dir");
        let spawner = LocalSpawner {
            shell: true,
            tmpdir: Some(dir.path().to_path_buf()),
        };
        let mut task = spawner
            .spawn(
                SubtaskId {
                    stage: 0,
                    task: 0,
                    try_num: 0,
                },
                Cow::from("cat $INPUT_FILE > $OUTPUT_FILE"),
                "test-output",
            )
            .await
            .expect("Spawning task");

        let output = task.wait().await.expect("Waiting for task");
        let output: WorkerInput<String> = serde_json::from_slice(&output).expect("Reading output");

        assert_eq!(output.input, "test-output");
    }
}
