//! Run workers as normal processes on the local system. This is generally only useful for
//! development and testing.

use error_stack::{IntoReport, Report, ResultExt};
use std::{borrow::Cow, path::PathBuf, process::ExitStatus};
use tokio::io::AsyncWriteExt;

use crate::manager::SubtaskId;

use super::{SpawnedTask, Spawner, TaskError};

pub struct LocalSpawner {}

#[async_trait::async_trait]
impl Spawner for LocalSpawner {
    type SpawnedTask = LocalSpawnedTask;

    async fn spawn(
        &self,
        task_id: SubtaskId,
        task_name: Cow<'static, str>,
        input: Vec<u8>,
    ) -> Result<Self::SpawnedTask, Report<TaskError>> {
        let dir = std::env::temp_dir();

        let input_path = dir.join(format!("{task_id}-input.json"));
        let output_path = dir.join(format!("{task_id}-output"));

        let mut input_file = tokio::fs::File::create(&input_path)
            .await
            .into_report()
            .change_context(TaskError::DidNotStart(false))
            .attach_printable("Failed to create temporary directory for input")?;

        input_file
            .write_all(&input)
            .await
            .into_report()
            .change_context(TaskError::DidNotStart(false))
            .attach_printable("Failed to write input definition")?;

        input_file
            .flush()
            .await
            .into_report()
            .change_context(TaskError::DidNotStart(false))
            .attach_printable("Failed to write input definition")?;

        let child_process = tokio::process::Command::new(task_name.as_ref())
            .env("INPUT_FILE", &input_path)
            .env("OUTPUT_FILE", &output_path)
            .spawn()
            .into_report()
            .change_context(TaskError::DidNotStart(false))
            .attach_printable_lazy(|| format!("Failed to start worker process {task_name}"))?;

        Ok(LocalSpawnedTask {
            input_path,
            output_path,
            child_process,
        })
    }
}

pub struct LocalSpawnedTask {
    input_path: PathBuf,
    output_path: PathBuf,
    child_process: tokio::process::Child,
}

impl LocalSpawnedTask {
    fn handle_exit_status(status: ExitStatus) -> Result<(), Report<TaskError>> {
        if status.success() {
            Ok(())
        } else {
            let retryable = status.code().unwrap_or(-1) == 2;
            Err(TaskError::Failed(retryable))
                .into_report()
                .attach_printable_lazy(|| {
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
            .into_report()
            .change_context(TaskError::Failed(false))
    }

    async fn wait(&mut self) -> Result<(), Report<TaskError>> {
        let result = self
            .child_process
            .wait()
            .await
            .into_report()
            .change_context(TaskError::Lost)?;

        Self::handle_exit_status(result)
    }

    fn output_location(&self) -> String {
        self.output_path.to_string_lossy().to_string()
    }
}

impl Drop for LocalSpawnedTask {
    fn drop(&mut self) {
        tokio::spawn(tokio::fs::remove_file(self.input_path.clone()));
    }
}
