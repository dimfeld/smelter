//! Run workers as normal processes on the local system. This is generally only useful for
//! development and testing.

use std::{borrow::Cow, path::PathBuf, process::ExitStatus};

use error_stack::{Report, ResultExt};
use smelter_job_manager::{SpawnedTask, Spawner, SubtaskId, TaskError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Default)]
pub struct LocalSpawner {
    shell: bool,
    tmpdir: Option<PathBuf>,
}

#[async_trait::async_trait]
impl Spawner for LocalSpawner {
    type SpawnedTask = LocalSpawnedTask;

    async fn spawn(
        &self,
        task_id: SubtaskId,
        task_name: Cow<'static, str>,
        input: Vec<u8>,
    ) -> Result<Self::SpawnedTask, Report<TaskError>> {
        let dir = self
            .tmpdir
            .as_deref()
            .map(Cow::from)
            .unwrap_or_else(|| Cow::from(std::env::temp_dir()));

        let input_path = dir.join(format!("{task_id}-input.json"));
        let output_path = dir.join(format!("{task_id}-output"));

        let mut input_file = tokio::fs::File::create(&input_path)
            .await
            .change_context(TaskError::DidNotStart(false))
            .attach_printable("Failed to create temporary directory for input")?;

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

        let (exec_name, args) = if self.shell {
            ("sh", vec!["-c", task_name.as_ref()])
        } else {
            (task_name.as_ref(), vec![])
        };

        let child_process = tokio::process::Command::new(exec_name)
            .args(args)
            .env("INPUT_FILE", &input_path)
            .env("OUTPUT_FILE", &output_path)
            .spawn()
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
        tokio::spawn(tokio::fs::remove_file(self.input_path.clone()));
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
                "test-output".as_bytes().to_vec(),
            )
            .await
            .expect("Spawning task");

        let output = task.wait().await.expect("Waiting for task");

        assert_eq!(
            String::from_utf8(output).expect("reading output to string"),
            "test-output"
        );
    }
}
