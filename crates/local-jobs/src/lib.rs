#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]

//! Spawn Smelter jobs as local tasks. This is mostly useful for testing and development.

use std::path::PathBuf;

use error_stack::{Report, ResultExt};
use smelter_job_manager::TaskError;
use smelter_worker::{WorkerInput, WorkerResult};

pub mod spawner;

/// The input and output locations of a task's data. A task can use this to read and write
/// to the proper locations.
pub struct LocalWorkerInfo {
    /// Where the task's input data should be found.
    pub input: PathBuf,
    /// Where the task should place its output data.
    pub output: PathBuf,
}

impl LocalWorkerInfo {
    /// Read the LocalWorkerInfo from the environment
    pub fn from_env() -> Result<Self, Report<TaskError>> {
        let input = std::env::var("INPUT_FILE").change_context(TaskError::Failed(false))?;
        let output = std::env::var("OUTPUT_FILE").change_context(TaskError::Failed(false))?;

        Ok(LocalWorkerInfo {
            input: PathBuf::from(input),
            output: PathBuf::from(output),
        })
    }

    /// Read the input file. You can also read manually from [`input`] if it makes
    /// sense for your task.
    pub async fn read_input<PAYLOAD: serde::de::DeserializeOwned + Send + 'static>(
        &self,
    ) -> Result<WorkerInput<PAYLOAD>, Report<TaskError>> {
        let input = self.input.clone();
        let worker_input = tokio::task::spawn_blocking(move || {
            let file = std::fs::File::open(input).change_context(TaskError::Failed(true))?;
            let file = std::io::BufReader::new(file);

            serde_json::from_reader::<_, WorkerInput<PAYLOAD>>(file)
                .change_context(TaskError::Failed(true))
        })
        .await
        .change_context(TaskError::Failed(true))??;

        worker_input.propagate_tracing_context();
        Ok(worker_input)
    }

    /// Write the output file. You can also write manually to [`output`] if it makes
    /// sense for your task.
    pub async fn write_output<DATA>(
        &self,
        result: impl Into<WorkerResult<DATA>>,
    ) -> Result<(), Report<TaskError>>
    where
        DATA: serde::Serialize + Send + 'static,
    {
        let result = result.into();
        let output = self.output.clone();
        tokio::task::spawn_blocking(move || {
            let file = std::fs::File::create(output).change_context(TaskError::Failed(true))?;
            let mut file = std::io::BufWriter::new(file);
            serde_json::to_writer(&mut file, &result).change_context(TaskError::Failed(true))
        })
        .await
        .change_context(TaskError::Failed(true))?
    }
}
