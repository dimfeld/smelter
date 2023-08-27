// #![warn(missing_docs)]
// #![warn(clippy::missing_docs_in_private_items)]

//! Spawn Smelter jobs as local tasks. This is mostly useful for testing and development.

use std::path::PathBuf;

use error_stack::{Report, ResultExt};
use smelter_job_manager::TaskError;
use smelter_worker::WorkerResult;

pub mod spawner;
pub mod worker;

pub struct LocalWorkerInfo {
    pub input: PathBuf,
    pub output: PathBuf,
}

impl LocalWorkerInfo {
    pub fn from_env() -> Result<Self, Report<TaskError>> {
        let input = std::env::var("INPUT_FILE").change_context(TaskError::Failed(false))?;
        let output = std::env::var("OUTPUT_FILE").change_context(TaskError::Failed(false))?;

        Ok(LocalWorkerInfo {
            input: PathBuf::from(input),
            output: PathBuf::from(output),
        })
    }

    pub async fn read_json_payload<PAYLOAD: serde::de::DeserializeOwned + Send + 'static>(
        &self,
    ) -> Result<PAYLOAD, Report<TaskError>> {
        let input = self.input.clone();
        tokio::task::spawn_blocking(move || {
            let file = std::fs::File::open(input).change_context(TaskError::Failed(true))?;
            let file = std::io::BufReader::new(file);

            serde_json::from_reader::<_, PAYLOAD>(file).change_context(TaskError::Failed(true))
        })
        .await
        .change_context(TaskError::Failed(true))?
    }

    pub async fn write_json_result<DATA>(
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
