use std::path::PathBuf;

use error_stack::{Report, ResultExt};
use smelter_job_manager::TaskError;

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

    pub async fn write_json_result(
        &self,
        data: impl serde::Serialize + Send + 'static,
    ) -> Result<(), Report<TaskError>> {
        let output = self.output.clone();
        tokio::task::spawn_blocking(move || {
            let file = std::fs::File::create(output).change_context(TaskError::Failed(true))?;
            let mut file = std::io::BufWriter::new(file);
            serde_json::to_writer(&mut file, &data).change_context(TaskError::Failed(true))
        })
        .await
        .change_context(TaskError::Failed(true))?
    }
}
