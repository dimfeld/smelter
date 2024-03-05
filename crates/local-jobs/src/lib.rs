#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]

//! Spawn Smelter jobs as processes running on the same system.

use std::{fmt::Debug, path::PathBuf};

use error_stack::{Report, ResultExt};
use smelter_worker::{WorkerInputPayload, WorkerOutput, WrapperError};

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
    pub fn from_env() -> Result<Self, Report<WrapperError>> {
        let input = std::env::var("INPUT_FILE")
            .change_context(WrapperError::Initializing)
            .attach_printable("Missing INPUT_FILE environment variable")?;
        let output = std::env::var("OUTPUT_FILE")
            .change_context(WrapperError::Initializing)
            .attach_printable("Missing OUTPUT_FILE environment variable")?;

        Ok(LocalWorkerInfo {
            input: PathBuf::from(input),
            output: PathBuf::from(output),
        })
    }

    /// Read the input file. You can also read manually from [`input`] if it makes
    /// sense for your task.
    pub async fn read_input<PAYLOAD: serde::de::DeserializeOwned + Send + 'static>(
        &self,
    ) -> Result<WorkerInputPayload<PAYLOAD>, Report<WrapperError>> {
        let input = self.input.clone();
        let worker_input = tokio::task::spawn_blocking(move || {
            let file = std::fs::File::open(&input)
                .change_context(WrapperError::ReadInput)
                .attach_printable_lazy(|| input.display().to_string())?;
            let file = std::io::BufReader::new(file);

            serde_json::from_reader::<_, WorkerInputPayload<PAYLOAD>>(file)
                .change_context(WrapperError::UnexpectedInput)
        })
        .await
        .change_context(WrapperError::ReadInput)??;

        worker_input.propagate_tracing_context();
        Ok(worker_input)
    }

    /// Write the output file. You can also write manually to [`output`] if it makes
    /// sense for your task.
    pub async fn write_output<DATA: Debug>(
        &self,
        output: WorkerOutput<DATA>,
    ) -> Result<(), Report<WrapperError>>
    where
        DATA: serde::Serialize + Send + 'static,
    {
        let output_path = self.output.clone();
        tokio::task::spawn_blocking(move || {
            let file = std::fs::File::create(&output_path)
                .change_context(WrapperError::WriteOutput)
                .attach_printable_lazy(|| output_path.display().to_string())?;
            let mut file = std::io::BufWriter::new(file);
            serde_json::to_writer(&mut file, &output).change_context(WrapperError::WriteOutput)
        })
        .await
        .change_context(WrapperError::WriteOutput)?
    }
}
