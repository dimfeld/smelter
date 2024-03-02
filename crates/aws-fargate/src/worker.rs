use std::{collections::HashMap, fmt::Debug, future::Future};

use aws_sdk_s3::primitives::ByteStream;
use error_stack::{Report, ResultExt};
use serde::{de::DeserializeOwned, Serialize};
use smelter_worker::{
    propagate_tracing_context, SubtaskId, WorkerError, WorkerInput, WorkerResult,
};
use thiserror::Error;
use tracing::{event, Level};

use crate::{AwsError, INPUT_LOCATION_VAR, OTEL_CONTEXT_VAR, OUTPUT_LOCATION_VAR, SUBTASK_ID_VAR};

#[derive(Debug, Error)]
pub enum FargateWorkerError {
    #[error("Error initializing worker")]
    Initializing,
    #[error("Failed to read input payload")]
    ReadInput,
    #[error("Failed to write output payload")]
    WriteOutput,
}

impl FargateWorkerError {
    pub fn retryable(&self) -> bool {
        match self {
            FargateWorkerError::Initializing => false,
            FargateWorkerError::ReadInput => true,
            FargateWorkerError::WriteOutput => true,
        }
    }
}

pub struct FargateWorker {
    pub input_path: Option<(String, String)>,
    pub output_path: (String, String),

    pub task_id: SubtaskId,
    #[cfg(feature = "opentelemetry")]
    pub trace_context: HashMap<String, String>,

    s3_client: aws_sdk_s3::Client,
}

impl Debug for FargateWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FargateWorker")
            .field("input_path", &self.input_path)
            .field("output_path", &self.output_path)
            .field("task_id", &self.task_id)
            .finish()
    }
}

impl FargateWorker {
    /// Read the container environment to initialize tracing context.
    pub fn from_env(s3_client: aws_sdk_s3::Client) -> Result<Self, Report<FargateWorkerError>> {
        let input_path = std::env::var(INPUT_LOCATION_VAR)
            .ok()
            .and_then(|input| crate::parse_s3_url(&input));

        let output = std::env::var(OUTPUT_LOCATION_VAR)
            .change_context(FargateWorkerError::Initializing)
            .attach_printable("Missing OUTPUT_FILE environment variable")?;

        let task_id = std::env::var(SUBTASK_ID_VAR)
            .change_context(FargateWorkerError::Initializing)
            .attach_printable("Missing SMELTER_TASK_ID environment variable")?;

        let task_id: SubtaskId = serde_json::from_str(&task_id)
            .change_context(FargateWorkerError::Initializing)
            .attach_printable("Invalid Subtask ID in SMELTER_TASK_ID environment variable")?;

        #[cfg(feature = "opentelemetry")]
        let trace_context = std::env::var(OTEL_CONTEXT_VAR)
            .ok()
            .and_then(|trace_context| serde_json::from_str(&trace_context).ok())
            .unwrap_or_default();

        Ok(FargateWorker {
            input_path,
            output_path: crate::parse_s3_url(&output)
                .ok_or(FargateWorkerError::Initializing)
                .attach_printable("Failed to parse OUTPUT_FILE as S3 URL")?,
            task_id,
            #[cfg(feature = "opentelemetry")]
            trace_context,
            s3_client,
        })
    }

    /// When the task does not require a payload, use this function to skip trying to read it,
    /// while still retrieving the relevant metadata and tracing context.
    pub fn initialize_without_payload(&self) -> WorkerInput<()> {
        let input = WorkerInput {
            task_id: self.task_id,
            trace_context: self.trace_context.clone(),
            input: (),
        };

        input.propagate_tracing_context();
        input
    }

    /// Read the input payload and initialize tracing context.
    pub async fn read_input<PAYLOAD: DeserializeOwned + Send + 'static>(
        &self,
    ) -> Result<WorkerInput<PAYLOAD>, Report<FargateWorkerError>> {
        #[cfg(feature = "opentelemetry")]
        propagate_tracing_context(&self.trace_context);

        let (bucket, path) = self
            .input_path
            .as_ref()
            .ok_or(FargateWorkerError::ReadInput)
            .attach_printable("Missing INPUT_FILE environment variable")?;

        let body = self
            .s3_client
            .get_object()
            .bucket(bucket)
            .key(path)
            .send()
            .await
            .map_err(AwsError::from)
            .change_context(FargateWorkerError::ReadInput)
            .attach_printable("Fetching input payload")?
            .body
            .collect()
            .await
            .change_context(FargateWorkerError::ReadInput)
            .attach_printable("Reading input payload")?
            .into_bytes();

        let payload = serde_json::from_slice(body.as_ref())
            .change_context(FargateWorkerError::ReadInput)
            .attach_printable("Deserializing input payload")?;

        let input = WorkerInput {
            task_id: self.task_id,
            #[cfg(feature = "opentelemetry")]
            trace_context: self.trace_context.clone(),
            input: payload,
        };

        Ok(input)
    }

    /// Write the worker result to a location that the spawner expects to find it.
    pub async fn write_output<DATA: Debug>(
        &self,
        result: impl Into<WorkerResult<DATA>>,
    ) -> Result<(), Report<FargateWorkerError>>
    where
        DATA: serde::Serialize + Send + 'static,
    {
        let (bucket, path) = &self.output_path;

        let body = serde_json::to_vec(&result.into())
            .change_context(FargateWorkerError::WriteOutput)
            .attach_printable("Serializing output payload")?;

        self.s3_client
            .put_object()
            .bucket(bucket)
            .key(path)
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(AwsError::from)
            .change_context(FargateWorkerError::WriteOutput)
            .attach_printable("Writing output payload")?;

        Ok(())
    }
}

/// Run a worker that doesn't expect any input payload, and record the output for the job spawner to retrieve.
pub async fn run_worker_without_input<F, FUT, OUTPUT, ERR>(
    f: F,
) -> Result<(), Report<FargateWorkerError>>
where
    F: FnOnce(WorkerInput<()>) -> FUT,
    FUT: Future<Output = Result<OUTPUT, ERR>>,
    ERR: std::error::Error,
    OUTPUT: Debug + Serialize + Send + 'static,
{
    let config = aws_config::load_from_env().await;
    let worker = FargateWorker::from_env(aws_sdk_s3::Client::new(&config))?;

    let input = worker.initialize_without_payload();
    let result = f(input).await;
    worker.write_output(result).await?;

    Ok(())
}

/// Run the worker and record the output for the job spawner to retrieve.
pub async fn run_worker<INPUT, F, FUT, OUTPUT, ERR>(f: F) -> Result<(), Report<FargateWorkerError>>
where
    F: FnOnce(WorkerInput<INPUT>) -> FUT,
    FUT: Future<Output = Result<OUTPUT, ERR>>,
    ERR: std::error::Error,
    INPUT: Debug + DeserializeOwned + Send + 'static,
    OUTPUT: Debug + Serialize + Send + 'static,
{
    let config = aws_config::load_from_env().await;
    let worker = trace_result(
        "Initializing",
        FargateWorker::from_env(aws_sdk_s3::Client::new(&config)),
    )?;

    let input = match trace_result("reading input", worker.read_input().await) {
        Ok(input) => input,
        Err(e) => {
            let retryable = e.current_context().retryable();
            let error = WorkerError::from_error(retryable, e);
            worker
                .write_output::<OUTPUT>(WorkerResult::Err(error))
                .await?;
            return Ok(());
        }
    };

    let result = trace_result("running worker", f(input).await);
    trace_result("writing output", worker.write_output(result).await)?;

    Ok(())
}

fn trace_result<R: Debug, E: Debug>(message: &str, result: Result<R, E>) -> Result<R, E> {
    match result {
        Ok(o) => {
            event!(Level::INFO, output=?o, "{}", message);
            Ok(o)
        }
        Err(e) => {
            event!(Level::ERROR, error=?e, "{}", message);
            Err(e)
        }
    }
}
