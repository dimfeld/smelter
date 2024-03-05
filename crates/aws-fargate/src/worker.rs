//! Worker-side code to run from inside a Fargate container
use std::{collections::HashMap, fmt::Debug, future::Future};

use aws_sdk_s3::primitives::ByteStream;
use error_stack::{Report, ResultExt};
use serde::{de::DeserializeOwned, Serialize};
use smelter_worker::{
    propagate_tracing_context, trace_result, SubtaskId, WorkerError, WorkerInput,
    WorkerInputPayload, WorkerOutput, WorkerResult, WrapperError,
};

use crate::{AwsError, INPUT_LOCATION_VAR, OTEL_CONTEXT_VAR, OUTPUT_LOCATION_VAR, SUBTASK_ID_VAR};

/// A convenience function to create an S3 client
pub async fn create_s3_client() -> aws_sdk_s3::Client {
    aws_sdk_s3::Client::new(&aws_config::load_from_env().await)
}

/// The [FargateWorker] manages error handling, payload I/O, and other Smelter-specfic framework
/// tasks.
pub struct FargateWorker {
    /// Where to read the input payload from
    pub input_path: Option<(String, String)>,
    /// Where to write the output payload
    pub output_path: (String, String),

    /// The Smelter task ID for this task
    pub task_id: SubtaskId,
    #[cfg(feature = "opentelemetry")]
    /// OpenTelemetry trace context
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
    pub fn from_env(s3_client: aws_sdk_s3::Client) -> Result<Self, Report<WrapperError>> {
        let input_path = std::env::var(INPUT_LOCATION_VAR)
            .ok()
            .and_then(|input| crate::parse_s3_url(&input));

        let output = std::env::var(OUTPUT_LOCATION_VAR)
            .change_context(WrapperError::Initializing)
            .attach_printable("Missing OUTPUT_FILE environment variable")?;

        let task_id = std::env::var(SUBTASK_ID_VAR)
            .change_context(WrapperError::Initializing)
            .attach_printable("Missing SMELTER_TASK_ID environment variable")?;

        let task_id: SubtaskId = serde_json::from_str(&task_id)
            .change_context(WrapperError::Initializing)
            .attach_printable("Invalid Subtask ID in SMELTER_TASK_ID environment variable")?;

        #[cfg(feature = "opentelemetry")]
        let trace_context = std::env::var(OTEL_CONTEXT_VAR)
            .ok()
            .and_then(|trace_context| serde_json::from_str(&trace_context).ok())
            .unwrap_or_default();

        Ok(FargateWorker {
            input_path,
            output_path: crate::parse_s3_url(&output)
                .ok_or(WrapperError::Initializing)
                .attach_printable("Failed to parse OUTPUT_FILE as S3 URL")?,
            task_id,
            #[cfg(feature = "opentelemetry")]
            trace_context,
            s3_client,
        })
    }

    /// Run the worker and record the output for the job spawner to retrieve.
    pub async fn run<INPUT, F, FUT, OUTPUT, ERR>(
        &self,
        input: WorkerInputPayload<INPUT>,
        f: F,
    ) -> Result<(), Report<WrapperError>>
    where
        F: FnOnce(WorkerInput<INPUT>) -> FUT,
        FUT: Future<Output = Result<OUTPUT, ERR>>,
        ERR: std::error::Error,
        INPUT: Debug + DeserializeOwned + Send + 'static,
        OUTPUT: Debug + Serialize + Send + 'static,
    {
        let result = smelter_worker::run_worker(input, f).await?;
        trace_result("writing output", self.write_output(result).await)?;
        Ok(())
    }

    /// Read the input payload. If it fails to read, this function will attempt to write an error
    /// to the output location before returning an error.
    pub async fn read_input<PAYLOAD: Debug + DeserializeOwned + Send + 'static>(
        &self,
    ) -> Result<WorkerInputPayload<PAYLOAD>, Report<WrapperError>> {
        let result = trace_result("reading input", self.read_input_internal::<PAYLOAD>().await);
        if let Err(e) = &result {
            let retryable = e.current_context().retryable();
            let error = WorkerError::from_error(retryable, e);
            self.write_output::<()>(WorkerOutput {
                result: WorkerResult::Err(error),
                #[cfg(feature = "stats")]
                stats: None,
            })
            .await?;
        }

        result
    }

    /// Read the input payload and initialize tracing context.
    async fn read_input_internal<PAYLOAD: DeserializeOwned + Send + 'static>(
        &self,
    ) -> Result<WorkerInputPayload<PAYLOAD>, Report<WrapperError>> {
        #[cfg(feature = "opentelemetry")]
        propagate_tracing_context(&self.trace_context);

        let (bucket, path) = self
            .input_path
            .as_ref()
            .ok_or(WrapperError::ReadInput)
            .attach_printable("Missing INPUT_FILE environment variable")?;

        let body = self
            .s3_client
            .get_object()
            .bucket(bucket)
            .key(path)
            .send()
            .await
            .map_err(AwsError::from)
            .change_context(WrapperError::ReadInput)
            .attach_printable("Fetching input payload")?
            .body
            .collect()
            .await
            .change_context(WrapperError::ReadInput)
            .attach_printable("Reading input payload")?
            .into_bytes();

        let payload = serde_json::from_slice(body.as_ref())
            .change_context(WrapperError::UnexpectedInput)
            .attach_printable("Deserializing input payload")?;

        let input = WorkerInputPayload {
            task_id: self.task_id,
            #[cfg(feature = "opentelemetry")]
            trace_context: self.trace_context.clone(),
            input: payload,
        };

        Ok(input)
    }

    /// When the task does not require a payload, use this function to skip trying to read it,
    /// while still retrieving the relevant metadata and tracing context.
    pub fn initialize_without_payload(&self) -> WorkerInputPayload<()> {
        let input = WorkerInputPayload {
            task_id: self.task_id,
            trace_context: self.trace_context.clone(),
            input: (),
        };

        input.propagate_tracing_context();
        input
    }

    /// Write the worker result to a location that the spawner expects to find it.
    pub async fn write_output<DATA: Debug>(
        &self,
        result: WorkerOutput<DATA>,
    ) -> Result<(), Report<WrapperError>>
    where
        DATA: serde::Serialize + Send + 'static,
    {
        let (bucket, path) = &self.output_path;

        let body = serde_json::to_vec(&result)
            .change_context(WrapperError::WriteOutput)
            .attach_printable("Serializing output payload")?;

        self.s3_client
            .put_object()
            .bucket(bucket)
            .key(path)
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(AwsError::from)
            .change_context(WrapperError::WriteOutput)
            .attach_printable("Writing output payload")?;

        Ok(())
    }
}
