#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]

//! Common code used for communicating between the job manager and worker tasks.

use std::{collections::HashMap, fmt::Debug, io::Read};

#[cfg(feature = "opentelemetry")]
use opentelemetry::sdk::propagation::TraceContextPropagator;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[cfg(feature = "stats")]
pub mod stats;

/// The ID for a subtask, which uniquely identifies it within a [Job].
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubtaskId {
    /// The ID of the job.
    pub job: Uuid,
    /// Which stage the subtask is running on.
    pub stage: u16,
    /// The index of the task within that stage.
    pub task: u32,
    /// Which retry of this task is being executed.
    pub try_num: u16,
}

impl std::fmt::Display for SubtaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{:03}-{:05}-{:02}",
            self.job, self.stage, self.task, self.try_num
        )
    }
}

#[cfg(feature = "opentelemetry")]
/// Encode the current trace context so that it can be passed across process lines.
pub fn get_trace_context() -> HashMap<String, String> {
    use opentelemetry::propagation::TextMapPropagator;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let span = tracing::Span::current();
    let context = span.context();
    let propagator = TraceContextPropagator::new();
    let mut fields = HashMap::new();
    propagator.inject_context(&context, &mut fields);
    fields
}

/// If tracing is enabled, propagate the trace context from the spawner into the current span.
pub fn propagate_tracing_context(trace_context: &HashMap<String, String>) {
    #![allow(unused_variables)]
    #[cfg(feature = "opentelemetry")]
    {
        use opentelemetry::propagation::TextMapPropagator;
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        let propagator = TraceContextPropagator::new();
        let context = propagator.extract(trace_context);
        let span = tracing::Span::current();
        span.set_parent(context);
    }
}

#[derive(Debug)]
#[cfg_attr(feature = "worker-side", derive(Serialize))]
#[cfg_attr(feature = "spawner-side", derive(Deserialize))]
/// The input payload that a worker will read when starting
pub struct WorkerInput<T> {
    /// The ID for this task
    pub task_id: SubtaskId,
    #[cfg(feature = "opentelemetry")]
    #[serde(default)]
    /// Propragated trace context so that this worker can show up as a child of the parent span
    /// from the spawner.
    pub trace_context: std::collections::HashMap<String, String>,
    /// Worker-specific input data
    pub input: T,
}

#[cfg(feature = "spawner-side")]
impl<T> WorkerInput<T> {
    /// Create a new [WorkerInput]
    pub fn new(task_id: SubtaskId, input: T) -> Self {
        #[cfg(feature = "opentelemetry")]
        let trace_context = get_trace_context();

        Self {
            task_id,
            #[cfg(feature = "opentelemetry")]
            trace_context,
            input,
        }
    }
}

#[cfg(feature = "worker-side")]
impl<T: DeserializeOwned + 'static> WorkerInput<T> {
    /// Propagate the trace context from the spawner into the current span.
    pub fn propagate_tracing_context(&self) {
        #[cfg(feature = "opentelemetry")]
        {
            propagate_tracing_context(&self.trace_context);
        }
    }

    /// Parse a [WorkerInput] and propagate the trace context, if present.
    pub fn parse(input: impl Read) -> Result<Self, serde_json::Error> {
        let input: WorkerInput<T> = serde_json::from_reader(input)?;

        input.propagate_tracing_context();

        Ok(input)
    }
}

#[cfg_attr(feature = "worker-side", derive(Serialize))]
#[cfg_attr(feature = "spawner-side", derive(Deserialize))]
#[derive(Debug)]
/// A serializable error returned from a worker
pub struct WorkerError {
    /// Whether the task can be retried after this error, or not
    pub retryable: bool,
    /// The serialized error
    pub error: String,
}

impl WorkerError {
    /// Create a [WorkerError] from any [Error](std:error::Error).
    #[cfg(feature = "worker-side")]
    pub fn from_error(retryable: bool, error: impl Debug) -> Self {
        Self {
            retryable,
            error: format!("{:?}", error),
        }
    }
}

#[cfg(feature = "worker-side")]
impl<E: std::error::Error> From<E> for WorkerError {
    /// Convert any [Error](std::error::Error) into a [WorkerError].
    /// This sets [WorkerError#retryable] to `true`.
    fn from(e: E) -> Self {
        Self {
            retryable: true,
            error: format!("{:?}", e),
        }
    }
}

#[cfg_attr(feature = "worker-side", derive(Serialize))]
#[cfg_attr(feature = "spawner-side", derive(Deserialize))]
#[derive(Debug)]
/// The output payload that the worker writes when a task finishes.
pub struct WorkerOutput<T: Debug> {
    /// The result of the task code
    pub result: WorkerResult<T>,
    #[cfg(feature = "stats")]
    /// OS-level statistics about the task
    pub stats: Option<crate::stats::Statistics>,
}

#[cfg(feature = "spawner-side")]
impl<T: Debug + DeserializeOwned> WorkerOutput<T> {
    /// Deserialize a WorkerOutput from a worker's output payload
    pub fn from_output_payload(data: &[u8]) -> WorkerOutput<T> {
        match serde_json::from_slice::<WorkerOutput<T>>(data) {
            Ok(output) => output,
            Err(e) => WorkerOutput {
                result: WorkerResult::Err(WorkerError::from_error(false, e)),
                #[cfg(feature = "stats")]
                stats: None,
            },
        }
    }
}

#[cfg_attr(feature = "worker-side", derive(Serialize))]
#[cfg_attr(feature = "spawner-side", derive(Deserialize))]
#[derive(Debug)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
/// The result of a worker task
pub enum WorkerResult<T: Debug> {
    /// The result data from the worker when it succeeded
    Ok(T),
    /// The error from the worker when it failed
    Err(WorkerError),
}

impl<T: Debug> WorkerResult<T> {
    /// Convert a [WorkerResult] into a [std::result::Result]
    pub fn into_result(self) -> Result<T, WorkerError> {
        match self {
            WorkerResult::Ok(r) => Ok(r),
            WorkerResult::Err(e) => Err(e),
        }
    }
}

impl<T: Debug, E: std::error::Error> From<Result<T, E>> for WorkerResult<T> {
    fn from(res: Result<T, E>) -> Self {
        match res {
            Ok(r) => WorkerResult::Ok(r),
            Err(e) => WorkerResult::Err(WorkerError::from(e)),
        }
    }
}

impl<T: Debug> From<WorkerResult<T>> for Result<T, WorkerError> {
    fn from(r: WorkerResult<T>) -> Result<T, WorkerError> {
        match r {
            WorkerResult::Ok(r) => Ok(r),
            WorkerResult::Err(e) => Err(e),
        }
    }
}

/// An error that the worker wrapper framework may encounter
#[derive(Debug, Error)]
pub enum WrapperError {
    /// Failed when initializing the worker environment
    #[error("Error initializing worker")]
    Initializing,
    /// Failed to read the input payload
    #[error("Failed to read input payload")]
    ReadInput,
    /// The input payload could not be serialized into the structure that the worker expected
    #[error("Unexpected input payload format")]
    UnexpectedInput,
    /// Failed to write the output payload
    #[error("Failed to write output payload")]
    WriteOutput,
}

impl WrapperError {
    /// Whether the error indicates a failure that could possibly be retried, or not
    pub fn retryable(&self) -> bool {
        match self {
            WrapperError::Initializing => false,
            WrapperError::ReadInput => true,
            WrapperError::UnexpectedInput => false,
            WrapperError::WriteOutput => true,
        }
    }
}
