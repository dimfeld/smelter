use std::{collections::HashMap, fmt::Debug, io::Read};

#[cfg(feature = "opentelemetry")]
use opentelemetry::sdk::propagation::TraceContextPropagator;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// The ID for a subtask, which uniquely identifies it within a [Job].
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubtaskId {
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
pub struct WorkerInput<T> {
    pub task_id: SubtaskId,
    #[cfg(feature = "opentelemetry")]
    #[serde(default)]
    pub trace_context: std::collections::HashMap<String, String>,
    pub input: T,
}

#[cfg(feature = "spawner-side")]
impl<T> WorkerInput<T> {
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
pub struct WorkerError {
    pub retryable: bool,
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
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum WorkerResult<T: Debug> {
    Ok(T),
    Err(WorkerError),
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

#[cfg(feature = "spawner-side")]
impl<T: Debug + DeserializeOwned> WorkerResult<T> {
    pub fn parse(data: &[u8]) -> Result<T, WorkerError> {
        match serde_json::from_slice::<WorkerResult<T>>(data) {
            Ok(WorkerResult::Ok(r)) => Ok(r),
            Ok(WorkerResult::Err(e)) => Err(e),
            Err(e) => Err(WorkerError::from_error(false, e)),
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
