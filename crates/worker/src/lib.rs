use serde::{de::DeserializeOwned, Deserialize, Serialize};

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
    pub fn from_error(retryable: bool, error: impl std::fmt::Debug) -> Self {
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
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum WorkerResult<T> {
    Ok(T),
    Err(WorkerError),
}

impl<T, E: std::error::Error> From<Result<T, E>> for WorkerResult<T> {
    fn from(res: Result<T, E>) -> Self {
        match res {
            Ok(r) => WorkerResult::Ok(r),
            Err(e) => WorkerResult::Err(WorkerError::from(e)),
        }
    }
}

impl<T> From<WorkerResult<T>> for Result<T, WorkerError> {
    fn from(r: WorkerResult<T>) -> Result<T, WorkerError> {
        match r {
            WorkerResult::Ok(r) => Ok(r),
            WorkerResult::Err(e) => Err(e),
        }
    }
}

#[cfg(feature = "spawner-side")]
impl<T: DeserializeOwned> WorkerResult<T> {
    pub fn parse(data: &[u8]) -> Result<T, WorkerError> {
        match serde_json::from_slice::<WorkerResult<T>>(data) {
            Ok(WorkerResult::Ok(r)) => Ok(r),
            Ok(WorkerResult::Err(e)) => Err(e),
            Err(e) => Err(WorkerError::from_error(false, e)),
        }
    }
}
