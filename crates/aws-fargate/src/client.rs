//! Functionality for building AWS clients
use std::marker::PhantomData;

use aws_sdk_ecs::{
    config::retry::ClassifyRetry,
    error::ProvideErrorMetadata,
    operation::{
        describe_tasks::DescribeTasksError, run_task::RunTaskError, stop_task::StopTaskError,
    },
};
use aws_sdk_s3::config::{interceptors::InterceptorContext, retry::RetryAction};
use aws_smithy_runtime_api::client::retries::classifiers::SharedRetryClassifier;

/// Build a configuration for the ECS client that will retry failed requests, including
/// rate-limited requests.
pub fn build_ecs_client_config(sdk_config: &aws_config::SdkConfig) -> aws_sdk_ecs::config::Builder {
    let mut builder = aws_sdk_ecs::config::Builder::from(sdk_config);

    let retry_config = aws_sdk_ecs::config::retry::RetryConfig::adaptive()
        .with_initial_backoff(std::time::Duration::from_secs(2))
        .with_max_attempts(5)
        .with_max_backoff(std::time::Duration::from_secs(20));

    builder.set_retry_config(Some(retry_config));

    // This should include all the types of operations that the spawner does. Unfortunate that we
    // need a separate one for every error type but it works out since there's only a few.
    builder.push_retry_classifier(SharedRetryClassifier::new(RateLimitErrorClassifier::<
        DescribeTasksError,
    >::new()));
    builder.push_retry_classifier(SharedRetryClassifier::new(RateLimitErrorClassifier::<
        RunTaskError,
    >::new()));
    builder.push_retry_classifier(SharedRetryClassifier::new(RateLimitErrorClassifier::<
        StopTaskError,
    >::new()));

    builder
}

/// An error classifier that triggers retries on ThrottlingException, which the SDK does not yet
/// handle.
#[derive(Debug, Default)]
pub struct RateLimitErrorClassifier<E> {
    _marker: PhantomData<E>,
}

impl<E> RateLimitErrorClassifier<E> {
    /// Create a new error RateLimitErrorClassifier
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<E> ClassifyRetry for RateLimitErrorClassifier<E>
where
    // Adding a trait bound for ProvideErrorMetadata allows us to inspect the error code.
    E: std::error::Error + ProvideErrorMetadata + Send + Sync + 'static,
{
    fn classify_retry(&self, ctx: &InterceptorContext) -> RetryAction {
        // Check for a result
        let output_or_error = ctx.output_or_error();
        // Check for an error
        let error = match output_or_error {
            Some(Ok(_)) | None => return RetryAction::NoActionIndicated,
            Some(Err(err)) => err,
        };

        // Downcast the generic error and extract the code
        let rate_throttle_error = error
            .as_operation_error()
            .and_then(|err| err.downcast_ref::<aws_sdk_ecs::error::SdkError<E>>())
            .and_then(|err| err.as_service_error())
            .and_then(|err| err.code())
            .map(|code| code == "ThrottlingException")
            .unwrap_or(false);

        if rate_throttle_error {
            // If the error is a ThrottlingException, return that we should retry.
            RetryAction::throttling_error()
        } else {
            // Otherwise, return that no action is indicated i.e. that this classifier doesn't require a retry.
            // Another classifier may still classify this response as retryable.
            RetryAction::NoActionIndicated
        }
    }

    fn name(&self) -> &'static str {
        "ThrottlingException Classifier"
    }
}
