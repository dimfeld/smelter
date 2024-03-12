//! Spawn tasks using the AWS Fargate API

use std::{any::Any, time::Duration};

use async_trait::async_trait;
use aws_sdk_ecs::types::{
    AssignPublicIp, AwsVpcConfiguration, Container, ContainerOverride, EphemeralStorage,
    KeyValuePair, NetworkConfiguration, Task, TaskOverride,
};
use aws_sdk_s3::primitives::ByteStream;
use backon::{ExponentialBuilder, Retryable};
use error_stack::{Report, ResultExt};
use serde::Serialize;
use smelter_job_manager::{SpawnedTask, SubtaskId, TaskError, TaskErrorKind};
use smelter_worker::get_trace_context;

use crate::{AwsError, INPUT_LOCATION_VAR, OTEL_CONTEXT_VAR, OUTPUT_LOCATION_VAR, SUBTASK_ID_VAR};

/// Arguments to tell a [FargateSpawner] how to run a container on Fargate
#[derive(Debug, Clone, Default)]
pub struct FargateTaskArgs {
    /// The task definition to run
    pub task_definition: String,
    /// The name of the primary container in the task. This is needed so that environment
    /// variables can be injected into the container.
    pub container_name: String,
    /// The cluster to run on, if not the default
    pub cluster: Option<String>,
    /// Inject environment variables into the container
    pub env: Vec<(String, String)>,
    /// Override the container's default command
    pub command: Vec<String>,

    /// The subnet IDs to use for this task. All subnets must be in the same VPC.
    /// At least one subnet is required.
    pub subnets: Vec<String>,
    /// The security group IDs to use for this task, if not the default security group for the VPC.
    pub security_groups: Vec<String>,
    /// If true, give this task a public IP address.
    pub assign_public_ip: bool,

    /// Override the CPU limit for the container. This is in units of 1024 = 1 vCPU
    pub cpu: Option<String>,
    /// Override the memory limit for the container, in MB if you don't provide a unit.
    pub memory: Option<String>,
    /// Override the amount of ephemeral storage available
    pub ephemeral_storage: Option<EphemeralStorage>,

    /// Wait this amount of time for the task to finish running
    pub run_timeout: Option<Duration>,
}

/// Spawn tasks using the AWS Fargate API
#[derive(Clone)]
pub struct FargateSpawner {
    /// A base path in S3 to store input and output data.
    pub s3_data_path: String,
    /// How often to poll a task's status while it runs.
    pub check_interval: Duration,
    /// How long to wait for the task to start before we give up.
    pub start_timeout: Option<Duration>,
    /// An client from the ECS SDK.
    pub ecs_client: aws_sdk_ecs::Client,
    /// An client from the S3 SDK.
    pub s3_client: aws_sdk_s3::Client,
}

impl std::fmt::Debug for FargateSpawner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FargateSpawner")
            .field("s3_data_path", &self.s3_data_path)
            .field("check_interval", &self.check_interval)
            .field("start_timeout", &self.start_timeout)
            .finish()
    }
}

impl FargateSpawner {
    /// Spawn the given task using a simple wrapper around the ECS API. For full control,
    /// use [spawn_task_set]. You can skip writing the input data by passing `()` for the
    /// input_data argument.
    pub async fn spawn<'a, INPUT: Serialize + 'static>(
        &self,
        task_id: SubtaskId,
        args: FargateTaskArgs,
        input_data: &INPUT,
    ) -> Result<SpawnedFargateContainer, Report<TaskError>> {
        let command = if args.command.is_empty() {
            None
        } else {
            Some(args.command)
        };

        let rnd: usize = rand::random();
        let s3_data_path = self.s3_data_path.trim_end_matches('/');
        let input_path = format!("{}/{}-input-{}.json", s3_data_path, task_id, rnd);
        let output_path = format!("{}/{}-output-{}.json", s3_data_path, task_id, rnd);

        let task_id_json = serde_json::to_string(&task_id)
            .change_context(TaskError::task_generation_failed(task_id))
            .attach_printable("Serializing task ID")?;

        let mut env = vec![
            KeyValuePair::builder()
                .name(OUTPUT_LOCATION_VAR)
                .value(&output_path)
                .build(),
            KeyValuePair::builder()
                .name(SUBTASK_ID_VAR)
                .value(task_id_json)
                .build(),
        ];

        #[cfg(feature = "opentelemetry")]
        {
            let trace_context_str = serde_json::to_string(&get_trace_context())
                .change_context(TaskError::task_generation_failed(task_id))
                .attach_printable("Serializing opentelemetry context")?;

            env.push(
                KeyValuePair::builder()
                    .name(OTEL_CONTEXT_VAR)
                    .value(&trace_context_str)
                    .build(),
            );
        }

        if input_data.type_id() != std::any::TypeId::of::<()>() {
            env.push(
                KeyValuePair::builder()
                    .name(INPUT_LOCATION_VAR)
                    .value(&input_path)
                    .build(),
            );

            let input_data = serde_json::to_vec(&input_data)
                .change_context(TaskError::task_generation_failed(task_id))
                .attach_printable("Serializing input data")?;
            self.write_input_payload(&task_id, &input_path, input_data)
                .await?;
        };

        env.extend(
            args.env
                .into_iter()
                .map(|(k, v)| KeyValuePair::builder().name(k).value(v).build()),
        );

        let op = self
            .ecs_client
            .run_task()
            .launch_type(aws_sdk_ecs::types::LaunchType::Fargate)
            .task_definition(args.task_definition)
            .set_cluster(args.cluster)
            .network_configuration(
                NetworkConfiguration::builder()
                    .awsvpc_configuration(
                        AwsVpcConfiguration::builder()
                            .set_subnets(Some(args.subnets))
                            .set_security_groups(Some(args.security_groups))
                            .set_assign_public_ip(Some(if args.assign_public_ip {
                                AssignPublicIp::Enabled
                            } else {
                                AssignPublicIp::Disabled
                            }))
                            .build()
                            .change_context(TaskError::task_generation_failed(task_id))?,
                    )
                    .build(),
            )
            .overrides(
                TaskOverride::builder()
                    .container_overrides(
                        ContainerOverride::builder()
                            .name(args.container_name)
                            .set_environment(Some(env))
                            .set_command(command)
                            .build(),
                    )
                    .set_cpu(args.cpu)
                    .set_memory(args.memory)
                    .set_ephemeral_storage(args.ephemeral_storage)
                    .build(),
            );

        let task = op
            .send()
            .await
            .map_err(AwsError::from)
            .change_context(TaskError::did_not_start(task_id, false))
            .attach_printable("Failed to start task")?;

        if let Some(failures) = &task.failures {
            if !failures.is_empty() {
                return Err(Report::new(TaskError::did_not_start(task_id, true))
                    .attach_printable(format!("Task failed: {failures:?}")));
            }
        }

        let task = task.tasks.and_then(|v| v.into_iter().next());

        let Some(task) = task else {
            return Err(Report::new(TaskError::did_not_start(task_id, true))
                .attach_printable("No tasks found in response"));
        };

        Ok(SpawnedFargateContainer {
            task_id,
            s3_client: self.s3_client.clone(),
            ecs_client: self.ecs_client.clone(),
            input_path,
            output_path,
            task,
            check_interval: self.check_interval,
            start_timeout: self
                .start_timeout
                .unwrap_or_else(|| Duration::from_secs(120)),
            run_timeout: args.run_timeout,
        })
    }

    async fn write_input_payload(
        &self,
        task_id: &SubtaskId,
        path: &str,
        data: Vec<u8>,
    ) -> Result<(), Report<TaskError>> {
        let (bucket, path) = crate::parse_s3_url(path)
            .ok_or(TaskError::task_generation_failed(*task_id))
            .attach_printable_lazy(|| format!("Invalid S3 URL for input payload: {path}"))?;

        self.s3_client
            .put_object()
            .bucket(&bucket)
            .key(&path)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(AwsError::from)
            .change_context(TaskError::task_generation_failed(*task_id))
            .attach_printable_lazy(|| format!("Writing input payload s3://{bucket}/{path}"))?;
        Ok(())
    }

    /// Create a new [FargateSpawnerForTask] from this spawner and the provided [FargateTaskArgs]
    pub fn for_task(&self, args: FargateTaskArgs) -> FargateSpawnerForTask {
        FargateSpawnerForTask::new(self.clone(), args)
    }
}

struct FargateSpawnerForTaskInner {
    spawner: FargateSpawner,
    args: FargateTaskArgs,
}

/// A [FargateSpawner] linked to a particular [FargateTaskArgs], for convenience.
#[derive(Clone)]
pub struct FargateSpawnerForTask(std::sync::Arc<FargateSpawnerForTaskInner>);

impl FargateSpawnerForTask {
    /// Create a new FargateSpawnerForTask
    pub fn new(spawner: FargateSpawner, args: FargateTaskArgs) -> Self {
        Self(std::sync::Arc::new(FargateSpawnerForTaskInner {
            spawner,
            args,
        }))
    }

    /// Spawn the given task
    pub async fn spawn<T: Serialize + 'static>(
        &self,
        task_id: SubtaskId,
        input_data: &T,
    ) -> Result<SpawnedFargateContainer, Report<TaskError>> {
        self.0
            .spawner
            .spawn(task_id, self.0.args.clone(), input_data)
            .await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskStatus {
    Loading,
    Started,
    Succeeded,
}

/// Tracking for a spawned Fargate task
pub struct SpawnedFargateContainer {
    task_id: SubtaskId,
    s3_client: aws_sdk_s3::Client,
    ecs_client: aws_sdk_ecs::Client,
    input_path: String,
    output_path: String,
    task: Task,
    check_interval: Duration,
    start_timeout: Duration,
    run_timeout: Option<Duration>,
}

impl SpawnedFargateContainer {
    async fn get_task_status(&self) -> Result<TaskStatus, Report<TaskError>> {
        (|| self.single_task_status_call())
            .retry(&ExponentialBuilder::default())
            // Task status can return MISSING if we try to read the status too soon after starting it,
            // so retry in that case.
            .when(|e| matches!(e.current_context().kind, TaskErrorKind::Lost))
            .await
    }

    async fn single_task_status_call(&self) -> Result<TaskStatus, Report<TaskError>> {
        let task_desc = self
            .ecs_client
            .describe_tasks()
            .set_cluster(self.task.cluster_arn().map(Into::into))
            .tasks(self.task.task_arn().unwrap())
            .send()
            .await
            .map_err(AwsError::from)
            .change_context(TaskError::failed(self.task_id, true))?;

        if let Some(failures) = &task_desc.failures {
            if let Some(failure) = failures.iter().next() {
                if failure.reason().map(|r| r == "MISSING").unwrap_or(false) {
                    return Err(Report::new(TaskError::lost(self.task_id)));
                }

                return Err(Report::new(TaskError::failed(self.task_id, false))
                    .attach_printable(format!("Task failed: {failures:?}")));
            }
        }

        let task = task_desc.tasks.and_then(|v| v.into_iter().next());

        let Some(task) = task else {
            return Err(Report::new(TaskError::lost(self.task_id)));
        };

        let status_label = task.last_status().unwrap_or_default();

        // See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/scheduling_tasks.html#task-lifecycle
        let status = match status_label {
            "PROVISIONING" | "PENDING" | "ACTIVATING" => TaskStatus::Loading,
            "RUNNING" | "DEACTIVATING" | "STOPPING" => TaskStatus::Started,
            "DEPROVISIONING" | "STOPPED" | "DELETED" => {
                self.get_stopped_container_status(task.containers().get(0))?
            }
            other => {
                return Err(Report::new(TaskError::lost(self.task_id))
                    .attach_printable(format!("Unknown task status {other}")))
            }
        };

        Ok(status)
    }

    async fn wait_for_status(
        &self,
        start_time: tokio::time::Instant,
        interval: Duration,
        timeout: Option<Duration>,
        wait_for_status: TaskStatus,
    ) -> Result<TaskStatus, Report<TaskError>> {
        let mut interval = tokio::time::interval(interval);

        loop {
            interval.tick().await;

            let status = self.get_task_status().await?;
            if status == wait_for_status || status == TaskStatus::Succeeded {
                return Ok(status);
            }

            if let Some(timeout) = timeout {
                if start_time.elapsed() > timeout {
                    return Err(Report::new(TaskError::timed_out(self.task_id)));
                }
            }
        }
    }

    async fn remove_input_payload(&self) -> Result<(), Report<TaskError>> {
        let (bucket, path) = crate::parse_s3_url(&self.input_path)
            .ok_or(TaskError::task_generation_failed(self.task_id))
            .attach_printable_lazy(|| {
                format!("Invalid S3 URL for input payload: {}", self.output_path)
            })?;

        self.s3_client
            .delete_object()
            .bucket(bucket)
            .key(path)
            .send()
            .await
            .map_err(AwsError::from)
            .change_context(TaskError::failed(self.task_id, true))
            .attach_printable("Removing input payload")?;

        Ok(())
    }

    async fn remove_output_payload(&self) -> Result<(), Report<TaskError>> {
        let (bucket, path) = crate::parse_s3_url(&self.output_path)
            .ok_or(TaskError::task_generation_failed(self.task_id))
            .attach_printable_lazy(|| {
                format!("Invalid S3 URL for output payload: {}", self.output_path)
            })?;

        self.s3_client
            .delete_object()
            .bucket(bucket)
            .key(path)
            .send()
            .await
            .map_err(AwsError::from)
            .change_context(TaskError::failed(self.task_id, true))
            .attach_printable("Removing output payload")?;

        Ok(())
    }

    async fn get_task_output(&self) -> Result<Vec<u8>, Report<TaskError>> {
        let (bucket, path) = crate::parse_s3_url(&self.output_path)
            .ok_or(TaskError::task_generation_failed(self.task_id))
            .attach_printable_lazy(|| {
                format!("Invalid S3 URL for output payload: {}", self.output_path)
            })?;

        let get = self
            .s3_client
            .get_object()
            .bucket(&bucket)
            .key(&path)
            .send()
            .await
            .map_err(AwsError::from)
            .change_context(TaskError::failed(self.task_id, true))
            .attach_printable_lazy(|| format!("Fetching output payload s3://{bucket}/{path}"))?;

        let data = get
            .body
            .collect()
            .await
            .change_context(TaskError::failed(self.task_id, true))
            .attach_printable_lazy(|| {
                format!("Reading output payload body s3://{bucket}/{path}")
            })?;
        Ok(data.into_bytes().to_vec())
    }

    async fn wait_for_task(&self) -> Result<(), Report<TaskError>> {
        // First wait for the container to start so we can fail fast if things totally break
        let result = self
            .wait_for_status(
                tokio::time::Instant::now(),
                Duration::from_secs(10),
                Some(self.start_timeout),
                TaskStatus::Started,
            )
            .await;

        if let Err(err) = result {
            if matches!(err.current_context().kind, TaskErrorKind::TimedOut) {
                return Err(Report::new(TaskError::did_not_start(self.task_id, true)));
            }
        }

        // Then do a few checks at a quick duration to see if the task didn't fail shortly after
        // starting.
        let mut initial_interval = tokio::time::interval(Duration::from_secs(10));
        let start_run = tokio::time::Instant::now();
        for _ in 0..6 {
            let status = self.get_task_status().await?;
            if status == TaskStatus::Succeeded {
                return Ok(());
            }

            initial_interval.tick().await;
        }

        // Once the task has started and seems to be running ok, check at the specified interval
        // until it's finished.
        self.wait_for_status(
            start_run,
            self.check_interval,
            self.run_timeout,
            TaskStatus::Succeeded,
        )
        .await?;
        Ok(())
    }

    fn get_stopped_container_status(
        &self,
        container: Option<&Container>,
    ) -> Result<TaskStatus, Report<TaskError>> {
        let Some(container) = container else {
            return Err(Report::new(TaskError::lost(self.task_id)));
        };

        let exit_code = container.exit_code.unwrap_or(-1);
        if exit_code == 0 {
            return Ok(TaskStatus::Succeeded);
        }

        let err = Report::new(TaskError::failed(self.task_id, true))
            .attach_printable(format!("task exited with code {exit_code}"))
            .attach_printable(format!(
                "task status: {}",
                container.last_status.as_deref().unwrap_or("unknown")
            ))
            .attach_printable(format!(
                "exit reason: {}",
                container.reason.as_deref().unwrap_or("unknown")
            ));

        Err(err)
    }
}

#[async_trait]
impl SpawnedTask for SpawnedFargateContainer {
    async fn runtime_id(&self) -> Result<String, TaskError> {
        Ok(self.task.task_arn().map(String::from).unwrap_or_default())
    }

    async fn wait(&mut self) -> Result<Vec<u8>, Report<TaskError>> {
        self.wait_for_task().await?;
        let output = self.get_task_output().await?;
        // We don't care about any error on removing the payloads.
        let _ =
            futures::future::join(self.remove_input_payload(), self.remove_output_payload()).await;
        Ok(output)
    }

    async fn kill(&mut self) -> Result<(), Report<TaskError>> {
        self.ecs_client
            .stop_task()
            .set_cluster(self.task.cluster_arn().map(Into::into))
            .task(self.task.task_arn().unwrap())
            .reason("aborted by job manager")
            .send()
            .await
            .map_err(AwsError::from)
            .change_context(TaskError::failed(self.task_id, true))?;
        Ok(())
    }
}
