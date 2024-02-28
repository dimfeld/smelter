use std::{any::Any, time::Duration};

use async_trait::async_trait;
use aws_sdk_ecs::types::{
    Container, ContainerOverride, EphemeralStorage, KeyValuePair, Task, TaskOverride,
};
use aws_sdk_s3::primitives::ByteStream;
use error_stack::{Report, ResultExt};
use serde::Serialize;
use smelter_job_manager::{SpawnedTask, SubtaskId, TaskError};

use crate::{INPUT_LOCATION_VAR, OUTPUT_LOCATION_VAR};

pub struct FargateSpawner {
    /// A base path in S3 to store input and output data.
    pub s3_data_path: String,
    /// How often to poll a task's status while it runs.
    pub check_interval: Duration,
    /// How long to wait for the task to start before we give up.
    pub start_timeout: Duration,
    /// An client from the ECS SDK.
    pub ecs_client: aws_sdk_ecs::Client,
    /// An client from the S3 SDK.
    pub s3_client: aws_sdk_s3::Client,
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
        let input_path = format!("{}/{}-input-{}.json", self.s3_data_path, task_id, rnd);
        let output_path = format!("{}/{}-output-{}.json", self.s3_data_path, task_id, rnd);

        let mut env = vec![KeyValuePair::builder()
            .name(OUTPUT_LOCATION_VAR)
            .value(&output_path)
            .build()];

        if input_data.type_id() != std::any::TypeId::of::<()>() {
            env.push(
                KeyValuePair::builder()
                    .name(INPUT_LOCATION_VAR)
                    .value(&input_path)
                    .build(),
            );

            let input_data = serde_json::to_vec(&input_data)
                .change_context(TaskError::TaskGenerationFailed)
                .attach_printable("Serializing input data")?;
            self.write_input_payload(&input_path, input_data).await?;
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
            .overrides(
                TaskOverride::builder()
                    .container_overrides(
                        ContainerOverride::builder()
                            .set_cpu(args.cpu)
                            .set_memory_reservation(args.memory)
                            .set_environment(Some(env))
                            .set_command(command)
                            .build(),
                    )
                    .set_ephemeral_storage(args.ephemeral_storage)
                    .build(),
            );

        let task = op
            .send()
            .await
            .change_context(TaskError::DidNotStart(false))?;
        // look at task.failures
        let task = task.tasks.and_then(|v| v.into_iter().next()).unwrap();

        Ok(SpawnedFargateContainer {
            s3_client: self.s3_client.clone(),
            ecs_client: self.ecs_client.clone(),
            output_path,
            task,
            check_interval: self.check_interval,
            start_timeout: self.start_timeout,
            run_timeout: args.run_timeout,
        })
    }

    async fn write_input_payload(
        &self,
        path: &str,
        data: Vec<u8>,
    ) -> Result<(), Report<TaskError>> {
        let (bucket, path) = crate::parse_s3_url(path)
            .ok_or(TaskError::TaskGenerationFailed)
            .attach_printable_lazy(|| format!("Invalid S3 URL for input payload: {path}"))?;

        self.s3_client
            .put_object()
            .bucket(bucket)
            .key(path)
            .body(ByteStream::from(data))
            .send()
            .await
            .change_context(TaskError::TaskGenerationFailed)
            .attach_printable("Writing input payload")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct FargateTaskArgs {
    /// The task definition to run
    pub task_definition: String,
    /// The cluster to run on, if not the default
    pub cluster: Option<String>,
    /// Inject environment variables into the container
    pub env: Vec<(String, String)>,
    /// Override the container's default command
    pub command: Vec<String>,

    /// Override the CPU limit for the container. This is in units of 1024 = 1 vCPU
    pub cpu: Option<i32>,
    /// Override the memory limit for the container, in MB
    pub memory: Option<i32>,
    /// Override the amount of ephemeral storage available
    pub ephemeral_storage: Option<EphemeralStorage>,

    /// Wait this amount of time for the task to finish running
    pub run_timeout: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskStatus {
    Loading,
    Started,
    Succeeded,
}

pub struct SpawnedFargateContainer {
    s3_client: aws_sdk_s3::Client,
    ecs_client: aws_sdk_ecs::Client,
    output_path: String,
    task: Task,
    check_interval: Duration,
    start_timeout: Duration,
    run_timeout: Option<Duration>,
}

impl SpawnedFargateContainer {
    async fn get_task_status(&self) -> Result<TaskStatus, Report<TaskError>> {
        let task_desc = self
            .ecs_client
            .describe_tasks()
            .set_cluster(self.task.cluster_arn().map(Into::into))
            .tasks(self.task.task_arn().unwrap())
            .send()
            .await
            .change_context(TaskError::Failed(true))?;

        let task = task_desc.tasks.unwrap().into_iter().next().unwrap();
        let status_label = task.last_status().unwrap_or_default();

        // See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/scheduling_tasks.html#task-lifecycle
        let status = match status_label {
            "PROVISIONING" | "PENDING" | "ACTIVATING" => TaskStatus::Loading,
            "RUNNING" | "DEACTIVATING" | "STOPPING" => TaskStatus::Started,
            "DEPROVISIONING" | "STOPPED" | "DELETED" => {
                get_stopped_container_status(task.containers().get(0))?
            }
            other => {
                return Err(Report::new(TaskError::Lost)
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
                    return Err(Report::new(TaskError::DidNotStart(true))
                        .attach_printable("Timed out waiting for task to start"));
                }
            }
        }
    }

    async fn get_task_output(&self) -> Result<Vec<u8>, Report<TaskError>> {
        let (bucket, path) = crate::parse_s3_url(&self.output_path)
            .ok_or(TaskError::TaskGenerationFailed)
            .attach_printable_lazy(|| {
                format!("Invalid S3 URL for input payload: {}", self.output_path)
            })?;

        let get = self
            .s3_client
            .get_object()
            .bucket(bucket)
            .key(path)
            .send()
            .await
            .change_context(TaskError::Failed(true))
            .attach_printable("Fetching output payload")?;

        let data = get
            .body
            .collect()
            .await
            .change_context(TaskError::Failed(true))
            .attach_printable("Reading output payload")?;
        Ok(data.into_bytes().to_vec())
    }

    async fn wait_for_task(&self) -> Result<(), Report<TaskError>> {
        // First wait for the container to start so we can fail fast if things totally break
        self.wait_for_status(
            tokio::time::Instant::now(),
            Duration::from_secs(10),
            Some(self.start_timeout),
            TaskStatus::Started,
        )
        .await?;

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
}

#[async_trait]
impl SpawnedTask for SpawnedFargateContainer {
    async fn runtime_id(&self) -> Result<String, TaskError> {
        Ok(self.task.task_arn().map(String::from).unwrap_or_default())
    }

    async fn wait(&mut self) -> Result<Vec<u8>, Report<TaskError>> {
        self.wait_for_task().await?;
        self.get_task_output().await
    }

    async fn kill(&mut self) -> Result<(), Report<TaskError>> {
        self.ecs_client
            .stop_task()
            .set_cluster(self.task.cluster_arn().map(Into::into))
            .task(self.task.task_arn().unwrap())
            .reason("aborted by job manager")
            .send()
            .await
            .change_context(TaskError::Failed(true))?;
        Ok(())
    }
}

fn get_stopped_container_status(
    container: Option<&Container>,
) -> Result<TaskStatus, Report<TaskError>> {
    let Some(container) = container else {
        return Err(Report::new(TaskError::Lost));
    };

    let exit_code = container.exit_code.unwrap_or(-1);
    if exit_code == 0 {
        return Ok(TaskStatus::Succeeded);
    }

    let err = Report::new(TaskError::Failed(true))
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
