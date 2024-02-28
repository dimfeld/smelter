use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_ecs::types::{ContainerOverride, EphemeralStorage, KeyValuePair, Task, TaskOverride};
use error_stack::Report;
use serde::Serialize;
use smelter_job_manager::{SpawnedTask, TaskError};

use crate::{INPUT_LOCATION_VAR, OUTPUT_LOCATION_VAR};

pub struct FargateSpawner {
    /// A base path to store input and output data.
    pub s3_data_path: String,
    /// How often to poll a task's status while it runs.
    pub check_interval: Duration,
    pub client: aws_sdk_ecs::Client,
}

impl FargateSpawner {
    /// Spawn the given task using a simple wrapper around the ECS API. For full control,
    /// use [spawn_task_set].
    pub async fn spawn(
        &self,
        args: FargateTaskArgs,
        input_data: Option<impl Serialize>,
    ) -> Result<SpawnedFargateContainer, TaskError> {
        let command = if args.command.is_empty() {
            None
        } else {
            Some(args.command)
        };

        let mut env = vec![KeyValuePair::builder()
            .name(OUTPUT_LOCATION_VAR)
            .value(&self.s3_data_path)
            .build()];

        if input_data.is_some() {
            env.push(
                KeyValuePair::builder()
                    .name(INPUT_LOCATION_VAR)
                    .value(&self.s3_data_path)
                    .build(),
            )
        };

        env.extend(
            args.env
                .into_iter()
                .map(|(k, v)| KeyValuePair::builder().name(k).value(v).build()),
        );

        let op = self
            .client
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

        let task = op.send().await?;
        // look at task.failures
        let task = task.tasks.and_then(|v| v.into_iter().next()).unwrap();

        Ok(SpawnedFargateContainer {
            client: self.client.clone(),
            task,
        })
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
}

enum TaskStatus {
    Loading,
    Started,
    Done,
}

pub struct SpawnedFargateContainer {
    client: aws_sdk_ecs::Client,
    task: Task,
}

impl SpawnedFargateContainer {
    async fn get_task_status(&self) -> Result<TaskStatus, TaskError> {
        let task_desc = self
            .client
            .describe_tasks()
            .set_cluster(self.task.cluster_arn().map(Into::into))
            .tasks(self.task.task_arn().unwrap())
            .send()
            .await?;

        let task = task_desc.tasks.unwrap().into_iter().next().unwrap();
        let task_status = task.last_status();
        // other task fields: started_at, stopped_at, stopped_reason
        // if the container is done, get the exit code to determine if it's done
        let container = task.containers()[0];
        let status = container.last_status.unwrap();
        let exit_code = container.exit_code;
        let reason = container.reason.unwrap();
        todo!()
    }
}

#[async_trait]
impl SpawnedTask for SpawnedFargateContainer {
    async fn runtime_id(&self) -> Result<String, TaskError> {
        todo!()
    }

    async fn wait(&mut self) -> Result<Vec<u8>, Report<TaskError>> {
        todo!()
    }

    async fn kill(&mut self) -> Result<(), Report<TaskError>> {
        todo!()
    }
}
