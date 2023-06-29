use crate::{
    scheduler::SchedulerBehavior,
    spawn::{Spawner, TaskError},
    TaskDefWithOutput, TaskType,
};
use error_stack::{IntoReport, Report, ResultExt};
use serde::Serialize;

pub struct JobManager<TASKTYPE: TaskType, SPAWNER: Spawner> {
    spawner: SPAWNER,
    task_type: TASKTYPE,
    scheduler: SchedulerBehavior,
}

impl<TASKTYPE: TaskType, SPAWNER: Spawner> JobManager<TASKTYPE, SPAWNER> {
    pub fn new(task_type: TASKTYPE, spawner: SPAWNER, scheduler: SchedulerBehavior) -> Self {
        Self {
            spawner,
            task_type,
            scheduler,
        }
    }

    pub async fn run(&self, task_def: TASKTYPE::TaskDef) -> Result<Vec<String>, Report<TaskError>> {
        let map_tasks = self
            .task_type
            .create_map_tasks(&task_def)
            .await
            .into_report()
            .change_context(TaskError::TaskGenerationFailed)?;

        let map_results = self.run_tasks(map_tasks).await?;

        let first_reducer_tasks = self
            .task_type
            .create_top_level_reducers(&task_def, &map_results)
            .await
            .into_report()
            .change_context(TaskError::TaskGenerationFailed)?;

        if first_reducer_tasks.is_empty() {
            return Ok(map_results
                .into_iter()
                .map(|task| task.output_location)
                .collect());
        }

        let mut reducer_results = self
            .run_tasks(first_reducer_tasks)
            .await?
            .into_iter()
            .map(|task| TaskDefWithOutput {
                task_def: task.task_def.into(),
                output_location: task.output_location,
            })
            .collect::<Vec<_>>();

        loop {
            let reducer_tasks = self
                .task_type
                .create_intermediate_reducers(&task_def, &reducer_results)
                .await
                .into_report()
                .change_context(TaskError::TaskGenerationFailed)?;
            if reducer_tasks.is_empty() {
                break;
            }

            reducer_results = self.run_tasks(reducer_tasks).await?;
        }

        Ok(reducer_results
            .into_iter()
            .map(|task| task.output_location)
            .collect())
    }

    async fn run_tasks<DEF: Serialize + Send>(
        &self,
        inputs: Vec<DEF>,
    ) -> Result<Vec<TaskDefWithOutput<DEF>>, TaskError> {
        let max_tasks = self.scheduler.max_concurrent_tasks.unwrap_or(usize::MAX);
        // Create a stream of futures and consume the stream with max concurrency

        todo!();
    }
}
