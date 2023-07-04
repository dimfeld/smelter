use std::collections::VecDeque;

use crate::{
    scheduler::{SchedulerBehavior, SlowTaskBehavior},
    spawn::{SpawnedTask, Spawner, TaskError},
    TaskDefWithOutput, TaskInfo, TaskType,
};
use ahash::{HashMap, HashSet, RandomState};
use error_stack::{IntoReport, IntoReportCompat, Report, ResultExt};
use futures::{select, stream::FuturesUnordered, StreamExt};

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

        let map_results = self.run_tasks_stage(map_tasks).await?;

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
            .run_tasks_stage(first_reducer_tasks)
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

            reducer_results = self.run_tasks_stage(reducer_tasks).await?;
        }

        Ok(reducer_results
            .into_iter()
            .map(|task| task.output_location)
            .collect())
    }

    async fn run_tasks_stage<DEF: TaskInfo + Send>(
        &self,
        inputs: Vec<DEF>,
    ) -> Result<Vec<TaskDefWithOutput<DEF>>, Report<TaskError>> {
        let max_concurrent_tasks = self.scheduler.max_concurrent_tasks.unwrap_or(usize::MAX);

        // An error report that can collect all the errors from all the tasks.
        let mut failures = error_stack::Report::new(TaskError::Failed);

        // when the number of in progress tasks drops below this number, retry all the remaining
        // tasks.
        let retry_all_at = match self.scheduler.slow_task_behavior {
            SlowTaskBehavior::Wait => 0,
            SlowTaskBehavior::RerunLastPercent(n) => inputs.len() * n / 100,
            SlowTaskBehavior::RerunLastN(n) => n,
        };

        let mut output_list = Vec::with_capacity(inputs.len());

        // A map of task index to the current try count.
        let mut unfinished = HashMap::with_capacity_and_hasher(inputs.len(), RandomState::new());

        let mut spawning = FuturesUnordered::new();
        let mut running = FuturesUnordered::new();

        // let mut retry_after = FuturesUnordered::new();

        let mut ready_to_run = VecDeque::with_capacity(inputs.len());

        let mut enqueue_task = |i: usize| {
            let try_count = unfinished.entry(i).and_modify(|c| *c += 1).or_insert(0);
            if *try_count < self.scheduler.max_retries {
                ready_to_run.push_back(i);
                true
            } else {
                false
            }
        };

        // Send the initial round of tasks.
        for (i, _) in inputs.iter().enumerate() {
            enqueue_task(i);
        }

        // When the number of futures drops below max_concurrent_tasks and there are more tasks
        // pending to run, run a task.
        //

        let mut failed = false;
        while !unfinished.is_empty() && !failed {
            // There is a task to run, and we have capacity to run it
            if running.len() < max_concurrent_tasks {
                if let Some(next_task) = ready_to_run.pop_front() {
                    let task_def = &inputs[next_task];
                    match task_def
                        .serialize_input()
                        .into_report()
                        .change_context(TaskError::TaskGenerationFailed)
                    {
                        Ok(task_input) => {
                            let task_spawn = self.spawner.spawn(
                                "a_task".to_string(),
                                task_def.spawn_name(),
                                task_input,
                            );

                            spawning.push(task_spawn);
                            continue;
                        }
                        Err(e) => {
                            failures = failures.attach_printable(e);
                            failed = true;
                            break;
                        }
                    }
                }
            }

            select! {
                task = spawning.select_next_some() => {
                    running.push(async {
                        // Wait for the task to finish
                        let mut task = task?;
                        let runtime_id = task.runtime_id().await?;
                        // TODO Also wait to see if we should kill this task.
                        task.wait().await.attach_printable_lazy(|| format!("Runtime ID {runtime_id}"))?;
                        Ok::<String, Report<TaskError>>(task.output_location())
                    });
                }

                finished = running.select_next_some() => {
                    todo!()

                }
            };
        }

        if failed {
            Err(failures)
        } else {
            Ok(output_list)
        }
    }
}
