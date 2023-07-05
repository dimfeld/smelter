use std::collections::VecDeque;
use std::fmt::Debug;

use crate::{
    scheduler::{SchedulerBehavior, SlowTaskBehavior},
    spawn::{SpawnedTask, Spawner, TaskError},
    TaskDefWithOutput, TaskInfo, TaskType,
};
use error_stack::{IntoReport, IntoReportCompat, Report, ResultExt};
use futures::{select, stream::FuturesUnordered, FutureExt, StreamExt};

#[derive(Debug)]
struct TaskTrackingInfo<INPUT: Debug> {
    input: INPUT,
    try_num: usize,
}

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
        let mut failures = error_stack::Report::new(TaskError::Failed(false));

        // when the number of in progress tasks drops below this number, retry all the remaining
        // tasks.
        let retry_all_at = match self.scheduler.slow_task_behavior {
            SlowTaskBehavior::Wait => 0,
            SlowTaskBehavior::RerunLastPercent(n) => inputs.len() * n / 100,
            SlowTaskBehavior::RerunLastN(n) => std::cmp::min(n, inputs.len() / 2),
        };

        let mut output_list = Vec::with_capacity(inputs.len());

        // A map of task index to the current try count.
        let mut unfinished = inputs
            .into_iter()
            .map(|input| Some(TaskTrackingInfo { input, try_num: 0 }))
            .collect::<Vec<_>>();
        let mut unfinished_count = unfinished.len();

        let mut spawning = FuturesUnordered::new();
        let mut running = FuturesUnordered::new();

        // let mut retry_after = FuturesUnordered::new();

        let mut ready_to_run = VecDeque::with_capacity(unfinished.len());

        // Send the initial round of tasks.
        for i in 0..unfinished.len() {
            ready_to_run.push_back(i);
        }

        // When the number of futures drops below max_concurrent_tasks and there are more tasks
        // pending to run, run a task.

        let mut failed = false;
        while !unfinished_count > 0 && !failed {
            // There is a task to run, and we have capacity to run it
            if running.len() < max_concurrent_tasks {
                if let Some(next_task) = ready_to_run.pop_front() {
                    let task = unfinished[next_task].as_ref().map(|task| {
                        let task_input = task
                            .input
                            .serialize_input()
                            .into_report()
                            .change_context(TaskError::TaskGenerationFailed)?;
                        let spawn_name = task.input.spawn_name();

                        Ok::<_, Report<TaskError>>((task_input, spawn_name))
                    });

                    match task {
                        Some(Ok((task_input, spawn_name))) => {
                            let task_spawn = self
                                .spawner
                                .spawn("a_task".to_string(), spawn_name, task_input)
                                .map(move |result| (next_task, result));

                            spawning.push(task_spawn);
                            continue;
                        }
                        Some(Err(e)) => {
                            // The task failed to even generate an input payload. This is not
                            // retryable so just fail.
                            failures = failures.attach_printable(e);
                            failed = true;
                            break;
                        }
                        None => {
                            // Another try of this task finished successfully between when it was enqueued and
                            // now.
                        }
                    }
                }
            }

            select! {
                (index, task) = spawning.select_next_some() => {
                    if let Some(input_def) = &unfinished[index] {
                        let try_num = input_def.try_num;
                        running.push(async move {
                            let mut task = task?;
                            let runtime_id = task.runtime_id().await?;
                            // TODO Also wait to see if we should kill this task.
                            task.wait().await
                                .attach_printable_lazy(|| format!("Job {index} try {try_num}"))
                                .attach_printable_lazy(|| format!("Runtime ID {runtime_id}"))?;
                            Ok::<_, Report<TaskError>>(task.output_location())
                        }.map(move |result| (index, result)));
                    }
                }

                finished = running.select_next_some() => {
                    let (index, result) = finished;
                    match result {
                        Ok(output_location) => {
                            if let Some(task_info) = unfinished[index].take() {
                                unfinished_count -= 1;
                                output_list.push(TaskDefWithOutput { task_def: task_info.input, output_location });

                                if unfinished_count < retry_all_at {
                                    // Re-enqueue all unfinished tasks. Some serverless platforms
                                    // can have very high latency in some cases, so this gets
                                    // around that issue. Since this isn't an error-based retry, we
                                    // don't increment the retry count.
                                    for (i, task) in unfinished.iter().enumerate() {
                                        if task.is_some() {
                                            ready_to_run.push_back(i);
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            if let Some(task_info) = unfinished[index].as_mut() {
                                if e.current_context().retryable() && task_info.try_num <= self.scheduler.max_retries {
                                    task_info.try_num += 1;
                                    ready_to_run.push_back(index);
                                } else {
                                    failed = true;
                                    failures = failures.attach_printable(e);
                                }
                            }
                        }
                    }


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
