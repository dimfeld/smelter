//! This is a sample application with a minimal use of the Smelter job manager.
//!
//! In this application, the manager and the workers all use the same executable running
//! in different modes. This makes the example easier to compile and run, but there is no requirement
//! for this to be the case, and real applications will often by simpler using separate
//! executables.

use std::{borrow::Cow, sync::Arc};

use clap::Parser;
use error_stack::Report;
use rand::Rng;
use smelter_job_manager::{
    Job, JobManager, LogSender, SchedulerBehavior, SpawnedTask, StageError, StatusSender, SubTask,
    SubtaskId, TaskError,
};
use smelter_local_jobs::spawner::LocalSpawner;
use tokio::task::JoinSet;

mod subtasks;

#[derive(Debug, Parser)]
pub struct Cli {
    /// The mode to run in, if not the manager.
    #[clap(long)]
    mode: Option<String>,

    /// The number of jobs to run
    #[clap(short, long, default_value = "10")]
    num_jobs: usize,

    /// Test when a job fails without being able to retry
    #[clap(long)]
    fail: bool,
}

#[tokio::main]
async fn main() {
    color_eyre::install().unwrap();
    let args = Cli::parse();

    if let Some(mode) = args.mode {
        match mode.as_str() {
            "generate-random" => subtasks::generate_random(args.fail).await,
            "add-values" => subtasks::add_values().await,
            _ => panic!("Unknown mode: {}", mode),
        }
    } else {
        run_manager(args).await;
    }
}

async fn run_manager(args: Cli) {
    let (status_sender, status_rx) = StatusSender::new(true);
    let scheduler = SchedulerBehavior {
        max_retries: 2,
        max_concurrent_tasks: Some(3),
        slow_task_behavior: smelter_job_manager::SlowTaskBehavior::Wait,
    };

    let max_total_jobs = Arc::new(tokio::sync::Semaphore::new(20));

    let manager = JobManager::new(status_sender)
        .with_scheduler_behavior(scheduler)
        .with_global_semaphore(max_total_jobs);

    // Create 10 independent Jobs. A Job manages all the subtasks for a related task.
    let mut joins = JoinSet::new();
    for i in 1..=args.num_jobs {
        let job = manager.new_job();
        joins.spawn(run_job(i, job, args.fail));
    }

    loop {
        tokio::select! {
            Ok(item) = status_rx.recv_async() => {
                println!("{}", item);
            }
            join = joins.join_next() => {
                if let Some(job) = join {
                    match job {
                        Ok((job_index, Ok(_))) => {
                            println!("Finished job {}", job_index);
                        }
                        Ok((job_index, Err(e))) => {
                            println!("Failed job {}: {e:?}", job_index);
                        }
                        Err(e) => {
                            println!("Failed job {:?}", e);
                        }
                    }
                } else {
                    break;
                }
            }
        }
    }
}

async fn run_job(
    job_index: usize,
    mut job: Job,
    fail: bool,
) -> (usize, Result<(), Report<StageError>>) {
    let (stage1_tx, stage1_rx) = job.add_stage().await;
    let (stage2_tx, stage2_rx) = job.add_stage().await;

    let fail_index = if fail {
        rand::thread_rng().gen_range(0..32)
    } else {
        usize::MAX
    };

    for i in 0..32 {
        stage1_tx
            .push(Task {
                job_index,
                task_index: i,
                mode: "generate-random",
                input: Vec::new(),
                fail: i == fail_index,
            })
            .await;
    }

    stage1_tx.finish();

    let mut stage2_index = 0;
    let mut values = Vec::new();
    while let Some(task) = stage1_rx.recv().await {
        let result = match task {
            Ok(result) => result,
            Err(e) => {
                println!("Task error {:?}", e);
                break;
            }
        };

        println!(
            "Task {} finished with result {}",
            result.task_def.description(),
            result.output
        );

        let value: usize = result.output.parse().unwrap();
        values.push(value);

        if values.len() < 4 {
            continue;
        }

        let task_values = std::mem::take(&mut values);
        stage2_tx
            .push(Task {
                job_index,
                task_index: stage2_index,
                mode: "add-values",
                input: task_values,
                fail: false,
            })
            .await;
        stage2_index += 1;
    }

    if !values.is_empty() {
        stage2_tx
            .push(Task {
                job_index,
                task_index: stage2_index,
                mode: "add-values",
                input: values,
                fail: false,
            })
            .await;
    }

    stage2_tx.finish();

    while let Some(task) = stage2_rx.recv().await {
        match task {
            Ok(task) => {
                println!(
                    "Task {} finished with result {}",
                    task.task_def.description(),
                    task.output
                );
            }
            Err(e) => {
                println!("Job {} failed with error {}", job_index, e);
            }
        }
    }

    let job_result = job.wait().await;

    (job_index, job_result)
}

#[derive(Debug, Clone)]
struct Task {
    job_index: usize,
    task_index: usize,
    fail: bool,
    mode: &'static str,
    input: Vec<usize>,
}

#[async_trait::async_trait]
impl SubTask for Task {
    type Output = String;

    fn description(&self) -> Cow<'static, str> {
        format!("{}:{}:{}", self.job_index, self.mode, self.task_index).into()
    }

    async fn spawn(
        &self,
        task_id: SubtaskId,
        logs: Option<LogSender>,
    ) -> Result<Box<dyn SpawnedTask>, Report<TaskError>> {
        let target = std::env::args().nth(0).unwrap();
        let mut command = tokio::process::Command::new(target);
        if self.fail {
            command.arg("--fail");
        }
        command.args(["--mode", self.mode]);

        let task = LocalSpawner::default()
            .spawn(task_id, logs, command, &self.input)
            .await?;
        Ok(Box::new(task))
    }
}
