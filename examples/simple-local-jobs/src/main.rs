//! This is a sample application with a minimal use of the Smelter job manager.
//!
//! In this application, the manager and the workers all use the same executable running
//! in different modes. This makes the example easier to compile and run, but there is no requirement
//! for this to be the case, and real applications will often by simpler using separate
//! executables.

use std::{borrow::Cow, sync::Arc};

use clap::Parser;
use error_stack::Report;
use smelter_job_manager::{
    Job, JobManager, LogSender, SchedulerBehavior, SpawnedTask, StatusSender, SubTask, SubtaskId,
    TaskError,
};
use smelter_local_jobs::spawner::LocalSpawner;
use tokio::task::JoinSet;

mod subtasks;

#[derive(Debug, Parser)]
pub struct Cli {
    /// The mode to run in, if not the manager.
    #[clap(long)]
    mode: Option<String>,
}

#[tokio::main]
async fn main() {
    color_eyre::install().unwrap();
    let args = Cli::parse();

    if let Some(mode) = args.mode {
        match mode.as_str() {
            "generate-random" => subtasks::generate_random().await,
            "add-values" => subtasks::add_values().await,
            _ => panic!("Unknown mode: {}", mode),
        }
    } else {
        run_manager().await;
    }
}

async fn run_manager() {
    let (status_sender, status_rx) = StatusSender::new(true);
    let scheduler = SchedulerBehavior {
        max_retries: 2,
        max_concurrent_tasks: Some(3),
        slow_task_behavior: smelter_job_manager::SlowTaskBehavior::Wait,
    };

    let max_total_jobs = Arc::new(tokio::sync::Semaphore::new(20));

    let manager = JobManager::new(scheduler, status_sender, Some(max_total_jobs));

    // Create 10 independent Jobs. A Job manages all the subtasks for a related task.
    let mut joins = JoinSet::new();
    for i in 1..=10 {
        let job = manager.new_job();
        joins.spawn(run_job(i, job));
    }

    loop {
        tokio::select! {
            Ok(item) = status_rx.recv_async() => {
                println!("{item}");
            }
            join = joins.join_next() => {
                if let Some(job) = join {
                    match job {
                        Ok(value) => {
                            println!("Finished job {:?}", value);
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

async fn run_job(job_index: usize, mut job: Job) -> usize {
    let (stage1_tx, stage1_rx) = job.add_stage().await;
    let (stage2_tx, stage2_rx) = job.add_stage().await;

    for i in 0..32 {
        stage1_tx
            .push(Task {
                job_index,
                task_index: i,
                mode: "generate-random",
                input: Vec::new(),
            })
            .await;
    }

    stage1_tx.finish();

    let mut stage2_index = 0;
    let mut values = Vec::new();
    while let Some(task) = stage1_rx.recv().await {
        let result = task.unwrap();
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

    job_index
}

#[derive(Debug, Clone)]
struct Task {
    job_index: usize,
    task_index: usize,
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
        command.args(["--mode", self.mode]);

        let task = LocalSpawner::default()
            .spawn(task_id, logs, command, &self.input)
            .await?;
        Ok(Box::new(task))
    }
}
