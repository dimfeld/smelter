use serde::{Deserialize, Serialize};
use sysinfo::System;

#[cfg_attr(feature = "worker-side", derive(Serialize))]
#[cfg_attr(feature = "spawner-side", derive(Deserialize))]
#[derive(Clone, Debug)]
pub struct Statistics {
    /// The average one-minute load average throughout the task
    avg_load_average: f64,
    /// The maximum one-minute load average throughout the task
    max_load_average: f64,
    /// The maximum amount of RAM used, in bytes.
    max_ram_used: u64,
    /// The system uptime when the worker started running
    uptime_at_start: u64,
    /// System uptime when the worker finished running
    uptime_at_end: u64,
}

pub struct StatisticsTracker {
    close_tx: tokio::sync::oneshot::Sender<()>,
    task_handle: tokio::task::JoinHandle<Statistics>,
}

impl StatisticsTracker {
    pub async fn finish(self) -> Option<Statistics> {
        drop(self.close_tx);
        self.task_handle.await.ok()
    }
}

/// Start a task to track system statistics. The task can be shut down by dropping the
/// returned [Receiver], and the [JoinHandle] will then return a [Statistics] object.
pub fn track_system_stats() -> StatisticsTracker {
    let (close_tx, mut close_rx) = tokio::sync::oneshot::channel();
    let task_handle = tokio::task::spawn(async move {
        let mut system = System::new();

        let mut stats = Statistics {
            avg_load_average: 0.0,
            max_load_average: 0.0,
            max_ram_used: 0,
            uptime_at_start: System::uptime(),
            uptime_at_end: 0,
        };

        let mut total_load_average = 0.0;
        let mut num_load_averages = 0;

        let mut check_interval = tokio::time::interval(std::time::Duration::from_secs(10));

        loop {
            tokio::select! {
                _ = check_interval.tick() => {
                    let load_avg = System::load_average();
                    total_load_average += load_avg.one;
                    num_load_averages += 1;
                    if load_avg.one > stats.max_load_average {
                        stats.max_load_average = load_avg.one;
                    }

                    system.refresh_memory_specifics(sysinfo::MemoryRefreshKind::new().with_ram());
                    let used_ram = system.used_memory();
                    if used_ram > stats.max_ram_used {
                        stats.max_ram_used = used_ram;
                    }
                }
                _ = &mut close_rx => {
                    break;
                }
            }
        }

        if num_load_averages > 0 {
            stats.avg_load_average = total_load_average / (num_load_averages as f64);
        }
        stats.uptime_at_end = System::uptime();
        stats
    });

    StatisticsTracker {
        close_tx,
        task_handle,
    }
}
