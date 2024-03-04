//! Worker statistics collection

use std::fmt::Display;

use serde::{Deserialize, Serialize};
use sysinfo::System;

/// OS-level statistics about the task
#[cfg_attr(feature = "worker-side", derive(Serialize))]
#[cfg_attr(feature = "spawner-side", derive(Deserialize))]
#[derive(Clone, Debug)]
pub struct Statistics {
    /// The average one-minute load average throughout the task
    pub avg_load_average: f64,
    /// The maximum one-minute load average throughout the task
    pub max_load_average: f64,
    /// The maximum amount of RAM used, in bytes.
    pub max_ram_used: u64,
    /// The system uptime when the worker started running
    pub uptime_at_start: u64,
    /// System uptime when the worker finished running
    pub uptime_at_end: u64,
}

impl Display for Statistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "load: avg {}, max {}, max_ram: {}MiB, start_at: ",
            self.avg_load_average,
            self.max_load_average,
            (self.max_ram_used as f64 / 1048576.0).round(),
        )?;

        Self::write_uptime_duration(f, self.uptime_at_start)?;
        write!(f, ", took: ")?;
        Self::write_uptime_duration(f, self.uptime_at_end - self.uptime_at_start)?;
        Ok(())
    }
}

impl Statistics {
    fn write_uptime_duration(f: &mut std::fmt::Formatter<'_>, uptime: u64) -> std::fmt::Result {
        let days = uptime / 86400;
        let hours = (uptime % 86400) / 3600;
        let minutes = (uptime % 3600) / 60;
        let seconds = uptime % 60;
        match (days, hours, minutes, seconds) {
            (0, 0, 0, _) => write!(f, "{}s", seconds),
            (0, 0, _, _) => write!(f, "{}m{}s", minutes, seconds),
            (0, _, _, _) => write!(f, "{}h{}m{}s", hours, minutes, seconds),
            (_, _, _, _) => write!(f, "{}d{}h{}m{}s", days, hours, minutes, seconds),
        }
    }
}

/// Runs a task that periodically collects statistics
pub struct StatisticsTracker {
    close_tx: tokio::sync::oneshot::Sender<()>,
    task_handle: tokio::task::JoinHandle<Statistics>,
}

impl StatisticsTracker {
    /// Close the collection task and return the collected [Statistics]
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
