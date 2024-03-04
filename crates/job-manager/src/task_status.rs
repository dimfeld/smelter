use std::{fmt::Display, sync::Arc};

use parking_lot::Mutex;

use crate::{SubtaskId, TaskErrorKind};

/// Information about a spawned task
#[derive(Debug, Clone)]
pub struct StatusUpdateSpawnedData {
    /// The ID of the task in the in the system that is hosting it
    pub runtime_id: String,
}

/// Information about a successful task
#[derive(Debug, Clone)]
pub struct StatusUpdateSuccessData {
    /// The raw output payload from the task. This should be a [WorkerOutput] object.
    pub output: Vec<u8>,
    /// If enabled, OS-level statistics from the task's process
    pub stats: Option<smelter_worker::stats::Statistics>,
}

/// Types of status updates
#[derive(Debug, Clone)]
pub enum StatusUpdateData {
    /// A task was spawned
    Spawned(StatusUpdateSpawnedData),
    /// A task was retried, with the given failure reason
    Retry(TaskErrorKind, String),
    /// A task failed and could not be retried
    Failed(TaskErrorKind, String),
    /// A task emitted a log message
    Log {
        /// True if the message was from stdout. False if it was from stderr
        stdout: bool,
        /// The log message
        message: String,
    },
    /// A task was cancelled
    Cancelled,
    /// A task finished successfully
    Success(StatusUpdateSuccessData),
}

impl StatusUpdateData {
    /// Apply a custom format for this status
    pub fn custom_format<'a>(
        &'a self,
        format: &'a StatusUpdateCustomFormatOptions,
    ) -> StatusUpdateDataDisplayCustom {
        StatusUpdateDataDisplayCustom {
            format: &format,
            data: self,
        }
    }
}

impl Display for StatusUpdateData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let verbose = StatusUpdateCustomFormatOptions::default();
        self.custom_format(&verbose).fmt(f)
    }
}

/// Verbosity levels for printing status fields
#[derive(Debug, Clone, Default)]
pub enum Verbosity {
    /// Print terse information and omit log messages
    Short,
    /// Print a useful amount of log information
    #[default]
    Standard,
    /// Print extra log information that is mostly useful for debugging
    Full,
}

/// Verbosity levels for printing status fields
#[derive(Debug, Clone, Default)]
pub struct StatusUpdateCustomFormatOptions {
    /// Verbosity level for [StatusUpdateData::Spawned]
    pub spawned: Verbosity,
    /// Verbosity level for [StatusUpdateData::Retry]
    pub retry: Verbosity,
    /// Verbosity level for [StatusUpdateData::Failed]
    pub failed: Verbosity,
    /// Verbosity level for [StatusUpdateData::Log]
    pub log: Verbosity,
    /// Verbosity level for [StatusUpdateData::Success]
    pub success: Verbosity,
}

/// A formatter for a [StatusUpdateData] with custom levels of [Verbosity] for each field
pub struct StatusUpdateDataDisplayCustom<'a> {
    /// The verbosity settings to use
    pub format: &'a StatusUpdateCustomFormatOptions,
    /// The data to print
    pub data: &'a StatusUpdateData,
}

impl Display for StatusUpdateDataDisplayCustom<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.data {
            StatusUpdateData::Spawned(data) => match self.format.spawned {
                Verbosity::Short => write!(f, "Spawned"),
                Verbosity::Standard => write!(f, "Spawned ID {}", data.runtime_id),
                Verbosity::Full => write!(f, "Spawned with ID {}", data.runtime_id),
            },
            StatusUpdateData::Retry(kind, message) => match self.format.retry {
                Verbosity::Short => write!(f, "Retry"),
                Verbosity::Standard => write!(f, "Retry: {}", kind),
                Verbosity::Full => write!(f, "Retry: {}", message),
            },
            StatusUpdateData::Failed(kind, message) => match self.format.failed {
                Verbosity::Short => write!(f, "Retry"),
                Verbosity::Standard => write!(f, "Retry: {}", kind),
                Verbosity::Full => write!(f, "Retry: {}", message),
            },
            StatusUpdateData::Log { message, stdout } => {
                let dest = if *stdout { "Stdout" } else { "Stderr" };
                match self.format.log {
                    Verbosity::Short => Ok(()),
                    Verbosity::Standard => write!(f, "{dest}: {}", message),
                    Verbosity::Full => write!(f, "Log {dest}: {}", message),
                }
            }
            StatusUpdateData::Cancelled => write!(f, "Cancelled"),
            StatusUpdateData::Success(data) => match self.format.success {
                Verbosity::Short => write!(f, "Success"),
                Verbosity::Standard => {
                    write!(f, "Success")?;
                    if let Some(stats) = &data.stats {
                        write!(f, " ({stats})")?;
                    }
                    Ok(())
                }
                Verbosity::Full => {
                    write!(f, "Success")?;
                    if let Some(stats) = &data.stats {
                        write!(f, " ({stats})")?;
                    }
                    write!(f, " Payload: {}", String::from_utf8_lossy(&data.output))?;
                    Ok(())
                }
            },
        }
    }
}

/// A task status update
#[derive(Debug, Clone)]
pub struct StatusUpdateItem {
    /// The ID of the task
    pub task_id: SubtaskId,
    /// The timestamp of the update
    pub timestamp: time::OffsetDateTime,
    /// The data of the update
    pub data: StatusUpdateData,
}

impl StatusUpdateItem {
    /// Create a custom formatter for this item
    pub fn custom_format<'a>(
        &'a self,
        format: &'a StatusUpdateCustomFormatOptions,
    ) -> StatusUpdateItemCustomFormat {
        StatusUpdateItemCustomFormat { format, item: self }
    }
}

impl StatusUpdateItem {
    /// Write timestamp and task ID from the item, without the data.
    pub fn write_header(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let time = self.timestamp.time();
        write!(
            f,
            "{date} {hour:02}:{minute:02}:{second:02}.{ms:03} {task_id}: ",
            date = self.timestamp.date(),
            hour = time.hour(),
            minute = time.minute(),
            second = time.second(),
            ms = time.millisecond(),
            task_id = self.task_id,
        )
    }
}

impl Display for StatusUpdateItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.write_header(f)?;
        write!(f, "{}", self.data)
    }
}

/// A formatter for a [StatusUpdateItem] with custom levels of [Verbosity] for each field in the
/// [StatusUpdateData]
pub struct StatusUpdateItemCustomFormat<'a> {
    format: &'a StatusUpdateCustomFormatOptions,
    item: &'a StatusUpdateItem,
}

impl<'a> Display for StatusUpdateItemCustomFormat<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.item.write_header(f)?;

        let data_format = StatusUpdateDataDisplayCustom {
            format: self.format,
            data: &self.item.data,
        };
        write!(f, "{data_format}")
    }
}

/// A wrapper around [StatusCollector] that only sends log messages
#[derive(Clone)]
pub struct LogSender {
    task_id: SubtaskId,
    sender: StatusSender,
}

impl LogSender {
    /// Send a log from a task
    pub fn send(&self, stdout: bool, message: String) {
        self.sender
            .add(self.task_id, StatusUpdateData::Log { stdout, message });
    }
}

impl std::fmt::Debug for LogSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogCollector")
            .field("task_id", &self.task_id)
            .finish()
    }
}

/// A channel that can be used to send status updates from the job manager
#[derive(Clone)]
pub struct StatusSender {
    tx: flume::Sender<StatusUpdateItem>,
    keep_logs: bool,
}

impl StatusSender {
    /// Create a new StatusSender and return the channel that will receive messages.
    pub fn new(keep_logs: bool) -> (StatusSender, flume::Receiver<StatusUpdateItem>) {
        let (tx, rx) = flume::unbounded();
        let sender = StatusSender { tx, keep_logs };
        (sender, rx)
    }

    /// Send a status update.
    pub fn add(&self, task_id: SubtaskId, data: impl Into<StatusUpdateData>) {
        let data = data.into();
        if !self.keep_logs && matches!(&data, StatusUpdateData::Log { .. }) {
            return;
        }

        self.tx
            .send(StatusUpdateItem {
                task_id,
                timestamp: time::OffsetDateTime::now_utc(),
                data,
            })
            .ok();
    }

    /// Create a copy of this collector that only sends logs
    pub fn as_log_sender(&self, task_id: SubtaskId) -> Option<LogSender> {
        self.keep_logs.then(|| LogSender {
            task_id,
            sender: self.clone(),
        })
    }
}

/// Collects status messages into a buffer which can be drained on command
#[derive(Clone)]
pub struct StatusCollector {
    /// Send status updates into this [StatusCollector]
    pub sender: StatusSender,
    buffer: Arc<Mutex<Vec<StatusUpdateItem>>>,
}

impl StatusCollector {
    /// Create a new StatusCollector and start a task to buffer the messages.
    pub fn new(estimated_num_tasks: usize, keep_logs: bool) -> Self {
        let (sender, rx) = StatusSender::new(keep_logs);
        let buffer = Arc::new(Mutex::new(Vec::with_capacity(estimated_num_tasks * 3 / 2)));
        let buf = buffer.clone();

        tokio::task::spawn(async move {
            while let Ok(item) = rx.recv_async().await {
                if keep_logs || !matches!(&item.data, StatusUpdateData::Log { .. }) {
                    let mut items = buf.lock();
                    items.push(item);
                }
            }
        });

        StatusCollector { sender, buffer }
    }

    /// Send a status update
    pub fn add(&self, task_id: SubtaskId, data: impl Into<StatusUpdateData>) {
        self.sender.add(task_id, data);
    }

    /// Create a copy of this collector that only sends logs
    pub fn as_log_sender(&self, task_id: SubtaskId) -> Option<LogSender> {
        self.sender.as_log_sender(task_id)
    }

    /// Return a copy of the status updates, starting from the beginning.
    pub fn read(&self) -> Vec<StatusUpdateItem> {
        self.buffer.lock().clone()
    }

    /// Return a copy of the status updates, starting from the requested index.
    pub fn read_from(&self, start: usize) -> Vec<StatusUpdateItem> {
        let items = self.buffer.lock();
        let start = start.min(items.len());
        items[start..].to_vec()
    }

    /// Return the status updates and clear the current buffer.
    pub async fn take(&self) -> Vec<StatusUpdateItem> {
        let mut items = self.buffer.lock();
        let next_len = std::cmp::max(std::cmp::min(items.len(), 1024), 16);
        let items = std::mem::replace(&mut *items, Vec::with_capacity(next_len));

        items
    }
}
