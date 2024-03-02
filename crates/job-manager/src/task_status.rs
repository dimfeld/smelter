use tokio::sync::oneshot;

use crate::SubtaskId;

#[derive(Debug, Clone)]
pub struct StatusUpdateSpawnedData {
    pub runtime_id: String,
}

#[derive(Debug, Clone)]
pub struct StatusUpdateSuccessData {
    pub output: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum StatusUpdateData {
    Spawned(StatusUpdateSpawnedData),
    Retry(String),
    Failed(String),
    Log { stdout: bool, message: String },
    Cancelled,
    Success(StatusUpdateSuccessData),
}

#[derive(Debug, Clone)]
pub struct StatusUpdateItem {
    pub task_id: SubtaskId,
    pub timestamp: time::OffsetDateTime,
    pub data: StatusUpdateData,
}

pub enum StatusUpdateOp {
    Item(StatusUpdateItem),
    ReadFrom((tokio::sync::oneshot::Sender<Vec<StatusUpdateItem>>, usize)),
    Take(tokio::sync::oneshot::Sender<Vec<StatusUpdateItem>>),
}

/// A wrapper around [StatusCollector] that only sends log messages
#[derive(Clone)]
pub struct LogSender {
    task_id: SubtaskId,
    sender: StatusSender,
}

impl LogSender {
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

#[derive(Clone)]
pub struct StatusSender {
    tx: flume::Sender<StatusUpdateOp>,
    keep_logs: bool,
}

impl StatusSender {
    /// Create a new StatusSender and return the channel that will receive messages.
    pub fn new(keep_logs: bool) -> (StatusSender, flume::Receiver<StatusUpdateOp>) {
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
            .send(StatusUpdateOp::Item(StatusUpdateItem {
                task_id,
                timestamp: time::OffsetDateTime::now_utc(),
                data,
            }))
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

#[derive(Clone)]
pub struct StatusCollector {
    pub sender: StatusSender,
}

impl StatusCollector {
    /// Create a new StatusCollector and start a task to buffer the messages.
    pub fn new(estimated_num_tasks: usize, keep_logs: bool) -> Self {
        let (sender, rx) = StatusSender::new(keep_logs);

        tokio::task::spawn(async move {
            let mut next_vec_size = estimated_num_tasks * 3 / 2;
            let mut items = Vec::with_capacity(next_vec_size);
            while let Ok(op) = rx.recv_async().await {
                match op {
                    StatusUpdateOp::Item(item) => {
                        if keep_logs || !matches!(&item.data, StatusUpdateData::Log { .. }) {
                            items.push(item);
                        }
                    }
                    StatusUpdateOp::ReadFrom((tx, start)) => {
                        let start = start.min(items.len());
                        tx.send(items[start..].to_vec()).ok();
                    }
                    StatusUpdateOp::Take(tx) => {
                        if items.len() > next_vec_size {
                            next_vec_size = 16;
                        } else {
                            next_vec_size = std::cmp::max(16, next_vec_size - items.len());
                        }

                        let items =
                            std::mem::replace(&mut items, Vec::with_capacity(next_vec_size));
                        tx.send(items).ok();
                    }
                }
            }
        });

        StatusCollector { sender }
    }

    pub fn add(&self, task_id: SubtaskId, data: impl Into<StatusUpdateData>) {
        self.sender.add(task_id, data);
    }

    /// Create a copy of this collector that only sends logs
    pub fn as_log_sender(&self, task_id: SubtaskId) -> Option<LogSender> {
        self.sender.as_log_sender(task_id)
    }

    /// Return a copy of the status updates, starting from the beginning.
    pub async fn read(&self) -> Vec<StatusUpdateItem> {
        let (tx, rx) = oneshot::channel();
        self.sender.tx.send(StatusUpdateOp::ReadFrom((tx, 0))).ok();
        rx.await.unwrap_or_default()
    }

    /// Return a copy of the status updates, starting from the requested index.
    pub async fn read_from(&self, start: usize) -> Vec<StatusUpdateItem> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .tx
            .send(StatusUpdateOp::ReadFrom((tx, start)))
            .ok();
        rx.await.unwrap_or_default()
    }

    /// Return the status updates and clear the current buffer.
    pub async fn take(&self) -> Vec<StatusUpdateItem> {
        let (tx, rx) = oneshot::channel();
        self.sender.tx.send(StatusUpdateOp::Take(tx)).ok();
        rx.await.unwrap_or_default()
    }
}
