use std::sync::Arc;

use error_stack::Report;
use tokio::sync::oneshot;

use crate::{manager::SubtaskId, spawn::TaskError};

#[derive(Debug, Clone)]
pub enum StatusUpdateData {
    Spawned(String),
    // Report is not clonable so just stick it on the heap.
    Retry(Arc<Report<TaskError>>),
    Failed(Arc<Report<TaskError>>),
    Cancelled,
    Success(String),
}

/// This structure can be passed to [StatusCollector::add] and does some of the conversions
/// for you, such as wrapping the Report in an Arc.
pub enum StatusUpdateInput {
    Spawned(String),
    Retry(Report<TaskError>),
    Failed(Report<TaskError>),
    Cancelled,
    Success(String),
}

impl From<StatusUpdateInput> for StatusUpdateData {
    fn from(value: StatusUpdateInput) -> Self {
        match value {
            StatusUpdateInput::Spawned(s) => StatusUpdateData::Spawned(s),
            StatusUpdateInput::Retry(report) => StatusUpdateData::Retry(Arc::new(report)),
            StatusUpdateInput::Failed(report) => StatusUpdateData::Failed(Arc::new(report)),
            StatusUpdateInput::Success(s) => StatusUpdateData::Success(s),
            StatusUpdateInput::Cancelled => StatusUpdateData::Cancelled,
        }
    }
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

#[derive(Clone)]
pub struct StatusCollector {
    tx: flume::Sender<StatusUpdateOp>,
}

impl StatusCollector {
    pub fn new(estimated_num_tasks: usize) -> Self {
        let (tx, rx) = flume::unbounded();
        let collector = StatusCollector { tx };

        tokio::task::spawn(async move {
            let mut next_vec_size = estimated_num_tasks * 3 / 2;
            let mut items = Vec::with_capacity(next_vec_size);
            while let Ok(op) = rx.recv_async().await {
                match op {
                    StatusUpdateOp::Item(item) => {
                        println!("item: {:?}", item);
                        items.push(item);
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

        collector
    }

    pub fn add(&self, task_id: SubtaskId, data: impl Into<StatusUpdateData>) {
        self.tx
            .send(StatusUpdateOp::Item(StatusUpdateItem {
                task_id,
                timestamp: time::OffsetDateTime::now_utc(),
                data: data.into(),
            }))
            .ok();
    }

    pub async fn read(&self) -> Vec<StatusUpdateItem> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(StatusUpdateOp::ReadFrom((tx, 0))).ok();
        rx.await.unwrap_or_default()
    }

    pub async fn read_from(&self, start: usize) -> Vec<StatusUpdateItem> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(StatusUpdateOp::ReadFrom((tx, start))).ok();
        rx.await.unwrap_or_default()
    }

    pub async fn take(&self) -> Vec<StatusUpdateItem> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(StatusUpdateOp::Take(tx)).ok();
        rx.await.unwrap_or_default()
    }
}
