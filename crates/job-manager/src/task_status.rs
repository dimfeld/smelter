use std::sync::Arc;

use error_stack::Report;
use tokio::sync::oneshot;

use crate::spawn::TaskError;

#[derive(Debug, Clone)]
pub enum StatusUpdateData {
    Spawned(String),
    // Report is not clonable so just stick it on the heap.
    Retry((usize, Arc<Report<TaskError>>)),
    Failed(Arc<Report<TaskError>>),
    Success(String),
}

/// This structure can be passed to [StatusCollector::add] and does some of the conversions
/// for you, such as wrapping the Report in an Arc.
pub enum StatusUpdateInput {
    Spawned(String),
    Retry((usize, Report<TaskError>)),
    Failed(Report<TaskError>),
    Success(String),
}

impl From<StatusUpdateInput> for StatusUpdateData {
    fn from(value: StatusUpdateInput) -> Self {
        match value {
            StatusUpdateInput::Spawned(s) => StatusUpdateData::Spawned(s),
            StatusUpdateInput::Retry((i, report)) => StatusUpdateData::Retry((i, Arc::new(report))),
            StatusUpdateInput::Failed(report) => StatusUpdateData::Failed(Arc::new(report)),
            StatusUpdateInput::Success(s) => StatusUpdateData::Success(s),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StatusUpdateItem {
    stage: String,
    task_index: usize,
    timestamp: time::OffsetDateTime,
    data: StatusUpdateData,
}

pub enum StatusUpdateOp {
    Item(StatusUpdateItem),
    Read(tokio::sync::oneshot::Sender<Vec<StatusUpdateItem>>),
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
            let mut items = Vec::with_capacity(estimated_num_tasks * 5 / 2);
            while let Ok(op) = rx.recv_async().await {
                match op {
                    StatusUpdateOp::Item(item) => {
                        items.push(item);
                    }
                    StatusUpdateOp::Read(tx) => {
                        tx.send(items.clone()).ok();
                    }
                }
            }
        });

        collector
    }

    pub fn add(&self, stage: String, task_index: usize, data: impl Into<StatusUpdateData>) {
        self.tx
            .send(StatusUpdateOp::Item(StatusUpdateItem {
                stage,
                task_index,
                timestamp: time::OffsetDateTime::now_utc(),
                data: data.into(),
            }))
            .ok();
    }

    pub async fn read(&self) -> Vec<StatusUpdateItem> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(StatusUpdateOp::Read(tx)).ok();
        rx.await.unwrap_or_default()
    }
}
