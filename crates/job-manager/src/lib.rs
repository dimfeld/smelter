pub mod spawn;

pub struct TaskDefWithOutput<DEF: Send> {
    task_def: DEF,
    output_location: String,
}

#[async_trait::async_trait]
pub trait TaskType {
    type TaskDef: Send;
    type MapTaskDef: serde::Serialize + Send;
    type ReducerTaskDef: serde::Serialize + Send;

    /// Given a single task definition, create a list of map tasks to run.
    async fn create_map_task(&self, task_def: &Self::TaskDef) -> Vec<Self::MapTaskDef>;

    /// Create reducer tasks to run on the output of the map tasks.
    async fn create_top_level_reducers(
        &self,
        task_def: &Self::TaskDef,
        subtasks: &[TaskDefWithOutput<Self::MapTaskDef>],
    ) -> Vec<Self::ReducerTaskDef>;

    /// Create reducers that that can run on the output of previous reducers. Each invocation
    /// of this function receives the list of [ReducerTaskDef]s for the previous set of reducers.
    /// If there are no further reducers to run, return an empty vector.
    async fn create_intermediate_reducers(
        &self,
        task_def: &Self::TaskDef,
        reducers: &[TaskDefWithOutput<Self::ReducerTaskDef>],
    ) -> Vec<Self::ReducerTaskDef> {
        Vec::new()
    }
}

pub struct JobManager {}

#[cfg(test)]
mod tests {
    use super::*;
}
