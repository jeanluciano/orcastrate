use kameo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::task::RunState;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubmitTask {
    pub task_name: String,
    pub args: String,
} 
#[derive(Debug, Clone)]
pub struct RunTask {
    pub task_name: String,
    pub task_id: Uuid,
    pub args: String,
}
#[derive(Debug, Clone)]
pub struct ScheduleTask {
    pub task_name: String,
    pub scheduled_at: i64,
}
pub struct TaskCompleted {
    pub result_string: String,
}

#[derive(Reply, Debug, Clone)]
pub struct OrcaReply {
    pub success: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransitionState {
    pub task_name: String,
    pub task_id: Uuid,
    pub new_state: RunState,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Recipient {
    Orca,
    Processor,
}
