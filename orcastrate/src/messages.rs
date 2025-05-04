use crate::types::TaskData;
use kameo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::task::RunState;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubmitTaskArgs {
    pub task_name: String,
    pub args: String,
    // Add other fields like options (delay, retries) here later if needed
} 


#[derive(Debug, Clone)]
pub struct RunTask {
    pub task_name: String,
    pub task_id: Uuid,
    pub args: String,
}

#[derive(Debug, Clone,Serialize, Deserialize)]
pub struct SubmitTask {
    pub task_name: String,
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

#[derive(Reply, Debug, Clone)]
pub struct GetOrcaFieldReply {
    pub field: TaskData,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrcaMessage<M> {
    pub message: M,
    pub recipient: Recipient,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum MessageType {
    Worker,
    Processor,
}
