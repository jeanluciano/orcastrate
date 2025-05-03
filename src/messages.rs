use crate::types::TaskData;
use kameo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Define StartTask message
#[derive(Debug, Clone)]
pub struct RunTask {
    pub task_name: String,
    pub task_id: Uuid,
}

#[derive(Debug, Clone)]
pub struct SubmitTask {
    pub task_name: String,
}

#[derive(Debug, Clone)]
pub struct ScheduleTask {
    pub task_name: String,
    pub scheduled_at: u64,
}

pub struct TaskCompleted {
    pub result_string: String,
}

#[derive(Reply, Debug, Clone)]
pub struct MatriarchReply {
    pub success: bool,
}

#[derive(Reply, Debug, Clone)]
pub struct GetOrcaFieldReply {
    pub field: TaskData,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MatriarchMessage<M> {
    pub message: M,
    pub recipient: ActorType,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum OrcaStates {
    Running,
    Completed,
    Failed,
    Submitted,
    Scheduled,
}

impl OrcaStates {
    pub fn from_string(s: &str) -> Option<OrcaStates> {
        match s {
            "Running" => Some(OrcaStates::Running),
            "Completed" => Some(OrcaStates::Completed),
            "Failed" => Some(OrcaStates::Failed),
            "Submitted" => Some(OrcaStates::Submitted),
            "Scheduled" => Some(OrcaStates::Scheduled),
            _ => None,
        }
    }
    pub fn display(&self) -> String {
        match self {
            OrcaStates::Running => "Running".to_string(),
            OrcaStates::Completed => "Completed".to_string(),
            OrcaStates::Failed => "Failed".to_string(),
            OrcaStates::Submitted => "Submitted".to_string(),
            OrcaStates::Scheduled => "Scheduled".to_string(),
        }
    }
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransitionState {
    pub task_name: String,
    pub task_id: Uuid,
    pub new_state: OrcaStates,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ActorType {
    Orca,
    Processor,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum MessageType {
    Worker,
    Processor,
}
