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

// Implement the Display trait
impl std::fmt::Display for OrcaStates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrcaStates::Running => write!(f, "Running"),
            OrcaStates::Completed => write!(f, "Completed"),
            OrcaStates::Failed => write!(f, "Failed"),
            OrcaStates::Submitted => write!(f, "Submitted"),
            OrcaStates::Scheduled => write!(f, "Scheduled"),
        }
    }
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
