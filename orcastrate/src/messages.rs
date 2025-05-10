use crate::task::RunState;
use kameo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::task::CachePolicy;
pub const TASK_COMPLETION_TOPIC: &str = "task-completions";

#[derive(Debug, Clone)]
pub struct CreateRun {
    pub task_name: String,
    pub args: Option<String>,
    pub delay: Option<i64>,
    pub cache_policy: CachePolicy,
    pub signature: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateSeer {
    pub state: RunState,
}

#[derive(Debug, Clone)]
pub struct GetResultById {
    pub task_id: Uuid,
}
pub struct HandleResult {
    pub timeout: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct Script {
    pub id: Uuid,
    pub task_name: String,
    pub args: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ScheduledScript {
    pub id: Uuid,
    pub task_name: String,
    pub args: Option<String>,
    pub submit_at: i64,
}

#[derive(Reply, Debug, Clone)]
pub struct OrcaReply {
    pub success: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransitionState {
    pub task_name: String,
    pub task_id: Uuid,
    pub args: String,
    pub new_state: RunState,
    pub result: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ListenForResult {
    pub task_id: Uuid,
}

