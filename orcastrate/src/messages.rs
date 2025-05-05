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
    pub scheduled_at: i64,
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



#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Recipient {
    Orca,
    Processor,
}
