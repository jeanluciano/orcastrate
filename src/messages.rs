use kameo::prelude::*;
use serde::{Serialize, Deserialize};
use crate::types::OrcaData;
use uuid::Uuid;

// Define StartTask message
#[derive(Debug, Clone)]
pub struct RunTask {
    pub task_name: String,
}
pub struct OrcaTaskCompleted {
    pub result_string: String,
}

#[derive(Reply)]
pub struct MatriarchReply  {
    pub success: bool,
}

#[derive(Reply, Debug, Clone)]
pub struct GetOrcaFieldReply {
    pub field: OrcaData,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MatriarchMessage<M> {
    pub message: M,
    pub recipient: ActorType,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum OrcaStates {
    Running,
    Completed,
    Failed,
    Submitted,
    Scheduled,
    Registered,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransitionState {
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

