use kameo::prelude::*;
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use crate::task::OrcaError;
use async_trait::async_trait;
use crate::worker::Worker;



#[derive(Reply)]
pub struct MatriarchReply  {
    pub success: bool,
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
pub struct OrcaRequest {
    pub task_id: String,
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

/// Trait for message recipients (running Orca tasks) stored by the Worker.
#[async_trait]
pub trait OrcaMessageRecipient: Send + Sync {
    /// Handles forwarding of MatriarchMessage<OrcaRequest>.
    /// Assumes the reply MatriarchReply is needed. Use ask semantics.
    async fn forward_matriarch_request(
        &self,
        message: MatriarchMessage<OrcaRequest>,
    ) -> Result<MatriarchReply, OrcaError>;
    // Add other message types the Worker needs to forward here...
}

/// Trait for spawning Orca<R> actors without the Worker knowing R.
#[async_trait]
pub trait ErasedOrcaSpawner: Send + Sync {
    /// Returns the name of the task to be spawned.
    fn name(&self) -> String;

    /// Spawns the specific Orca<R> actor.
    /// Takes ownership of self and the Worker's ActorRef.
    /// Returns a Box<dyn OrcaMessageRecipient> for the spawned actor.
    async fn spawn_and_get_recipient(
        self: Box<Self>,
        worker_ref: ActorRef<Worker>
    ) -> Result<Box<dyn OrcaMessageRecipient>, OrcaError>; // Use OrcaError from task.rs
}
