use uuid::Uuid;
use kameo::prelude::*;
use crate::worker::Worker;
use crate::messages::{OrcaRequest, OrcaStates, ActorType, MatriarchMessage, MatriarchReply};

use std::pin::Pin;
use std::future::Future;
use tokio::task::JoinHandle;
use serde::Serialize;
use serde_json;
use async_trait::async_trait;
use crate::messages::{OrcaMessageRecipient, ErasedOrcaSpawner};

#[derive(Debug)]
struct InternalTaskCompleted {
    result_string: String,
}

pub struct Orca<R: Serialize + Send + Sync + 'static> {
    id: Uuid,
    pub name: String,
    state: OrcaStatesTypes,
    task: Option<Pin<Box<dyn Future<Output = R> + Send + Sync>>>,
    handle: Option<JoinHandle<()>>,
    pub params: Vec<String>,
    pub worker: Option<ActorRef<Worker>>,
}

impl<R: Serialize + Send + Sync + 'static> Orca<R> {
    pub fn new(name: String, task_future: Pin<Box<dyn Future<Output = R> + Send + Sync >>) -> Self {
        let id = Uuid::new_v4();
        Self {
            id, 
            state: OrcaStatesTypes::Registered(OrcaState::new(0, vec![])),
            params: vec![],
            worker: None,
            name: name,
            task: Some(task_future),
            handle: None,
        }
    }
    pub async fn submit(&self) {
        println!("Task submitted: {}", self.id);
        if let Some(worker) = &self.worker {
            let reply = worker.tell(MatriarchMessage {
                message: OrcaRequest {
                    task_id: self.id.to_string(),
                    new_state: OrcaStates::Submitted,
                },
                recipient: ActorType::Processor,
            }).await;
            println!("Task submitted reply: {:?}", reply);
        } else {
            eprintln!("Cannot submit task {}: worker not set", self.id);
        }
    }
}

impl<R: Serialize + Send + Sync + 'static> Actor for Orca<R> {
    type Args = Self;
    type Error = OrcaError;

    async fn on_start(mut args: Self::Args, actor_ref: ActorRef<Orca<R>>) -> Result<Self, OrcaError> {
        println!("Task starting: {}", args.id);
        if let Some(task_future) = args.task.take() {
            let self_ref = actor_ref.clone();
            let task_id = args.id.clone();
            let handle = tokio::spawn(async move {
                println!("Executing future for task {}", task_id);
                let result: R = task_future.await;
                println!("Future completed for task {}", task_id);

                let result_string = match serde_json::to_string(&result) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("Failed to serialize result for task {}: {}", task_id, e);
                        format!("Serialization Error: {}", e)
                    }
                };

                let completion_msg = InternalTaskCompleted { result_string };
                if let Err(e) = self_ref.tell(completion_msg).await { 
                    eprintln!("Failed to send completion message for task {}: {}", task_id, e);
                }
            });
            args.handle = Some(handle); 
        } else {
            eprintln!("Task {} starting without a future to run!", args.id);
            return Err(OrcaError(format!("Task {} created without a future", args.id)));
        }
        
        Ok(args)
    }

    async fn on_stop(&mut self, _actor_ref: WeakActorRef<Orca<R>>, _reason: ActorStopReason) -> Result<(), OrcaError> {
        println!("Task stopping: {}", self.id);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
        Ok(())
    }
}   

impl<R: Serialize + Send + Sync + 'static> Message<MatriarchMessage<OrcaRequest>> for Orca<R> {
    type Reply = MatriarchReply;

    async fn handle(
        &mut self,
        message: MatriarchMessage<OrcaRequest>,
        ctx: &mut Context<Orca<R>, MatriarchReply>,
    ) -> Self::Reply {
        println!("Task {} received MatriarchMessage: {:?}", self.id, message.message.new_state);
        match message.message.new_state {
            OrcaStates::Submitted => {
                println!("Handling Submitted state for Task {}", self.id);
                 match &self.state {
                     OrcaStatesTypes::Registered(_) => {
                         self.state = OrcaStatesTypes::Submitted(OrcaState { state: Submitted {} });
                         println!("Task {} state updated to Submitted", self.id);
                     }
                     _ => {
                         eprintln!("Task {} received Submitted request but was not in Registered state", self.id);
                     }
                 }
            }
            _ => {
                println!("Task {} ignoring MatriarchMessage state: {:?}", self.id, message.message.new_state);
            }
        }
        MatriarchReply { success: true }
    }
}

impl<R: Serialize + Send + Sync + 'static> Message<InternalTaskCompleted> for Orca<R> {
    type Reply = ();

    async fn handle(
        &mut self,
        message: InternalTaskCompleted,
        _ctx: &mut Context<Orca<R>, Self::Reply>,
    ) -> Self::Reply {
        println!("Task {} received internal completion", self.id);
        
        self.state = OrcaStatesTypes::Completed(OrcaState { 
            state: Completed { 
                params: self.params.clone(),
                result: message.result_string
            }
        });
        println!("Task {} state updated to Completed", self.id);
        
        // TODO: Notify worker or other actors if needed
    }
}

#[async_trait]
impl<R: Serialize + Send + Sync + 'static> OrcaMessageRecipient for ActorRef<Orca<R>> {
    async fn forward_matriarch_request(
        &self,
        message: MatriarchMessage<OrcaRequest>,
    ) -> Result<MatriarchReply, OrcaError> {
        self.ask(message).await.map_err(|e| OrcaError(format!("Kameo error: {}", e)))
    }
}

/// Helper struct to hold information needed to spawn a specific Orca<R>.
pub struct OrcaSpawner<R: Serialize + Send + Sync + 'static> {
    name: String,
    task_future: Pin<Box<dyn Future<Output = R> + Send + Sync >>,
}

impl<R: Serialize + Send + Sync + 'static> OrcaSpawner<R> {
     // Factory method for creating the spawner
    pub fn new(name: String, task_future: Pin<Box<dyn Future<Output = R> + Send + Sync >>) -> Self {
        Self { name, task_future }
    }
}

#[async_trait]
impl<R: Serialize + Send + Sync + 'static> ErasedOrcaSpawner for OrcaSpawner<R> {
    fn name(&self) -> String {
        self.name.clone()
    }

    async fn spawn_and_get_recipient(
        self: Box<Self>,
        worker_ref: ActorRef<Worker>
    ) -> Result<Box<dyn OrcaMessageRecipient>, OrcaError> {
        // Create the Orca instance
        let mut orca = Orca::new(self.name, self.task_future);
        // Set the worker reference before spawning
        orca.worker = Some(worker_ref);
        // Assume Orca::spawn returns ActorRef directly on success
        let actor_ref = Orca::spawn(orca); // Simplify spawn call
        // Box the resulting ActorRef as the trait object
        Ok(Box::new(actor_ref))
    }
}

#[derive(Debug)]
pub struct OrcaError(String);

impl std::fmt::Display for OrcaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}




#[derive(Debug)]
pub enum OrcaStatesTypes {
    Registered(OrcaState<Registered>),
    Submitted(OrcaState<Submitted>),
    Scheduled(OrcaState<Scheduled>),
    Running(OrcaState<Running>),
    Completed(OrcaState<Completed>),
}

impl std::fmt::Display for OrcaStatesTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl OrcaStatesTypes {
    pub fn to_string(&self) -> String {
        match self {
            OrcaStatesTypes::Registered(state) => "Registered".to_string(),
            OrcaStatesTypes::Submitted(state) => "Submitted".to_string(),
            OrcaStatesTypes::Scheduled(state) => "Scheduled".to_string(),
            OrcaStatesTypes::Running(state) => "Running".to_string(),
            OrcaStatesTypes::Completed(state) => "Completed".to_string(),  
        }
    }
}

#[derive(Debug)]
pub struct OrcaState<S> {
    state: S,
}

#[derive(Debug)]
pub struct Registered {
    pub max_retries: u32,
    pub args: Vec<String>,
}

impl OrcaState<Registered> {
    pub fn new(max_retries: u32, args: Vec<String>) -> Self {
        Self { state: Registered { max_retries, args } }
    }
}

#[derive(Debug)]
pub struct Submitted {
}

impl From<OrcaState<Registered>> for OrcaState<Submitted> {
    fn from(state: OrcaState<Registered>) -> Self {
        OrcaState { state: Submitted {} }
    }
}

#[derive(Debug)]
pub struct Scheduled {
    pub delay: u64,
}

impl From<OrcaState<Registered>> for OrcaState<Scheduled> {
    fn from(state: OrcaState<Registered>) -> Self {
        OrcaState { state: Scheduled { delay: 0 } }
    }
}

#[derive(Debug)]
pub struct Running {
}

impl From<OrcaState<Submitted>> for OrcaState<Running> {
    fn from(state: OrcaState<Submitted>) -> Self {
        OrcaState { state: Running {} }
    }
}

impl From<OrcaState<Scheduled>> for OrcaState<Running> {
    fn from(state: OrcaState<Scheduled>) -> Self {
        OrcaState { state: Running {} }
    }
}

#[derive(Debug)]
pub struct Completed {
    pub params: Vec<String>,
    pub result: String,
}

impl From<OrcaState<Running>> for OrcaState<Completed> {
    fn from(state: OrcaState<Running>) -> Self {
        OrcaState { state: Completed { params: vec![], result: String::new() } }
    }
}