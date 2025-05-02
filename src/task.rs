use uuid::Uuid;
use kameo::prelude::*;
use crate::worker::Worker;
use crate::messages::*;
use tokio::task::JoinHandle;
use serde_json;
use async_trait::async_trait;
use crate::types::*;
use tracing::info;
pub struct Orca<R: OrcaTaskResult> {
    pub id: Uuid,
    pub name: String,
    state: OrcaStatesTypes,
    task: Option<TaskFuture<R>>,
    handle: Option<JoinHandle<()>>,
    pub params: Vec<String>,
    pub worker: Option<ActorRef<Worker>>,
}

impl<R: OrcaTaskResult> Orca<R> {
    pub fn new(id: Uuid, name: String, task_future: TaskFuture<R>) -> Self {
        let state = OrcaState::<Submitted>::new(0, vec![]);
        Self {
            id, 
            state: OrcaStatesTypes::Submitted(state),
            params: vec![],
            worker: None,
            name: name,
            task: Some(task_future),
            handle: None,
        }
    }
    pub async fn submit(&self) {
        if let Some(worker) = &self.worker {
            let result = worker.tell(MatriarchMessage {
                message: TransitionState {
                    task_name: self.name.clone(),
                    task_id: self.id,
                    new_state: OrcaStates::Submitted,
                },
                recipient: ActorType::Processor,
            }).await;
            match result {
                Ok(_) => {
                    println!("Initial submit request sent successfully for Task {}", self.id);
                }
                Err(e) => {
                    eprintln!("Failed to send initial submit request for Task {}: {}", self.id, e);
                }
            }
        } else {
            eprintln!("Cannot request initial submit for task {}: worker not set", self.id);
        }
    }

    async fn transition_to_state(&mut self, new_state_request: OrcaStates) -> Result<(), OrcaError> {
        

        let next_state_type = match (&self.state, new_state_request) {
            (OrcaStatesTypes::Submitted(_), OrcaStates::Running) => {
                 Ok(OrcaStatesTypes::Running(OrcaState { state: Running {} }))
            }
            (OrcaStatesTypes::Scheduled(_), OrcaStates::Running) => {
                Ok(OrcaStatesTypes::Running(OrcaState { state: Running {} }))
            }
             (OrcaStatesTypes::Running(_), OrcaStates::Completed) => {
                Ok(OrcaStatesTypes::Completed(OrcaState { 
                    state: Completed { 
                        params: self.params.clone(), 
                        result: "".to_string() 
                    } 
                 }))
             }
            (current, target) => {
                let err_msg = format!("Invalid state transition from {} to {:?} for Task {}", current, target, self.id);
                eprintln!("{}", err_msg);
                Err(OrcaError(err_msg))
            }
        };

        match next_state_type {
            Ok(next_state) => {
                self.state = next_state;
                let current_state_enum = self.state.to_orca_state_enum();

                info!("{}:{} → → → {}:{}", self.id, self.state.to_string(),self.id, current_state_enum.to_string());
                if let Some(worker) = &self.worker {
                    let _ = worker.tell(MatriarchMessage {
                        message: TransitionState {
                            task_name: self.name.clone(),
                            task_id: self.id,
                            new_state: current_state_enum,
                        },
                        recipient: ActorType::Processor,
                    }).await.map_err(|e| {
                         eprintln!("Failed to notify worker of state change for Task {}: {}", self.id, e);
                         OrcaError(format!("Worker notification failed: {}", e))
                    });
                } else {
                     eprintln!("Task {} cannot notify worker: worker not set", self.id);
                }
                 Ok(())
            }
             Err(e) => Err(e),
        }
    }
}



impl<R: OrcaTaskResult> Actor for Orca<R> {
    type Args = Self;
    type Error = OrcaError;

    async fn on_start(mut args: Self::Args, actor_ref: ActorRef<Orca<R>>) -> Result<Self, OrcaError> {
    
        if let Some(task_future) = args.task.take() {
            let self_ref = actor_ref.clone();
            let task_id = args.id.clone();
            let handle = tokio::spawn(async move {
                info!("Executing future for task {}", task_id);
                let result: R = task_future.await;
                info!("Future completed for task {}", task_id);

                let result_string = match serde_json::to_string(&result) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("Failed to serialize result for task {}: {}", task_id, e);
                        format!(r#"{{"error": "Serialization Error: {}"}}"#, e.to_string().replace('"', "\\\""))
                    }
                };

                let completion_msg = OrcaTaskCompleted { result_string };
                if let Err(e) = self_ref.ask(completion_msg).await { 
                    eprintln!("Failed to send internal completion message for task {}: {}", task_id, e);
                }
            });
            args.handle = Some(handle); 
        } else {
            eprintln!("Task {} starting without a future to run!", args.id);
        }
        
        Ok(args)
    }

    async fn on_stop(&mut self, _actor_ref: WeakActorRef<Orca<R>>, _reason: ActorStopReason) -> Result<(), OrcaError> {
        info!("Task stopping: {}", self.id);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
        Ok(())
    }
}



struct GetField {
    pub field: OrcaData,
}
impl<R: OrcaTaskResult> Message<GetField> for Orca<R> {
    type Reply = GetOrcaFieldReply;

    async fn handle(
        &mut self,
        _message: GetField,
        _ctx: &mut Context<Orca<R>, Self::Reply>,
    ) -> Self::Reply {
        match _message.field {
            OrcaData::Id(_id) => GetOrcaFieldReply { field: OrcaData::Id(self.id) },
            OrcaData::Name(_name) => GetOrcaFieldReply { field: OrcaData::Name(self.name.clone()) },
            OrcaData::State(_state) => GetOrcaFieldReply { field: OrcaData::State(self.state.clone()) },
            OrcaData::Params(_params) => GetOrcaFieldReply { field: OrcaData::Params(self.params.clone()) },
        }
    }
}

impl<R: OrcaTaskResult> Message<MatriarchMessage<TransitionState>> for Orca<R> {
    type Reply = MatriarchReply;

    async fn handle(
        &mut self,
        message: MatriarchMessage<TransitionState>,
        _ctx: &mut Context<Orca<R>, MatriarchReply>,
    ) -> Self::Reply {        
        match self.transition_to_state(message.message.new_state).await {
            Ok(_) => MatriarchReply { success: true },
            Err(e) => {
                eprintln!("State transition failed for Task {}: {}", self.id, e);
                MatriarchReply { success: false }
            }
        }
    }
}

impl<R: OrcaTaskResult> Message<OrcaTaskCompleted> for Orca<R> {
    type Reply = ();

    async fn handle(
        &mut self,
        message: OrcaTaskCompleted,
        _ctx: &mut Context<Orca<R>, Self::Reply>,
    ) -> Self::Reply {
        match self.transition_to_state(OrcaStates::Completed).await {
            Ok(_) => {
                if let OrcaStatesTypes::Completed(ref mut completed_state) = self.state {
                     completed_state.state.result = message.result_string;
                     info!("{}:{} → → → {}:{}", self.id, self.state.to_string(),self.id, OrcaStates::Completed.to_string());
                } else {
                     eprintln!("Task {} is not in Completed state after successful transition attempt!", self.id);
                }
            }
             Err(e) => {
                 eprintln!("Failed to transition Task {} to Completed state: {}", self.id, e);
             }
        }
    }
}

#[async_trait]
impl<R: OrcaTaskResult> SpawnedOrca for ActorRef<Orca<R>> {
    async fn forward_matriarch_request(
        &self,
        message: MatriarchMessage<TransitionState>,
    ) -> Result<MatriarchReply, OrcaError> {
        self.ask(message).await.map_err(|e| OrcaError(format!("Kameo error: {}", e)))
    }
    async fn get_id(&self) -> Result<Uuid, OrcaError> {
        let message = GetField { field: OrcaData::Id(Uuid::nil()) }; 
        let reply = self.ask(message).await
            .map_err(|e| OrcaError(format!("Kameo ask error: {}", e)))?;
        
        match reply.field {
            OrcaData::Id(id) => Ok(id),
            _ => Err(OrcaError("Unexpected reply type when asking for Id".to_string())),
        }
    }

    async fn get_field(&self, field: OrcaData) -> Result<OrcaData, OrcaError> {
        let message = GetField { field };
        let reply = self.ask(message).await
            .map_err(|e| OrcaError(format!("Kameo ask error: {}", e)))?;
        Ok(reply.field)
    }
}
#[async_trait]
pub trait SpawnedOrca: Send + Sync {
    async fn forward_matriarch_request(
        &self,
        message: MatriarchMessage<TransitionState>,
    ) -> Result<MatriarchReply, OrcaError>;
    async fn get_id(&self) -> Result<Uuid, OrcaError>;
    async fn get_field(&self, field: OrcaData) -> Result<OrcaData, OrcaError>;

}


#[async_trait]
pub trait OrcaSpawner: Send + Sync {
    fn name(&self) -> String;
    /// Spawns the specific Orca<R> actor.
    /// Takes ownership of self and the Worker's ActorRef.
    /// Returns a Box<dyn OrcaMessageRecipient> for the spawned actor.
    async fn spawn_and_get_orca(
        self: Box<Self>,
        id: Uuid,
        worker_ref: ActorRef<Worker>
    ) -> Result<Box<dyn SpawnedOrca>, OrcaError>; // Use OrcaError from task.rs
}

/// Helper struct to hold information needed to spawn a specific Orca<R>.
pub struct RegisteredTask<R: OrcaTaskResult> {
    name: String,
    task_future: TaskFuture<R>,
}

impl<R: OrcaTaskResult> RegisteredTask<R> {
    pub fn new(name: String, task_future: TaskFuture<R>) -> Self {
        Self { name, task_future }
    }
}

#[async_trait]
impl<R: OrcaTaskResult> OrcaSpawner for RegisteredTask<R> {
    fn name(&self) -> String {
        self.name.clone()
    }

    async fn spawn_and_get_orca(
        self: Box<Self>,
        id: Uuid,
        worker_ref: ActorRef<Worker>,

    ) -> Result<Box<dyn SpawnedOrca>, OrcaError> {
        let mut orca = Orca::new(id,self.name, self.task_future);
        // Set the worker reference before spawning
        orca.worker = Some(worker_ref);
        // Assume Orca::spawn returns ActorRef directly on success
        let actor_ref = Orca::spawn(orca); 
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

#[derive(Debug,Clone)]
pub enum OrcaStatesTypes {
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
            OrcaStatesTypes::Submitted(_state) => "Submitted".to_string(),
            OrcaStatesTypes::Scheduled(_state) => "Scheduled".to_string(),
            OrcaStatesTypes::Running(_state) => "Running".to_string(),
            OrcaStatesTypes::Completed(_state) => "Completed".to_string(),  
        }
    }
    pub fn to_orca_state_enum(&self) -> OrcaStates {
        match self {
            OrcaStatesTypes::Submitted(_) => OrcaStates::Submitted,
            OrcaStatesTypes::Scheduled(_) => OrcaStates::Scheduled,
            OrcaStatesTypes::Running(_) => OrcaStates::Running,
            OrcaStatesTypes::Completed(_) => OrcaStates::Completed,
        }
    }
}

#[derive(Debug,Clone)]
pub struct OrcaState<S> {
    state: S,
}


#[derive(Debug,Clone)]
pub struct Submitted {
    pub max_retries: u32,
    pub args: Vec<String>,
}

impl OrcaState<Submitted> {
    pub fn new(max_retries: u32, args: Vec<String>) -> Self {
        Self { state: Submitted { max_retries, args } }
    }
}

#[derive(Debug,Clone)]
pub struct Scheduled {
    pub delay: u64,
}

#[derive(Debug,Clone)]
pub struct Running {
}

#[derive(Debug,Clone)]
pub struct Completed {
    pub params: Vec<String>,
    pub result: String,
}