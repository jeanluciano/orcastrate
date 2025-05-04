use crate::messages::*;
use crate::types::*;
use crate::worker::Worker;
use async_trait::async_trait;
use kameo::Actor;
use kameo::prelude::{ActorRef, Context, Message, WeakActorRef, ActorStopReason};
use serde_json;
use tokio::task::JoinHandle;
use tracing::info;
use uuid::Uuid;
use std::future::Future;
use std::pin::Pin;
use serde::{Serialize, Deserialize};
use crate::processors::redis::Processor;
use crate::types::TaskResult;

pub type TaskFuture = Pin<Box<dyn Future<Output = Result<String, String>> + Send>>;
pub type TaskRunnerFn = fn(String) -> Result<TaskFuture, OrcaError>;

// Define StaticTaskDefinition (used by the macro)
// Needs to derive inventory::Collect
#[derive(Clone)]
pub struct StaticTaskDefinition {
    pub task_name: &'static str,
    pub runner: TaskRunnerFn,
}

inventory::collect!(StaticTaskDefinition); 


pub struct RegisteredTask<R: TaskResult> {
    pub name: String,
    pub task_future: TaskFutureGen<R>,
    pub worker_ref: ActorRef<Worker>,
}

impl<R: TaskResult> RegisteredTask<R> {
    pub fn new(name: String, task_future: TaskFutureGen<R>, worker_ref: ActorRef<Worker>) -> Self {
        Self { name, task_future, worker_ref }
    }
}

#[async_trait]
pub trait TaskRunner: Send + Sync {
    fn name(&self) -> String;
    async fn run(
        self: Box<Self>,
        id: Uuid,
        worker_ref: ActorRef<Worker>,
    ) -> Result<Box<dyn Run>, OrcaError>;
}

#[async_trait]
impl<R: TaskResult> TaskRunner for RegisteredTask<R> {
    fn name(&self) -> String {
        self.name.clone()
    }
    async fn run(
        self: Box<Self>,
        id: Uuid,
        worker_ref: ActorRef<Worker>,
    ) -> Result<Box<dyn Run>, OrcaError> {
        let orca = TaskRun::new(id, self.name, worker_ref, Some(self.task_future));
        let actor_ref = TaskRun::spawn(orca);
        Ok(Box::new(actor_ref))
    }
}

// TaskRun is the main actor that runs the task.
pub struct TaskRun<R: TaskResult> {
    pub id: Uuid,
    pub name: String,
    pub future: Option<TaskFutureGen<R>>,
    state: RunState,
    handle: Option<JoinHandle<()>>,
    pub worker: ActorRef<Worker>,
}

impl<R: TaskResult> TaskRun<R> {
    pub fn new(
        id: Uuid,
        name: String,
        worker: ActorRef<Worker>,
        future: Option<TaskFutureGen<R>>,
    ) -> Self {
        let state = Submitted::new(0, "".to_string());
        Self {
            id,
            future,
            state: RunState::Submitted(state),
            worker,
            name: name,
            handle: None,
        }
    }
    async fn transition_to_state(
        &mut self,
        new_state_request: RunState,
    ) -> Result<(), OrcaError> {
        let next_state_type = match (&self.state, new_state_request) {
            (RunState::Submitted(_), RunState::Running(_)) => {
                Ok(RunState::Running(Running {}))
            }
            (RunState::Scheduled(_), RunState::Running(_)) => {
                Ok(RunState::Running(Running {}))
            }
            (RunState::Running(_), RunState::Completed(_)) => {
                Ok(RunState::Completed(Completed {
                    params: vec![],
                    result: "".to_string(),
                }))
            }
            (current, target) => {
                let err_msg = format!(
                    "Invalid state transition from {} to {:?} for Task {}",
                    current, target, self.id
                );
                eprintln!("{}", err_msg);
                Err(OrcaError(err_msg))
            }
        };

        match next_state_type {
            Ok(next_state) => {
                self.state = next_state;

                info!(
                    "{}:{} → → → {}:{}",
                    self.id,
                    self.state.to_string(),
                    self.id,
                    self.state.to_string()
                );
                let _ = &self
                    .worker
                    .tell(OrcaMessage {
                        message: TransitionState {
                            task_name: self.name.clone(),
                            task_id: self.id,
                            new_state: self.state.clone(),
                        },
                        recipient: Recipient::Processor,
                    })
                    .await
                    .map_err(|e| {
                        eprintln!(
                            "Failed to notify worker of state change for Task {}: {}",
                            self.id, e
                        );
                        OrcaError(format!("Worker notification failed: {}", e))
                    });
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}




impl<R: TaskResult> Actor for TaskRun<R> {
    type Args = Self;
    type Error = OrcaError;

    async fn on_start(
        mut args: Self::Args,
        actor_ref: ActorRef<TaskRun<R>>,
    ) -> Result<Self, OrcaError> {
        let self_ref = actor_ref.clone();
        let task_id = args.id.clone();
        if let Some(future) = args.future.take() {
            let handle = tokio::spawn(async move {
                info!("Executing future for task {}", task_id);
                let result: R = future.await;
                info!("Future completed for task {}", task_id);

                let result_string = match serde_json::to_string(&result) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("Failed to serialize result for task {}: {}", task_id, e);
                        format!(
                            r#"{{"error": "Serialization Error: {}"}}"#,
                            e.to_string().replace('"', "\\\"")
                        )
                    }
                };

                let completion_msg = TaskCompleted { result_string };
                if let Err(e) = self_ref.tell(completion_msg).await {
                    eprintln!(
                        "Failed to send internal completion message for task {}: {}",
                        task_id, e
                    );
                }
            });
            args.handle = Some(handle);
            Ok(args)
        } else {
            Err(OrcaError(format!("Task {} started without a future.", task_id)))
        }
    }

    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<TaskRun<R>>,
        _reason: ActorStopReason,
    ) -> Result<(), OrcaError> {
        info!("Task stopping: {}", self.id);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
        Ok(())
    }
}
struct GetField {
    pub field: TaskData,
}
impl<R: TaskResult> Message<GetField> for TaskRun<R> {
    type Reply = GetOrcaFieldReply;

    async fn handle(
        &mut self,
        _message: GetField,
        _ctx: &mut Context<TaskRun<R>, Self::Reply>,
    ) -> Self::Reply {
        match _message.field {
            TaskData::Id(_id) => GetOrcaFieldReply {
                field: TaskData::Id(self.id),
            },
            TaskData::Name(_name) => GetOrcaFieldReply {
                field: TaskData::Name(self.name.clone()),
            },
            TaskData::State(_state) => GetOrcaFieldReply {
                field: TaskData::State(self.state.clone()),
            }
        }
    }
}

impl<R: TaskResult> Message<OrcaMessage<TransitionState>> for TaskRun<R> {
    type Reply = OrcaReply;

    async fn handle(
        &mut self,
        message: OrcaMessage<TransitionState>,
        _ctx: &mut Context<TaskRun<R>, OrcaReply>,
    ) -> Self::Reply {
        match self.transition_to_state(message.message.new_state).await {
            Ok(_) => OrcaReply { success: true },
            Err(e) => {
                eprintln!("State transition failed for Task {}: {}", self.id, e);
                OrcaReply { success: false }
            }
        }
    }
}

impl<R: TaskResult> Message<TaskCompleted> for TaskRun<R> {
    type Reply = ();

    async fn handle(
        &mut self,
        message: TaskCompleted,
        _ctx: &mut Context<TaskRun<R>, Self::Reply>,
    ) -> Self::Reply {
        match self.transition_to_state(RunState::Completed(Completed {
            params: vec![],
            result: message.result_string.clone(),
        })).await {
            Ok(_) => (),
            Err(e) => {
                eprintln!("State transition failed for Task {}: {}", self.id, e);
            }
        }

        // 2. Log the completion internally
        info!("Internal state updated to Completed for task {}", self.id);

        // 3. Notify the worker/processor about the external state change
        let notify_result = self.worker.tell(OrcaMessage {
            message: TransitionState {
                task_name: self.name.clone(),
                task_id: self.id,
                new_state: RunState::Completed(Completed {
                    params: vec![],
                    result: message.result_string.clone(),
                }),
            },
            recipient: Recipient::Processor, // Notify processor
        }).await;

        if let Err(e) = notify_result {
            eprintln!("Failed to notify worker of task {} completion: {}", self.id, e);
        }
    }
}

#[async_trait]
impl<R: TaskResult> Run for ActorRef<TaskRun<R>> {
    async fn forward_matriarch_request(
        &self,
        message: OrcaMessage<TransitionState>,
    ) -> Result<OrcaReply, OrcaError> {
        self.ask(message)
            .await
            .map_err(|e| OrcaError(format!("Kameo error: {}", e)))
    }
    async fn get_id(&self) -> Result<Uuid, OrcaError> {
        let message = GetField {
            field: TaskData::Id(Uuid::nil()),
        };
        let reply = self
            .ask(message)
            .await
            .map_err(|e| OrcaError(format!("Kameo ask error: {}", e)))?;

        match reply.field {
            TaskData::Id(id) => Ok(id),
            _ => Err(OrcaError(
                "Unexpected reply type when asking for Id".to_string(),
            )),
        }
    }

    async fn get_field(&self, field: TaskData) -> Result<TaskData, OrcaError> {
        let message = GetField { field };
        let reply = self
            .ask(message)
            .await
            .map_err(|e| OrcaError(format!("Kameo ask error: {}", e)))?;
        Ok(reply.field)
    }
}
#[async_trait]
pub trait Run: Send + Sync {
    async fn forward_matriarch_request(
        &self,
        message: OrcaMessage<TransitionState>,
    ) -> Result<OrcaReply, OrcaError>;
    async fn get_id(&self) -> Result<Uuid, OrcaError>;
    async fn get_field(&self, field: TaskData) -> Result<TaskData, OrcaError>;
}

#[derive(Debug)]
pub struct OrcaError(pub String);

impl std::fmt::Display for OrcaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for OrcaError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RunState {
    Submitted(Submitted),
    Scheduled(Scheduled),
    Running(Running),
    Completed(Completed),
    Failed(Failed),
}

impl std::fmt::Display for RunState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

pub enum RunStateEnum {
    Submitted,
    Scheduled,
    Running,
    Completed,
    Failed,
}

impl RunState {
    pub fn to_string(&self) -> String {
        match self {
            RunState::Submitted(_state) => "Submitted".to_string(),
            RunState::Scheduled(_state) => "Scheduled".to_string(),
            RunState::Running(_state) => "Running".to_string(),
            RunState::Completed(_state) => "Completed".to_string(),
            RunState::Failed(_state) => "Failed".to_string(),
        }
    }
    pub fn to_orca_state_enum(&self) -> RunStateEnum {
        match self {
            RunState::Submitted(_) => RunStateEnum::Submitted,
            RunState::Scheduled(_) => RunStateEnum::Scheduled,
            RunState::Running(_) => RunStateEnum::Running,
            RunState::Completed(_) => RunStateEnum::Completed,
            RunState::Failed(_) => RunStateEnum::Failed,
        }
    }
    pub fn from_string(s: &str) -> Option<RunState> {
        match s {
            "Submitted" => Some(RunState::Submitted(Submitted::new(0, "".to_string()))),
            "Scheduled" => Some(RunState::Scheduled(Scheduled { delay: 0 })),
            "Running" => Some(RunState::Running(Running {})),
            "Completed" => Some(RunState::Completed(Completed { params: vec![], result: "".to_string() })),
            "Failed" => Some(RunState::Failed(Failed { params: vec![], error: "".to_string() })),
            _ => None,
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Submitted {
    pub max_retries: u32,
    pub args: String,
}

impl Submitted {
    pub fn new(max_retries: u32, args: String) -> Self {
        Self {
            max_retries,
            args,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scheduled {
    pub delay: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Running {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Completed {
    pub params: Vec<String>,
    pub result: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Failed {
    pub params: Vec<String>,
    pub error: String,
}


// pub struct TaskFutureHandle {
//     pub name: String,
//     pub worker_ref: ActorRef<Worker>,
// }


// impl TaskFutureHandle {
//     pub async fn submit(self,args: TaskArgs) -> Self {
//         let message = SubmitTask { task_name: self.name.clone() };
//         let _ = self.worker_ref.tell(message).await;
//         self
//     }
// }

// Define the generic State wrapper struct
#[derive(Debug, Clone, Serialize, Deserialize)] // Add necessary derives
pub struct State<S> {
    // Make inner state public for access
    pub state: S,
}

// Add a constructor for State
impl<S> State<S> {
    pub fn new(state: S) -> Self {
        Self { state }
    }

    // Optional: provide access to inner state if needed elsewhere
    /*
    pub fn inner(&self) -> &S {
        &self.state
    }
    pub fn into_inner(self) -> S {
        self.state
    }
    */
}