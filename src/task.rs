use crate::messages::*;
use crate::types::*;
use crate::worker::Worker;
use async_trait::async_trait;
use kameo::prelude::*;
use serde_json;
use tokio::task::JoinHandle;
use tracing::info;
use uuid::Uuid;

pub struct RegisteredTask<R: TaskResult> {
    pub name: String,
    pub task_future: TaskFuture<R>,
    pub worker_ref: ActorRef<Worker>,
}

impl<R: TaskResult> RegisteredTask<R> {
    pub fn new(name: String, task_future: TaskFuture<R>, worker_ref: ActorRef<Worker>) -> Self {
        Self { name, task_future, worker_ref }
    }
}

#[async_trait]
pub trait TaskRunner: Send + Sync {
    fn name(&self) -> String;
    /// Spawns the specific Orca<R> actor.
    /// Takes ownership of self and the Worker's ActorRef.
    /// Returns a Box<dyn OrcaMessageRecipient> for the spawned actor.
    async fn run(
        self: Box<Self>,
        id: Uuid,
        worker_ref: ActorRef<Worker>,
    ) -> Result<Box<dyn Run>, OrcaError>; // Use OrcaError from task.rs
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
        let mut orca = TaskRun::new(id, self.name, worker_ref, Some(self.task_future));
        let actor_ref = TaskRun::spawn(orca);
        Ok(Box::new(actor_ref))
    }
}

// TaskRun is the main actor that runs the task.
pub struct TaskRun<R: TaskResult> {
    pub id: Uuid,
    pub name: String,
    pub future: Option<TaskFuture<R>>,
    state: RunState,
    handle: Option<JoinHandle<()>>,
    pub worker: ActorRef<Worker>,
}

impl<R: TaskResult> TaskRun<R> {
    pub fn new(
        id: Uuid,
        name: String,
        worker: ActorRef<Worker>,
        future: Option<TaskFuture<R>>,
    ) -> Self {
        let state = OrcaState::<Submitted>::new(0, vec![]);
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
        new_state_request: OrcaStates,
    ) -> Result<(), OrcaError> {
        let next_state_type = match (&self.state, new_state_request) {
            (RunState::Submitted(_), OrcaStates::Running) => {
                Ok(RunState::Running(OrcaState { state: Running {} }))
            }
            (RunState::Scheduled(_), OrcaStates::Running) => {
                Ok(RunState::Running(OrcaState { state: Running {} }))
            }
            (RunState::Running(_), OrcaStates::Completed) => {
                Ok(RunState::Completed(OrcaState {
                    state: Completed {
                        params: vec![],
                        result: "".to_string(),
                    },
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
                let current_state_enum = self.state.to_orca_state_enum();

                info!(
                    "{}:{} → → → {}:{}",
                    self.id,
                    self.state.to_string(),
                    self.id,
                    current_state_enum.to_string()
                );
                let _ = &self
                    .worker
                    .tell(MatriarchMessage {
                        message: TransitionState {
                            task_name: self.name.clone(),
                            task_id: self.id,
                            new_state: current_state_enum,
                        },
                        recipient: ActorType::Processor,
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

impl<R: TaskResult> Message<MatriarchMessage<TransitionState>> for TaskRun<R> {
    type Reply = MatriarchReply;

    async fn handle(
        &mut self,
        message: MatriarchMessage<TransitionState>,
        _ctx: &mut Context<TaskRun<R>, MatriarchReply>,
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

impl<R: TaskResult> Message<TaskCompleted> for TaskRun<R> {
    type Reply = ();

    async fn handle(
        &mut self,
        message: TaskCompleted,
        _ctx: &mut Context<TaskRun<R>, Self::Reply>,
    ) -> Self::Reply {
        match self.transition_to_state(OrcaStates::Completed).await {
            Ok(_) => {
                if let RunState::Completed(ref mut completed_state) = self.state {
                    completed_state.state.result = message.result_string;
                    info!(
                        "{}:{} → → → {}:{}",
                        self.id,
                        self.state.to_string(),
                        self.id,
                        OrcaStates::Completed.to_string()
                    );
                } else {
                    eprintln!(
                        "Task {} is not in Completed state after successful transition attempt!",
                        self.id
                    );
                }
            }
            Err(e) => {
                eprintln!(
                    "Failed to transition Task {} to Completed state: {}",
                    self.id, e
                );
            }
        }
    }
}

#[async_trait]
impl<R: TaskResult> Run for ActorRef<TaskRun<R>> {
    async fn forward_matriarch_request(
        &self,
        message: MatriarchMessage<TransitionState>,
    ) -> Result<MatriarchReply, OrcaError> {
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
        message: MatriarchMessage<TransitionState>,
    ) -> Result<MatriarchReply, OrcaError>;
    async fn get_id(&self) -> Result<Uuid, OrcaError>;
    async fn get_field(&self, field: TaskData) -> Result<TaskData, OrcaError>;
}

/// Helper struct to hold information needed to spawn a specific Orca<R>.

#[derive(Debug)]
pub struct OrcaError(String);

impl std::fmt::Display for OrcaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub enum RunState {
    Submitted(OrcaState<Submitted>),
    Scheduled(OrcaState<Scheduled>),
    Running(OrcaState<Running>),
    Completed(OrcaState<Completed>),
}

impl std::fmt::Display for RunState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl RunState {
    pub fn to_string(&self) -> String {
        match self {
            RunState::Submitted(_state) => "Submitted".to_string(),
            RunState::Scheduled(_state) => "Scheduled".to_string(),
            RunState::Running(_state) => "Running".to_string(),
            RunState::Completed(_state) => "Completed".to_string(),
        }
    }
    pub fn to_orca_state_enum(&self) -> OrcaStates {
        match self {
            RunState::Submitted(_) => OrcaStates::Submitted,
            RunState::Scheduled(_) => OrcaStates::Scheduled,
            RunState::Running(_) => OrcaStates::Running,
            RunState::Completed(_) => OrcaStates::Completed,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrcaState<S> {
    state: S,
}

#[derive(Debug, Clone)]
pub struct Submitted {
    pub max_retries: u32,
    pub args: Vec<String>,
}

impl OrcaState<Submitted> {
    pub fn new(max_retries: u32, args: Vec<String>) -> Self {
        Self {
            state: Submitted { max_retries, args },
        }
    }
}

#[derive(Debug, Clone)]
pub struct Scheduled {
    pub delay: u64,
}

#[derive(Debug, Clone)]
pub struct Running {}

#[derive(Debug, Clone)]
pub struct Completed {
    pub params: Vec<String>,
    pub result: String,
}



pub struct TaskHandle {
    pub name: String,
    pub worker_ref: ActorRef<Worker>,
}

impl TaskHandle {
    pub async fn submit(self) -> Self {
        let message = SubmitTask { task_name: self.name.clone() };
        let _ = self.worker_ref.tell(message).await;
        self
    }
}