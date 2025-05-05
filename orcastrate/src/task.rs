use crate::messages::*;
use crate::types::*;
use crate::worker::Worker;
use async_trait::async_trait;
use kameo::Actor;
use kameo::prelude::{ActorRef, ActorStopReason, Context, Message, WeakActorRef};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use tokio::task::JoinHandle;
use tracing::info;
use uuid::Uuid;
pub type TaskFuture = Pin<Box<dyn Future<Output = Result<String, String>> + Send>>;
pub type SerializedTaskFuture = fn(String) -> Result<TaskFuture, OrcaError>;

#[derive(Clone)]
pub struct StaticTaskDefinition {
    pub task_name: &'static str,
    pub task_future: SerializedTaskFuture,
}

inventory::collect!(StaticTaskDefinition);

// TaskRun is the main actor that runs the task.
pub struct TaskRun {
    pub id: Uuid,
    pub name: String,
    pub future: Option<SerializedTaskFuture>,
    state: RunState,
    handle: Option<JoinHandle<()>>,
    pub worker: ActorRef<Worker>,
}

impl TaskRun {
    pub fn new(
        id: Uuid,
        name: String,
        worker: ActorRef<Worker>,
        future: Option<SerializedTaskFuture>,
        args: String,
    ) -> Self {
        let state = Submitted::new(0, args);
        Self {
            id,
            future,
            state: RunState::Submitted(state),
            worker,
            name: name,
            handle: None,
        }
    }
    async fn transition_to_state(&mut self, new_state_request: RunState) -> Result<(), OrcaError> {
        let next_state_type = match (&self.state, new_state_request) {
            (RunState::Submitted(_), RunState::Running(_)) => Ok(RunState::Running(Running {})),
            (RunState::Scheduled(_), RunState::Running(_)) => Ok(RunState::Running(Running {})),
            (RunState::Running(_), RunState::Completed(_)) => Ok(RunState::Completed(Completed {
                params: vec![],
                result: "".to_string(),
            })),
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

impl Actor for TaskRun {
    type Args = Self;
    type Error = OrcaError;

    async fn on_start(
        mut args: Self::Args,
        actor_ref: ActorRef<TaskRun>,
    ) -> Result<Self, OrcaError> {
        let self_ref = actor_ref.clone();
        let task_id = args.id.clone();

        let serialized_args = match &args.state {
            RunState::Submitted(submitted_state) => {
                if submitted_state.args.is_empty() && !args.name.ends_with("returns_int") {
                    eprintln!("Warning: Task {} started with potentially empty args: '{}'", task_id, submitted_state.args);
                }
                submitted_state.args.clone()
            }
            _ => return Err(OrcaError(format!(
                "Task {} started in unexpected state: {}", task_id, args.state
            ))),
        };

        if let Some(future) = args.future.take() {
            let handle = tokio::spawn(async move {
                info!("Attempting to create future for task {}", task_id);
                match future(serialized_args) {
                    Ok(task_future) => {
                        info!("Executing future for task {}", task_id);
                        let result = task_future.await;
                        info!("Future completed for task {}: {:?}", task_id, result);

                        let result_string = match result {
                            Ok(s) => s,
                            Err(e) => {
                                eprintln!("Task {} future execution failed: {}", task_id, e);
                                format!(r#"{{"error": "{}"}}"#, e.replace('"', "\\\""))
                            }
                        };

                        let completion_msg = TaskCompleted { result_string };
                        if let Err(e) = self_ref.tell(completion_msg).await {
                            eprintln!(
                                "Failed to send internal completion message for task {}: {}",
                                task_id, e
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to *create* future for task {}: {}", task_id, e);
                    }
                }
            });
            args.handle = Some(handle);
            Ok(args)
        } else {
            Err(OrcaError(format!(
                "Task {} started without a future.",
                task_id
            )))
        }
    }

    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<TaskRun>,
        _reason: ActorStopReason,
    ) -> Result<(), OrcaError> {
        info!("Task stopping: {}", self.id);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
        Ok(())
    }
}

impl Message<OrcaMessage<TransitionState>> for TaskRun {
    type Reply = OrcaReply;

    async fn handle(
        &mut self,
        message: OrcaMessage<TransitionState>,
        _ctx: &mut Context<TaskRun, OrcaReply>,
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

impl Message<TaskCompleted> for TaskRun {
    type Reply = ();

    async fn handle(
        &mut self,
        message: TaskCompleted,
        _ctx: &mut Context<TaskRun, Self::Reply>,
    ) -> Self::Reply {
        match self
            .transition_to_state(RunState::Completed(Completed {
                params: vec![],
                result: message.result_string.clone(),
            }))
            .await
        {
            Ok(_) => (),
            Err(e) => {
                eprintln!("State transition failed for Task {}: {}", self.id, e);
            }
        }

        // 2. Log the completion internally
        info!("Internal state updated to Completed for task {}", self.id);

        // 3. Notify the worker/processor about the external state change
        let notify_result = self
            .worker
            .tell(OrcaMessage {
                message: TransitionState {
                    task_name: self.name.clone(),
                    task_id: self.id,
                    new_state: RunState::Completed(Completed {
                        params: vec![],
                        result: message.result_string.clone(),
                    }),
                },
                recipient: Recipient::Processor, // Notify processor
            })
            .await;

        if let Err(e) = notify_result {
            eprintln!(
                "Failed to notify worker of task {} completion: {}",
                self.id, e
            );
        }
    }
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
            "Completed" => Some(RunState::Completed(Completed {
                params: vec![],
                result: "".to_string(),
            })),
            "Failed" => Some(RunState::Failed(Failed {
                params: vec![],
                error: "".to_string(),
            })),
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
        Self { max_retries, args }
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
