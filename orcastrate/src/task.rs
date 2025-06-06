use crate::error::OrcaError;
use crate::messages::*;
use crate::seer::Seer;
use crate::worker::Worker;
use kameo::Actor;
use kameo::actor::RemoteActorRef;
use kameo::prelude::{ActorRef, ActorStopReason, Context, Message, WeakActorRef};
use redis;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use tokio::task::JoinHandle;
use tracing::info;
use uuid::Uuid;

//This will problably be bytes in the future.
pub type SerializedTaskData = String;
pub type TaskFuture =
    Pin<Box<dyn Future<Output = Result<SerializedTaskData, SerializedTaskData>> + Send>>;
pub type SerializedTaskFuture = fn(SerializedTaskData) -> Result<TaskFuture, OrcaError>;

#[derive(Clone)]
pub struct StaticTaskDefinition {
    pub task_name: &'static str,
    pub task_future: SerializedTaskFuture,
}
// !!!!!!Do not put any other types in this file. This interacts with the orcastrate-macro.!!!!!!!!!
inventory::collect!(StaticTaskDefinition);
// !!!!!!Do not put any other types in this file. This interacts with the orcastrate-macro.!!!!!!!!

pub struct TaskRun {
    pub id: String,
    pub name: String,
    pub future: Option<SerializedTaskFuture>,
    state: RunState,
    handle: Option<JoinHandle<()>>,
    pub worker: ActorRef<Worker>,
    pub args: Option<String>,
    max_retries: u32,
    result: Option<String>,
    seer: Option<RemoteActorRef<Seer>>,
    ttl: i64,
}

impl TaskRun {
    pub async fn new(
        id: String,
        name: String,
        worker: ActorRef<Worker>,
        future: Option<SerializedTaskFuture>,
        args: Option<String>,
        max_retries: Option<u32>,
    ) -> ActorRef<TaskRun> {
        let mut seer: Option<RemoteActorRef<Seer>> = None;

        let lookup_result = RemoteActorRef::<Seer>::lookup(format!("seer-{}", id).as_str()).await;
        match lookup_result {
            Ok(seer_ref) => seer = seer_ref,
            Err(_) => {
                info!(
                    "Error looking up seer for task {},will attempt on state change",
                    id
                );
                seer = None;
            }
        };

        let ttl = 60 * 60 * 24;
        let args = Self {
            id,
            future,
            state: RunState::Running,
            worker,
            name: name,
            handle: None,
            args: args,
            max_retries: max_retries.unwrap_or(0),
            result: None,
            seer,
            ttl,
        };
        let task = TaskRun::spawn(args);

        task
    }
    async fn transition_to_state(&mut self, new_state_request: RunState) -> Result<(), OrcaError> {
        let next_state_type = match (&self.state, new_state_request) {
            (RunState::Running, RunState::Completed) => Ok(RunState::Completed),
            (RunState::Running, RunState::Failed) => Ok(RunState::Failed),
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
                    .tell(TransitionState {
                        task_name: self.name.clone(),
                        task_id: self.id.clone(),
                        args: self.args.clone().unwrap_or("".to_string()),
                        new_state: self.state.clone(),
                        result: self.result.clone(),
                        ttl: self.ttl,
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

        let serialized_args = args.args.clone();
        // This is the reason that the future is optional.
        if let Some(future) = args.future.take() {
            let handle = tokio::spawn(async move {
                info!(
                    "Attempting to create future for task {} with retries: {}",
                    task_id, args.max_retries
                );
                match future(serialized_args.unwrap_or("".to_string())) {
                    Ok(task_future) => {
                        info!("Executing future for task {}", task_id);
                        let result = task_future.await;
                        info!("Future completed for task {}: {:?}", task_id, result);
                        let completion_msg = FutureResult { result: result };
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

// Message handlers

impl Message<TransitionState> for TaskRun {
    type Reply = OrcaReply;

    async fn handle(
        &mut self,
        message: TransitionState,
        _ctx: &mut Context<TaskRun, OrcaReply>,
    ) -> Self::Reply {
        match self.transition_to_state(message.new_state).await {
            Ok(_) => OrcaReply { success: true },
            Err(e) => {
                eprintln!("State transition failed for Task {}: {}", self.id, e);
                OrcaReply { success: false }
            }
        }
    }
}

#[derive(Debug)]
struct FutureResult {
    result: Result<String, String>, // Carries Ok(value) or Err(reason)
}

impl Message<FutureResult> for TaskRun {
    type Reply = (); // No reply needed

    async fn handle(
        &mut self,
        message: FutureResult,
        _ctx: &mut Context<TaskRun, Self::Reply>,
    ) -> Self::Reply {
        info!(
            "Received internal result for task {}: {:?}",
            self.id,
            message.result.is_ok()
        );
        let final_state;
        match message.result {
            Ok(res_str) => {
                self.result = Some(res_str);
                final_state = RunState::Completed;
            }
            Err(err_str) => {
                let error_json = format!(r#"{{"error": "{}"}}"#, err_str.replace('"', "\\\""));
                self.result = Some(error_json);
                final_state = RunState::Failed;
            }
        }

        let seer_ref = self.seer.as_ref();
        if let Some(seer) = seer_ref {
            let _ = seer
                .tell(&UpdateSeer {
                    state: final_state.clone(),
                })
                .await;
        } else {
            let lookup_result =
                RemoteActorRef::<Seer>::lookup(format!("seer-{}", self.id).as_str()).await;
            match lookup_result {
                Ok(seer) => {
                    let _ = seer
                        .unwrap()
                        .tell(&UpdateSeer {
                            state: final_state.clone(),
                        })
                        .await;
                }
                Err(_) => {
                    info!("Error looking up seer for task {}", self.id);
                }
            }
            match self.transition_to_state(final_state.clone()).await {
                Ok(_) => {
                    info!(
                        "Internal state updated to {} for task {}",
                        final_state, self.id
                    );
                }
                Err(e) => {
                    eprintln!(
                        "State transition to {} failed for Task {}: {}",
                        final_state, self.id, e
                    );
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CachePolicy {
    Omnipotent,
    Signature,
    Source,
}

impl redis::FromRedisValue for CachePolicy {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let s: String = redis::FromRedisValue::from_redis_value(v)?;
        match s.as_str() {
            "Omnipotent" => Ok(CachePolicy::Omnipotent),
            "Signature" => Ok(CachePolicy::Signature),
            "Source" => Ok(CachePolicy::Source),
            _ => Err(redis::RedisError::from(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid CachePolicy value",
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RunState {
    Running,
    Completed,
    Failed,
}

impl std::fmt::Display for RunState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl RunState {
    pub fn to_string(&self) -> String {
        match self {
            RunState::Running => "Running".to_string(),
            RunState::Completed => "Completed".to_string(),
            RunState::Failed => "Failed".to_string(),
        }
    }
    pub fn from_string(s: &str) -> Option<RunState> {
        match s {
            "Running" => Some(RunState::Running),
            "Completed" => Some(RunState::Completed),
            "Failed" => Some(RunState::Failed),
            _ => None,
        }
    }
}
