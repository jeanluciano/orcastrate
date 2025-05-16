use crate::error::OrcaError;
use crate::messages::{CreateTaskRun, GetResultById, ScheduledTask, SubmitTask, TransitionState};
use crate::redis::ensure_streams;
use crate::redis::gatekeeper::GateKeeper;
use crate::redis::statekeeper::StateKeeper;
use crate::redis::timekeeper::TimeKeeper;
use crate::seer::Handler;
use crate::swarm::RESERVED_SIGNATURES;
use crate::swarm::start_swarm;
use crate::task::CachePolicy;
use crate::task::{StaticTaskDefinition, TaskRun};
use chrono::Utc;
use inventory;
use kameo::Actor;
use kameo::prelude::{ActorRef, ActorSwarm, Context, Message};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tracing::{error, info};
use tracing_subscriber;
use uuid::Uuid;
pub struct WorkerConfig {
    id: Uuid,
    url: &'static str,
}

impl WorkerConfig {
    pub fn new(url: &'static str) -> Self {
        let subscriber = tracing_subscriber::fmt::Subscriber::builder()
            .compact()
            .without_time()
            .finish();

        tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");
        let id = Uuid::new_v4();

        Self { id, url }
    }
}

pub fn run(url: &'static str) -> ActorRef<Worker> {
    let worker = Worker::spawn(WorkerConfig::new(url));
    worker
}

pub struct Worker {
    id: Uuid,
    url: &'static str,
    pub registered_tasks: HashMap<String, StaticTaskDefinition>,
    pub task_runs: HashMap<String, ActorRef<TaskRun>>,
    swarm: &'static ActorSwarm,
    gatekeeper: ActorRef<GateKeeper>,
    timekeeper: ActorRef<TimeKeeper>,
    statekeeper: ActorRef<StateKeeper>,
}

impl Actor for Worker {
    type Args = WorkerConfig;
    type Error = OrcaError;

    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, OrcaError> {
        let client = redis::Client::open(args.url).unwrap();
        let redis = client.get_multiplexed_async_connection().await.unwrap();
        let gatekeeper = GateKeeper::new(args.id, redis.clone(), actor_ref);
        let timekeeper = TimeKeeper::new(args.id, redis.clone(), gatekeeper.clone());
        let statekeeper = StateKeeper::new(args.id, redis.clone());
        let mut registered_tasks: HashMap<String, StaticTaskDefinition> = HashMap::new();
        for task_def in inventory::iter::<StaticTaskDefinition> {
            registered_tasks.insert(task_def.task_name.to_string(), task_def.clone());
        }
        ensure_streams(redis.clone()).await.unwrap();
        let swarm = start_swarm(6969).await.unwrap();
        Ok(Worker {
            id: args.id,
            url: args.url,
            registered_tasks,
            task_runs: HashMap::new(),
            swarm,
            gatekeeper,
            timekeeper,
            statekeeper,
        })
    }
}

// Message handlers
impl Message<CreateTaskRun> for Worker {
    type Reply = Result<Handler, OrcaError>;
    async fn handle(
        &mut self,
        message: CreateTaskRun,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let task_id = match message.cache_policy {
            CachePolicy::Signature => message.signature.unwrap(),
            CachePolicy::Source => message.signature.unwrap(),
            CachePolicy::Omnipotent => Uuid::new_v4().to_string(),
        };

        if let Some(_orca) = self.registered_tasks.get(&message.task_name) {
            let processor_result_for_match: Result<(), OrcaError>;
            let now = Utc::now().timestamp_millis();
            let distributed = true;

            if let Some(delay) = message.delay {
                let submit_at_ms = now + delay * 1000;
                processor_result_for_match = self
                    .timekeeper
                    .tell(ScheduledTask {
                        id: task_id.clone(),
                        task_name: message.task_name.clone(),
                        args: message.args.clone(),
                        submit_at: submit_at_ms,
                        cache_policy: message.cache_policy,
                    })
                    .await
                    .map_err(|e| OrcaError(format!("Error sending scheduled task: {}", e)));
            } else {
                processor_result_for_match = self
                    .gatekeeper
                    .ask(SubmitTask {
                        id: task_id.clone(),
                        task_name: message.task_name,
                        args: message.args,
                        cache_policy: message.cache_policy,
                    })
                    .await
                    .map(|_| ())
                    .map_err(|e| OrcaError(format!("Error sending task: {}", e)));
            }

            match processor_result_for_match {
                Ok(()) => {
                    let run_handle =
                        Handler::new(self.statekeeper.clone(), task_id.clone(), distributed).await;
                    Ok(run_handle)
                }
                Err(e) => Err(e), // e is already OrcaError
            }
        } else {
            Err(OrcaError(format!(
                "Task {} not registered",
                message.task_name
            )))
        }
    }
}

impl Message<TransitionState> for Worker {
    type Reply = Result<(), OrcaError>;

    async fn handle(
        &mut self,
        message: TransitionState,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let res = self.statekeeper.ask(message).await;
        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                eprintln!("Error handling transition state: {}", e);
                Err(OrcaError(e.to_string()))
            }
        }
    }
}

impl Message<SubmitTask> for Worker {
    type Reply = Result<(), OrcaError>;

    async fn handle(
        &mut self,
        message: SubmitTask,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Check if result already exists
        let result_check = self
            .statekeeper
            .ask(GetResultById {
                task_id: message.id.clone(),
            })
            .await;

        if let Ok(_) = result_check {
            match result_check {
                Ok(Some(_)) => {
                    info!(
                        "Task {} already has a result, skipping execution",
                        message.id
                    );
                    return Ok(());
                }
                Ok(None) => {
                    info!(
                        "No existing result found for task {}, proceeding to execute.",
                        message.id
                    );
                }
                Err(e) => {
                    error!(
                        "Error checking for existing result for task {}: {:?}",
                        message.id, e
                    );
                }
            }
        }

        // Check and reserve signatures for Signature and Source policies
        if matches!(
            message.cache_policy,
            CachePolicy::Signature | CachePolicy::Source
        ) {
            let mut reserved = RESERVED_SIGNATURES.lock().unwrap();
            if reserved.contains(&message.id) {
                info!(
                    "Signature {} is already reserved, skipping execution",
                    message.id
                );
                return Ok(());
            }
            // Reserve the signature before proceeding
            reserved.insert(message.id.clone());
            info!("Reserved signature {} for execution", message.id);
        }

        let name = message.task_name.clone();
        let id = message.id.clone();
        let args = message.args.clone();
        let distributed = true;
        info!("Worker received script: {:?}", &message);
        if let Some(orca) = self.registered_tasks.get(&name) {
            let orca = TaskRun::new(
                id.clone(),
                name,
                ctx.actor_ref(),
                Some(orca.task_future.clone()),
                args,
                None,
                distributed,
            )
            .await;
            self.task_runs.insert(id, orca);
            Ok(())
        } else {
            Err(OrcaError(format!(
                "Task {} not registered",
                message.task_name
            )))
        }
    }
}

impl Message<GetResultById> for Worker {
    type Reply = Result<String, OrcaError>;

    async fn handle(
        &mut self,
        message: GetResultById,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let task_id = message.task_id.clone();
        let res = self.statekeeper.ask(message).await;
        let result_string = match res {
            Ok(result_string) => {
                if let Some(result) = result_string {
                    info!("Result: {}", &result);
                    Ok(result)
                } else {
                    info!("Result not found for task {}", task_id);
                    Err(OrcaError(format!("Result not found for task {}", task_id)))
                }
            }
            Err(send_error) => Err(OrcaError(send_error.to_string())),
        };
        result_string
    }
}

pub fn hash_string(input: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(input.as_bytes());
    format!("{:x}", hash.finalize())
}
