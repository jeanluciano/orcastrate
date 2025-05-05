use crate::messages::*;
use crate::task::RunState;
use crate::worker::Worker;
use kameo::Actor;
use kameo::prelude::*;
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use redis::streams::StreamReadReply;
use redis::{RedisError, RedisResult};
use tracing::debug;
use tracing::info;
use uuid::Uuid;

// Declare modules
mod gatekeeper;
mod timekeeper;

// Use items from modules
use gatekeeper::GateKeeper;
use timekeeper::TimeKeeper;

// Define constants within this file for now
const TASK_RUN_STREAM_KEY: &str = "orca:streams:tasks:run";
const TASK_SCHEDULED_STREAM_KEY: &str = "orca:streams:tasks:scheduled";
const TASK_RESULTS_STREAM_KEY: &str = "orca:streams:tasks:results";
const TASK_GROUP_KEY: &str = "worker";

// Spawns
pub struct Processor {
    id: Uuid,
    redis: MultiplexedConnection,
    worker: ActorRef<Worker>,
    timekeeper: Option<ActorRef<TimeKeeper>>,
    gatekeeper: Option<ActorRef<GateKeeper>>,
}

impl Message<TransitionState> for Processor {
    type Reply = OrcaReply;

    async fn handle(
        &mut self,
        message: TransitionState,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match message.new_state {
            RunState::Submitted(ref state) => {
                info!(
                    "Processor handling state {:?}: {:?}",
                    message.new_state, message.task_id
                );
                let task_id = message.task_id;
                let message = TransitionState {
                    task_name: message.task_name,
                    task_id: task_id,
                    new_state: RunState::Submitted(state.clone()),
                };
                let res = self.gatekeeper.as_ref().unwrap().ask(message).await;
                if let Err(e) = res {
                    eprintln!("Error writing state to results stream: {:?}", e);
                    return OrcaReply { success: false };
                }
            }
            RunState::Scheduled(_) => {
                info!(
                    "Processor forwarding scheduled state: {:?}",
                    message.task_id
                );
                if let Some(timekeeper) = &self.timekeeper {
                    // Forward the message to the scheduler actor
                    let send_result = timekeeper.tell(message).await;
                    if let Err(e) = send_result {
                        eprintln!("Error sending message to scheduler: {:?}", e);
                        return OrcaReply { success: false };
                    }
                } else {
                    eprintln!(
                        "Scheduler actor not available for task: {:?}",
                        message.task_id
                    );
                    return OrcaReply { success: false };
                }
            }
            _ => {
                info!(
                    "Processor received unhandled state: {:?}",
                    message.new_state
                );
            }
        }
        OrcaReply { success: true }
    }
}

#[messages]
impl Processor {
    pub async fn new(id: Uuid, redis_url: &str, worker: ActorRef<Worker>) -> Self {
        let client = redis::Client::open(redis_url).unwrap();
        let mut redis = client.get_multiplexed_async_connection().await.unwrap();

        if let Err(e) = Self::ensure_streams(&mut redis).await {
            eprintln!(
                "Failed to ensure Redis streams: {:?}. Processor initialization might fail.",
                e
            );
            // Depending on severity, might want to panic or handle differently
        }
        Self {
            id,
            redis,
            worker,
            timekeeper: None,
            gatekeeper: None, // Initialize runner as None
        }
    }

    async fn ensure_streams(redis: &mut MultiplexedConnection) -> Result<(), RedisError> {
        // Use the constants defined in this module
        let task_run_res: RedisResult<String> = redis
            .xgroup_create_mkstream(TASK_RUN_STREAM_KEY, TASK_GROUP_KEY, "$") // Use $ instead of 0 to ignore history
            .await;
        let scheduled_res: RedisResult<String> = redis
            .xgroup_create_mkstream(TASK_SCHEDULED_STREAM_KEY, TASK_GROUP_KEY, "$")
            .await;
        let results_res: RedisResult<String> = redis
            .xgroup_create_mkstream(TASK_RESULTS_STREAM_KEY, TASK_GROUP_KEY, "$")
            .await;

        // Check results, ignoring "BUSYGROUP" errors
        for res in [task_run_res, scheduled_res, results_res] {
            match res {
                Ok(_) => {}
                Err(e)
                    if e.kind() == redis::ErrorKind::ExtensionError
                        && e.to_string().contains("BUSYGROUP") =>
                {
                    // Group already exists, which is fine
                    debug!("Consumer group already exists (ignored): {}", e);
                }
                Err(e) => {
                    // Other Redis error, return it
                    eprintln!("Error creating stream/group: {:?}", e);
                    return Err(e);
                }
            }
        }
        Ok(())
    }
}

impl Actor for Processor {
    type Args = Self;
    type Error = RedisError;
    async fn on_start(
        mut args: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        debug!("Processor actor starting with ID: {}", args.id);
        let id = args.id;
        let scheduler_conn = args.redis.clone();
        let runner_conn = args.redis.clone();
        let worker_clone = args.worker.clone();

        // Spawn Scheduler
        let timekeeper = TimeKeeper::new(id, scheduler_conn, actor_ref.clone()).await;
        args.timekeeper = Some(TimeKeeper::spawn(timekeeper));

        // Spawn TaskRunner
        let gatekeeper = GateKeeper::new(id, runner_conn, worker_clone).await;
        args.gatekeeper = Some(GateKeeper::spawn(gatekeeper));

        Ok(args)
    }
}
