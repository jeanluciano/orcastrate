use crate::messages::*;
use crate::worker::Worker;
use kameo::prelude::*;
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use redis::{RedisError, RedisResult};
use tracing::debug;
use tracing::info;
use uuid::Uuid;
use crate::notify::Register;
// use crate::task::ListenForResult;
use crate::error::OrcaError;
// Declare modules
mod gatekeeper;
mod statekeeper;
mod timekeeper;
// Use items from modules
use gatekeeper::GateKeeper;
use statekeeper::StateKeeper;
use timekeeper::TimeKeeper;
// Define constants within this file for now
const GATEKEEPER_STREAM_KEY: &str = "orca:streams:gatekeeper";
const TIMEKEEPER_STREAM_KEY: &str = "orca:streams:timekeeper";
const STATEKEEPER_STREAM_KEY: &str = "orca:streams:statekeeper";
const TASK_GROUP_KEY: &str = "worker";

// Spawns
pub struct Processor {
    id: Uuid,
    redis: MultiplexedConnection,
    worker: ActorRef<Worker>,
    timekeeper: Option<ActorRef<TimeKeeper>>,
    gatekeeper: Option<ActorRef<GateKeeper>>,
    statekeeper: Option<ActorRef<StateKeeper>>,
}

impl Processor {
    pub async fn new(id: Uuid, redis_url: &str, worker: ActorRef<Worker>) -> Self {
        let client = redis::Client::open(redis_url).unwrap();
        let mut redis = client.get_multiplexed_async_connection().await.unwrap();

        if let Err(e) = Self::ensure_streams(&mut redis).await {
            eprintln!(
                "Failed to ensure Redis streams: {:?}. Processor initialization might fail.",
                e
            );
        }
        Self {
            id,
            redis,
            worker,
            timekeeper: None,
            gatekeeper: None,
            statekeeper: None,
        }
    }

    async fn ensure_streams(redis: &mut MultiplexedConnection) -> Result<(), RedisError> {
        // Use the constants defined in this module
        let gatekeeper_res: RedisResult<String> = redis
            .xgroup_create_mkstream(GATEKEEPER_STREAM_KEY, TASK_GROUP_KEY, "$") // Use $ instead of 0 to ignore history
            .await;
        let timekeeper_res: RedisResult<String> = redis
            .xgroup_create_mkstream(TIMEKEEPER_STREAM_KEY, TASK_GROUP_KEY, "$")
            .await;
        let statekeeper_res: RedisResult<String> = redis
            .xgroup_create_mkstream(STATEKEEPER_STREAM_KEY, TASK_GROUP_KEY, "$")
            .await;

        // Check results, ignoring "BUSYGROUP" errors
        for res in [gatekeeper_res, timekeeper_res, statekeeper_res] {
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
    type Error = OrcaError;
    async fn on_start(
        mut args: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        debug!("Processor actor starting with ID: {}", args.id);
        let id = args.id;
        let timekeeper_conn = args.redis.clone();
        let gatekeeper_conn = args.redis.clone();
        let statekeeper_conn = args.redis.clone();
        let worker_clone = args.worker.clone();

        // Spawn Scheduler
        let timekeeper = TimeKeeper::new(id, timekeeper_conn, actor_ref.clone());
        args.timekeeper = Some(timekeeper);

        let gatekeeper = GateKeeper::new(id, gatekeeper_conn, worker_clone);
        args.gatekeeper = Some(gatekeeper);

        let statekeeper = StateKeeper::new(id, statekeeper_conn);
        args.statekeeper = Some(statekeeper);

        Ok(args)
    }
}

impl Message<Script> for Processor {
    type Reply = Result<(), OrcaError>;

    async fn handle(
        &mut self,
        message: Script,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        info!("Processor received Script: {:?}", &message);
        let _ = self.gatekeeper.as_ref().unwrap().tell(message).await;
        Ok(())
    }
}

impl Message<ScheduledScript> for Processor {
    type Reply = OrcaReply;

    async fn handle(
        &mut self,
        message: ScheduledScript,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let _ = self.timekeeper.as_ref().unwrap().tell(message).await;
        OrcaReply { success: true }
    }
}

impl Message<TransitionState> for Processor {
    type Reply = OrcaReply;

    async fn handle(
        &mut self,
        message: TransitionState,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        info!("Processor received TransitionState: {:?}", message);
        let _ = self.statekeeper.as_ref().unwrap().tell(message).await;
        OrcaReply { success: true }
    }
}

impl Message<GetResultById> for Processor {
    type Reply = Result<String, OrcaError>;

    async fn handle(
        &mut self,
        message: GetResultById,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let res = self.statekeeper.as_ref().unwrap().ask(message).await;
        match res {
            Ok(reply) => Ok(reply),
            Err(e) => Err(OrcaError(e.to_string())),
        }
    }
}

impl Message<Register<ListenForResult>> for Processor {
    type Reply = OrcaReply;

    async fn handle(
        &mut self,
        message: Register<ListenForResult>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let _ = self.statekeeper.as_ref().unwrap().tell( message).await;
        OrcaReply { success: true }
    }
}
