use super::STATEKEEPER_STREAM_KEY;
use crate::error::OrcaError;
use crate::messages::*;
use crate::notify::MessageBus;
use crate::notify::{DeliveryStrategy, Register};
use crate::task::RunState;
use kameo::prelude::*;
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use std::collections::HashMap;
use tracing::{error, info};
use uuid::Uuid;
// StateKeeper is responsible for storing the state of the task in Redis and deleting task state when the
// time to live expires.keeps track of task and index of task in redis stream.
pub struct StateKeeper {
    id: Uuid,
    redis: MultiplexedConnection,
    tracked_tasks: HashMap<Uuid, String>,
    message_bus: ActorRef<MessageBus>,
}

impl StateKeeper {
    pub fn new(id: Uuid, redis: MultiplexedConnection) -> ActorRef<Self> {
        let message_bus = MessageBus::spawn(MessageBus::new(DeliveryStrategy::Guaranteed));
        StateKeeper::spawn(Self {
            id,
            redis,
            tracked_tasks: HashMap::new(),
            message_bus,
        })
    }
    async fn keep_state(&mut self, state: TransitionState) {
        let task_id = state.task_id;
        let task_name = state.task_name;
        let new_state = state.new_state;
        let args = state.args;
        let result = state.result;
        let res = self
            .redis
            .xadd::<&str, &str, &str, &str, String>(
                STATEKEEPER_STREAM_KEY,
                "*",
                &[
                    ("task_id", &task_id.to_string()),
                    ("task_name", &task_name),
                    ("new_state", &new_state.to_string()),
                    ("args", &args),
                    ("result", &result.unwrap_or("None".to_string())),
                ],
            )
            .await;
        match res {
            Ok(_) => {
                self.tracked_tasks.insert(task_id, res.unwrap());
                info!("StateKeeper kept state for task {}", task_id);
            }
            Err(e) => {
                error!(
                    "StateKeeper error keeping state for task {}: {:?}",
                    task_id, e
                );
            }
        }
    }
    async fn _delete_state(&mut self, _task_id: Uuid) {
        todo!()
    }
    async fn keep_result_hash(&mut self, task_id: Uuid, result: String) {
        let task_id_str = task_id.to_string();
        let res: redis::RedisResult<i64> = self.redis.hset(&task_id_str, "result", &result).await;

        match res {
            Ok(_) => {
                // let _ = self.message_bus.tell(Publish(ListenForResult { task_id: task_id })).await;
                info!("StateKeeper kept result {} for task {}", result, task_id);
            }
            Err(e) => error!(
                "StateKeeper error keeping result for task {}: {:?}",
                task_id, e
            ),
        }
    }
}


impl Message<TransitionState> for StateKeeper {
    type Reply = OrcaReply;

    async fn handle(
        &mut self,
        message: TransitionState,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        info!("StateKeeper received TransitionState: {:?}", message);

        match message.new_state {
            RunState::Running => self.keep_state(message).await,
            RunState::Completed => {
                self.keep_result_hash(message.task_id, message.result.unwrap())
                    .await
            }
            RunState::Failed => info!(
                "StateKeeper received Failed state for task {}",
                message.task_id
            ),
        }
        OrcaReply { success: true }
    }
}
impl Message<GetResultById> for StateKeeper {
    type Reply = Result<String, OrcaError>;

    async fn handle(
        &mut self,
        message: GetResultById,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let task_id = message.task_id;
        let res: redis::RedisResult<Option<String>> =
            self.redis.hget(&task_id.to_string(), "result").await;
        info!("StateKeeper received GetResultById: {:?}", &res);

        match res {
            Ok(result_string) => Ok(result_string.unwrap_or("None".to_string())),
            Err(e) => Err(OrcaError(e.to_string())),
        }
    }
}
impl Message<Register<ListenForResult>> for StateKeeper {
    type Reply = OrcaReply;

    async fn handle(
        &mut self,
        message: Register<ListenForResult>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        info!("StateKeeper received Register: {:?}", message);
        let _ = self.message_bus.tell(message).await;
        OrcaReply { success: true }
    }
}

impl Actor for StateKeeper {
    type Args = Self;
    type Error = OrcaError;
    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        info!("StateKeeper starting with ID: {}", args.id);

        Ok(args)
    }
    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}
