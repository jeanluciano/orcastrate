use super::STATEKEEPER_STREAM_KEY;
use crate::error::OrcaError;
use crate::messages::*;
use crate::task::RunState;
use kameo::prelude::*;
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use std::collections::HashMap;
use tracing::{error, info};
use uuid::Uuid;
pub struct StateKeeper {
    id: Uuid,
    redis: MultiplexedConnection,
    tracked_tasks: HashMap<String, String>,
}

impl StateKeeper {
    pub fn new(id: Uuid, redis: MultiplexedConnection) -> ActorRef<Self> {
        StateKeeper::spawn(Self {
            id,
            redis,
            tracked_tasks: HashMap::new(),
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
                self.tracked_tasks.insert(task_id.clone(), res.unwrap());
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
    async fn keep_result_hash(&mut self, task_id: String, result: String) {
        let res: redis::RedisResult<i64> = self.redis.hset(&task_id, "result", &result).await;

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
                self.keep_result_hash(message.task_id.clone(), message.result.unwrap())
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
    type Reply = Result<Option<String>, OrcaError>;

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
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => Ok(None),
            Err(e) => Err(OrcaError(e.to_string())),
        }
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
