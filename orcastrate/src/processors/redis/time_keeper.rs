use crate::messages::*;
use kameo::Actor;
use kameo::prelude::{ActorRef, Context, Message};
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, RedisError};
use uuid::Uuid;
use tracing::{info, debug};
use super::{Processor, TASK_GROUP_KEY, TASK_SCHEDULED_STREAM_KEY};
use crate::task::{RunState, Submitted};


pub struct TimeKeeper {
    id: Uuid,
    redis: MultiplexedConnection,
    processor: ActorRef<Processor>,
}

impl TimeKeeper {
    pub async fn new(id: Uuid, redis: MultiplexedConnection, processor: ActorRef<Processor>) -> Self {
        Self { id, redis, processor }
    }

    // Placeholder for handling scheduled tasks
    async fn handle_scheduled(&mut self, message: OrcaMessage<TransitionState>) {
        info!("TimeKeeper handling scheduled task: {:?}", message.message.task_id);
        // TODO: Implement scheduling logic (e.g., writing to TASK_SCHEDULED_STREAM_KEY)
    }

    pub async fn schedule_task(&mut self, task_name: &str, task_id: Uuid, scheduled_at: u64) {
        let scheduled_at_str = scheduled_at.to_string();
        let key_values: &[(&str, &str)] = &[
            ("task_name", &task_name),
            ("task_id", &task_id.to_string()),
            ("scheduled_at", &scheduled_at_str),
        ];
        let res = self
            .redis
            .xadd::<&str, &str, &str, &str, String>(TASK_SCHEDULED_STREAM_KEY, "*", &key_values)
            .await;
    }

    pub async fn process_scheduled_stream(
        id: Uuid,
        mut redis: MultiplexedConnection,
        processor: ActorRef<Processor>,
    ) {
        let opts = StreamReadOptions::default()
            .group(TASK_GROUP_KEY, &id.to_string())
            .count(10)
            .block(500);

        loop {
            let messages_result: Result<StreamReadReply, RedisError> = redis
                .xread_options(&[&TASK_SCHEDULED_STREAM_KEY], &[">"], &opts)
                .await;
            match messages_result {
                Ok(messages) => {
                    if !messages.keys.is_empty() {
                        let stream = &messages.keys[0];
                        if !stream.ids.is_empty() {
                            for message in &stream.ids {
                                let task_name_val = message
                                    .map
                                    .get("task_name")
                                    .expect("Missing task_name in stream message");
                                let task_name_str: String = redis::from_redis_value(task_name_val)
                                    .expect("task_name not a valid string");
                                let task_id_val = message
                                    .map
                                    .get("task_id")
                                    .expect("Missing task_id in stream message");
                                let task_id_str: String = redis::from_redis_value(task_id_val)
                                    .expect("task_id not a valid string");
                                let task_id = Uuid::parse_str(&task_id_str)
                                    .expect("task_id not a valid UUID");
                                let scheduled_at_val = message
                                    .map
                                    .get("scheduled_at")
                                    .expect("Missing scheduled_at in stream message");
                                let scheduled_at_str: String =
                                    redis::from_redis_value(scheduled_at_val)
                                        .expect("scheduled_at not a valid string");
                                let schedeled_at = scheduled_at_str
                                    .parse::<u128>()
                                    .expect("scheduled_at not a valid u128");
                                let now = tokio::time::Instant::now().elapsed().as_millis();
                                if now >= schedeled_at {
                                    let send_result = processor
                                        .tell(OrcaMessage {
                                            message: TransitionState {
                                                task_name: task_name_str,
                                                task_id,
                                                new_state: RunState::Submitted(Submitted {
                                                    max_retries: 0,
                                                    args: "".to_string(),
                                                }),
                                            },
                                            recipient: Recipient::Processor,
                                        })
                                        .await;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error reading from Redis stream: {:?}. Retrying...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }
    }
}

impl Message<OrcaMessage<TransitionState>> for TimeKeeper {
    type Reply = OrcaReply;

    async fn handle(
        &mut self,
        message: OrcaMessage<TransitionState>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match message.message.new_state {
            RunState::Scheduled(_) => {
               self.handle_scheduled(message).await;
            }
            _ => {
                // Should not receive other states, but log if it happens
                info!("TimeKeeper received unexpected state: {:?} for task: {:?}", message.message.new_state, message.message.task_id);
            }
        }
        OrcaReply { success: true }
    }
}

impl Actor for TimeKeeper {
    type Args = Self;
    type Error = RedisError;
    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, RedisError> {
        debug!("TimeKeeper starting with id: {}", args.id);
        let id = args.id;
        let task_redis = args.redis.clone();
        let task_processor = args.processor.clone();

        tokio::spawn(async move {
            Self::process_scheduled_stream(id, task_redis, task_processor).await;
        });
        Ok(args)
    }
}
