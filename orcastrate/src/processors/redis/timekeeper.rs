use super::{Processor, TASK_GROUP_KEY, TIMEKEEPER_STREAM_KEY};
use crate::messages::*;
use kameo::Actor;
use kameo::prelude::{ActorRef, Context, Message};
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, RedisError};
use tracing::{debug, info};
use uuid::Uuid;
use chrono::prelude::*;
use crate::task::CachePolicy;

pub struct TimeKeeper {
    id: Uuid,
    redis: MultiplexedConnection,
    processor: ActorRef<Processor>,
}

impl TimeKeeper {
    pub fn new(
        id: Uuid,
        redis: MultiplexedConnection,
        processor: ActorRef<Processor>,
    ) -> ActorRef<Self> {
        TimeKeeper::spawn(Self {
            id,
            redis,
            processor,
        })
    }

    // Placeholder for handling scheduled tasks
    async fn _handle_scheduled(&mut self, message: TransitionState) {
        info!("TimeKeeper handling scheduled task: {:?}", message.task_id);
        // TODO: Implement scheduling logic (e.g., writing to TASK_SCHEDULED_STREAM_KEY)
    }

    pub async fn schedule_task(mut redis: MultiplexedConnection, task: ScheduledTask) -> Result<(), RedisError> {
        let key_values: &[(&str, &str)] = &[
            ("task_name", &task.task_name),
            ("task_id", &task.id.to_string()),
            ("submit_at", &task.submit_at.to_string()),
            ("args", &task.args.unwrap_or("".to_string())),
            ("cache_policy", &format!("{:?}", task.cache_policy)),
        ];
        redis
            .xadd::<&str, &str, &str, &str, String>(TIMEKEEPER_STREAM_KEY, "*", &key_values)
            .await?;
        Ok(())
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
                .xread_options(&[&TIMEKEEPER_STREAM_KEY], &[">"], &opts)
                .await;
            match messages_result {
                Ok(messages) => {
                    if !messages.keys.is_empty() {
                        let stream = &messages.keys[0];
                        if !stream.ids.is_empty() {
                            for message_entry in &stream.ids {
                                let message_id = &message_entry.id;
                                let task_name_val = message_entry
                                    .map
                                    .get("task_name")
                                    .expect("Missing task_name in stream message");
                                let task_name_str: String = redis::from_redis_value(task_name_val)
                                    .expect("task_name not a valid string");
                                let task_id_val = message_entry
                                    .map
                                    .get("task_id")
                                    .expect("Missing task_id in stream message");
                                let task_id_str: String = redis::from_redis_value(task_id_val)
                                    .expect("task_id not a valid string");
                                let submit_at_val = message_entry
                                    .map
                                    .get("submit_at")
                                    .expect("Missing submit_at in stream message");
                                let submit_at_str: String =
                                    redis::from_redis_value(submit_at_val)
                                        .expect("submit_at not a valid string");
                                let submit_at = submit_at_str
                                    .parse::<i64>()
                                    .expect("submit_at not a valid i64");
                                let args_val = message_entry
                                    .map
                                    .get("args")
                                    .expect("Missing args in stream message");
                                let args: Option<String> = redis::from_redis_value(args_val)
                                    .expect("args not a valid string");
                                let now = Utc::now().timestamp_millis();
                                let cache_policy_val = message_entry
                                    .map
                                    .get("cache_policy")
                                    .expect("Missing cache_policy in stream message");
                                let cache_policy: CachePolicy = redis::from_redis_value(cache_policy_val)
                                    .expect("cache_policy not a valid string");
                                if now >= submit_at {
                                    info!(task_id = %task_id_str, task_name = %task_name_str, "TimeKeeper: Submitting due task");
                                    let send_result = processor
                                        .tell(SubmitTask {
                                            id: task_id_str.clone(),
                                            task_name: task_name_str.clone(),
                                            args: args.clone(),
                                            cache_policy: cache_policy,
                                        })
                                        .await;
                                    if let Err(e) = send_result {
                                        eprintln!("TimeKeeper: Error sending task {} to processor: {:?}", task_id_str, e);
                                    }
                                    else {
                                        let _awk:Result<i32, RedisError>= redis.xack(TIMEKEEPER_STREAM_KEY, TASK_GROUP_KEY, &[message_id]).await;
                                    }
                                } else {
                                    info!(task_id = %task_id_str, task_name = %task_name_str, current_time = %now, due_time = %submit_at, "TimeKeeper: Rescheduling task for later");
                                    match TimeKeeper::schedule_task(
                                        redis.clone(),
                                        ScheduledTask {
                                            id: task_id_str.clone(),
                                            task_name: task_name_str,
                                            args: args,
                                            submit_at,
                                            cache_policy: cache_policy,
                                        },
                                    )
                                    .await {
                                        Ok(_) => {
                                            let _awk:Result<i32, RedisError>= redis.xack(TIMEKEEPER_STREAM_KEY, TASK_GROUP_KEY, &[message_id]).await;
                                        }
                                        Err(e) => {
                                            eprintln!("TimeKeeper: Failed to reschedule task_id {}. Error: {:?}. Message will not be XACKed and retried later.", task_id_str, e);
                                        }
                                    }
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

impl Message<ScheduledTask> for TimeKeeper {
    type Reply = OrcaReply;

    async fn handle(
        &mut self,
        message: ScheduledTask,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match TimeKeeper::schedule_task(self.redis.clone(), message).await {
            Ok(_) => OrcaReply { success: true },
            Err(e) => {
                eprintln!("TimeKeeper: Failed to schedule task via handle: {:?}", e);
                OrcaReply { success: false }
            }
        }
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
            TimeKeeper::process_scheduled_stream(id, task_redis, task_processor).await;
        });
        Ok(args)
    }
}
