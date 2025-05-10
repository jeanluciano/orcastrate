use crate::messages::*;
use crate::worker::Worker;
use kameo::prelude::*;
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, RedisError, RedisResult};
use std::collections::HashMap;
use tracing::{debug, info};
use uuid::Uuid;

// Assuming these constants are defined in the parent module (redis.rs or redis/mod.rs)
// If not, they need to be defined here or passed in.
// For now, let's assume they are accessible via super::
use super::{GATEKEEPER_STREAM_KEY, TASK_GROUP_KEY};

pub struct GateKeeper {
    id: Uuid,
    redis: MultiplexedConnection,
    worker: ActorRef<Worker>,
}

impl GateKeeper {
    pub fn new(id: Uuid, redis: MultiplexedConnection, worker: ActorRef<Worker>) -> ActorRef<Self> {
        GateKeeper::spawn(Self { id, redis, worker })
    }

    async fn run_script_processing_loop(
        id: Uuid,
        mut redis: MultiplexedConnection,
        worker: ActorRef<Worker>,
        // processor: ActorRef<Processor>, // Add processor back if direct communication is needed
    ) {
        info!("GateKeeper starting processing loop for {}", id);
        let consumer_id = id.to_string();
        let opts = StreamReadOptions::default()
            .group(TASK_GROUP_KEY, &consumer_id)
            .count(10)
            .block(500); // Block for 500ms

        loop {
            let messages_result: Result<Option<StreamReadReply>, RedisError> = redis
                .xread_options(&[GATEKEEPER_STREAM_KEY], &[">"], &opts)
                .await;

            match messages_result {
                Ok(Some(messages)) => {
                    if !messages.keys.is_empty() {
                        let stream = &messages.keys[0];
                        if !stream.ids.is_empty() {
                            for message in &stream.ids {
                                Self::process_message(&worker, &message.map).await;
                                // Acknowledge the message after processing
                                let _ack: RedisResult<i64> = redis
                                    .xack(GATEKEEPER_STREAM_KEY, TASK_GROUP_KEY, &[&message.id])
                                    .await;
                            }
                        }
                    }
                    // No messages, loop continues after block timeout
                }
                Ok(None) => {
                    // Timeout, no messages received, continue loop
                }
                Err(e) => {
                    eprintln!(
                        "Error reading from Redis stream {}: {:?}. Retrying...",
                        GATEKEEPER_STREAM_KEY, e
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn process_message(worker: &ActorRef<Worker>, map: &HashMap<String, redis::Value>) {
        match Self::parse_script(map) {
            Ok(script) => {
                info!(
                    "GateKeeper processing Script: {}, args: {:?}",
                    script.id, script.args
                );
                let _ = worker.tell(script).await;
            }
            Err(e) => {
                eprintln!("Error parsing script: {}", e);
            }
        }
    }

    fn parse_script(map: &HashMap<String, redis::Value>) -> Result<SubmitTask, String> {
        let task_id_val = map.get("id").ok_or("Missing task_id")?;
        let task_id_str: String = redis::from_redis_value(task_id_val)
            .map_err(|e| format!("task_id not string: {}", e))?;
        let id = Uuid::parse_str(&task_id_str).map_err(|e| format!("task_id not UUID: {}", e))?;

        let task_name_val = map.get("task_name").ok_or("Missing task_name")?;
        let task_name: String = redis::from_redis_value(task_name_val)
            .map_err(|e| format!("task_name not string: {}", e))?;

        // Read the serialized state data
        let args_val = map.get("args").ok_or("Missing args")?;
        let args: Option<String> =
            redis::from_redis_value(args_val).map_err(|e| format!("args not string: {}", e))?;

        Ok(SubmitTask {
            task_name,
            id,
            args,
        })
    }

    pub async fn publish_script(&mut self, message: SubmitTask) -> RedisResult<String> {
        info!("GateKeeper submitting script: {:?}", message);

        let key_values: &[(&str, String)] = &[
            ("task_name", message.task_name.clone()),
            ("id", message.id.to_string()),
            ("args", message.args.clone().unwrap_or("{}".to_string())), // Store "{}" for None args
        ];

        // Use xadd with Vec<(&str, String)> because values are now owned Strings
        self.redis
            .xadd(GATEKEEPER_STREAM_KEY, "*", key_values)
            .await
    }
}

impl Message<SubmitTask> for GateKeeper {
    type Reply = Result<(), RedisError>;

    async fn handle(
        &mut self,
        message: SubmitTask,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let _ = self.publish_script(message).await;
        Ok(())
    }
}

impl Actor for GateKeeper {
    type Args = Self;
    type Error = RedisError;
    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        debug!("GateKeeper actor starting for ID: {}", args.id);
        let id = args.id;
        let redis_clone = args.redis.clone();
        let worker_clone = args.worker.clone();
        tokio::spawn(async move {
            GateKeeper::run_script_processing_loop(
                id,
                redis_clone,
                worker_clone, /*, processor_clone */
            )
            .await;
        });

        Ok(args)
    }
}
