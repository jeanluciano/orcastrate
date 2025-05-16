use crate::messages::*;
use crate::task::CachePolicy;
use crate::worker::Worker;
use kameo::prelude::*;
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, RedisError, RedisResult};
use std::collections::HashMap;
use tracing::{debug, error, info};
use uuid::Uuid;

use super::{GATEKEEPER_STREAM_KEY, TASK_GROUP_KEY};
use crate::messages::TASK_COMPLETION_TOPIC;
use crate::swarm::{SWARM_CMD_TX, SwarmControlCommand};
use libp2p::gossipsub::IdentTopic;

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
        let id = task_id_str.clone();

        let task_name_val = map.get("task_name").ok_or("Missing task_name")?;
        let task_name: String = redis::from_redis_value(task_name_val)
            .map_err(|e| format!("task_name not string: {}", e))?;

        // Read the serialized state data
        let args_val = map.get("args").ok_or("Missing args")?;
        let args: Option<String> =
            redis::from_redis_value(args_val).map_err(|e| format!("args not string: {}", e))?;

        let cache_policy_val = map.get("cache_policy").ok_or("Missing cache_policy")?;
        let cache_policy: CachePolicy = redis::from_redis_value(cache_policy_val)
            .map_err(|e| format!("cache_policy not string: {}", e))?;

        Ok(SubmitTask {
            task_name,
            id,
            args,
            cache_policy,
        })
    }

    pub async fn publish_script(&mut self, message: SubmitTask) -> RedisResult<String> {
        info!("GateKeeper preparing to submit script: {:?}", message);

        let task_id = message.id.clone(); // Clone id for potential later use

        // 1. Check if signature is already locally reserved (for Signature/Source policies)
        if matches!(
            message.cache_policy,
            CachePolicy::Signature | CachePolicy::Source
        ) {
            let reserved_set = crate::swarm::RESERVED_SIGNATURES.lock().unwrap();
            if reserved_set.contains(&task_id) {
                info!(
                    "GateKeeper: Signature {} is already locally reserved, skipping submission.",
                    task_id
                );
                // Return an error indicating it's already reserved
                return Err(RedisError::from(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists, // Use AlreadyExists for clarity
                    "Signature already reserved locally",
                )));
            }
            // Drop the lock here, we only needed to check
        }

        // 2. Prepare arguments for Redis xadd
        let key_values: &[(&str, String)] = &[
            ("task_name", message.task_name.clone()),
            ("id", task_id.clone()), // Use the cloned id
            ("args", message.args.clone().unwrap_or("{}".to_string())), // Store "{}" for None args
            ("cache_policy", format!("{:?}", message.cache_policy)),
        ];

        // 3. Attempt to add the task to the Redis stream
        match self
            .redis
            .xadd(GATEKEEPER_STREAM_KEY, "*", key_values)
            .await
        {
            Ok(stream_id) => {
                info!(
                    "GateKeeper successfully added task {} to stream {}. Stream ID: {}",
                    task_id, GATEKEEPER_STREAM_KEY, stream_id
                );

                // 4. On successful xadd, reserve locally and publish via Gossipsub (if applicable)
                if matches!(
                    message.cache_policy,
                    CachePolicy::Signature | CachePolicy::Source
                ) {
                    // a. Reserve locally
                    {
                        // Scope to ensure lock is dropped quickly
                        let mut reserved_set = crate::swarm::RESERVED_SIGNATURES.lock().unwrap();
                        reserved_set.insert(task_id.clone());
                    }
                    info!(
                        "GateKeeper locally reserved signature {} after successful Redis submission.",
                        task_id
                    );

                    // b. Send command to publish via Gossipsub
                    let cmd_tx_clone = SWARM_CMD_TX.lock().unwrap().as_ref().cloned();
                    if let Some(cmd_tx) = cmd_tx_clone {
                        let topic = IdentTopic::new(TASK_COMPLETION_TOPIC); // Use the appropriate topic const
                        let data = task_id.clone().into_bytes();
                        let cmd = SwarmControlCommand::PublishGossip { topic, data };
                        if let Err(e) = cmd_tx.send(cmd).await {
                            error!(
                                "GateKeeper failed to send PublishGossip command for signature {}: {:?}",
                                task_id, e
                            );
                            // Decide if this failure should revert the Redis add or just be logged.
                            // For now, we just log it, as the task is already in Redis.
                        } else {
                            info!(
                                "GateKeeper sent PublishGossip command for signature {}.",
                                task_id
                            );
                        }
                    } else {
                        error!(
                            "GateKeeper cannot publish signature {}: SWARM_CMD_TX not available.",
                            task_id
                        );
                    }
                }

                Ok(stream_id) // Return the stream ID on overall success
            }
            Err(e) => {
                error!(
                    "GateKeeper failed to add task {} to Redis stream {}: {:?}",
                    task_id, GATEKEEPER_STREAM_KEY, e
                );
                Err(e) // Return the Redis error
            }
        }
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
