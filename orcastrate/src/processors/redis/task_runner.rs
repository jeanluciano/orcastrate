use crate::messages::*;
use crate::worker::Worker;
use crate::task::RunState;
use kameo::prelude::*;
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, RedisError, RedisResult};
use std::collections::HashMap;
use tracing::{debug, info};
use uuid::Uuid;
use serde_json;

// Assuming these constants are defined in the parent module (redis.rs or redis/mod.rs)
// If not, they need to be defined here or passed in.
// For now, let's assume they are accessible via super::
use super::{Processor, TASK_GROUP_KEY, TASK_RUN_STREAM_KEY};

pub struct TaskRunner {
    id: Uuid,
    redis: MultiplexedConnection,
    processor: ActorRef<Processor>, // Keep processor ref if needed for communication back
    worker: ActorRef<Worker>,
}

impl TaskRunner {
    pub async fn new(
        id: Uuid,
        redis: MultiplexedConnection,
        processor: ActorRef<Processor>,
        worker: ActorRef<Worker>,
    ) -> Self {
        Self {
            id,
            redis,
            processor,
            worker,
        }
    }

    async fn run_task_processing_loop(
        id: Uuid,
        mut redis: MultiplexedConnection,
        worker: ActorRef<Worker>,
        // processor: ActorRef<Processor>, // Add processor back if direct communication is needed
    ) {
        info!("TaskRunner starting processing loop for {}", id);
        let consumer_id = id.to_string();
        let opts = StreamReadOptions::default()
            .group(TASK_GROUP_KEY, &consumer_id)
            .count(10)
            .block(500); // Block for 500ms

        loop {
            let messages_result: Result<Option<StreamReadReply>, RedisError> = redis
                .xread_options(&[TASK_RUN_STREAM_KEY], &[">"], &opts)
                .await;

            match messages_result {
                Ok(Some(messages)) => {
                    if !messages.keys.is_empty() {
                        let stream = &messages.keys[0];
                        if !stream.ids.is_empty() {
                            for message in &stream.ids {
                                Self::process_message(
                                    &worker,
                                    &message.map,
                                    &mut redis,
                                    &message.id,
                                )
                                .await;
                                // Acknowledge the message after processing
                                let _ack: RedisResult<i64> = redis
                                    .xack(TASK_RUN_STREAM_KEY, TASK_GROUP_KEY, &[&message.id])
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
                        TASK_RUN_STREAM_KEY, e
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn process_message(
        worker: &ActorRef<Worker>,
        map: &HashMap<String, redis::Value>,
        redis_conn: &mut MultiplexedConnection,
        message_id: &str,
    ) {
        match Self::parse_transition_state(map) {
            Ok(transition) => {
                info!(
                    "TaskRunner processing task: {}, state: {:?}",
                    transition.task_id, transition.new_state
                );
                match transition.new_state {
                    RunState::Submitted(state) => {
                        // Assuming RunTask is the message to start execution
                        let _send_result = worker
                            .tell(RunTask {
                                task_name: transition.task_name,
                                task_id: transition.task_id,
                                args: state.args,
                            })
                            .await;
                        // Handle potential send error?
                    }
                    _ => {
                        println!("TaskRunner processing other state: {:?}", transition.new_state);
                        // Handle potential ask error?
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "Error parsing message {} fields: {:?}. Acknowledging and skipping.",
                    message_id, e
                );
                // Optionally, move to a dead-letter queue instead of just acking
            }
        }
    }

    fn parse_transition_state(
        map: &HashMap<String, redis::Value>,
    ) -> Result<TransitionState, String> {
        let task_id_val = map.get("task_id").ok_or("Missing task_id")?;
        let task_id_str: String = redis::from_redis_value(task_id_val)
            .map_err(|e| format!("task_id not string: {}", e))?;
        let task_id =
            Uuid::parse_str(&task_id_str).map_err(|e| format!("task_id not UUID: {}", e))?;

        let task_name_val = map.get("task_name").ok_or("Missing task_name")?;
        let task_name: String = redis::from_redis_value(task_name_val)
            .map_err(|e| format!("task_name not string: {}", e))?;

        // Read the serialized state data
        let state_data_val = map.get("state_data").ok_or("Missing state_data")?;
        let state_data_str: String = redis::from_redis_value(state_data_val)
            .map_err(|e| format!("state_data not string: {}", e))?;

        // Deserialize the full RunState from the JSON string
        let new_state: RunState = serde_json::from_str(&state_data_str)
            .map_err(|e| format!("Failed to deserialize RunState: {}", e))?;

        Ok(TransitionState {
            task_name,
            task_id,
            new_state,
        })
    }

    pub async fn write_transition(&mut self, message: TransitionState) -> RedisResult<String> {
        info!("TaskRunner writing transition message: {:?}", message);
        
        // Serialize the entire RunState enum to JSON
        let state_data_str = serde_json::to_string(&message.new_state)
            .map_err(|e| {
                // Convert serde error to something RedisResult can represent, e.g., generic error
                // Ideally, define a custom error type that encompasses both.
                redis::RedisError::from((redis::ErrorKind::TypeError, "Failed to serialize RunState", e.to_string()))
            })?;

        // Get the state type string for potential indexing/filtering (optional but can be useful)
        let state_type_str = message.new_state.to_string();
        
        let key_values: &[(&str, String)] = &[
            ("task_name", message.task_name.clone()),
            ("task_id", message.task_id.to_string()),
            ("state_type", state_type_str), // Store the type name
            ("state_data", state_data_str), // Store the serialized state data
        ];

        // Use xadd with Vec<(&str, String)> because values are now owned Strings
        self.redis.xadd(TASK_RUN_STREAM_KEY, "*", key_values).await
    }
}


impl Message<TransitionState> for TaskRunner {
    type Reply = Result<(), RedisError>;

    async fn handle(
        &mut self,
        message: TransitionState,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let _ = self.write_transition(message).await;
        Ok(())
    }
}
impl Actor for TaskRunner {
    type Args = Self;
    type Error = RedisError;
    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        debug!("TaskRunner actor starting for ID: {}", args.id);
        let id = args.id;
        let redis_clone = args.redis.clone();
        let worker_clone = args.worker.clone();
        // let processor_clone = args.processor.clone(); // Clone if needed for the loop

        // Spawn the stream processing loop as a separate, long-running task
        tokio::spawn(async move {
            TaskRunner::run_task_processing_loop(
                id,
                redis_clone,
                worker_clone, /*, processor_clone */
            )
            .await;
        });

        Ok(args)
    }
}
