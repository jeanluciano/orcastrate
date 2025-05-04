use crate::messages::*;
use crate::worker::Worker;
use crate::task::RunState;
use kameo::prelude::*;
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, RedisError, RedisResult};
use std::collections::HashMap;
use tracing::info;
use uuid::Uuid;

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

    // Renamed from process_task_run_stream and made into an instance method
    // Spawns a long-running task to process the stream
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

        let new_state_val = map.get("new_state").ok_or("Missing new_state")?;
        let new_state_str: String = redis::from_redis_value(new_state_val)
            .map_err(|e| format!("new_state not string: {}", e))?;
        let new_state = RunState::from_string(&new_state_str)
            .ok_or_else(|| format!("Invalid OrcaState: {}", new_state_str))?;

        Ok(TransitionState {
            task_name,
            task_id,
            new_state,
        })
    }

    // This function seems misplaced in TaskRunner if its purpose is to write
    // state transitions initiated by the Processor.
    // Let's keep it for now, but it might need to move to Processor or be called differently.
    // It currently writes to TASK_RUN_STREAM_KEY which seems incorrect for results/status.
    // Consider renaming and changing the stream key (e.g., TASK_RESULTS_STREAM_KEY).
    pub async fn write_transition(&mut self, message: TransitionState) -> RedisResult<String> {
        info!("TaskRunner writing transition message: {:?}", message);
        let new_state_str = match message.new_state {
            RunState::Running(_) => "Running",
            RunState::Completed(_) => "Completed",
            RunState::Failed(_) => "Failed",
            RunState::Submitted(_) => "Submitted",
            RunState::Scheduled(_) => "Scheduled",
            // Consider adding _ => ??? or making OrcaStates non_exhaustive
        };
        let key_values: &[(&str, &str)] = &[
            ("task_name", &message.task_name),
            ("task_id", &message.task_id.to_string()),
            ("new_state", new_state_str),
        ];
        // TODO: Decide which stream this should write to. Using TASK_RUN_STREAM_KEY for now.
        self.redis.xadd(TASK_RUN_STREAM_KEY, "*", key_values).await
    }
}

impl Actor for TaskRunner {
    type Args = Self;
    type Error = RedisError;
    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        info!("TaskRunner actor starting for ID: {}", args.id);
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
