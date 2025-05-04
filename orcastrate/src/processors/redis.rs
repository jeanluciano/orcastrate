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
mod scheduler;
mod task_runner;

// Use items from modules
use scheduler::Scheduler;
use task_runner::TaskRunner;

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
    scheduler: Option<ActorRef<Scheduler>>,
    runner: Option<ActorRef<TaskRunner>>,
}

impl Message<OrcaMessage<TransitionState>> for Processor {
    type Reply = OrcaReply;

    async fn handle(
        &mut self,
        message: OrcaMessage<TransitionState>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match message.message.new_state {
            RunState::Submitted(ref state) => {
                info!(
                    "Processor handling state {:?}: {:?}",
                    message.message.new_state, message.message.task_id
                );
                let task_id = message.message.task_id;
                let message = TransitionState {
                    task_name: message.message.task_name,
                    task_id: task_id,
                    new_state: RunState::Submitted(state.clone()),
                };
                let res = self.runner.as_ref().unwrap().ask(message).await;
                if let Err(e) = res {
                    eprintln!("Error writing state to results stream: {:?}", e);
                    return OrcaReply { success: false };
                }
            }
            RunState::Scheduled(_) => {
                info!(
                    "Processor forwarding scheduled state: {:?}",
                    message.message.task_id
                );
                if let Some(scheduler) = &self.scheduler {
                    // Forward the message to the scheduler actor
                    let send_result = scheduler.tell(message).await;
                    if let Err(e) = send_result {
                        eprintln!("Error sending message to scheduler: {:?}", e);
                        return OrcaReply { success: false };
                    }
                } else {
                    eprintln!(
                        "Scheduler actor not available for task: {:?}",
                        message.message.task_id
                    );
                    return OrcaReply { success: false };
                }
            } 
            _ => { // Catch-all removed as all enum variants are covered
                  info!(
                      "Processor received unhandled state: {:?}",
                      message.message.new_state
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
        // Ensure streams exist before starting actors that might use them
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
            scheduler: None,
            runner: None, // Initialize runner as None
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

    // New method to write state transitions to the results stream
    async fn write_state_to_results_stream(
        &mut self,
        state: TransitionState,
    ) -> RedisResult<String> {
        info!(
            "Processor writing state {:?} for task {} to results stream",
            state.new_state, state.task_id
        );
        let new_state_str = match state.new_state {
            RunState::Running(_) => "Running",
            RunState::Completed(_) => "Completed",
            RunState::Failed(_) => "Failed",
            RunState::Submitted(_) => "Submitted",
            // Scheduled is handled by the Scheduler, shouldn't be written here directly
            RunState::Scheduled(_) => {
                eprintln!(
                    "Attempted to write Scheduled state directly via Processor::write_state_to_results_stream"
                );
                return Err(redis::RedisError::from((
                    redis::ErrorKind::InvalidClientConfig,
                    "Cannot write Scheduled state to results stream",
                )));
            }
        };
        let key_values: &[(&str, &str)] = &[
            ("task_name", &state.task_name),
            ("task_id", &state.task_id.to_string()),
            ("new_state", new_state_str),
        ];
        self.redis
            .xadd(TASK_RESULTS_STREAM_KEY, "*", key_values)
            .await
    }
}

impl Actor for Processor {
    type Args = Self;
    type Error = RedisError; // Use RedisError as the primary error source during startup
    async fn on_start(
        mut args: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        debug!("Processor actor starting with ID: {}", args.id);
        let id = args.id;
        let processor_conn = args.redis.clone(); // Clone connection for Processor itself if needed
        let scheduler_conn = args.redis.clone();
        let runner_conn = args.redis.clone();
        let worker_clone = args.worker.clone();
        let processor_ref_clone = actor_ref.clone(); // Clone ref for TaskRunner

        // Spawn Scheduler
        let scheduler = Scheduler::new(id, scheduler_conn, actor_ref.clone()).await;
        args.scheduler = Some(Scheduler::spawn(scheduler));
    

        // Spawn TaskRunner
        let task_runner = TaskRunner::new(id, runner_conn, processor_ref_clone, worker_clone).await;
        args.runner = Some(TaskRunner::spawn(task_runner));


        Ok(args)
    }
}
