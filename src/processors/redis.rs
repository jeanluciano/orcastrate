use kameo::prelude::*;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use redis::{RedisResult,RedisError};
use redis::streams::{StreamReadOptions,StreamReadReply};
use uuid::Uuid;
use kameo::Actor;
use crate::worker::Worker;
use crate::messages::*;
use tracing::{debug, info};

const TASK_RUN_STREAM_KEY: &str = "orca:streams:tasks:run";
const TASK_SCHEDULED_STREAM_KEY: &str = "orca:streams:tasks:scheduled";
const TASK_RESULTS_STREAM_KEY: &str = "orca:streams:tasks:results";
const TASK_GROUP_KEY: &str = "worker";

// single threaded stream processor.
pub struct Processor{
    id: Uuid,
    redis: MultiplexedConnection,

    worker: ActorRef<Worker>,
}



impl Message<MatriarchMessage<TransitionState>> for Processor {
    type Reply = MatriarchReply;

    async fn handle(
        &mut self,
        message: MatriarchMessage<TransitionState>,
        _ctx: &mut Context<Self,Self::Reply>,
    ) -> Self::Reply {
        match  message.message.new_state {
            OrcaStates::Submitted => {
                info!("Processor emitting submitted state: {:?}", message.message.task_id);
                self.write(message.message).await;
            }
            OrcaStates::Running => {
                info!("Processor emitting running state: {:?}", message.message.task_id);
                self.write(message.message).await;
            }
            _ => {
                info!("Processor emitting unknown state: {:?}", message.message.task_id);
            }
        }
        MatriarchReply { success: true }
    }
}

#[messages]
impl Processor{
    pub async fn new(id:Uuid, redis_url: &str, worker: ActorRef<Worker>) -> Self {
        let client = redis::Client::open(redis_url).unwrap();
        let mut redis  = client.get_multiplexed_async_connection().await.unwrap();
        Self::ensure_streams(&mut redis).await.unwrap();

        Self { 
            id,
            redis, 
            worker,
        }

    }
    
    async fn ensure_streams(redis: &mut MultiplexedConnection) -> Result<(), RedisError> {
        let task_run:RedisResult<String> = redis
            .xgroup_create_mkstream(TASK_RUN_STREAM_KEY, TASK_GROUP_KEY,0)
            .await;
        let scheduled:RedisResult<String> = redis
            .xgroup_create_mkstream(TASK_SCHEDULED_STREAM_KEY, TASK_GROUP_KEY,0)
            .await;
        let results:RedisResult<String> = redis
            .xgroup_create_mkstream(TASK_RESULTS_STREAM_KEY, TASK_GROUP_KEY,0)
            .await;
        for stream in [task_run, scheduled, results] {
            if let Err(e) = &stream {
                if e.to_string() == "BUSYGROUP:Consumer Group name already exists" {
                    return Result::Ok(())
                }
            }
            
        }
        Result::Ok(())
    }

    async fn run_process_loop(
        id: Uuid,
        mut redis: MultiplexedConnection,
        worker: ActorRef<Worker>
    ) {
        let opts = StreamReadOptions::default()
            .group(TASK_GROUP_KEY, &id.to_string())
            .count(10)
            .block(500);

        loop {
            let messages_result: Result<StreamReadReply, RedisError> = redis
                .xread_options(&[&TASK_RUN_STREAM_KEY], &[">"], &opts)
                .await;

            match messages_result {
                Ok(messages) => {
                    if !messages.keys.is_empty() {
                        let stream = &messages.keys[0];
                        if !stream.ids.is_empty() {
                            for message in &stream.ids {
                                info!("Received message ID: {}", message.id);
                                info!("values: {:?}", message.map);
                                // goof zone starting
                                let task_id_val = message.map.get("task_id").expect("Missing task_id in stream message");
                                let task_id_str: String = redis::from_redis_value(task_id_val).expect("task_id not a valid string");
                                let task_id = Uuid::parse_str(&task_id_str).expect("task_id not a valid UUID");
                                let task_name_val = message.map.get("task_name").expect("Missing task_name in stream message");
                                let task_name_str: String = redis::from_redis_value(task_name_val).expect("task_name not a valid string");
                                let new_state_val = message.map.get("new_state").expect("Missing new_state in stream message");
                                let new_state_str: String = redis::from_redis_value(new_state_val).expect("new_state not a valid string");
                                let new_state = OrcaStates::from_string(&new_state_str).expect("Invalid OrcaState string");
                                // goof zone ending
                                match new_state {
                                    OrcaStates::Submitted => {
                                        info!("Processor received submitted state: {:?}", task_id);
                                        let send_result = worker.tell(RunTask {
                                            task_name: task_name_str,
                                            task_id,
                                        }).await;
                                    }
                                    _ => {
                                        let send_result = worker.ask(MatriarchMessage {
                                            message: TransitionState {
                                                task_name: task_name_str,
                                                task_id,
                                                new_state,
                                            },
                                            recipient: ActorType::Orca,
                                        }).await;

                                if let Err(e) = send_result {
                                    eprintln!("Error sending message to worker: {:?}", e);
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

    pub async fn write(&mut self, message: TransitionState) {
        info!("Processor writing message: {:?}", message);
        let new_state = match message.new_state {
            OrcaStates::Running => "Running",
            OrcaStates::Completed => "Completed",
            OrcaStates::Failed => "Failed",
            OrcaStates::Submitted => "Submitted",
            OrcaStates::Scheduled => "Scheduled",
        };
        let key_values: &[(&str, &str)] = &[("task_name", &message.task_name), ("task_id", &message.task_id.to_string()), ("new_state", &new_state)];
        let res  = self.redis
            .xadd::<&str,&str, &str, &str,String>(TASK_RUN_STREAM_KEY, "*", &key_values)
            .await;
    }
}

impl Actor for Processor{
    type Args = Self;   
    type Error = RedisError;
    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, RedisError> {
        info!("Processor starting");
        let id = args.id;
        let redis_clone = args.redis.clone();
        let worker_clone = args.worker.clone();

        tokio::spawn(async move {
            Self::run_process_loop(id, redis_clone, worker_clone).await;
        });

        Ok(args)
    }
}
