
use kameo::prelude::*;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use redis::{RedisResult,RedisError};
use redis::streams::{StreamReadOptions,StreamReadReply};
use uuid::Uuid;
use kameo::Actor;
use crate::worker::{Worker};
use crate::messages::{MatriarchMessage, MatriarchReply, OrcaRequest, OrcaStates, ActorType};
use crate::task::{Orca};

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



impl Message<MatriarchMessage<OrcaRequest>> for Processor {
    type Reply = MatriarchReply;

    async fn handle(
        &mut self,
        message: MatriarchMessage<OrcaRequest>,
        _ctx: &mut Context<Self,Self::Reply>,
    ) -> Self::Reply {
        println!("Processor received task request: {:?}", message.message.task_id);
        match  message.message.new_state {
            OrcaStates::Running => {
                self.write(message.message).await;
            }
            _ => {}
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

    pub async fn process_loop(&mut self) {
        let opts = StreamReadOptions::default()
            .group(TASK_GROUP_KEY, &self.id.to_string())
            .count(10)
            .block(500);

        loop {
            let messages: StreamReadReply = self.redis
            .xread_options(&[&TASK_RUN_STREAM_KEY],&[">"], &opts).await.unwrap();
            if !messages.keys.is_empty() {

                let stream = &messages.keys[0];
                if stream.ids.len() > 0 {
                    for message in &stream.ids {
                        println!("{}", message.id);
                        let vals =  &message.map;
                        self.worker.ask(MatriarchMessage{
                            message: OrcaRequest{
                                task_id: "test".to_string(),
                                new_state: OrcaStates::Running,
                            },
                            recipient: ActorType::Orca,
                        }).await.unwrap();
                    }
                }
            }
        }
    }

    pub async fn write(&mut self, message: OrcaRequest) {
        println!("Processor received message: {:?}", message);
        let new_state = match message.new_state {
            OrcaStates::Running => "Running",
            OrcaStates::Completed => "Completed",
            OrcaStates::Failed => "Failed",
            OrcaStates::Submitted => "Submitted",
            OrcaStates::Scheduled => "Scheduled",
            OrcaStates::Registered => "Registered",
        };
        let key_values: &[(&str, &str)] = &[("task_id", &message.task_id), ("new_state", &new_state)];
        self.redis
            .xadd::<&str,&str, &str, &str,String>(TASK_RUN_STREAM_KEY, "*", &key_values)
            .await;
    }
}

impl Actor for Processor{
    type Args = Self;   
    type Error = RedisError;
    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, RedisError> {
        println!("Processor starting");
        let mut args = args;
        args.process_loop().await;
        Ok(args)
    }
}
