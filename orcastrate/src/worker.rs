use crate::error::OrcaError;
use crate::messages::{
    GetResultById, ScheduledScript, Script, StartRun, TransitionState,
};

use crate::processors::redis::Processor;
use crate::seer::Handler;
use crate::task::{StaticTaskDefinition, TaskRun};
use crate::swarm::{start_swarm, SWARM_CMD_TX, SwarmControlCommand};
use chrono::Utc;
use inventory;
use kameo::Actor;
use kameo::prelude::{ActorRef, Context, Message,ActorSwarm};
use std::collections::HashMap;
use tracing::{info, error, warn};
use tracing_subscriber;
use uuid::Uuid;
use libp2p::gossipsub::IdentTopic;
use crate::messages::TASK_COMPLETION_TOPIC;

pub struct Worker {
    id: Uuid,
    url: String,
    pub registered_tasks: HashMap<String, StaticTaskDefinition>,
    pub task_runs: HashMap<Uuid, ActorRef<TaskRun>>,
    processor: Option<ActorRef<Processor>>,
    swarm: Option<&'static ActorSwarm>,
}




impl Worker {
    pub fn new(url: String) -> Self {
        let subscriber = tracing_subscriber::fmt::Subscriber::builder()
            .compact()
            .without_time()
            .finish();

        tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");
        let id = Uuid::new_v4();
        let mut registered_tasks: HashMap<String, StaticTaskDefinition> = HashMap::new();
        for task_def in inventory::iter::<StaticTaskDefinition> {
            registered_tasks.insert(task_def.task_name.to_string(), task_def.clone());
        }
        Self {
            id,
            url,
            task_runs: HashMap::new(),
            registered_tasks,
            processor: None,
            swarm: None,
        }
    }
    pub async fn swarm(mut self) -> Result<Self, Box<dyn std::error::Error>> {
        let swarm = start_swarm(8020).await?;
        self.swarm = Some(swarm);
        Ok(self)
    }
    pub async fn run(self) -> ActorRef<Self> {
        let worker_actor = Self::spawn(self);
        worker_actor.wait_for_startup().await;
        worker_actor
    }

    async fn publish_signature_to_gossip(&self, signature: String) {
        if let Some(cmd_tx) = SWARM_CMD_TX.lock().unwrap().as_ref() {
            let topic = IdentTopic::new(TASK_COMPLETION_TOPIC);
            let data = signature.clone().into_bytes();
            info!("Worker {} sending PublishGossip command for signature: {}", self.id, signature);
            let cmd = SwarmControlCommand::PublishGossip { topic, data };
            if let Err(e) = cmd_tx.send(cmd).await {
                error!(
                    "Worker {} failed to send PublishGossip command to swarm loop for signature \'{}\': {:?}",
                    self.id, signature, e
                );
            } else {
                info!("Worker {} successfully sent PublishGossip command for signature '{}'.", self.id, signature);
            }
        } else {
            error!(
                "Worker {} cannot publish signature: SWARM_CMD_TX not available.", self.id
            );
        }
    }
}

impl Actor for Worker {
    type Args = Self;
    type Error = OrcaError;

    async fn on_start(mut args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, OrcaError> {
        let processor = Processor::spawn(Processor::new(args.id, &args.url, actor_ref).await);
        args.processor = Some(processor);
        Ok(args)
    }
}

// Message handlers
impl Message<StartRun> for Worker {
    type Reply = Result<Handler, OrcaError>;
    async fn handle(
        &mut self,
        message: StartRun,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let task_id = Uuid::new_v4();
        if let Some(_orca) = self.registered_tasks.get(&message.task_name) {
            let processor_result_for_match: Result<(), OrcaError>;
            let now = Utc::now().timestamp_millis();
            let distributed = self.swarm.is_some();
            if let Some(delay) = message.delay {
                let submit_at_ms = now + delay * 1000;
                processor_result_for_match = self
                    .processor
                    .as_ref()
                    .unwrap()
                    .tell(ScheduledScript {
                        id: task_id,
                        task_name: message.task_name.clone(),
                        args: message.args.clone(),
                        submit_at: submit_at_ms,
                    })
                    .await
                    .map_err(|e| OrcaError(format!("Error sending scheduled task: {}", e)));
            } else {
                processor_result_for_match = self
                    .processor
                    .as_ref()
                    .unwrap()
                    .ask(Script {
                        id: task_id,
                        task_name: message.task_name,
                        args: message.args,
                    })
                    .await
                    .map(|_| ())
                    .map_err(|e| OrcaError(format!("Error sending task: {}", e)));
            }

            match processor_result_for_match {
                Ok(()) => {
                    let run_handle =
                        Handler::new(self.processor.as_ref().unwrap().clone(), task_id, distributed).await;
                    Ok(run_handle)
                }
                Err(e) => Err(e), // e is already OrcaError
            }
        } else {
            Err(OrcaError(format!(
                "Task {} not registered",
                message.task_name
            )))
        }
    }
}

impl Message<TransitionState> for Worker {
    type Reply = Result<(), OrcaError>;

    async fn handle(
        &mut self,
        message: TransitionState,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let res = self.processor.as_ref().unwrap().ask(message).await;
        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                eprintln!("Error handling transition state: {}", e);
                Err(OrcaError(e.to_string()))
            }
        }
    }
}

impl Message<Script> for Worker {
    type Reply = Result<(), OrcaError>;

    async fn handle(
        &mut self,
        message: Script,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let name = &message.task_name;
        let id = message.id;
        let args = message.args.clone();
        let distributed = self.swarm.is_some();
        info!("Worker received script: {:?}", &message);
        if let Some(orca) = self.registered_tasks.get(name) {
            let orca = TaskRun::new(
                id,
                name.to_string(),
                ctx.actor_ref(),
                Some(orca.task_future.clone()),
                args,
                None,
                distributed
            ).await;
            self.task_runs.insert(id, orca);
            Ok(())
        } else {
            Err(OrcaError(format!(
                "Task {} not registered",
                message.task_name
            )))
        }
    }
}

impl Message<GetResultById> for Worker {
    type Reply = Result<String, OrcaError>;

    async fn handle(
        &mut self,
        message: GetResultById,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let res = self.processor.as_ref().unwrap().ask(message).await;
        let result_string = match res {
            Ok(result_string) => {
                info!("Result: {}", &result_string);
                Ok(result_string)
            }
            Err(send_error) => Err(OrcaError(send_error.to_string())),
        };
        result_string
    }
}
