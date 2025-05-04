use crate::messages::{
    OrcaMessage, OrcaReply, Recipient, RegisterTask, RunTask, ScheduleTask, SubmitTask,
    SubmitTaskArgs, TransitionState,
};
use crate::task::{
     RegisteredTask, Running, Run, Scheduled, StaticTaskDefinition,
    Submitted, TaskRunner, RunState,
};

use crate::processors::redis::Processor;
use kameo::Actor;
use kameo::prelude::{ActorRef, Context, Message};
use std::collections::HashMap;
use tracing::info;
use uuid::Uuid;
use thiserror::Error;
use crate::types::TaskResult;
use inventory;

pub struct Worker {
    id: Uuid,
    url: String,
    pub registered_tasks: HashMap<String, StaticTaskDefinition>,
    pub task_runs: HashMap<Uuid, Box<dyn Run>>,
    processor: Option<ActorRef<Processor>>,
}

impl Worker {
    pub fn new(url: String) -> Self {
        let id = Uuid::new_v4();
        let mut registered_tasks: HashMap<String, StaticTaskDefinition> = HashMap::new();
        for task_def in inventory::iter::<StaticTaskDefinition> {
            println!("Discovered task: {}", task_def.task_name);
            registered_tasks.insert(task_def.task_name.to_string(), task_def.clone());
        }
        Self {
            id,
            url,
            task_runs: HashMap::new(),
            registered_tasks,
            processor: None,
        }   
    }
    pub async fn run(self) -> ActorRef<Self> {
        let worker_actor = Self::spawn(self);
        worker_actor.wait_for_startup().await;
        worker_actor
    }
}

impl Actor for Worker {
    type Args = Self;
    type Error = WorkerError;

    async fn on_start(
        mut args: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, WorkerError> {

        let processor = Processor::spawn(Processor::new(args.id, &args.url, actor_ref).await);
        args.processor = Some(processor);
        Ok(args)
    }
}

impl Message<OrcaMessage<TransitionState>> for Worker {
    type Reply = OrcaReply;

    async fn handle(
        &mut self,
        message: OrcaMessage<TransitionState>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match message.recipient {
            Recipient::Processor => {
                info!(
                    "→ → {}:{} → → ",
                    message.message.task_id,
                    message.message.new_state.to_string()
                );
                if let Some(proc) = &self.processor {
                    match proc.ask(message).await {
                        Ok(reply) => reply,
                        Err(e) => {
                            eprintln!("Error asking processor: {}", e);
                            OrcaReply { success: false }
                        }
                    }
                } else {
                    eprintln!(
                        "Processor not available for task: {}",
                        message.message.task_id
                    );
                    OrcaReply { success: false }
                }
            }
            Recipient::Orca => {
                let task_id_clone = message.message.task_id.clone();
                info!(
                    "← ← {}:{} ← ← ",
                    task_id_clone,
                    message.message.new_state.to_string()
                );
                if let Some(recipient) = self.task_runs.get(&task_id_clone) {
                    match recipient.forward_matriarch_request(message).await {
                        Ok(reply) => reply,
                        Err(e) => {
                            eprintln!("Error asking orca task {}: {}", task_id_clone, e);
                            OrcaReply { success: false }
                        }
                    }
                } else {
                    eprintln!("Orca task {} not found or not running", task_id_clone);
                    OrcaReply { success: false }
                }
            }
        }
    }
}

// impl<R: TaskResult> Message<RegisterTask<R>> for Worker {
//     type Reply = Result<(), WorkerError>;

//     async fn handle(
//         &mut self,
//         message: RegisterTask<R>,
//         ctx: &mut Context<Self, Self::Reply>,
//     ) -> Self::Reply {
//         let task_name = message.task_name.clone();
//         let worker_ref = ctx.actor_ref();
//         let task = RegisteredTask::new(task_name.clone(), message.task_future, worker_ref.clone());
//         self.registered_tasks.insert(task_name, Box::new(task));
//         Ok(())
//     }
// }

impl Message<ScheduleTask> for Worker {
    type Reply = Result<(), WorkerError>;

    async fn handle(
        &mut self,
        message: ScheduleTask,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let task_id = Uuid::new_v4();
        let task_name = message.task_name.clone();
        let scheduled_at = message.scheduled_at;
        let task = self.registered_tasks.get_mut(&task_name).unwrap();
        let rest = self.processor.as_ref().unwrap().ask(OrcaMessage {
            message: TransitionState {
                task_name: task_name,
                task_id: task_id,
                new_state: RunState::Scheduled(Scheduled { delay: scheduled_at as u64 }),
            },
            recipient: Recipient::Processor,
        });
        Ok(())
    }
}

impl Message<SubmitTask> for Worker {
    type Reply = Result<(), WorkerError>;

    async fn handle(
        &mut self,
        message: SubmitTask,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let task_id = Uuid::new_v4();
        if let Some(_orca) = self.registered_tasks.get(&message.task_name) {
            info!("Submitting task: {}:{}", message.task_name, task_id);
            let res = self
                .processor
                .as_ref()
                .unwrap()
                .ask(OrcaMessage {
                    message: TransitionState {
                        task_name: message.task_name,
                        task_id: task_id,
                        new_state: RunState::Submitted(Submitted {
                            max_retries: 0,
                            args: message.args,
                        }),
                    },
                    recipient: Recipient::Processor,
                })
                .await;
            Ok(())
        } else {
            Err(WorkerError(format!(
                "Task {} not registered",
                message.task_name
            )))
        }
    }
}

impl Message<SubmitTaskArgs> for Worker {
    type Reply = Result<(), WorkerError>;

    async fn handle(
        &mut self,
        message: SubmitTaskArgs,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let task_id = Uuid::new_v4();
        if let Some(_orca) = self.registered_tasks.get(&message.task_name) {
            info!("Submitting task: {}:{}", message.task_name, task_id);
            let res = self
                .processor
                .as_ref()
                .unwrap()
                .ask(OrcaMessage {
                    message: TransitionState {
                        task_name: message.task_name,
                        task_id: task_id,
                        new_state: RunState::Submitted(Submitted {
                            max_retries: 0,
                            args: message.args,
                        }),
                    },
                    recipient: Recipient::Processor,
                })
                .await;
            Ok(())
        } else {
            Err(WorkerError(format!(
                "Task {} not registered",
                message.task_name
            )))
        }
    }
}

impl Message<RunTask> for Worker {
    type Reply = Result<(), WorkerError>;

    async fn handle(
        &mut self,
        message: RunTask,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let name = &message.task_name;

        if let Some(spawner) = self.registered_tasks.remove(name) {
            let id = message.task_id;
            let worker_ref = ctx.actor_ref();
            let future = (spawner.runner)(message.args);
            let result = future;
            Ok(())
        } else {
            Err(WorkerError(format!("Task {} not registered", name)))
        }
    }
}

#[derive(Error,Debug, Clone)]
pub struct WorkerError(pub String);

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Implement From<String> to allow `?` to convert String errors
impl From<String> for WorkerError {
    fn from(s: String) -> Self {
        WorkerError(s)
    }
}
