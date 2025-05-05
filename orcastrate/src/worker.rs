use crate::messages::{
    OrcaMessage, OrcaReply, Recipient, RunTask, ScheduleTask, SubmitTask,
    TransitionState,
};
use crate::task::{ RunState, Scheduled, StaticTaskDefinition, Submitted, TaskRun};

use crate::processors::redis::Processor;
use inventory;
use kameo::Actor;
use kameo::prelude::{ActorRef, Context, Message};
use std::collections::HashMap;
use thiserror::Error;
use tracing::info;
use tracing_subscriber;
use uuid::Uuid;
pub struct Worker {
    id: Uuid,
    url: String,
    pub registered_tasks: HashMap<String, StaticTaskDefinition>,
    pub task_runs: HashMap<Uuid, ActorRef<TaskRun>>,
    processor: Option<ActorRef<Processor>>,
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

impl Message<TransitionState> for Worker {
    type Reply = OrcaReply;
    async fn handle(
        &mut self,
        message: TransitionState,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let res = self.processor.as_ref().unwrap().ask(message).await;
        match res {
            Ok(reply) => reply,
            Err(e) => {
                eprintln!("Error handling transition state: {}", e);
                OrcaReply { success: false }
            }
        }
    }
}

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
        let res = self.processor.as_ref().unwrap().ask(TransitionState {
                task_name: task_name,
                task_id: task_id,
                new_state: RunState::Scheduled(Scheduled {
                    delay: scheduled_at as u64,
                }),
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
        println!("Submitting task: {}", message.task_name);
        let task_id = Uuid::new_v4();
        if let Some(_orca) = self.registered_tasks.get(&message.task_name) {
            info!("Submitting task: {}:{}", message.task_name, task_id);
            let res = self
                .processor
                .as_ref()
                .unwrap()
                .ask(TransitionState {
                    task_name: message.task_name,
                    task_id: task_id,
                    new_state: RunState::Submitted(Submitted {
                        max_retries: 0,
                        args: message.args,
                    }),
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
        let orca = TaskRun::new(
            message.task_id,
            name.to_string(),
            ctx.actor_ref(),
            Some(self.registered_tasks.get(name).unwrap().task_future.clone()),
            message.args,
        );
        let actor_ref = TaskRun::spawn(orca);
        self.task_runs.insert(message.task_id, actor_ref);
        Ok(())
    }
}

#[derive(Error, Debug, Clone)]
pub struct WorkerError(pub String);

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for WorkerError {
    fn from(s: String) -> Self {
        WorkerError(s)
    }
}
