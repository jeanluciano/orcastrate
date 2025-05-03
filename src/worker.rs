use uuid::Uuid;
use std::collections::HashMap;
use crate::processors::redis::Processor;
use crate::task::{RegisteredTask, TaskRunner, TaskHandle};
use crate::messages::*;
use crate::types::TaskResult;
use kameo::prelude::*;
use crate::task::Run;
use crate::types::TaskFuture;
use tracing::info;

pub struct Worker {
    id: Uuid,
    url: String,
    pub registered_tasks: HashMap<String, Box<dyn TaskRunner>>,
    pub running_tasks: HashMap<Uuid, Box<dyn Run>>,
    processor: Option<ActorRef<Processor>>,
} 

impl Worker {
    pub fn new(url: String) -> Self {
        let id = Uuid::new_v4();   
        Self { 
            id, 
            url,
            running_tasks: HashMap::new(), 
            registered_tasks: HashMap::new(),
            processor: None,
        }
    }   

    pub fn register_task<R>(&mut self, task: RegisteredTask<R>)
    where 
        R: TaskResult,
    {
        let name = task.name.clone();
        self.registered_tasks.insert(name, Box::new(task));

    }
} 

impl Actor for Worker {
    type Args = Self;
    type Error = WorkerError;

    async fn on_start(mut args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, WorkerError>  {
        info!("Worker starting: {}", args.id);
        let processor = Processor::spawn(Processor::new(args.id, &args.url, actor_ref).await);
        args.processor = Some(processor);
        Ok(args)
    }
}

impl Message<MatriarchMessage<TransitionState>> for Worker {
    type Reply = MatriarchReply;

    async fn handle(
        &mut self,
        message: MatriarchMessage<TransitionState>,
        _ctx: &mut Context<Self, Self::Reply>
    ) -> Self::Reply {
        match message.recipient {
            ActorType::Processor => {
                info!("→ → {}:{} → → ", message.message.task_id, message.message.new_state.to_string());
                if let Some(proc) = &self.processor {
                    match proc.ask(message).await {
                        Ok(reply) => reply,
                        Err(e) => {
                            eprintln!("Error asking processor: {}", e);
                            MatriarchReply { success: false }
                        }
                    }
                } else {
                    eprintln!("Processor not available for task: {}", message.message.task_id);
                    MatriarchReply { success: false }
                }
            }
            ActorType::Orca => {
                let task_id_clone = message.message.task_id.clone(); 
                info!("← ← {}:{} ← ← ", task_id_clone, message.message.new_state.to_string());
                if let Some(recipient) = self.running_tasks.get(&task_id_clone) {
                    match recipient.forward_matriarch_request(message).await {
                        Ok(reply) => reply,
                        Err(e) => {
                            eprintln!("Error asking orca task {}: {}", task_id_clone, e);
                            MatriarchReply { success: false }
                        }
                    }
                } else {
                    eprintln!("Orca task {} not found or not running", task_id_clone);
                    MatriarchReply { success: false }
                }
            }
        }
    }
}

pub struct RegisterTask<R> {
    pub task_name: String,
    pub task_future: TaskFuture<R>,
}


impl<R: TaskResult> Message<RegisterTask<R>> for Worker {
    type Reply = Result<TaskHandle, WorkerError>;

    async fn handle(
        &mut self,
        message: RegisterTask<R>,
        ctx: &mut Context<Self, Self::Reply>
    ) -> Self::Reply {
        let task_name = message.task_name.clone();
        let worker_ref = ctx.actor_ref();
        let task = RegisteredTask::new(task_name.clone(), message.task_future, worker_ref.clone());
        self.register_task(task);
        Ok(TaskHandle { name: task_name, worker_ref: worker_ref.clone() })
    }
}

impl Message<SubmitTask> for Worker {
    type Reply = Result<(), WorkerError>;

    async fn handle(
        &mut self,
        message: SubmitTask,
        _ctx: &mut Context<Self, Self::Reply>
    ) -> Self::Reply {
        let task_id = Uuid::new_v4();
        if let Some(_orca) = self.registered_tasks.get_mut(&message.task_name) {
            info!("Submitting task: {}:{}", message.task_name, task_id);
            let res = self.processor.as_ref().unwrap().ask(MatriarchMessage {
                message: TransitionState {
                    task_name: message.task_name,
                    task_id: task_id,
                    new_state: OrcaStates::Submitted,
                },
                recipient: ActorType::Processor,
            }).await;
            Ok(())
        } else {
            Err(WorkerError(format!("Task {} not registered", message.task_name)))
        }
    }
}

// --- Implement Handler for StartTask ---
impl Message<RunTask> for Worker {
    // Reply indicates success/failure of starting
    type Reply = Result<(), WorkerError>; 

    async fn handle(
        &mut self,
        message: RunTask,
        ctx: &mut Context<Self, Self::Reply>
    ) -> Self::Reply {
        let name = &message.task_name;

        if let Some(spawner) = self.registered_tasks.remove(name) {
            // Get self actor ref from context
            let id = message.task_id;
            let worker_ref = ctx.actor_ref();
            match spawner.run(id, worker_ref).await {
                Ok(recipient) => {
 
                    self.running_tasks.insert(id, recipient);
                    let _ = self.processor.as_ref().unwrap().tell(MatriarchMessage {
                        message: TransitionState {
                            task_name: name.clone(),
                            task_id: id,
                            new_state: OrcaStates::Running,
                        },
                        recipient: ActorType::Orca,
                    }).await;
                    Ok(())
                }
                Err(e) => {
                    eprintln!("Failed to spawn task {}: {}", name, e);
                    Err(WorkerError(format!("Failed to spawn task {}: {}", name, e)))
                }
            }
        } else {
            Err(WorkerError(format!("Task {} not registered", name)))
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkerError(String);

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}