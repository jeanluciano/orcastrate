use uuid::Uuid;
use std::collections::HashMap;
use crate::processors::redis::Processor;
use crate::task::OrcaSpawner;

use crate::messages::{MatriarchMessage, MatriarchReply, OrcaRequest, ActorType, OrcaStates, ErasedOrcaSpawner, OrcaMessageRecipient};
use kameo::prelude::*;
use std::future::Future;
use std::pin::Pin;
use serde::Serialize;


// Worker for running jobs inside of worker threads. Each worker thread is a consumer and keeps its last
// read position in the log. 


pub struct Worker {
    id: Uuid,
    url: String,
    pub registered_tasks: HashMap<String, Box<dyn ErasedOrcaSpawner>>,
    pub running_tasks: HashMap<String, Box<dyn OrcaMessageRecipient>>,
    processor: Option<ActorRef<Processor>>,
} 

// Define StartTask message
#[derive(Debug, Clone)]
pub struct StartTask {
    pub task_name: String,
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

    pub fn register_task<R>(&mut self, name: String, task_future: Pin<Box<dyn Future<Output = R> + Send + Sync >>) -> &mut Self 
    where 
        R: Serialize + Send + Sync + 'static,
    {
        let spawner = Box::new(OrcaSpawner::new(name.clone(), task_future));
        self.registered_tasks.insert(name, spawner);
        self
    }
} 



impl Actor for Worker {
    type Args = Self;
    type Error = WorkerError;

    async fn on_start(mut args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, WorkerError>  {
        println!("Worker starting: {}", args.id);
        let processor = Processor::spawn(Processor::new(args.id, &args.url, actor_ref).await);
        args.processor = Some(processor);
        Ok(args)
    }
}

impl Message<MatriarchMessage<OrcaRequest>> for Worker {
    type Reply = MatriarchReply;

    async fn handle(
        &mut self,
        message: MatriarchMessage<OrcaRequest>,
        ctx: &mut Context<Self, Self::Reply>
    ) -> Self::Reply {
        match message.recipient {
            ActorType::Processor => {
                println!("Handling Processor request for task: {}", message.message.task_id);
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
                println!("Handling Orca request for task: {}", task_id_clone);
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

// --- Implement Handler for StartTask ---
impl Message<StartTask> for Worker {
    // Reply indicates success/failure of starting
    type Reply = Result<(), WorkerError>; 

    async fn handle(
        &mut self,
        message: StartTask,
        ctx: &mut Context<Self, Self::Reply>
    ) -> Self::Reply {
        let name = &message.task_name;
        println!("Attempting to start task: {}", name);
        if let Some(spawner) = self.registered_tasks.remove(name) {
            // Get self actor ref from context
            let self_actor_ref = ctx.actor_ref();
            match spawner.spawn_and_get_recipient(self_actor_ref).await {
                Ok(recipient) => {
                    println!("Successfully spawned task: {}", name);
                    self.running_tasks.insert(name.to_string(), recipient);
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