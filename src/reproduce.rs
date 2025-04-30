use kameo::prelude::*;
use uuid::Uuid;
use std::collections::HashMap;

#[derive(Actor)]
struct TaskActor {
    name: String,
    worker: ActorRef<WorkerActor>,
}

impl TaskActor {
    fn new(name: String, worker: ActorRef<WorkerActor>) -> Self {
        Self { name, worker }
    }
    
}

#[derive(Actor)]
struct WorkerActor {
    id: Uuid,
    url: String,
    tasks: HashMap<String, TaskActor>,
}

impl WorkerActor {
    fn new(id: Uuid, url: String) -> Self {
        Self { id, url, tasks: HashMap::new() }
    }

    fn register_task(&mut self, name: String) {
        let task = TaskActor::new(name, self.clone());
        self.tasks.insert(name, task);
    }
    fn run(self) -> ActorRef<WorkerActor> {
        kameo::spawn(self)
    }
}

#[tokio::main]
async fn main() {
    let mut worker = WorkerActor::new(Uuid::new_v4(), "http://localhost:8080".to_string());
    worker.register_task("test".to_string());
    worker.run();
}
