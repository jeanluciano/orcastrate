use kameo::prelude::*;
use orcastra::worker::{Worker};
use orcastra::messages::*;

#[tokio::main]
async fn main() {
    let redis_url = "redis://localhost:6379";

    
    let mut worker = Worker::new(redis_url.to_string());
    
    let task_variable = "TaskVariable".to_string();
    
    let task1_name = "StringTask".to_string();
    let task1_future = Box::pin(async move {
        println!("StringTask starting...");
        println!("Task variable: {}", task_variable);
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        println!("StringTask finished!");
        "Hello from StringTask!".to_string()
    });
    worker.register_task(task1_name.clone(), task1_future);
    println!("Registered task: {}", task1_name);



    let task2_name = "IntegerTask".to_string();
    let task2_future = Box::pin(async move {
        println!("IntegerTask starting...");
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        let result = 42 * 2;
        println!("IntegerTask finished!");
        result
    });
    worker.register_task(task2_name.clone(), task2_future);
   
    println!("Spawning Worker actor...");
    let worker_actor = Worker::spawn(worker);
    println!("Worker actor spawned: {:?}", worker_actor.id());

    
    println!("Sending StartTask message for: {}", task1_name);
    let start_msg1 = RunTask { task_name: task1_name };
    match worker_actor.ask(start_msg1).await {
        Ok(_) => println!("Successfully started StringTask"),
        Err(e) => eprintln!("Actor communication error starting StringTask: {}", e),
    }
    println!("Sending StartTask message for: {}", task2_name);
    let start_msg2 = RunTask { task_name: task2_name };
    match worker_actor.ask(start_msg2).await {
        Ok(_) => println!("Successfully started IntegerTask"),
        Err(e) => eprintln!("Actor communication error starting IntegerTask: {}", e),
    }
    
    println!("Main loop running. Tasks are executing asynchronously...");
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}
