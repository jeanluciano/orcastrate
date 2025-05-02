use kameo::prelude::*;
use orcastra::worker::{Worker};
use orcastra::messages::*;
use tracing_subscriber;
use tracing::info;


#[tokio::main]
async fn main() {

    let subscriber = tracing_subscriber::fmt::Subscriber::builder()
        .compact()
        .finish();
    
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");
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
   

    let worker_actor = Worker::spawn(worker);
    println!("Worker actor spawned: {:?}", worker_actor.id());
    worker_actor.wait_for_startup().await;

    let start_msg1 = SubmitTask { task_name: task1_name };
    match worker_actor.ask(start_msg1).await {
        Ok(_) => println!("main submitted StringTask"),
        Err(e) => eprintln!("Actor communication error submitting StringTask: {}", e),
    }
 
    let start_msg2 = SubmitTask { task_name: task2_name };
    match worker_actor.ask(start_msg2).await {
        Ok(_) => println!("main submitted IntegerTask"),
        Err(e) => eprintln!("Actor communication error submitting IntegerTask: {}", e),
    }
    
    println!("Main loop running. Tasks are executing asynchronously...");
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}
