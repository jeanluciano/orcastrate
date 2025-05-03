use kameo::prelude::*;
use orcastra::worker::RegisterTask;
use orcastra::worker::Worker;

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::fmt::Subscriber::builder()
        .compact()
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");
    let redis_url = "redis://localhost:6379";

    let worker = Worker::new(redis_url.to_string());
    let worker_actor = Worker::spawn(worker);
    worker_actor.wait_for_startup().await;

    let task_variable = "TaskVariable".to_string();

    let task1_name = "StringTask".to_string();
    let task1_future = Box::pin(async move {
        println!("StringTask starting...");
        println!("Task variable: {task_variable}");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        println!("StringTask finished!");
        "Hello from StringTask!".to_string()
    });

    let task1 = worker_actor
        .ask(RegisterTask {
            task_name: task1_name.clone(),
            task_future: task1_future,
        })
        .await
        .unwrap();

    let task2_name = "IntegerTask".to_string();
    let task2_future = Box::pin(async move {
        println!("IntegerTask starting...");
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        let result = 42 * 2;
        println!("IntegerTask finished!");
        result
    });
    let task2 = worker_actor
        .ask(RegisterTask {
            task_name: task2_name.clone(),
            task_future: task2_future,
        })
        .await
        .unwrap();

    task1.submit().await;
    task2.submit().await;

    println!("Main loop running. Tasks are executing asynchronously...");
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}
