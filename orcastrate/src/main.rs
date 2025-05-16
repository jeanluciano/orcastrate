use orcastrate::worker::run;
use orcastrate_macro::orca_task;
use tokio::time::Duration;
use serde::{Serialize, Deserialize};

#[orca_task]
async fn my_async_task(url: String, count: i32) -> Result<String, String> {
    // No TaskContext
    println!("Running task: url={}, count={}", url, count);
    tokio::time::sleep(Duration::from_secs(1)).await;
    if count < 0 {
        Err("Count cannot be negative".to_string())
    } else {
        Ok(format!("Processed {} - Count: {}", url, count))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnySerdeType {
    option: Option<String>,
    active: bool,
}

#[orca_task]
async fn my_async_task_2(url: String, supports: AnySerdeType) -> Result<String, String> {
    println!("Running task: url={}, supports={:?}", url, supports);
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(format!("Processed {} - Supports: {:?}", url, supports))
}


#[tokio::main]
async fn main() {
    let worker = run("redis://localhost:6379");
    let async_task = my_async_task::register(worker.clone());
    let async_task_2 = my_async_task_2::register(worker.clone());

    let async_task = async_task
    .submit("https://example.com".to_string(), 10)
    .start(10)
    .await;

    let async_task_2 = async_task_2
    .submit("https://example.com".to_string(), AnySerdeType { option: Some("test".to_string()), active: true })
    .await;

    match async_task {
        Ok(async_task) => {
            let result = async_task.result(None).await.expect("Getting result failed");
            println!("Result: {}", result);
        }
        Err(e) => {
            println!("Error starting task: {}", e);
        }
    }

    match async_task_2 {
        Ok(async_task) => {
            let result = async_task.result(None).await.expect("Getting result failed");
            println!("Result: {}", result);
        }
        Err(e) => {
            println!("Error starting task: {}", e);
        }
    }
    println!("Main loop running. Tasks are executing asynchronously...");
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}
