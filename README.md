# Orcastra

```text
    ~                           ~              ~
       ~~~~     ~          ~~ ~        ~      ~    ~~
  ~~             _,--''-.     ~~        .-._       ~  ~~
            ,---\':::::::\\`.            \\_::`.,...__    ~
     ~     |::`.:::::::::::`.       ~    )::::::.--\'
           |:_:::`.::::::::::`-.__~____,\'::::(
 ~~~~       \\```-:::`-.o:::::::::\\:::::::::~::\\       ~~
             )` `` `.::::::::::::|:~~:::::::::|      ~   ~~
 ~~        ,\',\' ` `` \\::::::::,-/:_:::::::~~:/
         ,\',\'/` ,\' ` `\\::::::|,\'   `::~~::::/  ~~        ~
~       ( (  \\_ __,.-\' \\:-:,-\'.__.-\':::::::\'  ~    ~
    ~    \\`---\'\'   __..--\' `:::~::::::_:-\'
          `------\'\'      ~~  \\::~~:::\'
       ~~   `--..__  ~   ~   |::_:-\'                    ~~
   ~ ~~     /:,\'   `\'\'---.,--\':::\\          ~~       ~
  ~         ``           (:::::::|  ~~~            ~~    ~
~~      ~~             ~  \\:~~~:::             ~       ~~
             ~     ~~~     \\:::~::          ~~~     ~
    ~~ jrei      ~~    ~~~  ::::::                     ~~
          ~~~                \\::::   ~~
                       ~   ~~ `--\'
```

Orcastra is a Proof-of-Concept (PoC) job queue designed for processing tasks efficiently.

## Core Concepts

*   **Worker:** Manages asynchronous tasks.
*   **Task:** The individual units of work processed by the queue.
*   **Log Based:** Processors use log data structures. Only Redis streams and Kafka will be developed at first (could in theory work with a .txt file too!)
*   **Deployment:** Designed to run either on a single server or in a distributed environment.

## Road to Alpha

### Phase 1: Core Functionality
- ✅ Basic worker implementation
- ✅ Task processing with Redis Streams
- ✅ Macro-based task definition

### Phase 2: Advanced Features
- ⏳ **TimeKeeper:** Defer or schedule tasks for future execution, acts as single/distributed event loop
- ⏳ **Error Handling:** Implement comprehensive retry mechanisms and dead-letter queues
- ⏳ **StateKeeper:** Processor for storing and managing run states
- ⏳ **CI/CD:** Implementing sound testing and CI/CD for easier contribution

### Phase 3: Integration & Scaling
- 🔮 **Kafka:** Integrate for robust, scalable event streaming capabilities
- 🔮 **Python:** Use PYO3 to create Python bindings for cross-language support
- 🔮 **Serialization:** Implement [Cap'n Proto](https://capnproto.org/) for high-performance data serialization

## Getting Started

### Prerequisites
This project requires Rust **nightly** and a running **Redis** instance.

1.  **Install/Switch to Nightly Toolchain:**
    If you don't have it, install `rustup` first. Then, in the project directory, run:
    ```bash
    rustup override set nightly
    ```

2.  **Start Redis:**
    The simplest way is using Docker:
    ```bash
    docker run -d --name orcastra-redis -p 6379:6379 redis
    ```
    Ensure Redis is running on the default port (`6379`) at `localhost`.

3.  **Run the Project:**
    Execute the main example using Cargo:
    ```bash
    cargo run
    ```
    This will compile and run the example tasks defined in `src/main.rs`. You should see output indicating task submission and processing via Redis streams.
    
```rust
use orcastrate_macro::orca_task;

#[orca_task]
async fn my_async_task(url: String, count: i32) -> Result<String, String> {
    println!("Running task: url={}, count={}", url, count);
    // Perform your task logic here
    if count < 0 {
        Err("Count cannot be negative".to_string())
    } else {
        Ok(format!("Processed {} - Count: {}", url, count))
    }
}
```

2. Create a worker and register your tasks:

```rust
use orcastrate::worker::Worker;

// Initialize a worker with Redis connection
let worker = Worker::new("redis://localhost:6379".to_string())
    .run()
    .await;

// Register your task with the worker
let async_task = my_async_task::register(worker.clone());

// Submit your task for execution
let result = async_task
    .submit("https://example.com".to_string(), 10)
    .await;
```

For more examples and advanced usage, check the documentation.

## Contributing

_(Contribution guidelines will be added as the project matures.)_

## License

_(License information will be added here.)_
