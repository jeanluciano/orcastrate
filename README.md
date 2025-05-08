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



## Core Concepts

*   **Worker:** Core execution unit that manages and runs asynchronous tasks. In a distributed setup, workers leverage Libp2p to discover and communicate with each other, forming a dynamic peer-to-peer network.
*   **Task:** Individual, serializable units of work defined by users. Tasks are executed by Workers, and their state and results are tracked. In a distributed environment, tasks can be transparently processed by any available worker in the cluster.
*   **Distributed Actors & Networking:** Orcastra utilizes Libp2p for robust peer-to-peer networking, enabling workers to form a distributed cluster. This allows for direct communication, discovery, and data exchange between worker instances. The system leverages the Kameo actor framework on top of Libp2p for managing concurrent operations and simplifying distributed communication patterns.
*   **Log Based:** Processors use log data structures for task persistence and sequencing. Initially, Redis Streams and Kafka are the primary targets for development, though the design could theoretically accommodate other log-based backends.
*   **Deployment:** Designed for flexible deployment. Orcastra can run as a single-node instance for simpler use cases or scale horizontally into a distributed cluster of workers that automatically discover and coordinate with each other using Libp2p.

## Road to Cargo Package

### Phase 1: Core Functionality
- ✅ Basic worker implementation
- ✅ Task processing with Redis Streams
- ✅ Macro-based task definition


### Phase 2: Advanced Features
- ⏳ **TimeKeeper:** Defer or schedule tasks for future execution, acts as single/distributed event loop
- ⏳ **StateKeeper:** Processor for storing and managing run states (experimental)
- ⏳ **CI/CD:** Implementing sound testing and CI/CD for easier contribution
- ⏳ **Extensive Testing:** Implement comprehensive unit, integration, and potentially end-to-end tests to ensure reliability, robustness, and correct behavior under various scenarios before a Cargo release.

### Phase 3: Integration & Scaling
- 🔮 **Kafka Integration:** Integrate Kafka for robust, highly scalable event streaming capabilities, offering an alternative to Redis Streams for task queuing and state management.
- 🔮 **Python Bindings:** Utilize PyO3 to create Python bindings, allowing tasks to be defined and managed from Python, broadening Orcastra's usability.
- 🔮 **Cap'n Proto Serialization:** Implement Cap'n Proto for high-performance, zero-copy data serialization, optimizing message passing between workers and between workers and storage.(MMAAAYBEE)
- 🔮 **Advanced Caching Strategies:** Implement techniques like argument-based caching to avoid re-computing tasks with identical inputs, optimizing performance and resource usage.
- 🔮 **Self-Healing Workers:** Enhance worker resilience by enabling them to detect and automatically pick up tasks that were left in a 'Running' state (e.g., due to a previous worker instance crashing). This ensures high availability and task completion guarantees.

## Getting Started

### Prerequisites
This requires a running **Redis** instance.

1.  **Start Redis:**
    Ensure you have Redis installed. You can typically start it via the command line:
    ```bash
    redis-server
    ```
    By default, this will start Redis on `localhost:6379`. If your Redis configuration is different, ensure Orcastra is configured accordingly.

2.  **Run the Example:**
    Execute the main example using Cargo:
    ```bash
    cargo run
    ```