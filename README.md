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

Orcastra is a Proof-of-Concept (PoC) job queue designed for processing tasks (called **Orcas**) efficiently.

## Core Concepts

*   **Job Queue:** Manages asynchronous tasks.
*   **Orcas:** The individual units of work or tasks processed by the queue.
*   **Redis Streams:** Utilized for efficient message queuing and persistence.
*   **Deployment:** Designed to run either on a single server or in a distributed environment.

## Future Goals
*   **Schechduler** Defer or schecdule Orcas.
*   **Kafka:** Integrated for robust, scalable event streaming capabilities.
*   **Python** Use PYO3 to translate Parseltongue to Orca dialtect. 
*   **Serialization:** Implement [Cap'n Proto](https://capnproto.org/) for high-performance data serialization.
*   **API:** Simplify the user API through the use of macros.

## Getting Started

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

## Contributing

_(Contribution guidelines will be added as the project matures.)_

## License

_(License information will be added here.)_
