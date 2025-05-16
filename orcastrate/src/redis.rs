use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use redis::{RedisError, RedisResult};

use tracing::debug;

pub mod gatekeeper;
pub mod statekeeper;
pub mod timekeeper;

// Define constants within this file for now
const GATEKEEPER_STREAM_KEY: &str = "orca:streams:gatekeeper";
const TIMEKEEPER_STREAM_KEY: &str = "orca:streams:timekeeper";
const STATEKEEPER_STREAM_KEY: &str = "orca:streams:statekeeper";
const TASK_GROUP_KEY: &str = "worker";

pub async fn ensure_streams(mut redis: MultiplexedConnection) -> Result<(), RedisError> {
    // Use the constants defined in this module
    let gatekeeper_res: RedisResult<String> = redis
        .xgroup_create_mkstream(GATEKEEPER_STREAM_KEY, TASK_GROUP_KEY, "$") // Use $ instead of 0 to ignore history
        .await;
    let timekeeper_res: RedisResult<String> = redis
        .xgroup_create_mkstream(TIMEKEEPER_STREAM_KEY, TASK_GROUP_KEY, "$")
        .await;
    let statekeeper_res: RedisResult<String> = redis
        .xgroup_create_mkstream(STATEKEEPER_STREAM_KEY, TASK_GROUP_KEY, "$")
        .await;

    // Check results, ignoring "BUSYGROUP" errors
    for res in [gatekeeper_res, timekeeper_res, statekeeper_res] {
        match res {
            Ok(_) => {}
            Err(e)
                if e.kind() == redis::ErrorKind::ExtensionError
                    && e.to_string().contains("BUSYGROUP") =>
            {
                // Group already exists, which is fine
                debug!("Consumer group already exists (ignored): {}", e);
            }
            Err(e) => {
                // Other Redis error, return it
                eprintln!("Error creating stream/group: {:?}", e);
                return Err(e);
            }
        }
    }
    Ok(())
}
