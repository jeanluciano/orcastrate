use crate::error::OrcaError;
use crate::messages::{HandleResult, ListenForResult, OrcaReply, GetResultById};
use crate::notify::Register;
use crate::processors::redis::Processor;
use kameo::prelude::*;
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::info;
use uuid::Uuid;
use tokio::time::Duration;
pub struct Seer {
    task_id: Uuid,
    processor_ref: ActorRef<Processor>, 
    notify_result_ready: Arc<Notify>,
}

impl Seer {
    pub fn new(
        processor_ref: ActorRef<Processor>,
        task_id: Uuid,
    ) -> ActorRef<Seer> {
        let notify_result_ready = Arc::new(Notify::new());
        Seer::spawn(Self {
            task_id,
            processor_ref,
            notify_result_ready,
        })
    }
}

impl Actor for Seer {
    type Args = Self;
    type Error = OrcaError;

    async fn on_start(args: Self::Args, actor_ref: ActorRef<Seer>) -> Result<Self, OrcaError> {
        let recipient = actor_ref.clone();
        let message = Register(recipient.recipient::<ListenForResult>());
        let _ = args.processor_ref.tell(message).await;
        Ok(args)
    }
}

impl Message<ListenForResult> for Seer {
    type Reply = OrcaReply;

    async fn handle(
        &mut self,
        message: ListenForResult,
        _ctx: &mut Context<Seer, Self::Reply>,
    ) -> Self::Reply {
        info!("Received ListenForResult for task {}", message.task_id);
        self.notify_result_ready.notify_one();
        OrcaReply { success: true }
    }
}

impl Message<HandleResult> for Seer {
    type Reply = Result<String, OrcaError>;

    async fn handle(
        &mut self,
        message: HandleResult,
        _ctx: &mut Context<Seer, Self::Reply>,
    ) -> Self::Reply {
        let processor_get_result_msg = GetResultById { task_id: self.task_id };

        let initial_ask_result: Result<String, SendError<GetResultById, crate::error::OrcaError>> =
            self.processor_ref
                .ask(processor_get_result_msg.clone())
                .await;

        match initial_ask_result {
            Ok(result_str) if result_str != "None" => {
                return Ok(result_str);
            }
            Ok(_none_result) => {
                info!(
                    "Result not immediately available (is 'None') for task {}, will wait.",
                    self.task_id
                );
            }
            Err(send_error) => {
                return Err(OrcaError(format!(
                    "Initial ask to processor for task {} failed: {}",
                    self.task_id, send_error
                )));
            }
        }

        // Wait for notification or timeout
        if let Some(timeout_duration) = message.timeout {
            tokio::select! {
                _ = self.notify_result_ready.notified() => {
                    info!("Notified for result for task {}", self.task_id);
                    // Notification received, try fetching result again
                }
                _ = tokio::time::sleep(Duration::from_secs(timeout_duration as u64)) => {
                    info!("Timeout waiting for result for task {}", self.task_id);
                    // Timeout elapsed, try fetching result one last time or return timeout error
                }
            }
        } else {
            // Wait indefinitely
            info!(
                "Waiting indefinitely for result notification for task {}",
                self.task_id
            );
            self.notify_result_ready.notified().await;
            info!(
                "Notified for result for task {} (waited indefinitely)",
                self.task_id
            );
        }

        // Attempt to fetch the result again after waiting/notification
        let final_ask_result: Result<String, SendError<GetResultById, crate::error::OrcaError>> =
            self.processor_ref.ask(processor_get_result_msg).await;

        match final_ask_result {
            Ok(result_str) if result_str != "None" => Ok(result_str),
            Ok(_none_result) => Err(OrcaError(format!(
                "Result still 'None' for task {} after waiting",
                self.task_id
            ))),
            Err(send_error) => {
                return Err(OrcaError(format!(
                    "Final ask to processor for task {} failed: {}",
                    self.task_id, send_error
                )));
            }
        }
    }
}


pub struct Handler {
    seer_ref: ActorRef<Seer>,
}

impl Handler {
    pub fn new(actor_ref: ActorRef<Processor>, task_id: Uuid) -> Self {
        Self { seer_ref: Seer::new(actor_ref, task_id) }
    }
    pub async fn result(&self, timeout: impl Into<Option<i64>>) -> Result<String, OrcaError> {
        let timeout_option: Option<i64> = timeout.into();
        let result = self.seer_ref.ask(HandleResult { timeout: timeout_option }).await;
        match result {
            Ok(result) => Ok(result),
            Err(e) => Err(OrcaError(format!("Error getting result: {}", e))),
        }
    }
}


