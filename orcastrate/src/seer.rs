use crate::error::OrcaError;
use crate::messages::{GetResult, ListenForResult, OrcaReply};
use crate::notify::Register;
use crate::processors::redis::Processor;
use kameo::prelude::*;
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::info;
use uuid::Uuid;

pub struct Seer {

    task_id: Uuid,
    processor_ref: ActorRef<Processor>, 
    notify_result_ready: Arc<Notify>,
}

impl Seer {
    pub fn new(
        processor_ref: ActorRef<Processor>,
    ) -> ActorRef<Seer> {
        let task_id = Uuid::new_v4();
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

impl Message<GetResult> for Seer {
    type Reply = Result<String, OrcaError>;

    async fn handle(
        &mut self,
        message: GetResult,
        _ctx: &mut Context<Seer, Self::Reply>,
    ) -> Self::Reply {
        let processor_get_result_msg = message.clone();

        let initial_ask_result: Result<String, SendError<GetResult, crate::error::OrcaError>> =
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
                _ = tokio::time::sleep(timeout_duration) => {
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
        let final_ask_result: Result<String, SendError<GetResult, crate::error::OrcaError>> =
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
