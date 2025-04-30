use kameo::prelude::*;
use uuid::Uuid;
use crate::worker::Worker;
use crate::processors::redis::Processor;
use crate::messages::{MatriarchMessage, MatriarchReply, TaskRequest, ActorType};

pub struct Matriarch {
    id: Uuid,
    processor: Option<ActorRef<Processor>>,
    worker: Option<ActorRef<Worker>>,
}



impl Matriarch {
    pub async fn new(id: Uuid) -> Self {
        Self { id, processor: None, worker: None }
    }
}


impl Actor for Matriarch {
    type Error = MatriarchError;

    async fn on_start(&mut self, actor_ref: ActorRef<Self>) -> Result<(), MatriarchError> {
        Ok(())
    }
}

impl Message<MatriarchMessage<TaskRequest>> for Matriarch
{

    type Reply = ForwardedReply<MatriarchMessage<TaskRequest>,MatriarchReply>;

    async fn handle(
        &mut self,
        message: MatriarchMessage<TaskRequest>,
        ctx: &mut Context<Self,ForwardedReply<MatriarchMessage<TaskRequest>,MatriarchReply>>
    ) -> Self::Reply {
        match message.recipient {
            ActorType::Worker => {
                ctx.forward( &self.worker,message).await
            }
            ActorType::Processor => {
                ctx.forward( &self.processor,message).await
            }
        }
    }
}






#[derive(Debug, Clone)]
pub struct MatriarchError(String);

