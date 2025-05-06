use std::{
    any::{Any, TypeId},
    collections::HashMap,
    marker::PhantomData,
    time::Duration,
};

use kameo::prelude::*;


#[derive(Clone, Copy, Debug, Default, Hash, PartialEq, Eq)]
pub enum DeliveryStrategy {
    #[default]
    Guaranteed,
    BestEffort,
    TimedDelivery(Duration),
    Spawned,
    SpawnedWithTimeout(Duration),
}

#[derive(Actor, Debug, Default)]
pub struct MessageBus {
    subscriptions: HashMap<TypeId, Vec<Registration>>,
    delivery_strategy: DeliveryStrategy,
}

impl MessageBus {
    pub fn new(delivery_strategy: DeliveryStrategy) -> Self {
        MessageBus {
            subscriptions: HashMap::new(),
            delivery_strategy,
        }
    }

    fn unsubscribe<M: 'static>(&mut self, actor_id: &ActorID) {
        let type_id = TypeId::of::<M>();
        if let Some(recipients) = self.subscriptions.get_mut(&type_id) {
            recipients.retain(|reg| &reg.actor_id != actor_id);
            if recipients.is_empty() {
                self.subscriptions.remove(&type_id);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct Register<M: Send + 'static>(pub Recipient<M>);

impl<M: Send + 'static> Message<Register<M>> for MessageBus {
    type Reply = ();

    async fn handle(
        &mut self,
        Register(recipient): Register<M>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.subscriptions
            .entry(TypeId::of::<M>())
            .or_default()
            .push(Registration::new(recipient));
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unregister<M> {
    actor_id: ActorID,
    phantom: PhantomData<M>,
}

impl<M> Unregister<M> {
    pub fn new(actor_id: ActorID) -> Self {
        Unregister {
            actor_id,
            phantom: PhantomData,
        }
    }
}

impl<M: Send + 'static> Message<Unregister<M>> for MessageBus {
    type Reply = ();

    async fn handle(
        &mut self,
        Unregister { actor_id, .. }: Unregister<M>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.unsubscribe::<M>(&actor_id);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publish<M>(pub M);

impl<M: Clone + Send + 'static> Message<Publish<M>> for MessageBus {
    type Reply = ();

    async fn handle(
        &mut self,
        Publish(message): Publish<M>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let mut to_remove = Vec::new();

        if let Some(registrations) = self.subscriptions.get(&TypeId::of::<M>()) {
            for Registration {
                actor_id,
                recipient,
            } in registrations
            {
                let recipient: &Recipient<M> = recipient.downcast_ref().unwrap();
                match self.delivery_strategy {
                    DeliveryStrategy::Guaranteed => {
                        let res = recipient.tell(message.clone()).await;
                        if let Err(SendError::ActorNotRunning(_)) = res {
                            to_remove.push(*actor_id);
                        }
                    }
                    DeliveryStrategy::BestEffort => {
                        let res = recipient.tell(message.clone()).try_send();
                        if let Err(SendError::ActorNotRunning(_)) = res {
                            to_remove.push(*actor_id);
                        }
                    }
                    DeliveryStrategy::TimedDelivery(duration) => {
                        let res = recipient
                            .tell(message.clone())
                            .mailbox_timeout(duration)
                            .await;
                        if let Err(SendError::ActorNotRunning(_)) = res {
                            to_remove.push(*actor_id);
                        }
                    }
                    DeliveryStrategy::Spawned => {
                        let actor_id = *actor_id;
                        let recipient = recipient.clone();
                        let message = message.clone();
                        let message_bus_ref = ctx.actor_ref();
                        tokio::spawn(async move {
                            let res = recipient.tell(message).send().await;
                            if let Err(SendError::ActorNotRunning(_)) = res {
                                let _ = message_bus_ref.tell(Unregister::<M>::new(actor_id)).await;
                            }
                        });
                    }
                    DeliveryStrategy::SpawnedWithTimeout(duration) => {
                        let actor_id = *actor_id;
                        let recipient = recipient.clone();
                        let message = message.clone();
                        let message_bus_ref = ctx.actor_ref();
                        tokio::spawn(async move {
                            let res = recipient
                                .tell(message)
                                .mailbox_timeout(duration)
                                .send()
                                .await;
                            if let Err(SendError::ActorNotRunning(_)) = res {
                                let _ = message_bus_ref.tell(Unregister::<M>::new(actor_id)).await;
                            }
                        });
                    }
                }
            }
        }

        for actor_id in to_remove {
            self.unsubscribe::<M>(&actor_id);
        }
    }
}

#[derive(Debug)]
struct Registration {
    actor_id: ActorID,
    recipient: Box<dyn Any + Send + Sync>,
}

impl Registration {
    fn new<M: Send + 'static>(recipient: Recipient<M>) -> Self {
        Registration {
            actor_id: recipient.id(),
            recipient: Box::new(recipient),
        }
    }
}