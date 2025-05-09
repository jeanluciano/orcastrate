use futures::StreamExt;
use kameo::{
    prelude::*,
    remote::{
        ActorSwarmBehaviourEvent, ActorSwarmEvent, ActorSwarmHandler, SwarmBehaviour, SwarmRequest,
        SwarmResponse,
    },
};
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder, gossipsub,
    kad::{
        self,
        store::{MemoryStore, RecordStore},
    },
    mdns,
    request_response::{self, OutboundRequestId, ProtocolSupport, ResponseChannel},
    swarm::{NetworkBehaviour, SwarmEvent},
};
use std::{
    borrow::Cow,
    collections::hash_map::DefaultHasher,
    env::args,
    hash::{Hash, Hasher},
    io,
    time::Duration,
};

#[derive(NetworkBehaviour)]
struct CustomBehaviour {
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    actor_request_response: request_response::cbor::Behaviour<SwarmRequest, SwarmResponse>,
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
}

pub async fn start_swarm(port: u16) -> Result<&'static ActorSwarm, Box<dyn std::error::Error>> {
    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_quic()
        .with_behaviour(|key| {
            let kademlia = kad::Behaviour::new(
                key.public().to_peer_id(),
                MemoryStore::new(key.public().to_peer_id()),
            );
            let message_id_fn = |message: &gossipsub::Message| {
                let mut s = DefaultHasher::new();
                message.data.hash(&mut s);
                gossipsub::MessageId::from(s.finish().to_string())
            };

            // Set a custom gossipsub configuration
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
                .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message
                // signing)
                .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be propagated.
                .build()
                .map_err(io::Error::other)?; // Temporary hack because `build` does not return a proper `std::error::Error`.

            // build a gossipsub network behaviour
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;

            Ok(CustomBehaviour {
                kademlia,
                actor_request_response: request_response::cbor::Behaviour::new(
                    [(StreamProtocol::new("/kameo/1"), ProtocolSupport::Full)],
                    request_response::Config::default(),
                ),
                gossipsub,
                mdns: mdns::tokio::Behaviour::new(
                    mdns::Config::default(),
                    key.public().to_peer_id(),
                )?,
            })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    let (actor_swarm, mut swarm_handler) =
        ActorSwarm::bootstrap_manual(*swarm.local_peer_id()).unwrap();

    actor_swarm.listen_on(format!("/ip4/0.0.0.0/udp/{port}/quic-v1").parse()?);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                    Some(cmd) = swarm_handler.next_command() => swarm_handler.handle_command(&mut swarm, cmd),
                Some(event) = swarm.next() => {
                    handle_event(&mut swarm, &mut swarm_handler, event);
                }
            }
        }
    });

    Ok(actor_swarm)
}

fn handle_event(
    swarm: &mut Swarm<CustomBehaviour>,
    swarm_handler: &mut ActorSwarmHandler,
    event: SwarmEvent<CustomBehaviourEvent>,
) {
    match event {
        SwarmEvent::ConnectionClosed {
            peer_id,
            connection_id,
            endpoint,
            num_established,
            cause,
        } => {
            swarm_handler.handle_event(
                swarm,
                ActorSwarmEvent::ConnectionClosed {
                    peer_id,
                    connection_id,
                    endpoint,
                    num_established,
                    cause,
                },
            );
        }

        SwarmEvent::Behaviour(event) => match event {
            CustomBehaviourEvent::Kademlia(event) => swarm_handler.handle_event(
                swarm,
                ActorSwarmEvent::Behaviour(ActorSwarmBehaviourEvent::Kademlia(event)),
            ),
            CustomBehaviourEvent::ActorRequestResponse(event) => swarm_handler.handle_event(
                swarm,
                ActorSwarmEvent::Behaviour(ActorSwarmBehaviourEvent::RequestResponse(event)),
            ),
            CustomBehaviourEvent::Mdns(event) => swarm_handler.handle_event(
                swarm,
                ActorSwarmEvent::Behaviour(ActorSwarmBehaviourEvent::Mdns(event)),
            ),
            CustomBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                propagation_source: peer_id,
                message_id: id,
                message,
            }) => {
                println!(
                    "Got message: '{}' with id: {id} from peer: {peer_id}",
                    String::from_utf8_lossy(&message.data),
                );
            }
            _ => {}
        },
        _ => {}
    }
}

impl SwarmBehaviour for CustomBehaviour {
    fn ask(
        &mut self,
        peer: &PeerId,
        actor_id: ActorID,
        actor_remote_id: Cow<'static, str>,
        message_remote_id: Cow<'static, str>,
        payload: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        reply_timeout: Option<Duration>,
        immediate: bool,
    ) -> OutboundRequestId {
        self.actor_request_response.send_request(
            peer,
            SwarmRequest::Ask {
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                reply_timeout,
                immediate,
            },
        )
    }

    fn tell(
        &mut self,
        peer: &PeerId,
        actor_id: ActorID,
        actor_remote_id: Cow<'static, str>,
        message_remote_id: Cow<'static, str>,
        payload: Vec<u8>,
        mailbox_timeout: Option<Duration>,
        immediate: bool,
    ) -> OutboundRequestId {
        self.actor_request_response.send_request(
            peer,
            SwarmRequest::Tell {
                actor_id,
                actor_remote_id,
                message_remote_id,
                payload,
                mailbox_timeout,
                immediate,
            },
        )
    }

    fn link(
        &mut self,
        actor_id: ActorID,
        actor_remote_id: Cow<'static, str>,
        sibbling_id: ActorID,
        sibbling_remote_id: Cow<'static, str>,
    ) -> OutboundRequestId {
        self.actor_request_response.send_request(
            actor_id.peer_id().unwrap(),
            SwarmRequest::Link {
                actor_id,
                actor_remote_id,
                sibbling_id,
                sibbling_remote_id,
            },
        )
    }

    fn unlink(
        &mut self,
        actor_id: ActorID,
        actor_remote_id: Cow<'static, str>,
        sibbling_id: ActorID,
    ) -> OutboundRequestId {
        self.actor_request_response.send_request(
            actor_id.peer_id().unwrap(),
            SwarmRequest::Unlink {
                actor_id,
                actor_remote_id,
                sibbling_id,
            },
        )
    }

    fn signal_link_died(
        &mut self,
        dead_actor_id: ActorID,
        notified_actor_id: ActorID,
        notified_actor_remote_id: Cow<'static, str>,
        stop_reason: kameo::error::ActorStopReason,
    ) -> OutboundRequestId {
        self.actor_request_response.send_request(
            notified_actor_id.peer_id().unwrap(),
            SwarmRequest::SignalLinkDied {
                dead_actor_id,
                notified_actor_id,
                notified_actor_remote_id,
                stop_reason,
            },
        )
    }

    fn send_ask_response(
        &mut self,
        channel: ResponseChannel<kameo::remote::SwarmResponse>,
        result: Result<Vec<u8>, RemoteSendError<Vec<u8>>>,
    ) -> Result<(), SwarmResponse> {
        self.actor_request_response
            .send_response(channel, SwarmResponse::Ask(result))
    }

    fn send_tell_response(
        &mut self,
        channel: ResponseChannel<kameo::remote::SwarmResponse>,
        result: Result<(), RemoteSendError>,
    ) -> Result<(), SwarmResponse> {
        self.actor_request_response
            .send_response(channel, SwarmResponse::Tell(result))
    }

    fn send_link_response(
        &mut self,
        channel: ResponseChannel<SwarmResponse>,
        result: Result<(), RemoteSendError<kameo::error::Infallible>>,
    ) -> Result<(), SwarmResponse> {
        self.actor_request_response
            .send_response(channel, SwarmResponse::Link(result))
    }

    fn send_unlink_response(
        &mut self,
        channel: ResponseChannel<SwarmResponse>,
        result: Result<(), RemoteSendError<kameo::error::Infallible>>,
    ) -> Result<(), SwarmResponse> {
        self.actor_request_response
            .send_response(channel, SwarmResponse::Unlink(result))
    }

    fn send_signal_link_died_response(
        &mut self,
        channel: ResponseChannel<SwarmResponse>,
        result: Result<(), RemoteSendError<kameo::error::Infallible>>,
    ) -> Result<(), SwarmResponse> {
        self.actor_request_response
            .send_response(channel, SwarmResponse::SignalLinkDied(result))
    }

    fn kademlia_add_address(&mut self, peer: &PeerId, address: Multiaddr) -> kad::RoutingUpdate {
        self.kademlia.add_address(peer, address)
    }

    fn kademlia_set_mode(&mut self, mode: Option<kad::Mode>) {
        self.kademlia.set_mode(mode)
    }

    fn kademlia_get_record(&mut self, key: kad::RecordKey) -> kad::QueryId {
        self.kademlia.get_record(key)
    }

    fn kademlia_get_record_local(&mut self, key: &kad::RecordKey) -> Option<Cow<'_, kad::Record>> {
        self.kademlia.store_mut().get(key)
    }

    fn kademlia_put_record(
        &mut self,
        record: kad::Record,
        quorum: kad::Quorum,
    ) -> Result<kad::QueryId, kad::store::Error> {
        self.kademlia.put_record(record, quorum)
    }

    fn kademlia_put_record_local(&mut self, record: kad::Record) -> Result<(), kad::store::Error> {
        self.kademlia.store_mut().put(record)
    }

    fn kademlia_remove_record(&mut self, key: &kad::RecordKey) {
        self.kademlia.remove_record(key);
    }

    fn kademlia_remove_record_local(&mut self, key: &kad::RecordKey) {
        self.kademlia.store_mut().remove(key);
    }
}
