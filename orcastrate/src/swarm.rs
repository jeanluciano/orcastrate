use crate::messages::TASK_COMPLETION_TOPIC;
use futures::StreamExt;
use kameo::{
    prelude::*,
    remote::{
        ActorSwarmBehaviourEvent, ActorSwarmEvent, ActorSwarmHandler, SwarmBehaviour, SwarmRequest,
        SwarmResponse,
    },
};
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder,
    gossipsub::{
        Behaviour, ConfigBuilder, Event, IdentTopic, Message, MessageAuthenticity, MessageId,
        PublishError, ValidationMode,
    },
    kad::{
        self,
        store::{MemoryStore, RecordStore},
    },
    mdns,
    request_response::{self, OutboundRequestId, ProtocolSupport, ResponseChannel},
    swarm::{NetworkBehaviour, SwarmEvent},
    core::multiaddr::Protocol,
};
use once_cell::sync::Lazy;
use std::collections::HashSet;
use std::sync::Mutex;
use std::{
    borrow::Cow,
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    io,
    time::Duration,
};
use tokio::sync::mpsc;
use tracing::{error, info};

#[derive(NetworkBehaviour)]
struct CustomBehaviour {
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    actor_request_response: request_response::cbor::Behaviour<SwarmRequest, SwarmResponse>,
    gossipsub: Behaviour,
    mdns: mdns::tokio::Behaviour,
}

pub static RESERVED_SIGNATURES: Lazy<Mutex<HashSet<String>>> =
    Lazy::new(|| Mutex::new(HashSet::new()));

// Command for the swarm loop
#[derive(Debug)]
pub enum SwarmControlCommand {
    PublishGossip {
        topic: IdentTopic,
        data: Vec<u8>,
    },
}

// Channel for swarm commands
pub static SWARM_CMD_TX: Lazy<Mutex<Option<mpsc::Sender<SwarmControlCommand>>>> =
    Lazy::new(|| Mutex::new(None));

pub async fn start_swarm(port: u16) -> Result<&'static ActorSwarm, Box<dyn std::error::Error>> {
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<SwarmControlCommand>(32); // Buffer size 32

    // Store the sender globally (or pass it to Worker, which then passes to others)
    *SWARM_CMD_TX.lock().unwrap() = Some(cmd_tx);

    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_quic()
        .with_behaviour(|key| {
            let mut kademlia = kad::Behaviour::new(
                key.public().to_peer_id(),
                MemoryStore::new(key.public().to_peer_id()),
            );
            
        // Add bootstrap nodes !!!this only for local testing TODO: remove
            let bootstrap_nodes = vec![
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
            ];
            
            for addr in bootstrap_nodes {
                if let Ok(addr) = addr.parse::<Multiaddr>() {
                    kademlia.add_address(&key.public().to_peer_id(), addr);
                }
            }

            let message_id_fn = |message: &Message| {
                let mut s = DefaultHasher::new();
                message.data.hash(&mut s);
                MessageId::from(s.finish().to_string())
            };

            // Set a custom gossipsub configuration
            let gossipsub_config = ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
                .validation_mode(ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message
                // signing)
                .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be propagated.
                .build()
                .map_err(io::Error::other)?; // Temporary hack because `build` does not return a proper `std::error::Error`.

            // build a gossipsub network behaviour
            let gossipsub =
                Behaviour::new(MessageAuthenticity::Signed(key.clone()), gossipsub_config)?;

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
    let topic = IdentTopic::new(TASK_COMPLETION_TOPIC);

    // !!! needs to be handled #TODO
    if let Err(e) = swarm.behaviour_mut().gossipsub.subscribe(&topic) {
        error!("Failed to subscribe to topic '{}': {:?}", topic.hash(), e);
    } else {
        info!("Successfully subscribed to topic: '{}'", topic.hash());
    }

    tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(cmd_from_actor) = cmd_rx.recv() => { // Listen for commands from actors
                    match cmd_from_actor {
                        SwarmControlCommand::PublishGossip { topic, data } => {
                            info!("Swarm loop: Received PublishGossip command for topic: {}", topic.hash());
                            match swarm.behaviour_mut().gossipsub.publish(topic, data) {
                                Ok(msg_id) => info!("Swarm loop: Published gossip, msg_id: {:?}", msg_id),
                                Err(e) => error!("Swarm loop: Failed to publish gossip: {:?}", e),
                            }
                        }
                    }
                }
                Some(cmd_from_handler) = swarm_handler.next_command() => swarm_handler.handle_command(&mut swarm, cmd_from_handler),
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
            CustomBehaviourEvent::Mdns(mdns::Event::Discovered(list)) => {
                for (peer_id, multiaddr) in list {
                    swarm
                        .behaviour_mut()
                        .kademlia_add_address(&peer_id, multiaddr);
                    swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                }
            }

            CustomBehaviourEvent::Gossipsub(Event::Message {
                propagation_source: peer_id,
                message_id: id,
                message,
            }) => {
                let topic_hash_str = message.topic.as_str();
                if topic_hash_str == TASK_COMPLETION_TOPIC {
                    match String::from_utf8(message.data.clone()) {
                        Ok(signature) => {
                            info!(
                                "Gossipsub: Received signature '{}' on topic '{}' (id: {}) from peer: {}",
                                signature, topic_hash_str, id, peer_id
                            );
                            match RESERVED_SIGNATURES.lock() {
                                Ok(mut set) => {
                                    if set.insert(signature.clone()) {
                                        info!("Signature '{}' added to reserved set.", signature);
                                    } else {
                                        info!(
                                            "Signature '{}' was already in reserved set.",
                                            signature
                                        );
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to lock RESERVED_SIGNATURES to add '{}': {:?}",
                                        signature, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                "Gossipsub: Received non-UTF8 message on topic '{}' (id: {}) from peer: {}: {:?}",
                                topic_hash_str, id, peer_id, e
                            );
                        }
                    }
                } else {
                    // Optional: Log messages on other topics if unexpected
                    // info!("Gossipsub: Received message on unexpected topic: {}", topic_hash_str);
                }
            }
            _ => {}
        },
        _ => {}
    }
}

impl CustomBehaviour {
    fn reserve_task_signature(
        &mut self,
        task_signature: String,
    ) -> Result<MessageId, PublishError> {
        info!(
            "Attempting to publish reservation for signature: {}",
            task_signature
        );
        self.gossipsub.publish(
            IdentTopic::new(TASK_COMPLETION_TOPIC),
            task_signature.into_bytes(),
        )
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
