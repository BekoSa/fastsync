//! Periodic IPv4 UDP discovery for FastSync peers.

#![forbid(unsafe_code)]

use std::cmp;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use fastsync_protocol::{DiscoveryAnnouncement, PROTOCOL_VERSION};
use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::sync::{Semaphore, mpsc};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

/// The largest discovery payload accepted or sent.
///
/// This is the maximum UDP payload for an IPv4 packet and is therefore also
/// below the 64 KiB discovery protocol limit.
pub const MAX_DISCOVERY_PACKET_SIZE: usize = 65_507;

const INITIAL_RECEIVE_RETRY_DELAY: Duration = Duration::from_millis(50);
const MAX_RECEIVE_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Network and timing configuration for discovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveryConfig {
    /// Local IPv4 address on which announcements are received.
    pub bind_addr: SocketAddr,
    /// IPv4 broadcast destination. A unicast address is also valid, which is
    /// useful for constrained networks and tests.
    pub broadcast_addr: SocketAddr,
    /// Time between outgoing announcements.
    pub announce_interval: Duration,
    /// Maximum age of a peer update delivered after output backpressure.
    /// Consumers can use the same value to expire peers by `last_seen`.
    pub peer_ttl: Duration,
}

impl DiscoveryConfig {
    /// Validates configuration that would otherwise fail or panic at runtime.
    pub fn validate(&self) -> Result<()> {
        if !self.bind_addr.is_ipv4() {
            return Err(DiscoveryError::Ipv4Required {
                field: "bind_addr",
                address: self.bind_addr,
            });
        }
        if !self.broadcast_addr.is_ipv4() {
            return Err(DiscoveryError::Ipv4Required {
                field: "broadcast_addr",
                address: self.broadcast_addr,
            });
        }
        if self.announce_interval.is_zero() {
            return Err(DiscoveryError::ZeroAnnounceInterval);
        }
        if self.peer_ttl.is_zero() {
            return Err(DiscoveryError::ZeroPeerTtl);
        }

        Ok(())
    }
}

/// A validated announcement and its endpoints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveredPeer {
    /// The validated wire announcement.
    pub announcement: DiscoveryAnnouncement,
    /// Sync endpoint formed from the datagram source IP and advertised port.
    pub transfer_addr: SocketAddr,
    /// HTTP endpoint formed from the datagram source IP and advertised port.
    pub http_addr: SocketAddr,
    /// Monotonic time at which this datagram was received.
    pub last_seen: Instant,
}

impl DiscoveredPeer {
    /// Returns whether this observation has exceeded a peer TTL.
    pub fn is_stale(&self, now: Instant, peer_ttl: Duration) -> bool {
        now.saturating_duration_since(self.last_seen) > peer_ttl
    }
}

/// Errors encountered while constructing a discovery service.
#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("{field} must be an IPv4 socket address, got {address}")]
    Ipv4Required {
        field: &'static str,
        address: SocketAddr,
    },
    #[error("announce_interval must be greater than zero")]
    ZeroAnnounceInterval,
    #[error("peer_ttl must be greater than zero")]
    ZeroPeerTtl,
    #[error("output channel capacity must be greater than zero")]
    ZeroOutputCapacity,
    #[error("output channel capacity {capacity} exceeds the maximum of {max}")]
    OutputCapacityTooLarge { capacity: usize, max: usize },
    #[error("failed to encode discovery announcement as JSON")]
    Encode(#[source] serde_json::Error),
    #[error("encoded discovery announcement is {size} bytes; maximum is {max} bytes")]
    AnnouncementTooLarge { size: usize, max: usize },
    #[error("failed to bind discovery socket to {address}")]
    Bind {
        address: SocketAddr,
        #[source]
        source: io::Error,
    },
    #[error("failed to enable UDP broadcast on discovery socket")]
    EnableBroadcast(#[source] io::Error),
    #[error("failed to read the discovery socket's local address")]
    LocalAddress(#[source] io::Error),
}

/// Result type for discovery setup and execution.
pub type Result<T> = std::result::Result<T, DiscoveryError>;

/// Periodically announces this device and reports announcements from peers.
pub struct DiscoveryService {
    socket: UdpSocket,
    local_addr: SocketAddr,
    config: DiscoveryConfig,
    local_announcement: DiscoveryAnnouncement,
    encoded_announcement: Vec<u8>,
    peer_output: mpsc::Sender<DiscoveredPeer>,
}

impl DiscoveryService {
    /// Validates the configuration and announcement, then binds the UDP socket.
    ///
    /// `peer_output` is a Tokio bounded-channel sender. Each accepted peer
    /// announcement produces one update on that channel.
    pub async fn bind(
        config: DiscoveryConfig,
        local_announcement: DiscoveryAnnouncement,
        peer_output: mpsc::Sender<DiscoveredPeer>,
    ) -> Result<Self> {
        config.validate()?;

        let encoded_announcement =
            serde_json::to_vec(&local_announcement).map_err(DiscoveryError::Encode)?;
        if encoded_announcement.len() > MAX_DISCOVERY_PACKET_SIZE {
            return Err(DiscoveryError::AnnouncementTooLarge {
                size: encoded_announcement.len(),
                max: MAX_DISCOVERY_PACKET_SIZE,
            });
        }

        let socket =
            UdpSocket::bind(config.bind_addr)
                .await
                .map_err(|source| DiscoveryError::Bind {
                    address: config.bind_addr,
                    source,
                })?;
        socket
            .set_broadcast(true)
            .map_err(DiscoveryError::EnableBroadcast)?;
        let local_addr = socket.local_addr().map_err(DiscoveryError::LocalAddress)?;

        Ok(Self {
            socket,
            local_addr,
            config,
            local_announcement,
            encoded_announcement,
            peer_output,
        })
    }

    /// Creates the bounded peer channel and binds a service in one operation.
    pub async fn bind_with_channel(
        config: DiscoveryConfig,
        local_announcement: DiscoveryAnnouncement,
        output_capacity: usize,
    ) -> Result<(Self, mpsc::Receiver<DiscoveredPeer>)> {
        if output_capacity == 0 {
            return Err(DiscoveryError::ZeroOutputCapacity);
        }
        if output_capacity > Semaphore::MAX_PERMITS {
            return Err(DiscoveryError::OutputCapacityTooLarge {
                capacity: output_capacity,
                max: Semaphore::MAX_PERMITS,
            });
        }

        let (peer_output, peer_updates) = mpsc::channel(output_capacity);
        let service = Self::bind(config, local_announcement, peer_output).await?;
        Ok((service, peer_updates))
    }

    /// Returns the actual bound socket address, including an OS-assigned port.
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns this service's immutable configuration.
    pub const fn config(&self) -> &DiscoveryConfig {
        &self.config
    }

    /// Sends and receives announcements until `cancellation` is cancelled.
    ///
    /// Individual UDP send and receive failures are logged and retried. Once
    /// cancellation is observed, both loops stop and this method returns.
    pub async fn run(self, cancellation: CancellationToken) -> Result<()> {
        let send_loop = self.send_loop(cancellation.clone());
        let receive_loop = self.receive_loop(cancellation);

        tokio::join!(send_loop, receive_loop);
        Ok(())
    }

    async fn send_loop(&self, cancellation: CancellationToken) {
        let mut announcements = tokio::time::interval(self.config.announce_interval);
        announcements.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return,
                _ = announcements.tick() => {
                    let sent = tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => return,
                        sent = self.socket.send_to(
                            &self.encoded_announcement,
                            self.config.broadcast_addr,
                        ) => sent,
                    };

                    match sent {
                        Ok(sent) if sent != self.encoded_announcement.len() => {
                            warn!(
                                sent,
                                expected = self.encoded_announcement.len(),
                                destination = %self.config.broadcast_addr,
                                "sent a partial discovery announcement; will retry"
                            );
                        }
                        Ok(_) => {}
                        Err(error) => {
                            warn!(
                                %error,
                                destination = %self.config.broadcast_addr,
                                "failed to send discovery announcement; will retry"
                            );
                        }
                    }
                }
            }
        }
    }

    async fn receive_loop(&self, cancellation: CancellationToken) {
        let mut packet = vec![0_u8; MAX_DISCOVERY_PACKET_SIZE + 1];
        let mut retry_delay = INITIAL_RECEIVE_RETRY_DELAY;
        let mut output_open = true;

        loop {
            let received = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return,
                received = self.socket.recv_from(&mut packet) => received,
            };

            let (length, source) = match received {
                Ok(received) => {
                    retry_delay = INITIAL_RECEIVE_RETRY_DELAY;
                    received
                }
                Err(error) => {
                    warn!(%error, "failed to receive discovery datagram; will retry");
                    if wait_for_retry(&cancellation, retry_delay).await {
                        return;
                    }
                    retry_delay = cmp::min(retry_delay.saturating_mul(2), MAX_RECEIVE_RETRY_DELAY);
                    continue;
                }
            };

            let last_seen = Instant::now();
            let peer = match decode_datagram(
                &packet[..length],
                source,
                &self.local_announcement,
                last_seen,
            ) {
                Ok(peer) => peer,
                Err(rejection) => {
                    log_rejection(&rejection, source, length);
                    continue;
                }
            };

            if !output_open {
                continue;
            }

            let permit = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return,
                permit = self.peer_output.reserve() => permit,
            };

            match permit {
                Ok(permit) => {
                    if peer.is_stale(Instant::now(), self.config.peer_ttl) {
                        trace!(
                            device_id = %peer.announcement.device_id,
                            "dropping stale discovery update after output backpressure"
                        );
                    } else {
                        permit.send(peer);
                    }
                }
                Err(_) => {
                    debug!("discovery peer output closed; continuing network discovery");
                    output_open = false;
                }
            }
        }
    }
}

async fn wait_for_retry(cancellation: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => true,
        _ = tokio::time::sleep(delay) => false,
    }
}

#[derive(Debug)]
enum DatagramRejection {
    Oversized,
    Malformed(serde_json::Error),
    NonIpv4Source,
    OwnDevice,
    IncompatibleProtocol { received: u16 },
}

fn decode_datagram(
    packet: &[u8],
    source: SocketAddr,
    local_announcement: &DiscoveryAnnouncement,
    last_seen: Instant,
) -> std::result::Result<DiscoveredPeer, DatagramRejection> {
    if packet.len() > MAX_DISCOVERY_PACKET_SIZE {
        return Err(DatagramRejection::Oversized);
    }

    let source_ip = match source.ip() {
        IpAddr::V4(source_ip) => IpAddr::V4(source_ip),
        IpAddr::V6(_) => return Err(DatagramRejection::NonIpv4Source),
    };
    let announcement = serde_json::from_slice::<DiscoveryAnnouncement>(packet)
        .map_err(DatagramRejection::Malformed)?;

    if announcement.protocol_version != PROTOCOL_VERSION {
        return Err(DatagramRejection::IncompatibleProtocol {
            received: announcement.protocol_version,
        });
    }
    if announcement.device_id == local_announcement.device_id {
        return Err(DatagramRejection::OwnDevice);
    }

    let transfer_addr = announcement.transfer_address(source_ip);
    let http_addr = announcement.http_address(source_ip);
    Ok(DiscoveredPeer {
        announcement,
        transfer_addr,
        http_addr,
        last_seen,
    })
}

fn log_rejection(rejection: &DatagramRejection, source: SocketAddr, packet_size: usize) {
    match rejection {
        DatagramRejection::Oversized => debug!(
            %source,
            packet_size,
            maximum = MAX_DISCOVERY_PACKET_SIZE,
            "discarding oversized discovery datagram"
        ),
        DatagramRejection::Malformed(error) => debug!(
            %source,
            packet_size,
            %error,
            "discarding malformed discovery datagram"
        ),
        DatagramRejection::NonIpv4Source => debug!(
            %source,
            "discarding discovery datagram from a non-IPv4 source"
        ),
        DatagramRejection::OwnDevice => trace!(
            %source,
            "ignoring this device's discovery announcement"
        ),
        DatagramRejection::IncompatibleProtocol { received } => debug!(
            %source,
            expected = PROTOCOL_VERSION,
            received = *received,
            "discarding discovery announcement with an incompatible protocol version"
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use serde_json::json;
    use tokio::time::timeout;

    use super::*;

    type TestResult = std::result::Result<(), Box<dyn Error>>;

    fn announcement(device_id: &str) -> serde_json::Result<DiscoveryAnnouncement> {
        serde_json::from_value(json!({
            "device_id": device_id,
            "device_name": "test device",
            "protocol_version": PROTOCOL_VERSION,
            "port": 4010,
            "http_port": 4011,
            "agent_version": "test",
            "features": ["sync"]
        }))
    }

    #[test]
    fn parses_json_and_uses_the_datagram_source_ip() -> TestResult {
        let local = announcement("00000000-0000-4000-8000-000000000001")?;
        let remote = announcement("00000000-0000-4000-8000-000000000002")?;
        let mut wire_announcement = serde_json::to_value(&remote)?;
        wire_announcement["ip"] = json!("203.0.113.99");
        let packet = serde_json::to_vec(&wire_announcement)?;
        let source = SocketAddr::from(([192, 0, 2, 44], 55_555));
        let seen_at = Instant::now();

        let peer = decode_datagram(&packet, source, &local, seen_at)
            .map_err(|error| format!("packet was unexpectedly rejected: {error:?}"))?;

        assert_eq!(peer.announcement, remote);
        assert_eq!(
            peer.transfer_addr,
            SocketAddr::from(([192, 0, 2, 44], 4010))
        );
        assert_eq!(peer.http_addr, SocketAddr::from(([192, 0, 2, 44], 4011)));
        assert_eq!(peer.last_seen, seen_at);
        Ok(())
    }

    #[test]
    fn ignores_own_device_id() -> TestResult {
        let local = announcement("00000000-0000-4000-8000-000000000001")?;
        let packet = serde_json::to_vec(&local)?;

        let result = decode_datagram(
            &packet,
            SocketAddr::from(([127, 0, 0, 1], 5000)),
            &local,
            Instant::now(),
        );

        assert!(matches!(result, Err(DatagramRejection::OwnDevice)));
        Ok(())
    }

    #[test]
    fn rejects_incompatible_protocol_version() -> TestResult {
        let local = announcement("00000000-0000-4000-8000-000000000001")?;
        let mut remote = announcement("00000000-0000-4000-8000-000000000002")?;
        remote.protocol_version = PROTOCOL_VERSION.saturating_add(1);
        let packet = serde_json::to_vec(&remote)?;

        let result = decode_datagram(
            &packet,
            SocketAddr::from(([127, 0, 0, 1], 5000)),
            &local,
            Instant::now(),
        );

        assert!(matches!(
            result,
            Err(DatagramRejection::IncompatibleProtocol { received })
                if received == remote.protocol_version
        ));
        Ok(())
    }

    #[test]
    fn rejects_malformed_and_oversized_packets() -> TestResult {
        let local = announcement("00000000-0000-4000-8000-000000000001")?;
        let source = SocketAddr::from(([127, 0, 0, 1], 5000));

        assert!(matches!(
            decode_datagram(b"not json", source, &local, Instant::now()),
            Err(DatagramRejection::Malformed(_))
        ));

        let oversized = vec![b' '; MAX_DISCOVERY_PACKET_SIZE + 1];
        assert!(matches!(
            decode_datagram(&oversized, source, &local, Instant::now()),
            Err(DatagramRejection::Oversized)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn sends_and_receives_over_loopback_unicast() -> TestResult {
        let announcement_sink = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let local = announcement("00000000-0000-4000-8000-000000000001")?;
        let remote = announcement("00000000-0000-4000-8000-000000000002")?;
        let config = DiscoveryConfig {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            broadcast_addr: announcement_sink.local_addr()?,
            announce_interval: Duration::from_millis(25),
            peer_ttl: Duration::from_secs(1),
        };
        let (peer_output, mut peer_updates) = mpsc::channel(1);
        let service = DiscoveryService::bind(config, local.clone(), peer_output).await?;
        let discovery_addr = service.local_addr();
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let service_task = tokio::spawn(async move { service.run(task_cancellation).await });

        let mut outgoing = vec![0_u8; MAX_DISCOVERY_PACKET_SIZE + 1];
        let (outgoing_length, _) = timeout(
            Duration::from_secs(1),
            announcement_sink.recv_from(&mut outgoing),
        )
        .await??;
        let sent_announcement: DiscoveryAnnouncement =
            serde_json::from_slice(&outgoing[..outgoing_length])?;
        assert_eq!(sent_announcement, local);

        let remote_socket = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let remote_source = remote_socket.local_addr()?;
        let remote_packet = serde_json::to_vec(&remote)?;
        remote_socket
            .send_to(&remote_packet, discovery_addr)
            .await?;

        let peer = timeout(Duration::from_secs(1), peer_updates.recv())
            .await?
            .ok_or("peer output closed before an update was received")?;
        assert_eq!(peer.announcement, remote);
        assert_eq!(peer.transfer_addr.ip(), remote_source.ip());
        assert_eq!(peer.transfer_addr.port(), remote.port);
        assert_eq!(peer.http_addr.ip(), remote_source.ip());
        assert_eq!(peer.http_addr.port(), remote.http_port);

        cancellation.cancel();
        let service_result = timeout(Duration::from_secs(1), service_task).await?;
        service_result??;
        Ok(())
    }
}
