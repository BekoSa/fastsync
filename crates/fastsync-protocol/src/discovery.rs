use std::net::{IpAddr, SocketAddr};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{AGENT_VERSION, PROTOCOL_VERSION};

/// A LAN discovery announcement.
///
/// The transfer endpoint is formed from the datagram's source IP and `port`;
/// an address supplied inside an announcement is deliberately not trusted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryAnnouncement {
    pub device_id: Uuid,
    pub device_name: String,
    pub protocol_version: u16,
    pub port: u16,
    pub http_port: u16,
    pub agent_version: String,
    #[serde(default)]
    pub features: Vec<String>,
}

impl DiscoveryAnnouncement {
    pub fn new(
        device_id: Uuid,
        device_name: impl Into<String>,
        port: u16,
        http_port: u16,
        features: Vec<String>,
    ) -> Self {
        Self {
            device_id,
            device_name: device_name.into(),
            protocol_version: PROTOCOL_VERSION,
            port,
            http_port,
            agent_version: AGENT_VERSION.to_owned(),
            features,
        }
    }

    /// Combines the trusted datagram source IP with the advertised sync port.
    pub const fn transfer_address(&self, source_ip: IpAddr) -> SocketAddr {
        SocketAddr::new(source_ip, self.port)
    }

    /// Combines the trusted datagram source IP with the advertised HTTP port.
    pub const fn http_address(&self, source_ip: IpAddr) -> SocketAddr {
        SocketAddr::new(source_ip, self.http_port)
    }
}
