use std::num::{NonZeroU16, NonZeroU32};

use rammux::{
    config::{RammuxConfig, RammuxRole},
    connection::RammuxConnection,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

/// This example presents a possible implementation of the config negotiation required to run rammux.
///
/// See [PROTOCOL.md](https://github.com/Razz4780/rammux/blob/main/PROTOCOL.md) for more info.
#[tokio::main]
async fn main() {
    let (io_1, io_2) = tokio::io::duplex(4096);

    let mut config_1 = RammuxConfig::new();
    config_1.frame_limit = NonZeroU16::new(512).unwrap();
    config_1.max_inbound_streams = 8;
    config_1.local_recv_window = NonZeroU32::new(256 * 1024).unwrap();
    config_1.local_recv_pool = 8 * 1024 * 1024;

    let mut config_2 = RammuxConfig::new();
    config_2.frame_limit = NonZeroU16::new(32 * 1024).unwrap();
    config_2.max_inbound_streams = 256;
    config_2.local_recv_window = NonZeroU32::new(8 * 1024).unwrap();
    config_2.local_recv_pool = 1024 * 1024;

    tokio::join!(
        negotiate_rammux(RammuxRole::Client, io_1, config_1),
        negotiate_rammux(RammuxRole::Server, io_2, config_2),
    );
}

/// Negotiates rammux config and starts the connection.
async fn negotiate_rammux(role: RammuxRole, mut io: DuplexStream, mut config: RammuxConfig) {
    println!("[{role}] Negotiating with base config: {config:?}");

    let peer_config = PeerConfig {
        version: PROTOCOL_VERSION,
        frame_limit: config.frame_limit,
        max_inbound_streams: config.max_inbound_streams,
        recv_window: config.local_recv_window.get(),
        recv_pool: config.local_recv_pool,
    };
    io.write_all(&peer_config.encode())
        .await
        .expect("connection should not fail");

    let mut buffer = [0_u8; PeerConfig::LEN];
    io.read_exact(&mut buffer)
        .await
        .expect("connection should not fail");
    let peer_config = PeerConfig::decode(buffer);
    println!("[{role}] Received peer config: {peer_config:?}");

    // Frames carry no version, so the peers have to agree on it here. There is nothing to compromise on.
    assert_eq!(
        peer_config.version, PROTOCOL_VERSION,
        "the peer speaks another version of the rammux protocol"
    );

    config.frame_limit = config.frame_limit.min(peer_config.frame_limit);
    config.max_outbound_streams = config
        .max_outbound_streams
        .min(peer_config.max_inbound_streams);
    config.remote_recv_window =
        NonZeroU32::new(peer_config.recv_window).expect("received non-positive window");
    config.remote_recv_pool = peer_config.recv_pool;
    println!("[{role}] Negotiated final config: {config:?}");

    let rammux = RammuxConnection::new(role, io, config).expect("config is valid");
    // Now we could use the connection.

    println!("[{role}] Starting downgrade.");
    rammux
        .downgrade()
        .expect("connection was not downgraded")
        // We won't be using the IO transport after rammux,
        // so we can instruct `Downgraded` to eagerly close it.
        .with_shutdown()
        .await
        .expect("connection should not fail");
    println!("[{role}] Downgrade finished.");
}

/// The version of the rammux protocol (see `PROTOCOL.md`) that this example speaks.
const PROTOCOL_VERSION: u8 = 2;

/// Exchanged by peers before starting the rammux connection.
///
/// Allows for negotiating the connection parameters.
#[derive(Debug)]
struct PeerConfig {
    /// Version of the rammux protocol. Must match.
    pub version: u8,
    /// Our desired frame limit.
    ///
    /// Final config will use the smaller value.
    pub frame_limit: NonZeroU16,
    /// How many concurrent inbound streams we can serve.
    ///
    /// Final config will use the smaller value between this (we don't want to violate peer's resource limits)
    /// and [`RammuxConfig::max_outbound_streams`] (we don't want to violate local resource limits).
    pub max_inbound_streams: u32,
    /// Size of the base receive window of every stream we grant.
    pub recv_window: u32,
    /// Size of the pool of receive window that all streams we receive share.
    pub recv_pool: u32,
}

impl PeerConfig {
    const LEN: usize = 15;

    fn encode(&self) -> [u8; Self::LEN] {
        let mut result = [0_u8; Self::LEN];
        result[0] = self.version;
        result[1..3].copy_from_slice(&self.frame_limit.get().to_be_bytes());
        result[3..7].copy_from_slice(&self.max_inbound_streams.to_be_bytes());
        result[7..11].copy_from_slice(&self.recv_window.to_be_bytes());
        result[11..15].copy_from_slice(&self.recv_pool.to_be_bytes());
        result
    }

    fn decode(buffer: [u8; Self::LEN]) -> Self {
        let version = buffer[0];
        let frame_limit = u16::from_be_bytes([buffer[1], buffer[2]]);
        let frame_limit = NonZeroU16::new(frame_limit).expect("received non-positive frame limit");
        let max_inbound_streams = u32::from_be_bytes(buffer[3..7].try_into().unwrap());
        let recv_window = u32::from_be_bytes(buffer[7..11].try_into().unwrap());
        let recv_pool = u32::from_be_bytes(buffer[11..15].try_into().unwrap());
        Self {
            version,
            frame_limit,
            max_inbound_streams,
            recv_window,
            recv_pool,
        }
    }
}
