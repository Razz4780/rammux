use std::num::NonZeroU32;

use rammux::{
    config::{RammuxConfig, RammuxRole},
    connection::RammuxConnection,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

/// This example presents a possible implementation of the config negotiation required to run Rammux.
///
/// See [PROTOCOL.md](https://github.com/Razz4780/rammux/blob/main/PROTOCOL.md) for more info.
#[tokio::main]
async fn main() {
    let (io_1, io_2) = tokio::io::duplex(4096);

    let mut config_1 = RammuxConfig::new();
    config_1.frame_limit = NonZeroU32::new(512).unwrap();
    config_1.max_inbound_streams = 8;
    config_1.local_recv_window = NonZeroU32::new(256 * 1024).unwrap();

    let mut config_2 = RammuxConfig::new();
    config_2.frame_limit = NonZeroU32::new(32 * 1024).unwrap();
    config_2.max_inbound_streams = 256;
    config_2.local_recv_window = NonZeroU32::new(8 * 1024).unwrap();

    tokio::join!(
        negotiate_rammux(RammuxRole::Client, io_1, config_1),
        negotiate_rammux(RammuxRole::Server, io_2, config_2),
    );
}

/// Negotiates Rammux config and starts the connection.
async fn negotiate_rammux(role: RammuxRole, mut io: DuplexStream, mut config: RammuxConfig) {
    println!("[{role}] Negotiating with base config: {config:?}");

    let peer_config = PeerConfig {
        frame_limit: config.frame_limit,
        max_inbound_streams: config.max_inbound_streams,
        recv_window: config.local_recv_window.get(),
    };
    io.write_all(&peer_config.encode())
        .await
        .expect("connection should not fail");

    let mut buffer = [0_u8; 12];
    io.read_exact(&mut buffer)
        .await
        .expect("connection should not fail");
    let peer_config = PeerConfig::decode(buffer);
    println!("[{role}] Received peer config: {peer_config:?}");

    config.frame_limit = config.frame_limit.min(peer_config.frame_limit);
    config.max_outbound_streams = config
        .max_outbound_streams
        .min(peer_config.max_inbound_streams);
    config.remote_recv_window = peer_config.recv_window;
    println!("[{role}] Negotiated final config: {config:?}");

    let rammux = RammuxConnection::new(role, io, config);
    // Now we could use the connection.

    println!("[{role}] Starting downgrade.");
    rammux
        .downgrade()
        .expect("connection was not downgraded")
        // We won't be using the IO transport after Rammux,
        // so we can instruct `Downgraded` to eagerly close it.
        .with_shutdown()
        .await
        .expect("connection should not fail");
    println!("[{role}] Downgrade finished.");
}

/// Exchanged by peers before starting the Rammux connection.
///
/// Allows for negotiating the connection parameters.
#[derive(Debug)]
struct PeerConfig {
    /// Our desired frame limit.
    ///
    /// Final config will use the smaller value.
    pub frame_limit: NonZeroU32,
    /// How many concurrent inbound streams we can serve.
    ///
    /// Final config will use the smaller value between this (we don't want to violate peer's resource limits)
    /// and [`RammuxConfig::max_outbound_streams`] (we don't want to violate local resource limits).
    pub max_inbound_streams: u32,
    /// Initial size of the local receive windows.
    pub recv_window: u32,
}

impl PeerConfig {
    fn encode(&self) -> [u8; 12] {
        let mut result = [0_u8; 12];
        result[..4].copy_from_slice(&self.frame_limit.get().to_be_bytes());
        result[4..8].copy_from_slice(&self.max_inbound_streams.to_be_bytes());
        result[8..].copy_from_slice(&self.recv_window.to_be_bytes());
        result
    }

    fn decode(buffer: [u8; 12]) -> Self {
        let chunks = buffer.as_chunks::<4>().0;
        let frame_limit = u32::from_be_bytes(chunks[0]);
        let frame_limit = NonZeroU32::new(frame_limit).expect("received non-positive frame limit");
        let max_inbound_streams = u32::from_be_bytes(chunks[1]);
        let recv_window = u32::from_be_bytes(chunks[2]);
        Self {
            frame_limit,
            max_inbound_streams,
            recv_window,
        }
    }
}
