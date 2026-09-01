use std::{net::SocketAddr, path::PathBuf};

use schemars::JsonSchema;
use serde::Deserialize;

/// Echo server configuration.
#[derive(Deserialize, JsonSchema)]
pub struct ServerConfig {
    /// Address of the server's HTTP API.
    ///
    /// The server talks only HTTP/1.1 and serves one endpoint - `/echo`.
    /// The endpoint can be used to upgrade the connection to one of the following multiplexing protocols:
    /// `rammux`, `h2`, `yamux`. Configuration of the server's multiplexer is passed in the UPGRADE request headers.
    pub http_addr: SocketAddr,

    /// Address of the server's HTTPS API.
    ///
    /// The server talks only HTTP/1.1 and serves one endpoint - `/echo`.
    /// The endpoint can be used to upgrade the connection to one of the following multiplexing protocols:
    /// `rammux`, `h2`, `yamux`. Configuration of the server's multiplexer is passed in the UPGRADE request headers.
    pub https_addr: SocketAddr,

    /// Path to a PEM file with a TLS certificate and key to be used by the server.
    pub cert_path: PathBuf,
}

// COPIED FROM STALE CLAP APPROACH, todo turn into JSON config
//
//
// /// Configuration of the client.
// ///
// /// The client runs multiple communication iterations against the server.
// /// Each iteration runs some number of bulk streams and at most one ping pong stream.
// ///
// /// * Bulk stream sends data as fast as possible. These streams are used to measure throughput.
// /// * Ping pong stream sends messages, waiting for the server to respond after each messages.
// ///   This stream is used to measure latency.
// ///
// /// After each iteration, client logs statistics:
// /// * CPU time spent on the iteration, in ms (sum of user and sys time)
// /// * mean/p50/p99 throughput of each bulk stream
// /// * mean/p50/p99 latency of ping pong stream exchanges (if running a ping pong stream)
// /// * count of finished ping pong stream exchanges (if running a ping pong stream)
// /// * total elapsed time
// ///
// /// The iteration ends when all bulk streams finish. If there is an outstanding ping pong message,
// /// it is abandoned.
// ///
// /// If an iteration fails, the failure is logged and the iteration is retried (up to 3 times).
// #[derive(Parser)]
// struct ClientArgs {
//     /// Protocol to use.
//     #[arg(long, env = "PROTOCOL")]
//     protocol: Protocol,
//     /// Number of iterations to run.
//     #[arg(long, env = "ITERATIONS", default_value_t = NonZeroUsize::MIN)]
//     iterations: NonZeroUsize,
//     /// How many bulk streams to run in each iteration.
//     #[arg(long, env = "BULK_STREAMS")]
//     bulk_streams: NonZeroUsize,
//     /// How many data should each bulk stream send and receive, in bytes.
//     #[arg(long, env = "BULK_STREAM_DATA", default_value_t = NonZeroUsize::new(1024 * 1024).unwrap())]
//     bulk_stream_data: NonZeroUsize,
//     /// Size of the ping pong stream message, in bytes.
//     ///
//     /// If not passed, the client will not run the ping pong stream.
//     #[arg(long, env = "PING_PONG_SIZE")]
//     run_ping_pong: Option<NonZeroUsize>,
//     /// Path to a JSON file containinig
//     #[arg(long, env = "MUXER_CONFIG")]
//     muxer_config: PathBuf,
// }

// #[derive(Clone, Copy, Debug, ValueEnum)]
// enum Protocol {
//     Rammux,
//     Yamux,
//     H2,
// }
