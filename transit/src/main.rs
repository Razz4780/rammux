//! A standalone transit-window protocol, and the harness that tunes it.
//!
//! Three commands, and the split matters: the protocol has to be measured over
//! a link whose properties are known exactly, and the only way to know them is
//! to build the link.
//!
//! * `client` and `server` are the two ends of one measured connection. They
//!   are separate processes because each needs its own network namespace.
//! * `harness` builds the link, starts both, and collects what they logged.
//!
//! The protocol itself is this package's library, starting at
//! [`transit::Transit`]; the decisions it was tuned to are in
//! [`transit::window`]. Everything in this binary is measurement scaffolding
//! around it, and none of it is needed to use the protocol.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use clap::{Parser, Subcommand};

mod args;
mod client;
mod harness;
mod server;
mod sockopt;
mod traced;

use crate::{
    args::ProtocolArgs,
    harness::{Link, run_harness},
};

#[derive(Parser)]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a client that talks to the TCP server at `addr`
    /// with a transit-window protocol.
    Client {
        /// Server address.
        #[arg(short, long)]
        addr: SocketAddr,
        #[command(flatten)]
        measurement: MeasurementArgs,
        #[command(flatten)]
        protocol: ProtocolArgs,
    },
    /// Run a server that accepts one TCP connection on `addr`,
    /// applies a transit-window protocol, and echoes back all data.
    Server {
        /// Bind address.
        #[arg(short, long)]
        addr: SocketAddr,
        #[command(flatten)]
        protocol: ProtocolArgs,
    },
    /// Runs both server and client in isolated network namespaces,
    /// connected with a link impaired with tc netem.
    ///
    /// Needs no privileges of its own: it re-runs itself as root of a new user
    /// namespace. Run it as root instead where unprivileged user namespaces
    /// are disabled.
    ///
    /// Exits clearly on SIGINT (including any cleanup: veth, namespaces, etc.)
    Harness {
        /// Link bandwidth, in mbit/s.
        #[arg(short, long)]
        bandwidth: f64,
        /// Link delay in each direction, in milliseconds.
        #[arg(short, long)]
        delay: u64,
        /// Link packet loss, in each direction.
        ///
        /// Given as a fraction, e.g. `0.01` for 1% of packets dropped.
        #[arg(short, long)]
        loss: f64,
        /// Path to a directory where client and server logs will be redirected.
        ///
        /// The directory will be created if not found.
        /// Directory will contain following files:
        /// * `client.stdout`
        /// * `client.stderr`
        /// * `server.stdout`
        /// * `server.stderr`
        #[arg(long, default_value = "./harness-logs")]
        output: PathBuf,
        #[command(flatten)]
        measurement: MeasurementArgs,
        #[command(flatten)]
        protocol: ProtocolArgs,
    },
}

/// How long to measure for, and how much of that to throw away.
#[derive(clap::Args, Debug, Clone, Copy)]
pub struct MeasurementArgs {
    /// How long the client runs, in seconds.
    #[arg(long, default_value_t = 20)]
    pub duration: u64,
    /// Leading seconds left out of the goodput and the latency percentiles.
    ///
    /// A connection's own ramp is not what the window is being judged on, and
    /// it is slow enough to dominate a short run.
    #[arg(long, default_value_t = 3)]
    pub ramp: u64,
}

impl MeasurementArgs {
    /// These settings as a command line, for the harness to hand to the
    /// processes it starts.
    pub fn to_argv(self) -> Vec<String> {
        vec![
            "--duration".to_string(),
            self.duration.to_string(),
            "--ramp".to_string(),
            self.ramp.to_string(),
        ]
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = Args::parse();
    match args.command {
        Command::Harness {
            bandwidth,
            delay,
            loss,
            output,
            measurement,
            protocol,
        } => match run_harness(
            Link {
                bandwidth,
                delay,
                loss,
            },
            output,
            measurement,
            protocol,
        )
        .await
        {
            Ok(0) => {},
            Ok(code) => std::process::exit(code),
            Err(error) => {
                eprintln!("Harness failed: {error}");
                std::process::exit(1);
            },
        },
        Command::Client {
            addr,
            measurement,
            protocol,
        } => {
            client::run(
                addr,
                protocol,
                Duration::from_secs(measurement.duration),
                Duration::from_secs(measurement.ramp),
            )
            .await;
        },
        Command::Server { addr, protocol } => server::run(addr, protocol).await,
    }
}
