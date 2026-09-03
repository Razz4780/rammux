use anyhow::Context;
use clap::{Parser, Subcommand};
use std::{path::PathBuf, process::ExitCode};
use tokio_rustls::rustls;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

mod client;
mod config;
mod cpu;
mod rammux_rtt;
mod server;
mod signal;
mod stream_util;
mod tls;

/// Multi-purpose CLI tool for benchmarking rammux against other multiplexing frameworks.
#[derive(Parser)]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

impl Args {
    fn json_log(&self) -> bool {
        match &self.command {
            Command::Client { command } | Command::Server { command } => match command {
                PeerCommand::Run { json_log, .. } => *json_log,
                PeerCommand::Schema => false,
            },
            Command::GenerateCert => false,
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// Echo server talking all benchmarked multiplexing protocols.
    Server {
        #[clap(subcommand)]
        command: PeerCommand,
    },
    /// Client talking one of the benchmarked multiplexing protocols.
    Client {
        #[clap(subcommand)]
        command: PeerCommand,
    },
    /// Generate a self-signed certificate to be used when benchmarking protocols with TLS
    /// and print it to stdout.
    GenerateCert,
}

#[derive(Subcommand)]
enum PeerCommand {
    /// Print JSON schema of the configuration file.
    Schema,
    /// Run the benchmark side.
    Run {
        /// Whether to print the logs in JSON format.
        #[arg(long, short, env = "JSON_LOG", default_value_t = false)]
        json_log: bool,

        /// Path to the config file.
        #[arg(long, short, env = "CONFIG_PATH")]
        config_path: PathBuf,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    rustls::crypto::ring::default_provider()
        .install_default()
        .unwrap();

    let args = Args::parse();

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_file(true)
        .with_line_number(true)
        .with_writer(std::io::stderr);
    let fmt_layer = if args.json_log() {
        fmt_layer.with_ansi(false).json().boxed()
    } else {
        fmt_layer.with_ansi(true).pretty().boxed()
    };
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(EnvFilter::from_default_env())
        .init();

    let result = match args.command {
        Command::GenerateCert => generate_and_print_cert(),
        Command::Client {
            command: PeerCommand::Schema,
        } => print_schema(schemars::schema_for!(config::ClientConfig)),
        Command::Client {
            command: PeerCommand::Run { config_path, .. },
        } => client::run(&config_path).await,
        Command::Server {
            command: PeerCommand::Schema,
        } => print_schema(schemars::schema_for!(config::ServerConfig)),
        Command::Server {
            command: PeerCommand::Run { config_path, .. },
        } => server::run(&config_path).await,
    };

    match result {
        Ok(()) => {
            tracing::info!("Exiting normally");
            ExitCode::SUCCESS
        },
        Err(error) => {
            tracing::error!(error = format!("{error:#}"), "Exiting due to failure");
            ExitCode::FAILURE
        },
    }
}

fn print_schema(schema: schemars::Schema) -> anyhow::Result<()> {
    let serialized = serde_json::to_string_pretty(&schema).context("failed to serialize schema")?;
    println!("{serialized}");
    Ok(())
}

fn generate_and_print_cert() -> anyhow::Result<()> {
    let cert = tls::generate_cert()?;
    print!("{}", cert.cert.pem());
    print!("{}", cert.signing_key.serialize_pem());
    Ok(())
}
