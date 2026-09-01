//! Exposes the link emulator over TCP so contenders in any language can share it.
//!
//! The in-process harness hands each protocol an `EmuStream` directly. That is
//! not available to a contender in another language, and the kernel path in
//! this container has no `netem`, so shaped loopback can limit rate but adds no
//! propagation delay - which is exactly the variable flow control exists to
//! cope with. This proxy closes that gap: it accepts a TCP connection, dials
//! the real server, and splices the two through an emulated pair, so both ends
//! see the same rate limit, delay, loss and congestion surrogate as the
//! in-process harness.
//!
//! Every contender, Rust included, must go through it for a run to be
//! comparable: the two extra loopback hops are part of the measurement.

use clap::Parser;
use rammux_perf::emu::{EmuOpts, emu_pair};
use tokio::net::{TcpListener, TcpStream};

#[derive(Parser, Debug)]
struct Args {
    /// Address to accept contender connections on.
    #[arg(long, default_value = "127.0.0.1:0")]
    listen: String,
    /// Address of the real server to forward each connection to.
    #[arg(long)]
    upstream: String,

    #[arg(long, default_value_t = 100.0)]
    rate_mbit: f64,
    #[arg(long, default_value_t = 40.0)]
    rtt_ms: f64,
    #[arg(long, default_value_t = 0.0)]
    loss_pct: f64,
    #[arg(long, default_value_t = 0.0)]
    loss_burst_ms: f64,
    #[arg(long, default_value_t = 0.0)]
    jitter_ms: f64,
    #[arg(long, default_value_t = 4096)]
    sndbuf_kb: u64,
    #[arg(long, default_value_t = false)]
    shared_capacity: bool,
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let listener = TcpListener::bind(&args.listen).await?;
    // Printed so a caller that asked for port 0 can learn the real port.
    println!("listening {}", listener.local_addr()?);

    let mut seed = args.seed;
    loop {
        let (client, _) = listener.accept().await?;
        let _ = client.set_nodelay(true);
        // A connection that cannot reach the server must not take the proxy
        // down with it: readiness probes and aborted runs both land here, and
        // the next real connection still has to be served.
        let upstream = match TcpStream::connect(&args.upstream).await {
            Ok(upstream) => upstream,
            Err(error) => {
                eprintln!("upstream {} unreachable: {error}", args.upstream);
                continue;
            },
        };
        let _ = upstream.set_nodelay(true);

        // Each connection gets its own emulated link, seeded distinctly so
        // concurrent connections do not share a loss sequence.
        seed = seed.wrapping_add(1);
        let opts = EmuOpts {
            rate_mbit: args.rate_mbit,
            rtt_ms: args.rtt_ms,
            loss_pct: args.loss_pct,
            loss_burst_ms: args.loss_burst_ms,
            jitter_ms: args.jitter_ms,
            sndbuf_kb: args.sndbuf_kb,
            shared_capacity: args.shared_capacity,
            seed,
        };
        let (mut near, mut far, _, _) = emu_pair(&opts);

        tokio::spawn(async move {
            let mut client = client;
            let mut upstream = upstream;
            // client <-> near, and far <-> upstream: bytes cross the emulated
            // link between `near` and `far` in both directions.
            let a = tokio::io::copy_bidirectional(&mut client, &mut near);
            let b = tokio::io::copy_bidirectional(&mut far, &mut upstream);
            tokio::select! {
                _ = a => {},
                _ = b => {},
            }
        });
    }
}
