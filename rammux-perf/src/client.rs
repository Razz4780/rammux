//! The benchmark client: runs the workload against the echo server and
//! reports what it measured.

use std::{num::NonZero, path::Path, time::Duration};

use anyhow::Context;
use tokio::{net::TcpStream, time::Instant};

use crate::{
    client::{
        muxer::{H2Muxer, QuicMuxer, RammuxMuxer, YamuxMuxer},
        workload::Workload,
    },
    config::{ClientConfig, MuxerConfig},
    signal::ExitSignal,
};

mod connect;
mod muxer;
mod workload;

/// How many iterations can fail before we abort.
const MAX_FAILED_ITERATIONS: usize = 3;
/// How long [`ClientConfig::await_endpoint`] is waited for.
const GATE_TIMEOUT: Duration = Duration::from_secs(300);
/// How often it is retried meanwhile.
const GATE_INTERVAL: Duration = Duration::from_millis(500);

pub async fn run(config: &Path) -> anyhow::Result<()> {
    let config = {
        let raw = std::fs::read(config)
            .with_context(|| format!("failed to read config file at {}", config.display()))?;
        serde_json::from_slice::<ClientConfig>(&raw)
            .with_context(|| format!("failed to parse config file at {}", config.display()))?
    };
    let mut signal = ExitSignal::new()?;
    let workload = Workload {
        bulk_streams: config.bulk_streams.get(),
        bulk_stream_data: config.bulk_stream_data.get(),
        ping_pong_size: config.ping_pong_size.map(NonZero::get),
    };

    tracing::info!(
        protocol = config.muxer.protocol(),
        server_addr = %config.server_addr,
        with_tls = config.cert_path.is_some(),
        iterations = config.iterations,
        bulk_streams = config.bulk_streams,
        bulk_stream_data = config.bulk_stream_data,
        ping_pong_size = config.ping_pong_size.map(|size| size.get()),
        await_endpoint = config.await_endpoint.as_deref(),
        "Starting the client",
    );

    // Wait for the server to be reachable.
    if let Some(endpoint) = &config.await_endpoint {
        tokio::select! {
            () = &mut signal => {
                tracing::info!("Received an exit signal");
                return Ok(());
            },
            result = await_endpoint(endpoint) => result?,
        }
    }

    let mut samples = Samples::default();
    let mut iteration = 1;
    let mut failures = 0;
    while iteration <= config.iterations.get() {
        let result = tokio::select! {
            () = &mut signal => {
                tracing::info!("Received an exit signal");
                return Ok(());
            },
            result = run_iteration(&config, &workload) => result,
        };
        match result {
            Ok(new_samples) => {
                tracing::info!(iteration, "Iteration finished");
                iteration += 1;
                samples.extend(new_samples);
            },
            Err(error) if failures < MAX_FAILED_ITERATIONS => {
                tracing::warn!(
                    iteration,
                    error = format!("{error:#}"),
                    "Iteration failed, retrying",
                );
                failures += 1;
                continue;
            },
            Err(error) => {
                return Err(error.context(format!("iteration {iteration} failed")));
            },
        }
    }

    // This log is expected, in this format.
    let mean_bulk_elapsed = mean(&samples.bulk_elapsed);
    let mean_ping_pong_latency = mean(&samples.ping_pong_latency);
    // `percentile` takes a percentage, so these are 50 and 99 rather than
    // 0.50 and 0.99 - the fractions asked for the 0.5th and 0.99th
    // percentiles, which on any sample smaller than 200 is the fastest
    // exchange in the run, twice.
    let p50_ping_pong_latency = percentile(&mut samples.ping_pong_latency, 50.0);
    let p99_ping_pong_latency = percentile(&mut samples.ping_pong_latency, 99.0);
    tracing::info!(
        iterations = config.iterations.get(),
        failures,
        mean_bulk_elapsed_micros = mean_bulk_elapsed.as_micros() as u64,
        mean_ping_pong_latency_micros = mean_ping_pong_latency.as_micros() as u64,
        p50_ping_pong_latency_micros = p50_ping_pong_latency.as_micros() as u64,
        p99_ping_pong_latency_micros = p99_ping_pong_latency.as_micros() as u64,
        completed_ping_pongs = samples.ping_pong_latency.len(),
        total_time_micros = samples.elapsed.as_micros() as u64,
        total_cpu_time_micros = samples.cpu.as_micros() as u64,
        "Finished all iterations",
    );

    Ok(())
}

/// Blocks until `endpoint` accepts a TCP connection.
///
/// Retries rather than failing on the first refusal, because the whole point
/// is that the endpoint is not there yet - in a cluster run its name does not
/// even resolve until the orchestrator creates it.
async fn await_endpoint(endpoint: &str) -> anyhow::Result<()> {
    let started = Instant::now();
    let mut attempts = 0_u64;
    loop {
        attempts += 1;
        match TcpStream::connect(endpoint).await {
            Ok(..) => {
                tracing::info!(
                    endpoint,
                    attempts,
                    waited_s = started.elapsed().as_secs_f32(),
                    "Start gate opened",
                );
                return Ok(());
            },
            Err(error) if started.elapsed() >= GATE_TIMEOUT => {
                return Err(anyhow::Error::new(error).context(format!(
                    "{endpoint} did not accept a connection within {GATE_TIMEOUT:?}"
                )));
            },
            Err(..) => tokio::time::sleep(GATE_INTERVAL).await,
        }
    }
}

/// Connects, runs the workload once, and closes the connection.
///
/// Three of the four protocols get to the multiplexer the same way, over an
/// HTTP/1.1 upgrade that carries their config to the server.
/// QUIC does not, and the server has to know before the handshake.
/// Only the connecting procedure differs.
async fn run_iteration(config: &ClientConfig, workload: &Workload) -> anyhow::Result<Samples> {
    match &config.muxer {
        MuxerConfig::Rammux(muxer) => {
            let upgraded = connect::connect(config, config.muxer.protocol(), muxer).await?;
            let muxer = RammuxMuxer::new(upgraded, muxer);
            workload::run(muxer, workload).await
        },
        MuxerConfig::Yamux(muxer) => {
            let upgraded = connect::connect(config, config.muxer.protocol(), muxer).await?;
            let muxer = YamuxMuxer::new(upgraded, muxer)?;
            workload::run(muxer, workload).await
        },
        MuxerConfig::H2(muxer) => {
            let upgraded = connect::connect(config, config.muxer.protocol(), muxer).await?;
            let muxer = H2Muxer::new(upgraded, muxer).await?;
            workload::run(muxer, workload).await
        },
        MuxerConfig::Quic(muxer) => {
            let muxer =
                QuicMuxer::connect(config.server_addr, config.cert_path.as_deref(), muxer).await?;
            workload::run(muxer, workload).await
        },
    }
}

#[derive(Default)]
struct Samples {
    /// How long each bulk stream took.
    bulk_elapsed: Vec<Duration>,
    /// Round trip of each ping pong exchange.
    ping_pong_latency: Vec<Duration>,
    /// Total elapsed time.
    elapsed: Duration,
    /// Total CPU time.
    cpu: Duration,
}

impl Samples {
    /// Folds another iteration's samples in.
    pub fn extend(&mut self, other: Samples) {
        self.bulk_elapsed.extend(other.bulk_elapsed);
        self.ping_pong_latency.extend(other.ping_pong_latency);
        self.elapsed += other.elapsed;
        self.cpu += other.cpu;
    }
}

fn mean(values: &[Duration]) -> Duration {
    if values.is_empty() {
        // `div_f64(0.0)` is a NaN, and `Duration` panics on those. A run with
        // no ping pong stream has no latencies, which is a configuration
        // rather than a fault.
        return Duration::ZERO;
    }
    let sum = values.iter().copied().sum::<Duration>();
    sum.div_f64(values.len() as f64)
}

fn percentile(values: &mut [Duration], p: f64) -> Duration {
    if values.is_empty() {
        return Duration::ZERO;
    }
    values.sort_unstable();
    let rank = ((p / 100.0) * values.len() as f64).ceil() as usize;
    values[rank.clamp(1, values.len()) - 1]
}

#[cfg(test)]
mod test {
    use super::*;

    /// `percentile` takes a percentage, and the difference matters: asking it
    /// for 0.50 rather than 50.0 gets the 0.5th percentile, which on any
    /// sample smaller than 200 is the fastest exchange in the run. It is a
    /// silent wrong answer - the number still looks like a latency - and it
    /// makes p50 and p99 the same minimum, so every latency comparison a
    /// campaign draws from them is noise.
    #[test]
    fn percentiles_are_percentages() {
        let mut values: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
        assert_eq!(percentile(&mut values, 50.0), Duration::from_millis(50));
        assert_eq!(percentile(&mut values, 99.0), Duration::from_millis(99));
        assert_eq!(percentile(&mut values, 100.0), Duration::from_millis(100));
        // Nearest rank never falls off either end.
        assert_eq!(percentile(&mut values, 0.0), Duration::from_millis(1));
    }

    /// An empty sample is a configuration, not a fault: a run with no ping
    /// pong stream has no latencies. `Duration` panics on the NaN that
    /// dividing by a zero length would produce.
    #[test]
    fn an_empty_sample_summarises_to_zero() {
        assert_eq!(mean(&[]), Duration::ZERO);
        assert_eq!(percentile(&mut [], 99.0), Duration::ZERO);
    }
}
