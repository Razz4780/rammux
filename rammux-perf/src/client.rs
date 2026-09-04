//! The benchmark client: runs the workload against the echo server and
//! reports what it measured.

use std::{path::Path, time::Duration};

use anyhow::Context;
use base64::Engine;
use serde::Serialize;
use tokio::time::Instant;

use crate::{
    client::{
        muxer::{H2Muxer, Muxer, QuicMuxer, RammuxMuxer, YamuxMuxer},
        workload::{Measurements, Workload},
    },
    config::{ClientConfig, MuxerConfig},
    cpu,
    signal::ExitSignal,
};

mod connect;
mod muxer;
mod workload;

/// How many times a failed iteration is retried.
const MAX_RETRIES: usize = 3;
/// How long [`ClientConfig::await_endpoint`] is waited for.
const GATE_TIMEOUT: Duration = Duration::from_secs(300);
/// How often it is retried meanwhile.
const GATE_INTERVAL: Duration = Duration::from_millis(200);

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
        ping_pong_size: config.ping_pong_size.map(|size| size.get()),
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
    for iteration in 1..=config.iterations.get() {
        let mut attempt = 1;
        let report = loop {
            let result = tokio::select! {
                () = &mut signal => {
                    tracing::info!("Received an exit signal");
                    return Ok(());
                },
                result = run_iteration(&config, &workload) => result,
            };
            match result {
                Ok(report) => break report,
                Err(error) if attempt <= MAX_RETRIES => {
                    tracing::warn!(
                        iteration,
                        attempt,
                        error = format!("{error:#}"),
                        "Iteration failed, retrying",
                    );
                    attempt += 1;
                },
                Err(error) => {
                    return Err(error.context(format!(
                        "iteration {iteration} failed on all {attempt} attempts"
                    )));
                },
            }
        };
        samples.extend(report.log(iteration, attempt));
    }

    // The client is run on its own as well as under the cluster runner, so it
    // summarises what it measured rather than leaving a wall of samples.
    println!();
    println!(
        "{} over {} iterations",
        config.muxer.protocol(),
        config.iterations,
    );
    samples.print_rows();

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
        match tokio::net::TcpStream::connect(endpoint).await {
            Ok(..) => {
                tracing::info!(
                    endpoint,
                    attempts,
                    waited_ms = millis(started.elapsed()),
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
/// HTTP/1.1 upgrade that carries their config to the server. QUIC does not:
/// it is UDP, its TLS handshake is the connection's own, and its windows are
/// transport parameters the server has to know before the handshake - so it
/// connects on its own terms. Only the connecting differs; what is measured
/// afterwards is identical, which is what [`measure`] is for.
async fn run_iteration(config: &ClientConfig, workload: &Workload) -> anyhow::Result<Report> {
    match &config.muxer {
        MuxerConfig::Rammux(muxer) => {
            let upgraded = upgrade(config, muxer).await?;
            measure(RammuxMuxer::new(upgraded, muxer), workload).await
        },
        MuxerConfig::Yamux(muxer) => {
            let upgraded = upgrade(config, muxer).await?;
            measure(YamuxMuxer::new(upgraded, muxer)?, workload).await
        },
        MuxerConfig::H2(muxer) => {
            let upgraded = upgrade(config, muxer).await?;
            measure(H2Muxer::new(upgraded, muxer).await?, workload).await
        },
        MuxerConfig::Quic(muxer) => {
            let connection =
                QuicMuxer::connect(config.server_addr, config.cert_path.as_deref(), muxer).await?;
            measure(connection, workload).await
        },
    }
}

/// Upgrades a fresh HTTP/1.1 connection, carrying `muxer` to the server.
async fn upgrade<M: Serialize>(
    config: &ClientConfig,
    muxer: &M,
) -> anyhow::Result<hyper::upgrade::Upgraded> {
    let encoded = serde_json::to_vec(muxer).expect("failed to serialize muxer config");
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(encoded)
        .into_bytes();
    connect::connect(config, config.muxer.protocol(), encoded).await
}

/// Runs the workload, timing and costing only that.
///
/// Connection setup is deliberately outside: every protocol pays a different
/// handshake, and this benchmark is about what happens afterwards.
async fn measure<M: Muxer>(muxer: M, workload: &Workload) -> anyhow::Result<Report> {
    let started = Instant::now();
    let cpu_before = cpu::cpu_time()?;
    let measurements = workload::run(muxer, workload).await?;
    Ok(Report {
        elapsed: started.elapsed(),
        cpu: cpu::cpu_time()?.saturating_sub(cpu_before),
        measurements,
        bulk_stream_data: workload.bulk_stream_data,
    })
}

/// One iteration's results.
struct Report {
    elapsed: Duration,
    cpu: Duration,
    measurements: Measurements,
    /// Bytes each bulk stream sent, and read back, so a bulk sample can be
    /// read as a rate without knowing the config.
    bulk_stream_data: usize,
}

impl Report {
    /// Logs one event per measurement, and one for the iteration itself.
    ///
    /// Per sample rather than per iteration, because percentiles over a run
    /// have to come from every sample at once. Summarising here and averaging
    /// the summaries later would give a p99 of p99s, which is not the p99 of
    /// anything: with four bulk streams an iteration's p99 is just its slowest
    /// stream, and the middle of those is not the run's tail.
    fn log(&self, iteration: usize, attempt: usize) -> Samples {
        let mut samples = Samples::default();

        for (stream, elapsed) in self.measurements.bulk_elapsed.iter().enumerate() {
            // Everything downstream is derived from the microseconds that get
            // logged, so that a summary taken here and one taken from the log
            // are the same number rather than nearly the same number.
            let elapsed_us = micros(*elapsed);
            let elapsed_ms = elapsed_us as f64 / 1e3;
            // bits over microseconds is megabits over seconds, the 1e6 on each
            // side cancelling.
            let mbps = self.bulk_stream_data as f64 * 8.0 / elapsed_us as f64;
            tracing::info!(
                iteration,
                attempt,
                stream,
                elapsed_us,
                bytes = self.bulk_stream_data,
                mbps,
                "Bulk stream finished",
            );
            samples.bulk_mbps.push(mbps);
            samples.bulk_elapsed_ms.push(elapsed_ms);
        }

        for (exchange, latency) in self.measurements.latencies.iter().enumerate() {
            let elapsed_us = micros(*latency);
            tracing::info!(
                iteration,
                attempt,
                exchange,
                elapsed_us,
                "Ping pong exchange finished",
            );
            samples.latency_ms.push(elapsed_us as f64 / 1e3);
        }

        // The iteration's own cost, which is per iteration and not per sample.
        tracing::info!(
            iteration,
            attempt,
            elapsed_ms = millis(self.elapsed),
            cpu_ms = millis(self.cpu),
            bulk_streams = self.measurements.bulk_elapsed.len(),
            ping_pong_count = self.measurements.latencies.len(),
            "Iteration finished",
        );
        samples
    }
}

/// Every sample a run produced, across all of its iterations.
///
/// Shared with the cluster runner, which rebuilds one of these from the job's
/// log and summarises it the same way, so a run reports the same numbers
/// whichever side of the cluster it was started from.
#[derive(Default)]
pub struct Samples {
    /// Throughput of each bulk stream, in Mbit/s.
    pub bulk_mbps: Vec<f64>,
    /// How long each bulk stream took, in ms.
    pub bulk_elapsed_ms: Vec<f64>,
    /// Round trip of each ping pong exchange, in ms.
    pub latency_ms: Vec<f64>,
}

impl Samples {
    /// Folds another iteration's samples in.
    pub fn extend(&mut self, other: Samples) {
        self.bulk_mbps.extend(other.bulk_mbps);
        self.bulk_elapsed_ms.extend(other.bulk_elapsed_ms);
        self.latency_ms.extend(other.latency_ms);
    }

    /// Prints one row per metric, over every sample the run produced.
    pub fn print_rows(&self) {
        println!("                        mean         p50         p99       count");
        print_row("  throughput mbit/s ", &self.bulk_mbps);
        print_row("  bulk elapsed ms   ", &self.bulk_elapsed_ms);
        print_row("  latency ms        ", &self.latency_ms);
    }
}

fn print_row(label: &str, values: &[f64]) {
    let summary = Summary::of(values.iter().copied());
    println!(
        "{label}{:>11.3} {:>11.3} {:>11.3} {:>11}",
        summary.mean,
        summary.p50,
        summary.p99,
        values.len(),
    );
}

/// Whole microseconds, as a type the log fields render as a number.
///
/// Microseconds rather than milliseconds because a ping pong exchange on a
/// fast link is a fraction of one, and rounding those to zero would throw the
/// distribution away.
fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// Whole milliseconds, as a type the log fields render as a number.
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Mean and percentiles of a sample. Empty samples summarise to `NaN`.
pub struct Summary {
    pub mean: f64,
    pub p50: f64,
    pub p99: f64,
}

impl Summary {
    pub fn of(values: impl IntoIterator<Item = f64>) -> Self {
        let mut values: Vec<f64> = values.into_iter().collect();
        values.sort_by(f64::total_cmp);
        let percentile = |p: f64| -> f64 {
            if values.is_empty() {
                return f64::NAN;
            }
            // Nearest rank.
            let rank = ((p / 100.0) * values.len() as f64).ceil() as usize;
            values[rank.clamp(1, values.len()) - 1]
        };
        Self {
            mean: if values.is_empty() {
                f64::NAN
            } else {
                values.iter().sum::<f64>() / values.len() as f64
            },
            p50: percentile(50.0),
            p99: percentile(99.0),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::fmt::MakeWriter;

    use super::*;
    use crate::client::workload::Measurements;

    /// Collects a subscriber's output so a test can read it back.
    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Capture {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// The client logs the samples and the cluster runner reads them back out
    /// of the job's log. Nothing but the field names connects the two, so a
    /// rename on one side would leave the other quietly summarising an empty
    /// set - a run that looks like it worked and reports nothing. This runs
    /// the real logging through a real JSON subscriber and parses the result
    /// with the real parser.
    #[test]
    fn the_cluster_runner_reads_back_what_the_client_logged() {
        let report = Report {
            elapsed: Duration::from_millis(1234),
            cpu: Duration::from_millis(567),
            measurements: Measurements {
                bulk_elapsed: vec![
                    Duration::from_micros(23_178),
                    Duration::from_micros(25_003),
                    Duration::from_micros(19_004),
                ],
                latencies: vec![
                    Duration::from_micros(714),
                    Duration::from_micros(422),
                    Duration::from_micros(1_900),
                ],
            },
            bulk_stream_data: 4 * 1024 * 1024,
        };

        let capture = Capture::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(capture.clone())
            .finish();
        let logged = tracing::subscriber::with_default(subscriber, || report.log(3, 2));
        let raw = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();

        let (iterations, parsed) = crate::k8s::parse_log(&raw);

        assert_eq!(parsed.bulk_mbps, logged.bulk_mbps);
        assert_eq!(parsed.bulk_elapsed_ms, logged.bulk_elapsed_ms);
        assert_eq!(parsed.latency_ms, logged.latency_ms);
        assert_eq!(parsed.bulk_mbps.len(), 3);
        assert_eq!(parsed.latency_ms.len(), 3);
        // Microseconds are the unit on the wire precisely so that a
        // sub-millisecond exchange survives the trip.
        assert_eq!(parsed.latency_ms[1], 0.422);

        assert_eq!(
            iterations.len(),
            1,
            "the iteration's own cost is logged too"
        );
        assert_eq!(iterations[0].iteration, 3);
        assert_eq!(iterations[0].attempt, 2);
        assert_eq!(iterations[0].elapsed_ms, 1234);
        assert_eq!(iterations[0].cpu_ms, 567);
        assert_eq!(iterations[0].bulk_streams, 3);
        assert_eq!(iterations[0].ping_pong_count, 3);
    }

    /// A percentile has to come from the samples, not from other percentiles.
    #[test]
    fn percentiles_come_from_every_sample_at_once() {
        // Two iterations of four streams. Each iteration's own p99 is its
        // slowest stream - 10 and 100 - and the middle of those is 55. The
        // run's real p99 is 100, which is what taking it over all eight
        // samples gives.
        let mut samples = Samples::default();
        samples.bulk_mbps.extend([1.0, 2.0, 3.0, 10.0]);
        samples.bulk_mbps.extend([4.0, 5.0, 6.0, 100.0]);
        let p99 = Summary::of(samples.bulk_mbps.iter().copied()).p99;
        assert_eq!(p99, 100.0, "a percentile of percentiles would give 55");
    }
}
