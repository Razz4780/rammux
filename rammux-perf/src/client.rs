//! The benchmark client: runs the workload against the echo server and
//! reports what it measured.

use std::{path::Path, time::Duration};

use anyhow::Context;
use base64::Engine;
use tokio::time::Instant;

use crate::{
    client::{
        muxer::{H2Muxer, RammuxMuxer, YamuxMuxer},
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
        "Starting the client",
    );

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
        report.log(iteration, attempt);
    }

    Ok(())
}

/// Connects, runs the workload once, and closes the connection.
async fn run_iteration(config: &ClientConfig, workload: &Workload) -> anyhow::Result<Report> {
    let upgraded = match &config.muxer {
        MuxerConfig::Rammux(muxer) => {
            let muxer = serde_json::to_vec(muxer).expect("failed to serialize muxer config");
            let muxer = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(muxer)
                .into_bytes();
            connect::connect(config, "rammux", muxer).await?
        },
        MuxerConfig::Yamux(muxer) => {
            let muxer = serde_json::to_vec(muxer).expect("failed to serialize muxer config");
            let muxer = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(muxer)
                .into_bytes();
            connect::connect(config, "yamux", muxer).await?
        },
        MuxerConfig::H2(muxer) => {
            let muxer = serde_json::to_vec(muxer).expect("failed to serialize muxer config");
            let muxer = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(muxer)
                .into_bytes();
            connect::connect(config, "h2", muxer).await?
        },
    };

    let started = Instant::now();
    let cpu_before = cpu::cpu_time()?;
    let measurements = match &config.muxer {
        MuxerConfig::Rammux(muxer) => {
            workload::run(RammuxMuxer::new(upgraded, muxer), workload).await?
        },
        MuxerConfig::Yamux(muxer) => {
            workload::run(YamuxMuxer::new(upgraded, muxer), workload).await?
        },
        MuxerConfig::H2(muxer) => {
            workload::run(H2Muxer::new(upgraded, muxer).await?, workload).await?
        },
    };

    Ok(Report {
        elapsed: started.elapsed(),
        cpu: cpu::cpu_time()?.saturating_sub(cpu_before),
        measurements,
    })
}

/// One iteration's results.
struct Report {
    elapsed: Duration,
    cpu: Duration,
    measurements: Measurements,
}

impl Report {
    fn log(&self, iteration: usize, attempt: usize) {
        let throughput = Summary::of(
            self.measurements
                .bulk_bytes_per_sec
                .iter()
                .map(|bytes_per_sec| bytes_per_sec * 8.0 / 1e6),
        );
        let latency = Summary::of(
            self.measurements
                .latencies
                .iter()
                .map(|latency| latency.as_secs_f64() * 1e3),
        );
        tracing::info!(
            iteration,
            attempt,
            elapsed_ms = millis(self.elapsed),
            cpu_ms = millis(self.cpu),
            bulk_streams = self.measurements.bulk_bytes_per_sec.len(),
            bulk_mbps_mean = throughput.mean,
            bulk_mbps_p50 = throughput.p50,
            bulk_mbps_p99 = throughput.p99,
            ping_pong_count = self.measurements.latencies.len(),
            latency_ms_mean = latency.mean,
            latency_ms_p50 = latency.p50,
            latency_ms_p99 = latency.p99,
            "Iteration finished",
        );
    }
}

/// Whole milliseconds, as a type the log fields render as a number.
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Mean and percentiles of a sample. Empty samples summarise to `NaN`.
struct Summary {
    mean: f64,
    p50: f64,
    p99: f64,
}

impl Summary {
    fn of(values: impl IntoIterator<Item = f64>) -> Self {
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
