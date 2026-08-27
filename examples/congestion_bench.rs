//! Research bench for rammux congestion behavior.
//!
//! Runs rammux (or a raw stream) over a real TCP connection or over a
//! userspace emulated link, and samples goodput, ping RTT, window state
//! and link queues to CSV on stdout.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use futures::{SinkExt, StreamExt};
use rammux::{
    RammuxError,
    config::{RammuxConfig, RammuxRole},
    connection::{RammuxConnection, RammuxProgress},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::Instant,
};

#[derive(Parser, Debug)]
struct Args {
    /// rammux or raw byte stream.
    #[arg(long, value_enum, default_value_t = Mode::Rammux)]
    mode: Mode,
    /// Real TCP on loopback (shape externally) or userspace emulated link.
    #[arg(long, value_enum, default_value_t = TransportKind::Emu)]
    transport: TransportKind,

    // ---- emulated link parameters ----
    /// Link rate in Mbit/s.
    #[arg(long, default_value_t = 100.0)]
    rate_mbit: f64,
    /// Round-trip propagation delay in ms.
    #[arg(long, default_value_t = 20.0)]
    rtt_ms: f64,
    /// Random loss applied to the TCP model, percent.
    #[arg(long, default_value_t = 0.0)]
    loss_pct: f64,
    /// Mean duration of a loss burst in ms (0 = independent Bernoulli losses).
    #[arg(long, default_value_t = 0.0)]
    loss_burst_ms: f64,
    /// One-way delivery jitter bound in ms.
    #[arg(long, default_value_t = 0.0)]
    jitter_ms: f64,
    /// Emulated socket send buffer per direction, KiB.
    #[arg(long, default_value_t = 4096)]
    sndbuf_kb: u64,
    /// Both directions draw from one shared capacity pool (half-duplex media
    /// like Wi-Fi airtime) instead of having independent full-duplex capacity.
    #[arg(long, default_value_t = false)]
    shared_capacity: bool,
    /// In bi mode, delay the reverse (server-side) writers by this many seconds.
    #[arg(long, default_value_t = 0.0)]
    reverse_delay_s: f64,
    /// RNG seed.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    // ---- workload ----
    /// Concurrent rammux streams.
    #[arg(long, default_value_t = 8)]
    streams: usize,
    /// Bytes to send per stream; 0 means unbounded (run until --duration-s).
    #[arg(long, default_value_t = 0)]
    bytes_per_stream: u64,
    /// Hard stop for the run, seconds.
    #[arg(long, default_value_t = 60.0)]
    duration_s: f64,
    /// uni: client sends, server reads. bi: both directions.
    #[arg(long, value_enum, default_value_t = Direction::Uni)]
    direction: Direction,
    /// Application write chunk, KiB.
    #[arg(long, default_value_t = 64)]
    chunk_kb: usize,
    /// Artificial delay between reads on the receiving side, microseconds.
    #[arg(long, default_value_t = 0)]
    reader_delay_us: u64,

    // ---- rammux config ----
    #[arg(long, default_value_t = 5000)]
    ping_interval_ms: u64,
    #[arg(long, default_value_t = 4096)]
    global_window_kb: u32,
    #[arg(long, default_value_t = 64)]
    stream_window_kb: u32,
    #[arg(long, default_value_t = 16)]
    frame_limit_kb: u32,
    /// Transit (in-flight) window initial size, KiB. 0 disables it.
    #[arg(long, default_value_t = 0)]
    transit_kb: u32,
    /// Transit window autotune limit, KiB.
    #[arg(long, default_value_t = 4096)]
    transit_max_kb: u32,

    // ---- sampling ----
    #[arg(long, default_value_t = 500)]
    sample_ms: u64,

    /// TCP congestion control for --transport tcp (must be in the allowed list).
    #[arg(long, default_value = "reno")]
    cc: String,
}

#[derive(Clone, Copy, ValueEnum, Debug, PartialEq)]
enum Mode {
    Rammux,
    Raw,
}

#[derive(Clone, Copy, ValueEnum, Debug, PartialEq)]
enum TransportKind {
    Tcp,
    Emu,
}

#[derive(Clone, Copy, ValueEnum, Debug, PartialEq)]
enum Direction {
    Uni,
    Bi,
}

#[path = "support/emu.rs"]
mod emu;
use emu::{DirGauges, EmuOpts, emu_pair};

// ---------------------------------------------------------------------------
// Workload
// ---------------------------------------------------------------------------

fn rammux_config(args: &Args) -> RammuxConfig {
    let mut config = RammuxConfig::new();
    config.frame_limit = (args.frame_limit_kb * 1024).try_into().unwrap();
    config.local_recv_window = (args.stream_window_kb * 1024).try_into().unwrap();
    config.remote_recv_window = args.stream_window_kb * 1024;
    config.ping_interval = Duration::from_millis(args.ping_interval_ms);
    config.global_recv_window = args.global_window_kb as usize * 1024;
    config.local_transit_window = args.transit_kb * 1024;
    config.remote_transit_window = args.transit_kb * 1024;
    config.transit_window_max = args.transit_max_kb * 1024;
    config
}

struct Counters {
    delivered_to_client: AtomicU64,
    delivered_to_server: AtomicU64,
}

async fn drive_conn<IO>(
    mut conn: RammuxConnection<IO>,
    args: &Args,
    counters: Arc<Counters>,
    started: Instant,
) -> Result<(), RammuxError>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let role = conn.role();
    let is_client = role == RammuxRole::Client;
    let mut to_start = if is_client { args.streams } else { 0 };

    let mut sample = tokio::time::interval(Duration::from_millis(args.sample_ms));
    sample.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        while to_start > 0 {
            match conn.try_start_outbound() {
                Ok(Some(duplex)) => {
                    to_start -= 1;
                    let write = true;
                    let read = args.direction == Direction::Bi;
                    spawn_stream(
                        duplex,
                        args,
                        write,
                        read,
                        counters.clone(),
                        is_client,
                        Duration::ZERO,
                    );
                },
                Ok(None) => break,
                Err(error) => return Err(error),
            }
        }

        tokio::select! {
            progress = conn.progress() => match progress? {
                RammuxProgress::Inbound(duplex) => {
                    let write = args.direction == Direction::Bi;
                    let read = true;
                    spawn_stream(
                        duplex,
                        args,
                        write,
                        read,
                        counters.clone(),
                        is_client,
                        Duration::from_secs_f64(args.reverse_delay_s),
                    );
                },
                RammuxProgress::Downgraded(..) => return Ok(()),
                RammuxProgress::Empty => {},
            },
            _ = sample.tick() => {
                let stats = conn.stats();
                let rtt_ms = stats.rtt.map(|d| d.as_secs_f64() * 1e3).unwrap_or(-1.0);
                println!(
                    "{:.3},{role},stats,rtt_ms={rtt_ms:.2},pool_kb={},in={},out={},tc_kb={},tw_kb={}",
                    started.elapsed().as_secs_f64(),
                    stats.available_global_recv_window / 1024,
                    stats.inbound_streams,
                    stats.outbound_streams,
                    stats.transit_send_credit.map(|c| (c / 1024) as i64).unwrap_or(-1),
                    stats.transit_recv_window.map(|w| (w / 1024) as i64).unwrap_or(-1),
                );
            }
        }
    }
}

fn spawn_stream(
    duplex: rammux::stream::RammuxDuplex,
    args: &Args,
    write: bool,
    read: bool,
    counters: Arc<Counters>,
    on_client: bool,
    write_delay: Duration,
) {
    let chunk = Bytes::from(vec![0u8; args.chunk_kb * 1024]);
    let target = args.bytes_per_stream;
    let reader_delay = Duration::from_micros(args.reader_delay_us);
    let (mut sink, mut stream) = duplex.into_split();
    tokio::spawn(async move {
        let writer = async {
            if write {
                if !write_delay.is_zero() {
                    tokio::time::sleep(write_delay).await;
                }
                let mut sent = 0u64;
                while target == 0 || sent < target {
                    let chunk = if target > 0 && target - sent < chunk.len() as u64 {
                        chunk.clone().split_to((target - sent) as usize)
                    } else {
                        chunk.clone()
                    };
                    sent += chunk.len() as u64;
                    if sink.feed(chunk).await.is_err() {
                        return;
                    }
                }
                let _ = sink.close().await;
            } else {
                let _ = sink.close().await;
            }
        };
        let reader = async {
            if read {
                let counter = if on_client {
                    &counters.delivered_to_client
                } else {
                    &counters.delivered_to_server
                };
                while let Some(data) = stream.next().await {
                    counter.fetch_add(data.len() as u64, Ordering::Relaxed);
                    if !reader_delay.is_zero() {
                        tokio::time::sleep(reader_delay).await;
                    }
                }
            }
        };
        tokio::join!(writer, reader);
    });
}

async fn run_rammux<IO>(io_client: IO, io_server: IO, args: &Args, counters: Arc<Counters>)
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let started = Instant::now();
    let client = RammuxConnection::new(RammuxRole::Client, io_client, rammux_config(args));
    let server = RammuxConnection::new(RammuxRole::Server, io_server, rammux_config(args));

    let client_task = drive_conn(client, args, counters.clone(), started);
    let server_task = drive_conn(server, args, counters.clone(), started);
    let deadline = tokio::time::sleep(Duration::from_secs_f64(args.duration_s));

    tokio::select! {
        results = futures::future::join(client_task, server_task) => {
            match results {
                (Ok(()), Ok(())) => println!("{:.3},both,done,", started.elapsed().as_secs_f64()),
                (client, server) => {
                    if let Err(error) = client {
                        println!("{:.3},client,failed,{error}", started.elapsed().as_secs_f64());
                    }
                    if let Err(error) = server {
                        println!("{:.3},server,failed,{error}", started.elapsed().as_secs_f64());
                    }
                },
            }
        },
        _ = deadline => {
            println!("{:.3},both,deadline,", started.elapsed().as_secs_f64());
        }
    }
}

async fn run_raw<IO>(io_client: IO, io_server: IO, args: &Args, counters: Arc<Counters>)
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let started = Instant::now();
    let chunk = Bytes::from(vec![0u8; args.chunk_kb * 1024]);
    let bi = args.direction == Direction::Bi;

    let (mut cr, mut cw) = tokio::io::split(io_client);
    let (mut sr, mut sw) = tokio::io::split(io_server);

    let counters_2 = counters.clone();
    let chunk_2 = chunk.clone();
    let client_writer = tokio::spawn(async move {
        loop {
            if cw.write_all(&chunk_2).await.is_err() {
                return;
            }
        }
    });
    let server_reader = tokio::spawn(async move {
        let mut buf = vec![0u8; 256 * 1024];
        loop {
            match sr.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    counters_2
                        .delivered_to_server
                        .fetch_add(n as u64, Ordering::Relaxed);
                },
            }
        }
    });
    let counters_3 = counters.clone();
    let server_writer = tokio::spawn(async move {
        if !bi {
            return;
        }
        loop {
            if sw.write_all(&chunk).await.is_err() {
                return;
            }
        }
    });
    let client_reader = tokio::spawn(async move {
        let mut buf = vec![0u8; 256 * 1024];
        loop {
            match cr.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    counters_3
                        .delivered_to_client
                        .fetch_add(n as u64, Ordering::Relaxed);
                },
            }
        }
    });

    tokio::time::sleep(Duration::from_secs_f64(args.duration_s)).await;
    println!("{:.3},both,deadline,", started.elapsed().as_secs_f64());
    client_writer.abort();
    server_reader.abort();
    server_writer.abort();
    client_reader.abort();
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("# {args:?}");
    println!("# csv: t_s,side,event,details");

    let counters = Arc::new(Counters {
        delivered_to_client: AtomicU64::new(0),
        delivered_to_server: AtomicU64::new(0),
    });

    // Goodput + link gauge sampler.
    let sampler_counters = counters.clone();
    let sample_ms = args.sample_ms;
    let spawn_sampler = |gauges: Option<(Arc<DirGauges>, Arc<DirGauges>)>| {
        tokio::spawn(async move {
            let started = Instant::now();
            let mut last_c = 0u64;
            let mut last_s = 0u64;
            let mut interval = tokio::time::interval(Duration::from_millis(sample_ms));
            loop {
                interval.tick().await;
                let c = sampler_counters.delivered_to_client.load(Ordering::Relaxed);
                let s = sampler_counters.delivered_to_server.load(Ordering::Relaxed);
                let dt = sample_ms as f64 / 1000.0;
                let to_client_mbps = (c - last_c) as f64 * 8.0 / 1e6 / dt;
                let to_server_mbps = (s - last_s) as f64 * 8.0 / 1e6 / dt;
                last_c = c;
                last_s = s;
                let link = gauges
                    .as_ref()
                    .map(|(ab, ba)| {
                        format!(
                            "q_ab_kb={},q_ba_kb={},cwnd_ab_kb={},cwnd_ba_kb={},stall_ab_ms={},stall_ba_ms={},rtos_ab={},rtos_ba={}",
                            ab.queued.load(Ordering::Relaxed) / 1024,
                            ba.queued.load(Ordering::Relaxed) / 1024,
                            ab.cwnd.load(Ordering::Relaxed) / 1024,
                            ba.cwnd.load(Ordering::Relaxed) / 1024,
                            ab.stall_ms.load(Ordering::Relaxed),
                            ba.stall_ms.load(Ordering::Relaxed),
                            ab.rtos.load(Ordering::Relaxed),
                            ba.rtos.load(Ordering::Relaxed),
                        )
                    })
                    .unwrap_or_default();
                println!(
                    "{:.3},link,goodput,to_client_mbps={to_client_mbps:.2},to_server_mbps={to_server_mbps:.2},{link}",
                    started.elapsed().as_secs_f64(),
                );
            }
        })
    };

    match args.transport {
        TransportKind::Emu => {
            let opts = EmuOpts {
                rate_mbit: args.rate_mbit,
                rtt_ms: args.rtt_ms,
                loss_pct: args.loss_pct,
                loss_burst_ms: args.loss_burst_ms,
                jitter_ms: args.jitter_ms,
                sndbuf_kb: args.sndbuf_kb,
                shared_capacity: args.shared_capacity,
                seed: args.seed,
            };
            let (io_client, io_server, g_ab, g_ba) = emu_pair(&opts);
            spawn_sampler(Some((g_ab, g_ba)));
            match args.mode {
                Mode::Rammux => run_rammux(io_client, io_server, &args, counters.clone()).await,
                Mode::Raw => run_raw(io_client, io_server, &args, counters.clone()).await,
            }
        },
        TransportKind::Tcp => {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let io_client = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let io_server = listener.accept().await.unwrap().0;
            for stream in [&io_client, &io_server] {
                stream.set_nodelay(true).unwrap();
                let sock = socket2::SockRef::from(stream);
                sock.set_tcp_congestion(args.cc.as_bytes())
                    .expect("failed to set congestion control");
            }
            spawn_sampler(None);
            match args.mode {
                Mode::Rammux => run_rammux(io_client, io_server, &args, counters.clone()).await,
                Mode::Raw => run_raw(io_client, io_server, &args, counters.clone()).await,
            }
        },
    }
}
