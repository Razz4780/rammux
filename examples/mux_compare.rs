//! Comparative benchmark: rammux vs yamux (rust-yamux) vs hyper/HTTP-2.
//!
//! All three run identical workloads over identical transports (userspace
//! emulated link or real TCP), with per-protocol tuning knobs exposed so each
//! can be run at its best configuration.
//!
//! Workloads (client opens all streams; data flows client -> server):
//! * bulk: N unbounded streams, fixed duration, aggregate goodput.
//! * jobs: N streams x B bytes, time until the server has received all bytes.
//! * echo: N unbounded bulk streams plus one 1 KiB echo stream; every echo
//!   round trip is reported (application-level latency under load).

#[path = "support/emu.rs"]
mod emu;

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use emu::{EmuOpts, emu_pair};
use futures::{SinkExt, StreamExt};
use rammux::{
    config::{RammuxConfig, RammuxRole},
    connection::{RammuxConnection, RammuxProgress},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    time::Instant,
};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, value_enum)]
    proto: Proto,
    #[arg(long, value_enum, default_value_t = TransportKind::Emu)]
    transport: TransportKind,
    #[arg(long, value_enum, default_value_t = Workload::Bulk)]
    workload: Workload,

    // ---- link ----
    #[arg(long, default_value_t = 100.0)]
    rate_mbit: f64,
    #[arg(long, default_value_t = 20.0)]
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

    // ---- workload ----
    #[arg(long, default_value_t = 8)]
    streams: usize,
    #[arg(long, default_value_t = 1024)]
    bytes_per_stream_kb: u64,
    #[arg(long, default_value_t = 30.0)]
    duration_s: f64,
    #[arg(long, default_value_t = 64)]
    chunk_kb: usize,
    #[arg(long, default_value_t = 500)]
    sample_ms: u64,

    // ---- rammux tuning ----
    #[arg(long, default_value_t = 64)]
    r_stream_window_kb: u32,
    #[arg(long, default_value_t = 4096)]
    r_global_window_kb: u32,
    #[arg(long, default_value_t = 256)]
    r_transit_kb: u32,
    #[arg(long, default_value_t = 4096)]
    r_transit_max_kb: u32,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    r_clean: bool,
    /// Use the clean-probe RTT for stream autotune and the transit rate
    /// meter too, not just the transit growth policy.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    r_clean_all: bool,

    // ---- yamux tuning ----
    /// Max connection receive window in MiB; 0 keeps the yamux default (1 GiB).
    #[arg(long, default_value_t = 0)]
    y_conn_window_mb: usize,
    #[arg(long, default_value_t = 16)]
    y_split_kb: usize,

    // ---- hyper/h2 tuning ----
    #[arg(long, default_value_t = 64)]
    h_stream_window_kb: u32,
    #[arg(long, default_value_t = 1024)]
    h_conn_window_kb: u32,
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    h_adaptive: bool,
    #[arg(long, default_value_t = 16)]
    h_frame_kb: u32,
}

#[derive(Clone, Copy, ValueEnum, Debug, PartialEq)]
enum Proto {
    Rammux,
    Yamux,
    H2,
}

#[derive(Clone, Copy, ValueEnum, Debug, PartialEq)]
enum TransportKind {
    Emu,
    Tcp,
}

#[derive(Clone, Copy, ValueEnum, Debug, PartialEq)]
enum Workload {
    Bulk,
    Jobs,
    Echo,
}

struct Metrics {
    delivered: AtomicU64,
    started: Instant,
}

impl Metrics {
    fn echo_sample(&self, rtt: Duration) {
        println!(
            "{:.3},client,echo,rtt_ms={:.2}",
            self.started.elapsed().as_secs_f64(),
            rtt.as_secs_f64() * 1e3,
        );
    }
}

fn chunk(args: &Args) -> Bytes {
    Bytes::from(vec![0u8; args.chunk_kb * 1024])
}

const ECHO_CHUNK: usize = 1024;

// ---------------------------------------------------------------------------
// main + scaffolding
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("# {args:?}");
    let metrics = Arc::new(Metrics {
        delivered: AtomicU64::new(0),
        started: Instant::now(),
    });

    // Goodput sampler.
    {
        let metrics = metrics.clone();
        let sample_ms = args.sample_ms;
        tokio::spawn(async move {
            let mut last = 0u64;
            let mut interval = tokio::time::interval(Duration::from_millis(sample_ms));
            loop {
                interval.tick().await;
                let now = metrics.delivered.load(Ordering::Relaxed);
                let mbps = (now - last) as f64 * 8.0 / 1e6 / (sample_ms as f64 / 1000.0);
                last = now;
                println!(
                    "{:.3},link,goodput,to_server_mbps={mbps:.2}",
                    metrics.started.elapsed().as_secs_f64(),
                );
            }
        });
    }

    // Jobs completion watcher.
    let jobs_done = {
        let metrics = metrics.clone();
        let target = if args.workload == Workload::Jobs {
            args.streams as u64 * args.bytes_per_stream_kb * 1024
        } else {
            u64::MAX
        };
        async move {
            loop {
                if metrics.delivered.load(Ordering::Relaxed) >= target {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    };

    let deadline = tokio::time::sleep(Duration::from_secs_f64(args.duration_s));
    let work = run(&args, metrics.clone());

    tokio::select! {
        _ = jobs_done => {
            println!(
                "{:.3},both,done,elapsed_s={:.3}",
                metrics.started.elapsed().as_secs_f64(),
                metrics.started.elapsed().as_secs_f64(),
            );
        },
        _ = deadline => {
            println!("{:.3},both,deadline,", metrics.started.elapsed().as_secs_f64());
        },
        result = work => {
            match result {
                Ok(()) => println!("{:.3},both,finished,", metrics.started.elapsed().as_secs_f64()),
                Err(error) => println!(
                    "{:.3},both,failed,{error}",
                    metrics.started.elapsed().as_secs_f64(),
                ),
            }
        },
    }
    use std::io::Write;
    let _ = std::io::stdout().flush();
    std::process::exit(0);
}

async fn run(args: &Args, metrics: Arc<Metrics>) -> Result<(), String> {
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
            let (io_client, io_server, ..) = emu_pair(&opts);
            dispatch(args, metrics, io_client, io_server).await
        },
        TransportKind::Tcp => {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let io_client = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let io_server = listener.accept().await.unwrap().0;
            io_client.set_nodelay(true).unwrap();
            io_server.set_nodelay(true).unwrap();
            dispatch(args, metrics, io_client, io_server).await
        },
    }
}

async fn dispatch<IO>(
    args: &Args,
    metrics: Arc<Metrics>,
    io_client: IO,
    io_server: IO,
) -> Result<(), String>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    match args.proto {
        Proto::Rammux => run_rammux(args, metrics, io_client, io_server).await,
        Proto::Yamux => run_yamux(args, metrics, io_client, io_server).await,
        Proto::H2 => run_h2(args, metrics, io_client, io_server).await,
    }
}

/// Bytes each data stream should carry; u64::MAX = unbounded.
fn stream_target(args: &Args) -> u64 {
    match args.workload {
        Workload::Jobs => args.bytes_per_stream_kb * 1024,
        Workload::Bulk | Workload::Echo => u64::MAX,
    }
}

// ---------------------------------------------------------------------------
// rammux
// ---------------------------------------------------------------------------

fn rammux_config(args: &Args) -> RammuxConfig {
    let mut config = RammuxConfig::new();
    config.local_recv_window = (args.r_stream_window_kb * 1024).try_into().unwrap();
    config.remote_recv_window = args.r_stream_window_kb * 1024;
    config.global_recv_window = args.r_global_window_kb as usize * 1024;
    config.local_transit_window = args.r_transit_kb * 1024;
    config.remote_transit_window = args.r_transit_kb * 1024;
    config.transit_window_max = args.r_transit_max_kb * 1024;
    config.transit_clean_probe = args.r_clean;
    config.clean_rtt_sizing = args.r_clean_all;
    config.max_inbound_streams = args.streams as u32 + 2;
    config.max_outbound_streams = args.streams as u32 + 2;
    config
}

async fn run_rammux<IO>(
    args: &Args,
    metrics: Arc<Metrics>,
    io_client: IO,
    io_server: IO,
) -> Result<(), String>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let client = RammuxConnection::new(RammuxRole::Client, io_client, rammux_config(args));
    let server = RammuxConnection::new(RammuxRole::Server, io_server, rammux_config(args));

    let target = stream_target(args);
    let chunk = chunk(args);
    let echo = args.workload == Workload::Echo;

    // Client: open streams and run writers.
    let client_task = {
        let metrics = metrics.clone();
        async move {
            let mut conn = client;
            let mut opened = 0usize;
            let total = args.streams + usize::from(echo);
            loop {
                while opened < total {
                    match conn.try_start_outbound() {
                        Ok(Some(duplex)) => {
                            let is_echo = echo && opened == 0;
                            opened += 1;
                            let chunk = chunk.clone();
                            let metrics = metrics.clone();
                            tokio::spawn(async move {
                                let (mut sink, mut stream) = duplex.into_split();
                                let marker = if is_echo { b"E" } else { b"D" };
                                if sink.send(Bytes::from_static(marker)).await.is_err() {
                                    return;
                                }
                                // The marker byte counts toward the stream's payload.
                                if is_echo {
                                    let payload = Bytes::from(vec![0u8; ECHO_CHUNK]);
                                    loop {
                                        let sent = Instant::now();
                                        if sink.send(payload.clone()).await.is_err() {
                                            return;
                                        }
                                        let mut got = 0usize;
                                        while got < ECHO_CHUNK {
                                            match stream.next().await {
                                                Some(data) => got += data.len(),
                                                None => return,
                                            }
                                        }
                                        metrics.echo_sample(sent.elapsed());
                                    }
                                } else {
                                    let mut sent = 1u64.min(target);
                                    while sent < target {
                                        let part = if target - sent < chunk.len() as u64 {
                                            chunk.clone().split_to((target - sent) as usize)
                                        } else {
                                            chunk.clone()
                                        };
                                        sent += part.len() as u64;
                                        if sink.feed(part).await.is_err() {
                                            return;
                                        }
                                    }
                                    let _ = sink.close().await;
                                }
                            });
                        },
                        Ok(None) => break,
                        Err(error) => return Err(error.to_string()),
                    }
                }
                match conn.progress().await {
                    Ok(_) => {},
                    Err(error) => return Err(error.to_string()),
                }
            }
        }
    };

    // Server: read and count; echo the first stream in echo mode.
    let server_task = {
        let metrics = metrics.clone();
        async move {
            let mut conn = server;
            loop {
                match conn.progress().await {
                    Ok(RammuxProgress::Inbound(duplex)) => {
                        let metrics = metrics.clone();
                        tokio::spawn(async move {
                            let (mut sink, mut stream) = duplex.into_split();
                            // First byte marks the stream type.
                            let first = match stream.next().await {
                                Some(data) => data,
                                None => return,
                            };
                            let is_echo = first[0] == b'E';
                            if !is_echo {
                                metrics
                                    .delivered
                                    .fetch_add(first.len() as u64, Ordering::Relaxed);
                            }
                            if is_echo {
                                while let Some(data) = stream.next().await {
                                    if sink.send(data).await.is_err() {
                                        return;
                                    }
                                }
                            } else {
                                while let Some(data) = stream.next().await {
                                    metrics
                                        .delivered
                                        .fetch_add(data.len() as u64, Ordering::Relaxed);
                                }
                            }
                        });
                    },
                    Ok(_) => {},
                    Err(error) => return Err(error.to_string()),
                }
            }
        }
    };

    tokio::select! {
        r = client_task => r,
        r = server_task => r,
    }
}

// ---------------------------------------------------------------------------
// yamux
// ---------------------------------------------------------------------------

async fn run_yamux<IO>(
    args: &Args,
    metrics: Arc<Metrics>,
    io_client: IO,
    io_server: IO,
) -> Result<(), String>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    use tokio_util::compat::TokioAsyncReadCompatExt;

    let mut cfg = yamux::Config::default();
    cfg.set_max_num_streams(args.streams + 2);
    cfg.set_split_send_size(args.y_split_kb * 1024);
    if args.y_conn_window_mb > 0 {
        cfg.set_max_connection_receive_window(Some(args.y_conn_window_mb * 1024 * 1024));
    }

    let client = yamux::Connection::new(io_client.compat(), cfg.clone(), yamux::Mode::Client);
    let server = yamux::Connection::new(io_server.compat(), cfg, yamux::Mode::Server);

    let target = stream_target(args);
    let chunk = chunk(args);
    let echo = args.workload == Workload::Echo;
    let total = args.streams + usize::from(echo);

    // Client driver: opens `total` streams, spawns writers, keeps polling.
    let client_task = {
        let metrics = metrics.clone();
        let chunk = chunk.clone();
        async move {
            let mut conn = client;
            let mut to_open = total;
            let mut opened = 0usize;
            futures::future::poll_fn(move |cx| {
                loop {
                    if to_open > 0 {
                        match conn.poll_new_outbound(cx) {
                            Poll::Ready(Ok(stream)) => {
                                let is_echo = echo && opened == 0;
                                opened += 1;
                                to_open -= 1;
                                spawn_yamux_client_stream(
                                    stream,
                                    is_echo,
                                    target,
                                    chunk.clone(),
                                    metrics.clone(),
                                );
                                continue;
                            },
                            Poll::Ready(Err(error)) => {
                                return Poll::Ready(Err(error.to_string()));
                            },
                            Poll::Pending => {},
                        }
                    }
                    // Drive the connection.
                    match conn.poll_next_inbound(cx) {
                        Poll::Ready(Some(Ok(_ignored_inbound))) => continue,
                        Poll::Ready(Some(Err(error))) => {
                            return Poll::Ready(Err(error.to_string()));
                        },
                        Poll::Ready(None) => return Poll::Ready(Ok(())),
                        Poll::Pending => return Poll::Pending,
                    }
                }
            })
            .await
        }
    };

    // Server driver: accepts inbound streams and spawns handlers.
    let server_task = {
        let metrics = metrics.clone();
        async move {
            let mut conn = server;
            futures::future::poll_fn(move |cx| {
                loop {
                    match conn.poll_next_inbound(cx) {
                        Poll::Ready(Some(Ok(stream))) => {
                            spawn_yamux_server_stream(stream, metrics.clone());
                        },
                        Poll::Ready(Some(Err(error))) => {
                            return Poll::Ready(Err(error.to_string()));
                        },
                        Poll::Ready(None) => return Poll::Ready(Ok(())),
                        Poll::Pending => return Poll::Pending,
                    }
                }
            })
            .await
        }
    };

    tokio::select! {
        r = client_task => r,
        r = server_task => r,
    }
}

fn spawn_yamux_client_stream(
    mut stream: yamux::Stream,
    is_echo: bool,
    target: u64,
    chunk: Bytes,
    metrics: Arc<Metrics>,
) {
    use futures::io::{AsyncReadExt, AsyncWriteExt};
    tokio::spawn(async move {
        let marker: &[u8] = if is_echo { b"E" } else { b"D" };
        if stream.write_all(marker).await.is_err() {
            return;
        }
        // The marker byte counts toward the stream's payload.
        if is_echo {
            let payload = vec![0u8; ECHO_CHUNK];
            let mut buf = vec![0u8; ECHO_CHUNK];
            loop {
                let sent = Instant::now();
                if stream.write_all(&payload).await.is_err() {
                    return;
                }
                if stream.flush().await.is_err() {
                    return;
                }
                let mut got = 0usize;
                while got < ECHO_CHUNK {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => got += n,
                    }
                }
                metrics.echo_sample(sent.elapsed());
            }
        } else {
            let mut sent = 1u64.min(target);
            while sent < target {
                let take = (chunk.len() as u64).min(target - sent) as usize;
                if stream.write_all(&chunk[..take]).await.is_err() {
                    return;
                }
                sent += take as u64;
            }
            let _ = stream.close().await;
        }
    });
}

fn spawn_yamux_server_stream(mut stream: yamux::Stream, metrics: Arc<Metrics>) {
    use futures::io::{AsyncReadExt, AsyncWriteExt};
    tokio::spawn(async move {
        let mut marker = [0u8; 1];
        if stream.read_exact(&mut marker).await.is_err() {
            return;
        }
        let is_echo = marker[0] == b'E';
        if !is_echo {
            metrics.delivered.fetch_add(1, Ordering::Relaxed);
        }
        let mut buf = vec![0u8; 256 * 1024];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    if is_echo {
                        if stream.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                        let _ = stream.flush().await;
                    } else {
                        metrics.delivered.fetch_add(n as u64, Ordering::Relaxed);
                    }
                },
            }
        }
    });
}

// ---------------------------------------------------------------------------
// hyper / HTTP-2
// ---------------------------------------------------------------------------

/// Client request body: either a zero-filled generator or a channel (echo).
enum ClientBody {
    Gen { remaining: u64, chunk: Bytes },
    Chan(tokio::sync::mpsc::Receiver<Bytes>),
}

impl http_body::Body for ClientBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        match self.get_mut() {
            ClientBody::Gen { remaining, chunk } => {
                if *remaining == 0 {
                    return Poll::Ready(None);
                }
                let take = (*remaining).min(chunk.len() as u64) as usize;
                *remaining -= take as u64;
                let data = chunk.slice(..take);
                Poll::Ready(Some(Ok(http_body::Frame::data(data))))
            },
            ClientBody::Chan(rx) => match rx.poll_recv(cx) {
                Poll::Ready(Some(data)) => Poll::Ready(Some(Ok(http_body::Frame::data(data)))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

/// Server response body for the echo route.
struct ChanBody(tokio::sync::mpsc::Receiver<Bytes>);

impl http_body::Body for ChanBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        match self.get_mut().0.poll_recv(cx) {
            Poll::Ready(Some(data)) => Poll::Ready(Some(Ok(http_body::Frame::data(data)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

async fn run_h2<IO>(
    args: &Args,
    metrics: Arc<Metrics>,
    io_client: IO,
    io_server: IO,
) -> Result<(), String>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    use http_body_util::BodyExt;
    use hyper_util::rt::{TokioExecutor, TokioIo};

    let echo = args.workload == Workload::Echo;
    let target = stream_target(args);
    let chunk = chunk(args);
    let max_streams = args.streams as u32 + 2;

    // Server.
    let server_metrics = metrics.clone();
    let service = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
        let metrics = server_metrics.clone();
        async move {
            match req.uri().path() {
                "/echo" => {
                    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(4);
                    let mut body = req.into_body();
                    tokio::spawn(async move {
                        while let Some(Ok(frame)) = body.frame().await {
                            if let Ok(data) = frame.into_data() {
                                if tx.send(data).await.is_err() {
                                    return;
                                }
                            }
                        }
                    });
                    Ok::<_, std::convert::Infallible>(hyper::Response::new(ChanBody(rx)))
                },
                _ => {
                    let mut body = req.into_body();
                    while let Some(Ok(frame)) = body.frame().await {
                        if let Ok(data) = frame.into_data() {
                            metrics
                                .delivered
                                .fetch_add(data.len() as u64, Ordering::Relaxed);
                        }
                    }
                    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(1);
                    drop(tx);
                    Ok(hyper::Response::new(ChanBody(rx)))
                },
            }
        }
    });

    let mut server_builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
    server_builder
        .initial_stream_window_size(args.h_stream_window_kb * 1024)
        .initial_connection_window_size(args.h_conn_window_kb * 1024)
        .adaptive_window(args.h_adaptive)
        .max_frame_size(args.h_frame_kb * 1024)
        .max_concurrent_streams(max_streams);
    let server_conn = server_builder.serve_connection(TokioIo::new(io_server), service);

    // Client.
    let mut client_builder = hyper::client::conn::http2::Builder::new(TokioExecutor::new());
    client_builder
        .initial_stream_window_size(args.h_stream_window_kb * 1024)
        .initial_connection_window_size(args.h_conn_window_kb * 1024)
        .adaptive_window(args.h_adaptive)
        .max_frame_size(args.h_frame_kb * 1024)
        .initial_max_send_streams(max_streams as usize)
        .max_concurrent_streams(max_streams);

    let (mut send_request, client_conn) = client_builder
        .handshake::<_, ClientBody>(TokioIo::new(io_client))
        .await
        .map_err(|e| e.to_string())?;

    let requests = {
        let metrics = metrics.clone();
        async move {
            // Echo stream first, if requested.
            if echo {
                let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(1);
                let req = hyper::Request::builder()
                    .method("POST")
                    .uri("http://bench/echo")
                    .body(ClientBody::Chan(rx))
                    .unwrap();
                let response = send_request.send_request(req);
                let metrics = metrics.clone();
                tokio::spawn(async move {
                    let Ok(response) = response.await else { return };
                    let mut body = response.into_body();
                    let payload = Bytes::from(vec![0u8; ECHO_CHUNK]);
                    loop {
                        let sent = Instant::now();
                        if tx.send(payload.clone()).await.is_err() {
                            return;
                        }
                        let mut got = 0usize;
                        while got < ECHO_CHUNK {
                            match body.frame().await {
                                Some(Ok(frame)) => {
                                    if let Ok(data) = frame.into_data() {
                                        got += data.len();
                                    }
                                },
                                _ => return,
                            }
                        }
                        metrics.echo_sample(sent.elapsed());
                    }
                });
            }

            for _ in 0..args.streams {
                let req = hyper::Request::builder()
                    .method("POST")
                    .uri("http://bench/data")
                    .body(ClientBody::Gen {
                        remaining: target,
                        chunk: chunk.clone(),
                    })
                    .unwrap();
                let response = send_request.send_request(req);
                tokio::spawn(async move {
                    let _ = response.await;
                });
            }
            // Keep send_request alive for the whole run.
            std::future::pending::<()>().await;
            drop(send_request);
            Ok::<(), String>(())
        }
    };

    tokio::select! {
        r = server_conn => r.map_err(|e| e.to_string()),
        r = client_conn => r.map_err(|e| e.to_string()),
        r = requests => r,
    }
}
