//! The measuring end.
//!
//! It writes filler forever and, once per round, slips a 512 byte marker into
//! the stream and times how long the echo takes to come back.
//!
//! # Why this metric
//!
//! What it measures is head-of-line delay under load, and that is the quantity
//! the window governs: the marker sits behind exactly whatever the protocol
//! allowed in flight, so `(W - BDP) / rate` of standing queue shows up in it
//! one for one. There is no prioritisation to be had here and none is wanted -
//! a protocol with no streams cannot move the marker forward, so the number is
//! attributable to the window and nothing else.
//!
//! The clock starts at the first *attempt* to write the marker, not at the
//! write that succeeded. Time the marker spends waiting for credit is delay the
//! window is responsible for, and starting the clock later would hide exactly
//! the cost of setting the window too small.
//!
//! # Why the numbers are shaped the way they are
//!
//! * **The ramp is excluded.** A connection's own start-up is not what a window
//!   is being judged on, and on a long path it is slow enough to dominate a
//!   short run.
//! * **The window trajectory is logged, from both ends.** Every finding in this
//!   protocol's tuning came out of that log rather than out of the summary, and
//!   an end only ever sees the window it *grants*, which governs the direction
//!   it receives on - never the one limiting its own sending.
//! * **The spread is reported, not just the median.** Latency here has been
//!   bimodal more than once; a median over a few runs lands in one cluster or
//!   the other and reads as a large effect that is not there.
//!
//! # Why this is one hand-written future
//!
//! Reading and writing have to share a single waker. Both drive the same window
//! underneath, and splitting the connection across two tasks lets one half's
//! registration displace the other's - so a connection that went pending for
//! want of credit could sleep through the update that granted it.

use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    time::{Instant, Sleep, sleep_until},
};

use transit::{Role, Transit};

use crate::{
    args::{Mode, ProtocolArgs},
    sockopt,
    traced::{self, Traced},
};

/// Filler the stream is padded with between markers.
const FILLER: u8 = b'0';
/// The marker whose echo is timed.
const MARKER: u8 = b'1';
/// How much marker each round sends.
const MARKER_LEN: usize = 512;
/// Filler written before the first round.
///
/// The first marker should meet a loaded connection, not an idle one: a marker
/// timed against an empty pipe measures the path, not the window.
const WARMUP_BYTES: usize = 64 * 1024;
/// Largest single write, and the read buffer size.
const CHUNK: usize = 64 * 1024;
/// How often the window trajectory is logged.
const TRACE_INTERVAL: Duration = Duration::from_millis(500);

static FILLER_CHUNK: [u8; CHUNK] = [FILLER; CHUNK];
static MARKER_CHUNK: [u8; MARKER_LEN] = [MARKER; MARKER_LEN];

/// Connects, measures for `duration`, and prints the result.
pub async fn run(addr: SocketAddr, protocol: ProtocolArgs, duration: Duration, ramp: Duration) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let conn = loop {
        match TcpStream::connect(addr).await {
            Ok(conn) => break conn,
            Err(error) if Instant::now() >= deadline => {
                panic!("failed to connect to the server: {error}")
            },
            Err(..) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    };
    let info = sockopt::prepare(&conn, protocol.send_buffer).unwrap();
    println!(
        "Connected to {addr} as {}, {info}",
        conn.local_addr().unwrap()
    );
    println!("CONFIG {} duration={duration:?}", protocol.describe());

    let outcome = match protocol.mode {
        Mode::Raw => Bench::new(conn, duration, ramp).await,
        Mode::Transit => {
            Bench::new(
                // The client pays for the probe: one measurement of a path
                // serves both ends, and the reported value is what the server
                // sizes from.
                Transit::new(conn, protocol.config(Role::Initiator)),
                duration,
                ramp,
            )
            .await
        },
    };
    let report = match outcome {
        Ok(report) => report,
        Err(error) => {
            eprintln!("Measurement failed: {error}");
            std::process::exit(1);
        },
    };
    println!("{}", report.line());
}

/// What the run is up to on the write side.
enum Phase {
    /// Loading the connection before the first marker.
    Warmup { remaining: usize },
    /// Writing the marker itself.
    Marker { remaining: usize },
    /// Padding while the marker's echo is outstanding.
    Filler,
}

/// A marker whose echo has not come back yet.
struct Round {
    /// Taken at the *first attempt* to write the marker, not at the write that
    /// succeeded: time the marker spends waiting for credit is exactly the
    /// delay the window is responsible for.
    started: Instant,
    /// Marker bytes still to come back.
    remaining: usize,
}

/// One timed marker round trip.
struct Sample {
    at: Duration,
    latency: Duration,
}

pub struct Report {
    goodput_bps: f64,
    samples: Vec<Duration>,
    clean_rtt: Option<Duration>,
    final_window: Option<u32>,
    probes: u64,
    probe_timeouts: u64,
    probe_interval: Duration,
    stalls: u64,
    /// Read off the socket after the run, once autotuning has settled.
    socket: sockopt::SocketInfo,
}

impl Report {
    /// Nearest-rank percentile over the steady-state samples.
    fn percentile(&self, p: f64) -> Duration {
        if self.samples.is_empty() {
            return Duration::ZERO;
        }
        let rank = ((p / 100.0) * self.samples.len() as f64).ceil() as usize;
        self.samples[rank.saturating_sub(1).min(self.samples.len() - 1)]
    }

    fn line(&self) -> String {
        let ms = |d: Duration| d.as_secs_f64() * 1000.0;
        format!(
            "RESULT goodput_mbps={:.1} samples={} p50_ms={:.1} p90_ms={:.1} p99_ms={:.1} \
             max_ms={:.1} clean_rtt_ms={:.1} final_window={} probes={} probe_timeouts={} \
             probe_interval_ms={:.0} stalls={} {}",
            self.goodput_bps * 8.0 / 1e6,
            self.samples.len(),
            ms(self.percentile(50.0)),
            ms(self.percentile(90.0)),
            ms(self.percentile(99.0)),
            ms(self.samples.last().copied().unwrap_or_default()),
            self.clean_rtt.map_or(0.0, ms),
            self.final_window.unwrap_or(0),
            self.probes,
            self.probe_timeouts,
            ms(self.probe_interval),
            self.stalls,
            self.socket,
        )
    }
}

/// The measurement loop.
///
/// Hand-written rather than two tasks over a split connection: reading and
/// writing have to share one waker, because both of them drive the same window
/// underneath, and a split would let one half's registration displace the
/// other's.
struct Bench<IO> {
    io: IO,
    phase: Phase,
    round: Option<Round>,
    read_buf: Box<[u8]>,

    started: Instant,
    deadline: Instant,
    /// End of the ramp. Everything before it is left out of the numbers.
    steady_from: Instant,
    /// Payload echoed back by the end of the ramp.
    steady_mark: Option<u64>,
    payload_in: u64,

    samples: Vec<Sample>,
    /// Most recent echo, reported by the trace as the loaded round trip.
    last_echo: Option<Duration>,
    next_trace: Instant,
    timer: Pin<Box<Sleep>>,
}

impl<IO> Bench<IO> {
    fn new(io: IO, duration: Duration, ramp: Duration) -> Self {
        let now = Instant::now();
        Self {
            io,
            phase: Phase::Warmup {
                remaining: WARMUP_BYTES,
            },
            round: None,
            read_buf: vec![0; CHUNK].into_boxed_slice(),
            started: now,
            deadline: now + duration,
            steady_from: now + ramp,
            steady_mark: None,
            payload_in: 0,
            samples: Vec::new(),
            last_echo: None,
            next_trace: now + TRACE_INTERVAL,
            timer: Box::pin(sleep_until(now)),
        }
    }

    /// Opens a round. Called immediately before the first write attempt, so
    /// the clock starts where the handoff says it should.
    fn begin_round(&mut self) {
        self.round = Some(Round {
            started: Instant::now(),
            remaining: MARKER_LEN,
        });
        self.phase = Phase::Marker {
            remaining: MARKER_LEN,
        };
    }
}

impl<IO> Bench<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin + Traced,
{
    /// Writes until the connection stops taking bytes. `true` if anything
    /// moved.
    fn pump_write(&mut self, cx: &mut Context<'_>) -> io::Result<bool> {
        let mut progress = false;
        loop {
            let chunk = match &self.phase {
                Phase::Warmup { remaining } => &FILLER_CHUNK[..(*remaining).min(CHUNK)],
                Phase::Marker { remaining } => &MARKER_CHUNK[MARKER_LEN - *remaining..],
                Phase::Filler => &FILLER_CHUNK[..],
            };
            let written = match Pin::new(&mut self.io).poll_write(cx, chunk) {
                Poll::Pending => return Ok(progress),
                Poll::Ready(Ok(0)) => return Err(io::ErrorKind::WriteZero.into()),
                Poll::Ready(Ok(written)) => written,
                Poll::Ready(Err(error)) => return Err(error),
            };
            progress = true;
            match &mut self.phase {
                Phase::Warmup { remaining } => {
                    *remaining -= written;
                    if *remaining == 0 {
                        self.begin_round();
                    }
                },
                Phase::Marker { remaining } => {
                    *remaining -= written;
                    if *remaining == 0 {
                        self.phase = Phase::Filler;
                    }
                },
                Phase::Filler => {},
            }
        }
    }

    /// Reads until the connection stops producing bytes. `true` if anything
    /// moved.
    fn pump_read(&mut self, cx: &mut Context<'_>) -> io::Result<bool> {
        let mut progress = false;
        loop {
            let mut buf = ReadBuf::new(&mut self.read_buf);
            match Pin::new(&mut self.io).poll_read(cx, &mut buf) {
                Poll::Pending => return Ok(progress),
                Poll::Ready(Err(error)) => return Err(error),
                Poll::Ready(Ok(())) => {},
            }
            let read = buf.filled().len();
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the server closed the connection",
                ));
            }
            // The echo returns exactly what was sent, so counting marker bytes
            // is enough to find the round's end: rounds never overlap, because
            // the next marker only goes out once this one is home.
            let markers = buf.filled().iter().filter(|byte| **byte == MARKER).count();
            progress = true;
            self.payload_in += read as u64;

            let Some(round) = self.round.as_mut() else {
                continue;
            };
            round.remaining = round.remaining.saturating_sub(markers);
            if round.remaining > 0 {
                continue;
            }
            let latency = round.started.elapsed();
            self.last_echo = Some(latency);
            self.samples.push(Sample {
                at: self.started.elapsed(),
                latency,
            });
            self.begin_round();
        }
    }

    fn trace(&self, now: Instant) {
        let echo = self
            .last_echo
            .map_or(0.0, |echo| echo.as_secs_f64() * 1000.0);
        println!(
            "{}",
            traced::line(&self.io, (now - self.started).as_secs_f64(), echo)
        );
    }

    fn report(&mut self, now: Instant) -> Report {
        let steady = (now - self.steady_from)
            .as_secs_f64()
            .max(f64::MIN_POSITIVE);
        let echoed = self.payload_in - self.steady_mark.unwrap_or(0);
        let mut samples: Vec<Duration> = self
            .samples
            .iter()
            .filter(|sample| self.started + sample.at >= self.steady_from)
            .map(|sample| sample.latency)
            .collect();
        samples.sort_unstable();
        let stats = self.io.stats();
        Report {
            goodput_bps: echoed as f64 / steady,
            samples,
            clean_rtt: stats.and_then(|stats| stats.clean_rtt),
            final_window: stats.map(|stats| stats.window),
            probes: stats.map_or(0, |stats| stats.probes),
            probe_timeouts: stats.map_or(0, |stats| stats.probe_timeouts),
            probe_interval: stats.map_or(Duration::ZERO, |stats| stats.probe_interval),
            stalls: stats.map_or(0, |stats| stats.stalls),
            socket: traced::socket_info(&self.io),
        }
    }
}

impl<IO> Future for Bench<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin + Traced,
{
    type Output = io::Result<Report>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // Reading can complete a round, which puts a marker back on the write
        // side; writing can free the connection to deliver one. Neither is
        // done until the other has stopped moving.
        loop {
            let wrote = this.pump_write(cx)?;
            let read = this.pump_read(cx)?;
            if !wrote && !read {
                break;
            }
        }

        let now = Instant::now();
        if this.steady_mark.is_none() && now >= this.steady_from {
            this.steady_mark = Some(this.payload_in);
        }
        if now >= this.next_trace {
            this.trace(now);
            this.next_trace = now + TRACE_INTERVAL;
        }
        if now >= this.deadline {
            return Poll::Ready(Ok(this.report(now)));
        }

        // The connection may go quiet, so the clock has to be able to wake us
        // on its own.
        this.timer
            .as_mut()
            .reset(this.next_trace.min(this.deadline));
        let _ = this.timer.as_mut().poll(cx);
        Poll::Pending
    }
}
