//! The echo end of the measurement.
//!
//! The echo has to be transparent - whatever latency the client measures must
//! be the protocol's, not this end's - and that rules out the obvious loop.
//! `read().await` then `write_all().await` parks the task in whichever call is
//! waiting, so credit arriving while it is parked in the read is not acted on
//! until the read returns, and data arriving while it is parked in the write
//! is not read until the write completes. Each is a stall the protocol did not
//! cause. Over a raw socket it is worse still: a server that stops reading
//! lets its receive buffer fill and closes the peer's window.
//!
//! So the echo is one hand-written future that tries both directions on every
//! wake, holding at most one chunk being written and one read ahead. Any deeper
//! and the echo would itself be a standing queue the marker has to wait in.

use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpListener,
    time::{Instant, Sleep, sleep_until},
};

use transit::{MAX_PAYLOAD, Role, Transit};

use crate::{
    args::{Mode, ProtocolArgs},
    sockopt,
    traced::{self, Traced},
};

/// How often the echo end logs its own window.
const TRACE_INTERVAL: Duration = Duration::from_millis(500);

/// Most the echo will hold between reading and writing back.
const ECHO_BUFFER: usize = 8 * MAX_PAYLOAD as usize;

/// Accepts one connection and echoes it back until the client hangs up.
pub async fn run(addr: SocketAddr, protocol: ProtocolArgs) {
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("Listening on {addr} ({})", listener.local_addr().unwrap());
    let (conn, peer) = listener.accept().await.unwrap();
    drop(listener);
    let info = sockopt::prepare(&conn, protocol.send_buffer).unwrap();
    println!("Accepted a connection from {peer}, {info}");
    println!("CONFIG {}", protocol.describe());

    let echoed = match protocol.mode {
        Mode::Raw => Echo::new(conn).await,
        // The client initiates the probe, so this end only answers.
        Mode::Transit => Echo::new(Transit::new(conn, protocol.config(Role::Responder))).await,
    };
    match echoed {
        Ok(bytes) => println!("Echoed {bytes} bytes"),
        Err(error) => {
            eprintln!("Echo failed: {error}");
            std::process::exit(1);
        },
    }
}

/// The echo loop: everything read, written back in order, both directions
/// attempted on every wake.
struct Echo<IO> {
    io: IO,
    /// Read and not yet written back.
    pending: BytesMut,
    eof: bool,
    echoed: u64,
    started: Instant,
    next_trace: Instant,
    timer: Pin<Box<Sleep>>,
}

impl<IO> Echo<IO> {
    fn new(io: IO) -> Self {
        let now = Instant::now();
        Self {
            io,
            pending: BytesMut::with_capacity(ECHO_BUFFER),
            eof: false,
            echoed: 0,
            started: now,
            next_trace: now + TRACE_INTERVAL,
            timer: Box::pin(sleep_until(now)),
        }
    }
}

impl<IO> Echo<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// Writes back until the connection stops taking bytes. `true` if any moved.
    fn pump_write(&mut self, cx: &mut Context<'_>) -> io::Result<bool> {
        let mut progress = false;
        while !self.pending.is_empty() {
            match Pin::new(&mut self.io).poll_write(cx, &self.pending) {
                Poll::Pending => return Ok(progress),
                Poll::Ready(Ok(0)) => return Err(io::ErrorKind::WriteZero.into()),
                Poll::Ready(Ok(written)) => {
                    self.pending.advance(written);
                    self.echoed += written as u64;
                    progress = true;
                },
                Poll::Ready(Err(error)) => return Err(error),
            }
        }
        Ok(progress)
    }

    /// Reads ahead while there is room. `true` if any bytes arrived.
    fn pump_read(&mut self, cx: &mut Context<'_>) -> io::Result<bool> {
        let mut progress = false;
        while !self.eof && self.pending.len() < ECHO_BUFFER {
            let room = ECHO_BUFFER - self.pending.len();
            self.pending.reserve(room);
            let spare = self.pending.spare_capacity_mut();
            let mut buf = ReadBuf::uninit(&mut spare[..room]);
            match Pin::new(&mut self.io).poll_read(cx, &mut buf) {
                Poll::Pending => return Ok(progress),
                Poll::Ready(Err(error)) => return Err(error),
                Poll::Ready(Ok(())) => {
                    let read = buf.filled().len();
                    if read == 0 {
                        self.eof = true;
                    } else {
                        // SAFETY: `poll_read` filled exactly this many bytes of
                        // the spare capacity it was handed.
                        unsafe { self.pending.set_len(self.pending.len() + read) };
                        progress = true;
                    }
                },
            }
        }
        Ok(progress)
    }
}

impl<IO> Future for Echo<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin + Traced,
{
    type Output = io::Result<u64>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // A write can free room to read into, and a read can give the write
        // something to send. Neither is done until the other has stopped
        // moving - and whichever went pending has left a waker behind.
        loop {
            let wrote = this.pump_write(cx)?;
            let read = this.pump_read(cx)?;
            if !wrote && !read {
                break;
            }
        }
        if this.eof && this.pending.is_empty() {
            std::task::ready!(Pin::new(&mut this.io).poll_flush(cx))?;
            return Poll::Ready(Ok(this.echoed));
        }

        // This end's window is the one limiting the client's sending, so it is
        // the one to look at when throughput is short.
        let now = Instant::now();
        if now >= this.next_trace {
            println!(
                "{}",
                traced::line(&this.io, (now - this.started).as_secs_f64(), 0.0)
            );
            this.next_trace = now + TRACE_INTERVAL;
        }
        this.timer.as_mut().reset(this.next_trace);
        let _ = this.timer.as_mut().poll(cx);
        Poll::Pending
    }
}
