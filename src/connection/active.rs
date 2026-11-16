//! A running rammux connection: frames in, frames out.

use std::{
    convert::Infallible,
    io,
    task::{Context, Poll},
};

use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::Instant,
};

use crate::{
    config::{RammuxConfig, RammuxRole},
    connection::{RammuxStats, pings::Pings, streams::Streams},
    error::ErrorKind,
    stream::{RammuxDuplex, updates::StreamUpdate},
    wire::{
        clock,
        codec::{
            RammuxCodec,
            decoder::{DecodedFrame, InboundFrame},
            encoder::{OutFrame, OutStream},
        },
    },
};

#[cfg(test)]
use crate::wire::header::Pong;

/// Credit frames in a row, after which a ready data frame goes first. See [`Active::poll_send`].
pub(super) const CREDIT_BURST: usize = 32;

/// A running rammux connection.
///
/// It reads and processes the inbound frames, and writes what the streams and the `PING`/`PONG`
/// exchange have to send. The frames themselves are the business of the [`RammuxCodec`], the streams
/// are the business of [`Streams`], and the exchange is the business of [`Pings`].
pub struct Active<IO> {
    codec: RammuxCodec<IO>,
    streams: Streams,
    pings: Pings,
    /// Credit frames written in a row, since the last data frame or the last time data went first.
    credit_run: usize,
}

/// What an inbound frame did.
pub enum Received {
    /// The frame was processed, and it started the given stream, if any.
    Frame(Option<RammuxDuplex>),
    /// The peer sent `TERM`.
    Term,
}

impl<IO> Active<IO> {
    pub fn new(io: IO, role: RammuxRole, config: &RammuxConfig) -> Self {
        Self {
            codec: RammuxCodec::new(io, config.frame_limit, config.inbound_frame_timeout),
            pings: Pings::new(config.ping_interval),
            streams: Streams::new(role, config.clone()),
            credit_run: 0,
        }
    }

    /// Starts a new stream, unless the configured limit or the IDs are exhausted.
    pub fn open_outbound(&mut self) -> Option<RammuxDuplex> {
        self.streams.open_outbound()
    }

    /// Returns the transport, and drops the streams.
    pub fn into_codec(self) -> RammuxCodec<IO> {
        self.codec
    }

    pub fn stats(&self) -> RammuxStats {
        let tx = self.streams.tx_flow();
        RammuxStats {
            inbound_streams: stream_count(self.streams.inbound_count()),
            outbound_streams: stream_count(self.streams.outbound_count()),
            srtt: self.streams.srtt().get(),
            pool_tx_used: tx.tx_pool.used(),
            pool_rx_used: self.streams.rx_flow().pool.used(),
            turn_queue_len: count(tx.turns.len()),
            advance_held: tx.advance.used(),
        }
    }

    /// Returns the `PONG` we owe.
    #[cfg(test)]
    pub fn owed_pong(&self) -> Option<Pong> {
        self.pings.owed_pong()
    }
}

impl<IO> Active<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// Reads and processes the next inbound frame.
    ///
    /// The parts of the frame are processed in the order of the protocol:
    /// the connection part (`PONG`, `PING`), then the stream part.
    pub fn poll_receive(&mut self, cx: &mut Context<'_>) -> Poll<Result<Received, ErrorKind>> {
        let frame = std::task::ready!(self.codec.poll_next_unpin(cx))
            .ok_or(io::Error::from(io::ErrorKind::UnexpectedEof))??;
        let InboundFrame {
            header,
            data,
            header_at,
        } = match frame {
            DecodedFrame::Frame(frame) => frame,
            DecodedFrame::Terminate => return Poll::Ready(Ok(Received::Term)),
        };

        // A `PONG` gives the round trip time on our clock. Its age does not matter:
        // it can wait in a queue for as long as the queue is.
        if let Some(pong) = header.pong {
            let now = self.codec.clock().micros(header_at);
            let rtt = clock::elapsed(now, pong.t_echo);
            self.streams.observe_rtt(rtt);
        }
        if let Some(t_ping) = header.ping {
            self.pings.received_ping(t_ping);
        }
        let new_stream = match header.stream {
            Some(part) => self.streams.receive(part, data)?,
            None => None,
        };
        Poll::Ready(Ok(Received::Frame(new_stream)))
    }

    /// Writes frames until there is nothing to send, or the transport takes no more.
    pub fn poll_send(&mut self, cx: &mut Context<'_>) -> Poll<Result<Infallible, ErrorKind>> {
        loop {
            std::task::ready!(self.codec.poll_ready_unpin(cx))?;

            // Compose the next frame. The order of importance is:
            // 1. `PONG`, the peer measures its round trip with it,
            // 2. `PING`, same,
            // 3. stream control,
            // 4. stream data.
            // What goes first cannot wait behind what goes later,
            // but it rides along if the later frame is going out anyway.
            // A window update must not wait behind the data of other streams, because the peer's sender
            // waits for it. Stream control never starves stream data all the same, for two reasons:
            // 1. Credit is bounded by what arrived. A credit frame returns at least `T_c` bytes that the
            //    application read since the last one (or is the one `FIN_READ` of a stream), and the
            //    connection reads inbound frames in bounded batches between its writes.
            // 2. After `CREDIT_BURST` credit frames in a row, data goes first if a data frame is ready.
            // The stream part is composed first, because whether the frame carries `DATA` or `WINDOW`
            // decides whether a `PING` may ride on it.
            let stream = match self.poll_stream_part(cx) {
                Poll::Ready(update) => Some(OutStream::from(update)),
                Poll::Pending => None,
            };
            let carries_flow = stream
                .as_ref()
                .is_some_and(|stream| stream.credit > 0 || !stream.data.is_empty());
            let srtt = self.streams.srtt().get();
            let (pong, ping) = self
                .pings
                .take_frame_part(cx, Instant::now(), srtt, carries_flow);
            let frame = OutFrame {
                ping,
                pong,
                stream,
                term: false,
            };
            if frame.is_empty() {
                let _ = self.codec.poll_flush_unpin(cx)?;
                return Poll::Pending;
            }
            self.codec.start_send_unpin(frame)?;
        }
    }

    /// Polls for the stream part of the next frame: stream control before stream data,
    /// except after [`CREDIT_BURST`] credit frames in a row (see [`Self::poll_send`]).
    fn poll_stream_part(&mut self, cx: &mut Context<'_>) -> Poll<StreamUpdate> {
        let data_first = self.credit_run >= CREDIT_BURST;
        if data_first {
            self.credit_run = 0;
            if let Poll::Ready(update) = self.streams.poll_data(cx) {
                return Poll::Ready(update);
            }
        }
        if let Poll::Ready(update) = self.streams.poll_credit(cx) {
            self.credit_run += 1;
            return Poll::Ready(update);
        }
        if !data_first && let Poll::Ready(update) = self.streams.poll_data(cx) {
            self.credit_run = 0;
            return Poll::Ready(update);
        }
        Poll::Pending
    }
}

/// Converts a count of queue entries for [`RammuxStats`].
fn count(n: usize) -> u32 {
    u32::try_from(n).expect("there are fewer than u32::MAX queue entries")
}

/// Converts a count of streams for [`RammuxStats`].
fn stream_count(n: usize) -> u16 {
    u16::try_from(n).expect("there are at most `MAX_STREAMS_PER_SIDE` streams per side")
}
