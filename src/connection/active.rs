//! A running rammux connection: frames in, frames out.

use std::{
    convert::Infallible,
    io,
    task::{Context, Poll},
};

use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    config::{RammuxConfig, RammuxRole},
    connection::{RammuxStats, pings::Pings, streams::Streams},
    error::ErrorKind,
    stream::RammuxDuplex,
    wire::{
        clock,
        codec::{
            RammuxCodec,
            decoder::{DecodedFrame, InboundFrame},
            encoder::OutFrame,
        },
    },
};

#[cfg(test)]
use crate::wire::header::Pong;

/// A running rammux connection.
///
/// It reads and processes the inbound frames, and writes what the streams and the `PING`/`PONG`
/// exchange have to send. The frames themselves are the business of the [`RammuxCodec`], the streams
/// are the business of [`Streams`], and the exchange is the business of [`Pings`].
pub struct Active<IO> {
    codec: RammuxCodec<IO>,
    streams: Streams,
    pings: Pings,
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
            pings: Pings::new(config.ping_interval, config.ping_bytes.get()),
            streams: Streams::new(role, config.clone()),
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
        let flow = self.streams.flow();
        RammuxStats {
            inbound_streams: count(self.streams.inbound_count()),
            outbound_streams: count(self.streams.outbound_count()),
            srtt: flow.srtt.get(),
            pool_tx_used: flow.tx_pool.used(),
            pool_rx_used: flow.rx_pool.used(),
            turn_queue_len: count(flow.turns.len()),
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
            self.streams.flow_mut().srtt.observe(rtt);
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
            let (pong, ping) = self.pings.take_frame_part(cx);
            let stream = match self.streams.poll_update(cx) {
                Poll::Ready(update) => Some(update.into()),
                Poll::Pending => None,
            };
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
            self.pings.data_sent(frame.data_len());
            self.codec.start_send_unpin(frame)?;
        }
    }
}

/// Converts a count of streams or queue entries for [`RammuxStats`].
fn count(n: usize) -> u32 {
    u32::try_from(n).expect("there are fewer than u32::MAX streams")
}
