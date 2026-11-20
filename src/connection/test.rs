use std::{
    num::{NonZeroU16, NonZeroU32},
    time::Duration,
};

use bytes::Bytes;
use futures::{FutureExt, SinkExt, StreamExt, stream::FuturesUnordered};
use rstest::rstest;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    runtime::Builder,
};

use crate::{
    config::{RammuxConfig, RammuxRole},
    connection::{RammuxConnection, RammuxProgress},
    stream::RammuxDuplex,
};

mod peer_model;
mod sim;
mod stream_end;
mod wire;

const DATA: Bytes = Bytes::from_static(&[b'A'; 64 * 1024]);

#[test]
#[should_panic(expected = "no reactor running")]
fn new_panics_outside_of_a_tokio_runtime() {
    let (io, _peer) = tokio::io::duplex(64);
    let _ = RammuxConnection::new(RammuxRole::Client, io, RammuxConfig::new());
}

#[test]
#[should_panic(expected = "timers are disabled")]
fn new_panics_without_the_time_driver() {
    let runtime = Builder::new_current_thread().build().unwrap();
    let _guard = runtime.enter();
    let (io, _peer) = tokio::io::duplex(64);
    let _ = RammuxConnection::new(RammuxRole::Client, io, RammuxConfig::new());
}

#[rstest]
#[tokio::test]
async fn two_sides(#[values(1, 16, 64)] streams: u32) {
    let (io_1, io_2) = tokio::io::duplex(512);
    let mut config = RammuxConfig::new();
    config.frame_limit = NonZeroU16::new(256).unwrap();
    config.local_recv_window = NonZeroU32::new(1024).unwrap();
    config.remote_recv_window = NonZeroU32::new(1024).unwrap();
    config.ping_interval = Duration::from_millis(25);
    config.max_inbound_streams = 16;
    config.max_outbound_streams = 16;
    let conn_1 = RammuxConnection::new(RammuxRole::Client, io_1, config.clone());
    let conn_2 = RammuxConnection::new(RammuxRole::Server, io_2, config.clone());
    tokio::join!(
        run_rammux(conn_1, streams, 32 * 1024).then(verify_io_clean),
        run_rammux(conn_2, streams, 32 * 1024).then(verify_io_clean),
    );
}

async fn run_rammux(
    mut conn: RammuxConnection<DuplexStream>,
    streams: u32,
    data_in_stream: usize,
) -> DuplexStream {
    let mut remaining_inbound = streams;
    let mut remaining_outbound = streams;
    let mut remaining_finished = streams * 2;
    let mut futs = FuturesUnordered::new();

    let downgraded = loop {
        if remaining_outbound > 0
            && let Some(stream) = conn.try_start_outbound().unwrap()
        {
            remaining_outbound -= 1;
            futs.push(run_stream(stream, data_in_stream));
            continue;
        }
        let progress = tokio::select! {
            Some(..) = futs.next() => {
                remaining_finished -= 1;
                if remaining_finished == 0 {
                    break conn.downgrade().unwrap();
                } else {
                    continue;
                }
            }
            progress = conn.progress() => progress,
        };
        match progress.unwrap() {
            RammuxProgress::Empty => {},
            RammuxProgress::Downgraded(downgraded) => break downgraded,
            RammuxProgress::Inbound(stream) => {
                remaining_inbound = remaining_inbound.checked_sub(1).unwrap();
                futs.push(run_stream(stream, data_in_stream));
            },
        }
    };

    assert_eq!(remaining_outbound, 0);
    assert_eq!(remaining_inbound, 0);
    while futs.next().await.is_some() {}

    downgraded.await.unwrap()
}

async fn run_stream(stream: RammuxDuplex, data: usize) {
    let (mut sink, mut stream) = stream.into_split();
    tokio::join!(
        async {
            let mut remaining = data;
            while remaining > 0 {
                let chunk = if DATA.len() <= remaining {
                    DATA.clone()
                } else {
                    DATA.clone().split_to(remaining)
                };
                remaining -= chunk.len();
                sink.feed(chunk).await.unwrap();
            }
            sink.close().await.unwrap();
        },
        async {
            let mut read = 0;
            while let Some(chunk) = stream.next().await {
                read += chunk.unwrap().len();
            }
            assert_eq!(read, data);
        },
    );
}

async fn verify_io_clean(io: DuplexStream) {
    let (mut read, mut write) = tokio::io::split(io);
    tokio::join!(
        async {
            let mut buf = Vec::new();
            read.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, b"B");
        },
        async {
            write.write_all(b"B").await.unwrap();
            write.shutdown().await.unwrap();
        },
    );
}
