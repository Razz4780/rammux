use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use rammux::{
    config::{RammuxConfig, RammuxRole},
    connection::{RammuxConnection, RammuxProgress},
};
use tokio::io::DuplexStream;

const DATA: Bytes = Bytes::from_static(&[37; 64 * 1024]);

/// This example presents the flow control mechanism implemented in Rammux.
///
/// See [PROTOCOL.md](https://github.com/Razz4780/rammux/blob/main/PROTOCOL.md) for more info.
#[tokio::main]
async fn main() {
    let (io_1, io_2) = tokio::io::duplex(4096);
    let client = RammuxConnection::new(RammuxRole::Client, io_1, RammuxConfig::new());
    let server = RammuxConnection::new(RammuxRole::Server, io_2, RammuxConfig::new());
    tokio::join!(run_client(client), run_server(server));
}

/// Opens two streams and floods both with repeated [`DATA`].
async fn run_client(mut rammux: RammuxConnection<DuplexStream>) {
    let role = rammux.role();

    for _ in 0..2 {
        let mut stream = rammux
            .try_start_outbound()
            .expect("connection should not fail")
            .expect("config allows for at least two outbound streams");
        println!("[{role}] Started stream {}", stream.id());
        tokio::spawn(async move {
            loop {
                if stream.send(DATA.clone()).await.is_ok() {
                    println!(
                        "[{role}] Sent and flushed {} bytes in stream {}",
                        DATA.len(),
                        stream.id(),
                    );
                } else {
                    println!("[{role}] Stream {} was closed.", stream.id());
                    break;
                }
            }
        });
    }

    let downgraded = loop {
        let progress = rammux.progress().await.unwrap();
        match progress {
            RammuxProgress::Inbound(..) => panic!("we don't expect any more streams"),
            RammuxProgress::Downgraded(downgraded) => {
                println!("[{role}] Peer started downgrade.");
                break downgraded;
            },
            RammuxProgress::Empty => {},
        }
    };

    downgraded
        .with_shutdown()
        .await
        .expect("connection should not fail");
    println!("[{role}] Downgrade finished.");
}

/// Accepts two streams. Does not read from the first stream, but reads slowly from the second stream.
/// Second stream remains functional even though the first stream is blocked.
///
/// Downgrades the connection 10 seconds after accepting both streams.
async fn run_server(mut rammux: RammuxConnection<DuplexStream>) {
    let role = rammux.role();

    let mut missing_streams = 2_usize;

    loop {
        let progress = rammux.progress().await.expect("connection should not fail");
        match progress {
            RammuxProgress::Downgraded(..) => {
                panic!("we don't expect downgrade from the other side")
            },
            RammuxProgress::Inbound(mut stream) => {
                missing_streams = missing_streams
                    .checked_sub(1)
                    .expect("we don't expect more than two streams");
                if missing_streams == 0 {
                    tokio::spawn(async move {
                        println!(
                            "[{role}] Accepted new stream {}, reading data slowly.",
                            stream.id()
                        );
                        while let Some(chunk) = stream.next().await {
                            println!(
                                "[{role}] Read {} bytes from stream {}.",
                                chunk.len(),
                                stream.id()
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    });
                    break;
                } else {
                    tokio::spawn(async move {
                        println!(
                            "[{role}] Accepted new stream {}, not reading the data.",
                            stream.id()
                        );
                        let _ = stream;
                    });
                }
            },
            RammuxProgress::Empty => {},
        }
    }

    let mut sleep = Box::pin(tokio::time::sleep(Duration::from_secs(10)));
    loop {
        tokio::select! {
            _ = &mut sleep => break,
            progress = rammux.progress() => {
                let progress = progress.expect("connection should not fail");
                match progress {
                    RammuxProgress::Inbound(..) => panic!("we don't expect any more streams"),
                    RammuxProgress::Downgraded(..) => panic!("we don't expect downgrade from the other side"),
                    RammuxProgress::Empty => {},
                }
            }
        }
    }

    println!("[{role}] Starting downgrade.");
    rammux
        .downgrade()
        .expect("was not downgraded")
        .with_shutdown()
        .await
        .expect("connection should not fail");
    println!("[{role}] Downgrade finished.");
}
