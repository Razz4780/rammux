use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use rammux::{
    config::{RammuxConfig, RammuxRole},
    connection::{RammuxConnection, RammuxProgress},
};
use tokio::io::DuplexStream;

const DATA: Bytes = Bytes::from_static(&[37; 64 * 1024]);

/// This example presents the flow control mechanism implemented in rammux.
///
/// See [PROTOCOL.md](https://github.com/Razz4780/rammux/blob/main/PROTOCOL.md) for more info.
#[tokio::main]
async fn main() {
    let (io_1, io_2) = tokio::io::duplex(4096);
    let client = RammuxConnection::new(RammuxRole::Client, io_1, RammuxConfig::new())
        .expect("config is valid");
    let server = RammuxConnection::new(RammuxRole::Server, io_2, RammuxConfig::new())
        .expect("config is valid");
    tokio::join!(run_client(client), run_server(server));
}

/// Opens two streams and floods both with repeated [`DATA`].
async fn run_client(mut rammux: RammuxConnection<DuplexStream>) {
    let role = rammux.role();

    let mut writers = Vec::new();
    for _ in 0..2 {
        let mut stream = rammux
            .try_start_outbound()
            .expect("connection should not fail")
            .expect("config allows for at least two outbound streams");
        println!("[{role}] Started stream {}", stream.id());
        writers.push(tokio::spawn(async move {
            let mut sent = 0;
            loop {
                match stream.send(DATA.clone()).await {
                    Ok(()) => {
                        sent += DATA.len();
                        println!(
                            "[{role}] Sent and flushed {} bytes in stream {}",
                            DATA.len(),
                            stream.id(),
                        );
                    },
                    // The server never reads the first stream, so its sends stop once it has as much in flight
                    // as flow control allows, while the second one keeps going. The downgrade ends both.
                    Err(error) => {
                        println!(
                            "[{role}] Stream {} ended after {sent} bytes: {error}.",
                            stream.id()
                        );
                        break;
                    },
                }
            }
        }));
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

    for writer in writers {
        writer.await.expect("writer should not panic");
    }
}

/// Accepts two streams. Does not read from the first stream, but reads slowly from the second stream.
/// Second stream remains functional even though the first stream is blocked.
///
/// Downgrades the connection 5 seconds after accepting both streams.
async fn run_server(mut rammux: RammuxConnection<DuplexStream>) {
    let role = rammux.role();

    let mut missing_streams = 2_usize;
    // Kept until the end without being read, so that its sender stays blocked.
    let mut blocked = None;

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
                            match chunk {
                                Ok(chunk) => println!(
                                    "[{role}] Read {} bytes from stream {}.",
                                    chunk.len(),
                                    stream.id()
                                ),
                                // The client never closes the stream: the downgrade ends it.
                                Err(error) => {
                                    println!("[{role}] Stream {} ended: {error}.", stream.id());
                                },
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    });
                    break;
                } else {
                    println!(
                        "[{role}] Accepted new stream {}, not reading the data.",
                        stream.id()
                    );
                    blocked = Some(stream);
                }
            },
            RammuxProgress::Empty => {},
        }
    }

    let mut sleep = Box::pin(tokio::time::sleep(Duration::from_secs(5)));
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

    if let Some(stream) = &blocked {
        println!("[{role}] Stream {} was never read.", stream.id());
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
