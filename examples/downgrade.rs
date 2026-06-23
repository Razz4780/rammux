use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use rammux::{
    config::{RammuxConfig, RammuxRole},
    connection::{RammuxConnection, RammuxProgress},
};
use tokio::io::DuplexStream;

const DATA: Bytes = Bytes::from_static(&[21; 64 * 1024]);

/// This example presents the correct downgrade flow.
///
/// See [PROTOCOL.md](https://github.com/Razz4780/rammux/blob/main/PROTOCOL.md) and [`Downgraded`](rammux::Downgraded)
/// doc for more info.
#[tokio::main]
async fn main() {
    let (io_1, io_2) = tokio::io::duplex(4096);
    let client = RammuxConnection::new(RammuxRole::Client, io_1, RammuxConfig::new());
    let server = RammuxConnection::new(RammuxRole::Server, io_2, RammuxConfig::new());
    tokio::join!(run_client(client), run_server(server));
}

/// Opens an Rammux stream and sends [`DATA`] through it, then downgrades the connection.
async fn run_client(mut rammux: RammuxConnection<DuplexStream>) {
    let role = rammux.role();

    let mut stream = rammux
        .try_start_outbound()
        .expect("connection should not fail")
        .expect("config allows for at least one outbound stream");
    println!("[{role}] Started stream {}.", stream.id());

    let mut writer_task = tokio::spawn(async move {
        stream
            .feed(DATA.clone())
            .await
            .expect("unexpected end of stream");
        // `close` here flushes the sink.
        // When it completes, we can be sure that `RammuxConnection`
        // sends the data, even if we immediately start the downgrade.
        stream.close().await.expect("unexpected end of stream");
        println!("[{role}] Sent and flushed all data.");
    });

    loop {
        let progress = tokio::select! {
            result = &mut writer_task => {
                result.expect("writer should successfully write all data");
                break;
            },
            progress = rammux.progress() => progress,
        };
        match progress.expect("connection should not fail") {
            RammuxProgress::Inbound(..) => panic!("we don't expect more streams"),
            RammuxProgress::Downgraded(..) => {
                panic!("we don't expect downgrade fromt the other side")
            },
            RammuxProgress::Empty => {},
        }
    }

    println!("[{role}] Starting downgrade.");
    rammux
        .downgrade()
        .expect("connection was not downgraded")
        // We won't be using the IO transport after Rammux,
        // so we can instruct `Downgraded` to eagerly close it.
        .with_shutdown()
        .await
        .expect("connection should not fail");
    println!("[{role}] Downgrade finished.");
}

/// Receive a new stream with [`DATA`], then receives downgrade of the connection.
async fn run_server(mut rammux: RammuxConnection<DuplexStream>) {
    let role = rammux.role();

    let mut stream = loop {
        let progress = rammux.progress().await.expect("connection should not fail");
        match progress {
            RammuxProgress::Inbound(stream) => {
                println!("[{role}] Received new stream {}.", stream.id());
                break stream.into_split().1;
            },
            RammuxProgress::Downgraded(..) => {
                panic!("we don't expect downgrade before receving all data")
            },
            RammuxProgress::Empty => {},
        }
    };

    let reader_task = tokio::spawn(async move {
        let mut received = 0;
        while received < DATA.len() {
            let chunk = stream.next().await.expect("unexpected end of stream");
            received += chunk.len();
        }
        println!("[{role}] Received all data.");
    });

    let downgraded = loop {
        let progress = rammux.progress().await.expect("connection should not fail");
        match progress {
            RammuxProgress::Inbound(..) => panic!("we don't expect more streams"),
            RammuxProgress::Empty => {},
            RammuxProgress::Downgraded(downgraded) => {
                println!("[{role}] Peer started downgrade.");
                break downgraded;
            },
        }
    };

    reader_task
        .await
        .expect("reader should successfully read all data");
    downgraded
        // We won't be using the IO transport after Rammux,
        // so we can instruct `Downgraded` to eagerly close it.
        .with_shutdown()
        .await
        .expect("connection should not fail");
    println!("[{role}] Downgrade finished.");
}
