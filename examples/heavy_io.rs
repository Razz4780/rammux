use std::{
    fmt, io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Instant,
};

use bytes::Bytes;
use clap::{Parser, Subcommand, ValueEnum};
use futures::{SinkExt, StreamExt, task::AtomicWaker};
use rammux::{
    config::{RammuxConfig, RammuxRole},
    connection::{RammuxConnection, RammuxProgress},
    stream::RammuxDuplex,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf},
    net::{TcpListener, TcpStream, UnixStream},
};

#[derive(Parser)]
struct Args {
    /// Kind of transport to use for the IO.
    #[arg(long, value_enum, default_value_t = TransportKind::Memory)]
    transport: TransportKind,
    /// Size for the optional read buffer to apply on top of the IO transport.
    ///
    /// rammux decoder makes a lot of small reads,
    /// so using a read buffer might improve performance.
    #[arg(long)]
    read_buffer: Option<usize>,
    /// Size of data chunk used when writing to a rammux stream or the raw connection.
    #[arg(long, default_value_t = 64 * 1024)]
    chunk_size: usize,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run two sides of a rammux connection.
    Rammux {
        /// How many streams should be started within the connection.
        ///
        /// Each side will start half of the streams.
        #[arg(long, default_value_t = 64)]
        num_streams: usize,
        /// How many bytes should each side send within a single stream.
        #[arg(long, default_value_t = 4 * 1024 * 1024)]
        num_bytes_in_stream: usize,
    },
    /// Run two sides of a raw connection.
    Raw {
        /// How many bytes should each side send within the connection.
        #[arg(long, default_value_t = 64 * 4 * 1024 * 1024)]
        num_bytes: usize,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum TransportKind {
    /// TCP connection on localhost.
    Tcp,
    /// UNIX socket connection.
    Unix,
    /// In-memory duplex pipe with 64kb capacity.
    Memory,
}

/// This example presents rammux implementation performance.
///
/// See `--help` output for more info.
#[tokio::main]
async fn main() {
    let args = Args::parse();
    match (args.transport, args.read_buffer) {
        (TransportKind::Tcp, None) => {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let io_1 = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let io_2 = listener.accept().await.unwrap().0;
            handle_command_with_io(Transport::from(io_1), Transport::from(io_2), args).await;
        },
        (TransportKind::Tcp, Some(cap)) => {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let io_1 = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let io_1 = BufReader::with_capacity(cap, Transport::from(io_1));
            let io_2 = listener.accept().await.unwrap().0;
            let io_2 = BufReader::with_capacity(cap, Transport::from(io_2));
            handle_command_with_io(io_1, io_2, args).await;
        },
        (TransportKind::Unix, None) => {
            let (io_1, io_2) = UnixStream::pair().unwrap();
            handle_command_with_io(Transport::from(io_1), Transport::from(io_2), args).await;
        },
        (TransportKind::Unix, Some(cap)) => {
            let (io_1, io_2) = UnixStream::pair().unwrap();
            let io_1 = BufReader::with_capacity(cap, Transport::from(io_1));
            let io_2 = BufReader::with_capacity(cap, Transport::from(io_2));
            handle_command_with_io(io_1, io_2, args).await;
        },
        (TransportKind::Memory, None) => {
            let (io_1, io_2) = tokio::io::duplex(64 * 1024);
            handle_command_with_io(Transport::from(io_1), Transport::from(io_2), args).await;
        },
        (TransportKind::Memory, Some(cap)) => {
            let (io_1, io_2) = tokio::io::duplex(64 * 1024);
            let io_1 = BufReader::with_capacity(cap, Transport::from(io_1));
            let io_2 = BufReader::with_capacity(cap, Transport::from(io_2));
            handle_command_with_io(io_1, io_2, args).await;
        },
    }
}

async fn handle_command_with_io<IO>(io_1: IO, io_2: IO, args: Args)
where
    IO: AsyncRead + AsyncWrite + Unpin,
    TransportStats: From<IO>,
{
    let chunk = Bytes::from(vec![0_u8; args.chunk_size]);

    match args.command {
        Command::Rammux {
            num_streams,
            num_bytes_in_stream,
        } => {
            let client = RammuxConnection::new(RammuxRole::Client, io_1, RammuxConfig::new());
            let server = RammuxConnection::new(RammuxRole::Server, io_2, RammuxConfig::new());

            let started_at = Instant::now();
            let (client, server) = tokio::join!(
                handle_rammux_conn(client, num_streams, num_bytes_in_stream, chunk.clone()),
                handle_rammux_conn(server, num_streams, num_bytes_in_stream, chunk),
            );
            let elapsed = started_at.elapsed();
            println!("Finished after {elapsed:?}");
            println!("Client transport: {:?}", TransportStats::from(client));
            println!("Server transport: {:?}", TransportStats::from(server));
        },
        Command::Raw { num_bytes } => {
            let started_at = Instant::now();
            let (client, server) = tokio::join!(
                handle_raw_stream(io_1, num_bytes, chunk.clone()),
                handle_raw_stream(io_2, num_bytes, chunk),
            );
            let elapsed = started_at.elapsed();
            println!("Finished after {elapsed:?}");
            println!("Client transport: {:?}", TransportStats::from(client));
            println!("Server transport: {:?}", TransportStats::from(server));
        },
    }
}

async fn handle_rammux_conn<IO>(
    mut conn: RammuxConnection<IO>,
    streams: usize,
    stream_data: usize,
    chunk: Bytes,
) -> IO
where
    IO: AsyncWrite + AsyncRead + Unpin,
{
    let mut inbound = streams / 2;
    let mut outbound = streams - inbound;
    if conn.role() == RammuxRole::Client {
        std::mem::swap(&mut inbound, &mut outbound);
    }

    let barrier = Arc::new(Barrier::new(streams));

    let downgraded = loop {
        if outbound > 0
            && let Some(stream) = conn
                .try_start_outbound()
                .expect("connection should not fail")
        {
            tokio::spawn(handle_rammux_stream(
                stream,
                stream_data,
                chunk.clone(),
                barrier.clone(),
            ));
            outbound -= 1;
        }

        let progress = tokio::select! {
            _ = barrier.wait() => {
                break None;
            }
            progress = conn.progress() => progress.expect("connection should not fail"),
        };
        match progress {
            RammuxProgress::Inbound(stream) => {
                inbound = inbound
                    .checked_sub(1)
                    .expect("got more inbound streams than expected");
                tokio::spawn(handle_rammux_stream(
                    stream,
                    stream_data,
                    chunk.clone(),
                    barrier.clone(),
                ));
            },
            RammuxProgress::Downgraded(downgraded) => break Some(downgraded),
            RammuxProgress::Empty => {},
        }
    };

    barrier.wait().await;

    let downgraded = downgraded.unwrap_or_else(|| conn.downgrade().expect("was not downgraded"));
    downgraded
        .with_shutdown()
        .await
        .expect("connection should not fail")
}

async fn handle_rammux_stream(
    stream: RammuxDuplex,
    target: usize,
    chunk: Bytes,
    barrier: Arc<Barrier>,
) {
    let (mut sink, mut stream) = stream.into_split();

    tokio::join!(
        async {
            let mut remaining = target;
            while remaining > 0 {
                let chunk = if chunk.len() > remaining {
                    chunk.clone().split_to(remaining)
                } else {
                    chunk.clone()
                };
                remaining -= chunk.len();
                sink.feed(chunk).await.expect("unexpected stream end");
            }
            sink.close().await.expect("unexpected stream end");
        },
        async {
            let mut remaining = target;
            while remaining > 0 {
                let chunk = stream.next().await.expect("unexpected stream end");
                remaining = remaining
                    .checked_sub(chunk.len())
                    .expect("got more bytes than expected");
            }
        },
    );

    barrier.finished();
}

async fn handle_raw_stream<IO>(stream: IO, target: usize, chunk: Bytes) -> IO
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let (mut reader, mut writer) = tokio::io::split(stream);

    tokio::join!(
        async {
            let mut remaining = target;
            while remaining > 0 {
                let chunk = if chunk.len() > remaining {
                    chunk.clone().split_to(remaining)
                } else {
                    chunk.clone()
                };
                let written = writer.write(&chunk).await.expect("unexpected stream end");
                remaining -= written;
            }
            writer.shutdown().await.expect("unexpected stream end");
        },
        async {
            let mut remaining = target;
            let mut buffer = Vec::with_capacity(chunk.len());
            while remaining > 0 {
                let read = reader
                    .read_buf(&mut buffer)
                    .await
                    .expect("unexpected stream end");
                if read == 0 {
                    panic!("unexpected stream end");
                }
                remaining -= read;
                buffer.clear();
            }
        },
    );

    reader.unsplit(writer)
}

struct Barrier {
    remaining: AtomicUsize,
    waker: AtomicWaker,
}

impl Barrier {
    fn new(limit: usize) -> Self {
        Self {
            remaining: AtomicUsize::new(limit),
            waker: Default::default(),
        }
    }

    fn finished(&self) {
        if self.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.waker.wake();
        }
    }

    async fn wait(&self) {
        futures::future::poll_fn(|cx| {
            self.waker.register(cx.waker());
            if self.remaining.load(Ordering::Relaxed) == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

struct Transport<IO> {
    io: IO,
    stats: TransportStats,
}

impl<IO> From<IO> for Transport<IO> {
    fn from(value: IO) -> Self {
        Self {
            io: value,
            stats: Default::default(),
        }
    }
}

impl<IO> AsyncWrite for Transport<IO>
where
    IO: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let bytes = std::task::ready!(Pin::new(&mut this.io).poll_write(cx, buf))?;
        this.stats.writes += 1;
        this.stats.bytes_written += bytes;
        Poll::Ready(Ok(bytes))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.io).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let bytes = std::task::ready!(Pin::new(&mut this.io).poll_write_vectored(cx, bufs))?;
        this.stats.writes += 1;
        this.stats.bytes_written += bytes;
        Poll::Ready(Ok(bytes))
    }
}

impl<IO> AsyncRead for Transport<IO>
where
    IO: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let filled_prev = buf.filled().len();
        std::task::ready!(Pin::new(&mut this.io).poll_read(cx, buf))?;
        this.stats.reads += 1;
        this.stats.bytes_read += buf.filled().len() - filled_prev;
        Poll::Ready(Ok(()))
    }
}

#[derive(Default)]
struct TransportStats {
    writes: usize,
    reads: usize,
    bytes_written: usize,
    bytes_read: usize,
}

impl fmt::Debug for TransportStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransportStats")
            .field("writes", &self.writes)
            .field("bytes_written", &self.bytes_written)
            .field(
                "avg_write_size",
                &self
                    .bytes_written
                    .checked_div(self.writes)
                    .unwrap_or_default(),
            )
            .field("reads", &self.reads)
            .field("bytes_read", &self.bytes_read)
            .field(
                "avg_read_size",
                &self.bytes_read.checked_div(self.reads).unwrap_or_default(),
            )
            .finish()
    }
}

impl<IO> From<Transport<IO>> for TransportStats {
    fn from(value: Transport<IO>) -> Self {
        value.stats
    }
}

impl<IO> From<BufReader<Transport<IO>>> for TransportStats
where
    IO: AsyncRead + Unpin,
{
    fn from(value: BufReader<Transport<IO>>) -> Self {
        value.into_inner().into()
    }
}
