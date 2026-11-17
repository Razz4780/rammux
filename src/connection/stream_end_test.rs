//! How streams end, as the applications on the two sides of a connection see it.

use std::{
    future::Future,
    io,
    num::{NonZeroU16, NonZeroU32},
    pin::pin,
    time::Duration,
};

use bytes::Bytes;
use futures::{FutureExt, SinkExt, Stream, StreamExt};
use tokio::{
    io::DuplexStream,
    sync::mpsc,
    task::{JoinHandle, unconstrained},
};

use crate::{
    config::{RammuxConfig, RammuxRole},
    connection::{RammuxConnection, RammuxProgress},
    stream::{RammuxDuplex, StreamError},
};

/// How many times a future is polled, with the connections driven in between, before a test gives up on it.
const POLLS: usize = 1000;

/// Small windows and no pool, so that a sender quickly waits for its reader.
fn config() -> RammuxConfig {
    let mut config = RammuxConfig::new();
    config.frame_limit = NonZeroU16::new(256).unwrap();
    config.local_recv_window = NonZeroU32::new(1024).unwrap();
    config.remote_recv_window = NonZeroU32::new(1024).unwrap();
    config.local_recv_pool = 0;
    config.remote_recv_pool = 0;
    config
}

/// One side of the connection, and the streams the peer started.
struct Side {
    conn: RammuxConnection<DuplexStream>,
    inbound: Vec<RammuxDuplex>,
}

impl Side {
    fn new(role: RammuxRole, io: DuplexStream) -> Self {
        Self {
            conn: RammuxConnection::new(role, io, config()).unwrap(),
            inbound: Vec::new(),
        }
    }

    /// Lets the connection do everything it can do now. Returns whether it received anything.
    fn run(&mut self) -> bool {
        let mut received = false;
        while let Some(progress) = unconstrained(self.conn.progress()).now_or_never() {
            received = true;
            match progress.unwrap() {
                RammuxProgress::Inbound(stream) => self.inbound.push(stream),
                RammuxProgress::Empty => {},
                RammuxProgress::Downgraded(..) => panic!("unexpected downgrade"),
            }
        }
        received
    }
}

/// Two connected sides: A is the client, B is the server.
struct Pair {
    a: Side,
    b: Side,
}

fn pair() -> Pair {
    let (io_a, io_b) = tokio::io::duplex(1 << 20);
    Pair {
        a: Side::new(RammuxRole::Client, io_a),
        b: Side::new(RammuxRole::Server, io_b),
    }
}

impl Pair {
    /// Lets both connections do everything they can do now.
    fn run(&mut self) {
        // A round in which neither side receives anything leaves nothing in transit:
        // what A sends, B receives in the same round, and what B sends, A receives.
        while self.a.run() | self.b.run() | self.a.run() {}
    }

    /// Drives both connections until the future completes.
    fn drive<F: Future>(&mut self, fut: F) -> F::Output {
        let mut fut = pin!(fut);
        for _ in 0..POLLS {
            if let Some(out) = fut.as_mut().now_or_never() {
                return out;
            }
            self.run();
        }
        panic!("the future does not complete");
    }

    /// Starts a stream on A. Its first write tells B about it.
    ///
    /// Returns the ends of the stream on A and on B.
    fn open(&mut self, first: &'static [u8]) -> (RammuxDuplex, RammuxDuplex) {
        let mut a = self.a.conn.try_start_outbound().unwrap().unwrap();
        self.drive(a.send(Bytes::from_static(first))).unwrap();
        self.run();
        let b = self.b.inbound.pop().expect("B learned about the stream");
        (a, b)
    }
}

/// Reads the stream to its end. Returns the data, and the error that ended the stream, if one did.
///
/// Checks that nothing follows the error. Calls `drive` while there is nothing to read.
fn read_to_end<S>(stream: &mut S, mut drive: impl FnMut()) -> (Vec<u8>, Option<StreamError>)
where
    S: Stream<Item = Result<Bytes, StreamError>> + Unpin,
{
    let mut data = Vec::new();
    for _ in 0..POLLS {
        match stream.next().now_or_never() {
            Some(Some(Ok(chunk))) => data.extend_from_slice(&chunk),
            Some(Some(Err(error))) => {
                let next = stream.next().now_or_never().expect("the stream is done");
                assert!(next.is_none(), "{next:?} after {error}");
                return (data, Some(error));
            },
            Some(None) => return (data, None),
            None => drive(),
        }
    }
    panic!("the stream does not end");
}

/// For streams of a connection that is gone: nothing can make progress any more.
fn nothing() {}

/// Bytes that tell where in the chunk they are.
fn chunk(len: usize) -> Bytes {
    (0..len)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect::<Vec<_>>()
        .into()
}

const LATE: Bytes = Bytes::from_static(b"late");

#[tokio::test(start_paused = true)]
async fn close_is_a_clean_end() {
    let mut pair = pair();
    let (a, mut b) = pair.open(b"abc");
    let (mut sink, _reader) = a.into_split();
    pair.drive(sink.close()).unwrap();
    assert_eq!(read_to_end(&mut b, || pair.run()), (b"abc".to_vec(), None));
}

#[tokio::test(start_paused = true)]
async fn a_dropped_sink_aborts() {
    let mut pair = pair();
    let (a, mut b) = pair.open(b"abc");
    let (sink, _reader) = a.into_split();
    drop(sink);
    assert_eq!(
        read_to_end(&mut b, || pair.run()),
        (b"abc".to_vec(), Some(StreamError::PeerAborted))
    );
}

#[tokio::test(start_paused = true)]
async fn a_dropped_duplex_aborts() {
    let mut pair = pair();
    let (a, mut b) = pair.open(b"abc");
    drop(a);
    assert_eq!(
        read_to_end(&mut b, || pair.run()),
        (b"abc".to_vec(), Some(StreamError::PeerAborted))
    );
}

#[tokio::test(start_paused = true)]
async fn a_dropped_sink_discards_what_the_connection_did_not_take() {
    let mut pair = pair();
    let (a, mut b) = pair.open(b"abc");
    assert_eq!(
        pair.drive(b.next()).unwrap().unwrap(),
        Bytes::from_static(b"abc")
    );

    // B does not read, so the connection of A takes only what the window lets it send.
    let (mut sink, _reader) = a.into_split();
    let chunk = chunk(4096);
    pair.drive(sink.feed(chunk.clone())).unwrap();
    pair.run();
    drop(sink);

    let (data, error) = read_to_end(&mut b, || pair.run());
    assert!(!data.is_empty());
    assert!(data.len() < chunk.len(), "{}", data.len());
    assert_eq!(data, chunk[..data.len()]);
    assert_eq!(error, Some(StreamError::PeerAborted));
}

#[tokio::test(start_paused = true)]
async fn a_sink_dropped_during_close_closes_cleanly() {
    let mut pair = pair();
    let (a, mut b) = pair.open(b"abc");
    let (mut sink, _reader) = a.into_split();
    // More than the window: the rest goes after the drop, as B reads.
    let chunk = chunk(4096);
    sink.feed(chunk.clone()).now_or_never().unwrap().unwrap();
    assert!(
        sink.close().now_or_never().is_none(),
        "the connection did not take the data yet"
    );
    drop(sink);

    let (data, error) = read_to_end(&mut b, || pair.run());
    assert_eq!(data, [b"abc".as_slice(), &chunk].concat());
    assert_eq!(error, None);
}

#[tokio::test(start_paused = true)]
async fn the_writer_learns_that_the_peer_stopped_reading() {
    let mut pair = pair();
    let (a, b) = pair.open(b"abc");
    let (_b_sink, b_reader) = b.into_split();
    drop(b_reader);
    pair.run();

    let (mut sink, _reader) = a.into_split();
    let error = Err(StreamError::PeerStoppedReading);
    for _ in 0..2 {
        assert_eq!(pair.drive(sink.send(LATE)), error);
        assert_eq!(pair.drive(sink.flush()), error);
        assert_eq!(pair.drive(sink.close()), error);
    }
}

#[tokio::test(start_paused = true)]
async fn a_closed_sink() {
    let mut pair = pair();
    let (mut sink, _reader) = pair
        .a
        .conn
        .try_start_outbound()
        .unwrap()
        .unwrap()
        .into_split();
    pair.drive(sink.close()).unwrap();
    for _ in 0..2 {
        assert_eq!(pair.drive(sink.send(LATE)), Err(StreamError::SinkClosed));
        assert_eq!(pair.drive(sink.flush()), Ok(()));
        assert_eq!(pair.drive(sink.close()), Ok(()));
    }
}

#[tokio::test(start_paused = true)]
async fn start_send_without_readiness() {
    let mut pair = pair();
    let (mut sink, _reader) = pair
        .a
        .conn
        .try_start_outbound()
        .unwrap()
        .unwrap()
        .into_split();
    sink.start_send_unpin(Bytes::from_static(b"a")).unwrap();
    // The connection did not take the first write yet.
    assert_eq!(
        sink.start_send_unpin(Bytes::from_static(b"b")),
        Err(StreamError::SinkNotReady)
    );

    pair.drive(futures::future::poll_fn(|cx| sink.poll_ready_unpin(cx)))
        .unwrap();
    sink.start_send_unpin(Bytes::from_static(b"b")).unwrap();
    pair.drive(sink.close()).unwrap();
    pair.run();
    let mut b = pair.b.inbound.pop().unwrap();
    assert_eq!(read_to_end(&mut b, || pair.run()), (b"ab".to_vec(), None));
}

#[tokio::test(start_paused = true)]
async fn a_failed_connection_ends_its_streams() {
    let mut pair = pair();
    let (a, mut b) = pair.open(b"abc");
    pair.drive(b.send(Bytes::from_static(b"xyz"))).unwrap();
    pair.run();

    // The transport of B goes away, while A has not read what B sent.
    let Pair {
        a: mut side_a,
        b: side_b,
    } = pair;
    drop(side_b);
    let error = side_a
        .conn
        .progress()
        .now_or_never()
        .expect("the connection sees the end of the transport")
        .unwrap_err();
    assert!(error.is_io(), "{error}");

    let (mut sink, mut reader) = a.into_split();
    assert_eq!(
        read_to_end(&mut reader, nothing),
        (b"xyz".to_vec(), Some(StreamError::ConnectionFailed))
    );
    let error = Err(StreamError::ConnectionFailed);
    assert_eq!(sink.send(LATE).now_or_never().unwrap(), error);
    assert_eq!(sink.flush().now_or_never().unwrap(), error);
    assert_eq!(sink.close().now_or_never().unwrap(), error);
}

#[tokio::test(start_paused = true)]
async fn a_downgrade_ends_the_open_streams() {
    let mut pair = pair();
    let (a, mut b) = pair.open(b"abc");
    assert_eq!(
        pair.drive(b.next()).unwrap().unwrap(),
        Bytes::from_static(b"abc")
    );
    let Pair {
        a: side_a,
        b: mut side_b,
    } = pair;

    // A starts the downgrade.
    let mut downgraded = side_a.conn.downgrade().unwrap();
    let (mut a_sink, mut a_reader) = a.into_split();
    assert_eq!(
        read_to_end(&mut a_reader, nothing),
        (Vec::new(), Some(StreamError::ConnectionDowngraded))
    );
    let error = Err(StreamError::ConnectionDowngraded);
    assert_eq!(a_sink.send(LATE).now_or_never().unwrap(), error);
    assert_eq!(a_sink.flush().now_or_never().unwrap(), error);
    assert_eq!(a_sink.close().now_or_never().unwrap(), error);

    // B gets the `TERM` of A.
    assert!(unconstrained(&mut downgraded).now_or_never().is_none());
    let _downgraded_b = loop {
        match side_b.conn.progress().now_or_never() {
            Some(Ok(RammuxProgress::Downgraded(downgraded))) => break downgraded,
            Some(Ok(RammuxProgress::Empty)) => {},
            other => panic!("{other:?}"),
        }
    };
    let (mut b_sink, mut b_reader) = b.into_split();
    assert_eq!(
        read_to_end(&mut b_reader, nothing),
        (Vec::new(), Some(StreamError::ConnectionDowngraded))
    );
    assert_eq!(b_sink.send(LATE).now_or_never().unwrap(), error);
    assert_eq!(b_sink.flush().now_or_never().unwrap(), error);
    assert_eq!(b_sink.close().now_or_never().unwrap(), error);
}

#[tokio::test(start_paused = true)]
async fn a_dropped_connection_ends_its_streams() {
    let mut pair = pair();
    let (a, _b) = pair.open(b"abc");
    // A sink that completed its close stays closed.
    let (mut closed, _closed_reader) = pair
        .a
        .conn
        .try_start_outbound()
        .unwrap()
        .unwrap()
        .into_split();
    pair.drive(closed.close()).unwrap();

    let Pair {
        a: side_a,
        b: _side_b,
    } = pair;
    drop(side_a);

    let (mut sink, mut reader) = a.into_split();
    assert_eq!(
        read_to_end(&mut reader, nothing),
        (Vec::new(), Some(StreamError::ConnectionDropped))
    );
    let error = Err(StreamError::ConnectionDropped);
    assert_eq!(sink.send(LATE).now_or_never().unwrap(), error);
    assert_eq!(sink.flush().now_or_never().unwrap(), error);
    assert_eq!(sink.close().now_or_never().unwrap(), error);

    assert_eq!(
        closed.send(LATE).now_or_never().unwrap(),
        Err(StreamError::SinkClosed)
    );
    assert_eq!(closed.flush().now_or_never().unwrap(), Ok(()));
    assert_eq!(closed.close().now_or_never().unwrap(), Ok(()));
}

#[tokio::test(start_paused = true)]
async fn a_clean_end_survives_the_connection() {
    let mut pair = pair();
    let (a, b) = pair.open(b"abc");
    let (mut b_sink, _b_reader) = b.into_split();
    pair.drive(b_sink.send(Bytes::from_static(b"xyz"))).unwrap();
    pair.drive(b_sink.close()).unwrap();
    pair.run();

    let Pair {
        a: side_a,
        b: _side_b,
    } = pair;
    drop(side_a);
    let (_sink, mut reader) = a.into_split();
    assert_eq!(read_to_end(&mut reader, nothing), (b"xyz".to_vec(), None));
}

#[tokio::test(start_paused = true)]
async fn an_abort_survives_the_connection() {
    let mut pair = pair();
    let (a, b) = pair.open(b"abc");
    let (mut b_sink, _b_reader) = b.into_split();
    pair.drive(b_sink.send(Bytes::from_static(b"xyz"))).unwrap();
    drop(b_sink);
    pair.run();

    let Pair {
        a: side_a,
        b: _side_b,
    } = pair;
    drop(side_a);
    let (_sink, mut reader) = a.into_split();
    assert_eq!(
        read_to_end(&mut reader, nothing),
        (b"xyz".to_vec(), Some(StreamError::PeerAborted))
    );
}

#[test]
fn stream_errors_convert_to_io_errors() {
    for (error, kind) in [
        (StreamError::PeerAborted, io::ErrorKind::ConnectionReset),
        (StreamError::PeerStoppedReading, io::ErrorKind::BrokenPipe),
        (
            StreamError::ConnectionFailed,
            io::ErrorKind::ConnectionAborted,
        ),
        (
            StreamError::ConnectionDowngraded,
            io::ErrorKind::ConnectionAborted,
        ),
        (
            StreamError::ConnectionDropped,
            io::ErrorKind::ConnectionAborted,
        ),
        (StreamError::SinkClosed, io::ErrorKind::BrokenPipe),
        (StreamError::SinkNotReady, io::ErrorKind::Other),
    ] {
        let converted = io::Error::from(error);
        assert_eq!(converted.kind(), kind, "{error}");
        let inner = converted
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<StreamError>());
        assert_eq!(inner, Some(&error));
    }
}

#[tokio::test(start_paused = true)]
async fn a_close_before_a_downgrade_is_a_clean_end() {
    let mut pair = pair();
    let (a, mut b) = pair.open(b"abc");
    let (mut sink, _reader) = a.into_split();
    // The data is flushed already, so the close only has `FIN_WRITE` to hand over.
    let Pair {
        a: mut side_a,
        b: mut side_b,
    } = pair;
    let mut close = pin!(sink.close());
    for _ in 0..POLLS {
        if let Some(result) = close.as_mut().now_or_never() {
            result.unwrap();
            break;
        }
        side_a.run();
    }

    // A downgrades right after the close completed.
    let mut downgraded = side_a.conn.downgrade().unwrap();
    assert!(unconstrained(&mut downgraded).now_or_never().is_none());
    let _downgraded_b = loop {
        match side_b.conn.progress().now_or_never() {
            Some(Ok(RammuxProgress::Downgraded(downgraded))) => break downgraded,
            Some(Ok(RammuxProgress::Empty)) => {},
            other => panic!("{other:?}"),
        }
    };
    assert_eq!(read_to_end(&mut b, nothing), (b"abc".to_vec(), None));
}

/// Starts a stream on A, and a task that writes more to it than the window lets in and then waits for
/// `close()`, or for `flush()`. B does not read, so the task does not finish. Returns the task, and the end
/// of the stream on B.
async fn pending_call(
    pair: &mut Pair,
    close: bool,
) -> (JoinHandle<Result<(), StreamError>>, RammuxDuplex) {
    let (a, b) = pair.open(b"abc");
    let mut task = tokio::spawn(async move {
        let (mut sink, _reader) = a.into_split();
        sink.feed(chunk(4096)).await?;
        if close {
            sink.close().await
        } else {
            sink.flush().await
        }
    });
    for _ in 0..10 {
        // `let ()`: the oldest tokio that the crate allows marks the output of `yield_now` as must-use.
        let () = tokio::task::yield_now().await;
        pair.run();
    }
    assert!((&mut task).now_or_never().is_none());
    (task, b)
}

/// Lets the task run, with `run` called in between, and returns its result. Fails if nothing woke the task.
async fn woken<T>(task: JoinHandle<T>, mut run: impl FnMut()) -> T {
    for _ in 0..10 {
        let () = tokio::task::yield_now().await;
        run();
    }
    task.now_or_never()
        .expect("the pending call was not woken")
        .unwrap()
}

#[rstest::rstest]
#[tokio::test(start_paused = true)]
async fn a_pending_close_or_flush_is_woken_when_the_direction_ends(
    #[values(true, false)] close: bool,
) {
    {
        // The peer stops reading.
        let mut pair = pair();
        let (task, b) = pending_call(&mut pair, close).await;
        drop(b);
        assert_eq!(
            woken(task, || pair.run()).await,
            Err(StreamError::PeerStoppedReading)
        );
    }

    {
        // The connection is dropped.
        let mut pair = pair();
        let (task, _b) = pending_call(&mut pair, close).await;
        let Pair {
            a: side_a,
            b: _side_b,
        } = pair;
        drop(side_a);
        assert_eq!(
            woken(task, nothing).await,
            Err(StreamError::ConnectionDropped)
        );
    }

    {
        // The connection fails.
        let mut pair = pair();
        let (task, _b) = pending_call(&mut pair, close).await;
        let Pair {
            a: mut side_a,
            b: side_b,
        } = pair;
        drop(side_b);
        assert!(side_a.conn.progress().now_or_never().unwrap().is_err());
        assert_eq!(
            woken(task, nothing).await,
            Err(StreamError::ConnectionFailed)
        );
    }

    {
        // The connection is downgraded.
        let mut pair = pair();
        let (task, _b) = pending_call(&mut pair, close).await;
        let Pair {
            a: side_a,
            b: _side_b,
        } = pair;
        let _downgraded = side_a.conn.downgrade().unwrap();
        assert_eq!(
            woken(task, nothing).await,
            Err(StreamError::ConnectionDowngraded)
        );
    }
}

/// Streams in both directions, with the connections and the applications on the threads of a multi-thread
/// runtime, so that streams are woken from other threads than the ones that poll them. Every stream carries
/// all of its data and ends cleanly.
///
/// The runtime cannot pause time, so this test runs in real time. Nothing in it sleeps.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_end_cleanly_with_the_applications_on_other_threads() {
    const STREAMS: usize = 48;
    const BYTES: usize = 256 * 1024;

    /// Writes [`BYTES`] in writes of varying size and closes, while reading the peer's [`BYTES`] to the end.
    /// Both now and then let other tasks run. Returns how many bytes it read.
    async fn talk(stream: RammuxDuplex, seed: u64) -> usize {
        let (mut sink, mut reader) = stream.into_split();
        let next = |n: u64| {
            n.wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407)
        };
        let write = tokio::spawn(async move {
            let mut left = BYTES;
            let mut n = seed;
            while left > 0 {
                n = next(n);
                let len = (1 + usize::try_from(n >> 33).unwrap() % 8192).min(left);
                sink.send(Bytes::from(vec![1_u8; len])).await.unwrap();
                left -= len;
                if (n >> 20).is_multiple_of(8) {
                    let () = tokio::task::yield_now().await;
                }
            }
            sink.close().await.unwrap();
        });
        let read = tokio::spawn(async move {
            let mut read = 0;
            let mut n = !seed;
            while let Some(chunk) = reader.next().await {
                read += chunk.unwrap().len();
                n = next(n);
                if (n >> 20).is_multiple_of(8) {
                    let () = tokio::task::yield_now().await;
                }
            }
            read
        });
        write.await.unwrap();
        read.await.unwrap()
    }

    let mut config = RammuxConfig::new();
    config.frame_limit = NonZeroU16::new(4096).unwrap();
    config.local_recv_window = NonZeroU32::new(4096).unwrap();
    config.remote_recv_window = NonZeroU32::new(4096).unwrap();
    config.local_recv_pool = 64 * 1024;
    config.remote_recv_pool = 64 * 1024;
    let (io_a, io_b) = tokio::io::duplex(64 * 1024);
    let mut a = RammuxConnection::new(RammuxRole::Client, io_a, config.clone()).unwrap();
    let mut b = RammuxConnection::new(RammuxRole::Server, io_b, config).unwrap();

    let mut talks = Vec::new();
    for seed in 0..STREAMS {
        let stream = a.try_start_outbound().unwrap().unwrap();
        talks.push(tokio::spawn(talk(stream, seed as u64)));
    }
    let conn_a = tokio::spawn(async move {
        loop {
            a.progress().await.unwrap();
        }
    });
    let (inbound, mut inbound_talks) = mpsc::unbounded_channel();
    let conn_b = tokio::spawn(async move {
        let mut seed = 1000;
        loop {
            if let RammuxProgress::Inbound(stream) = b.progress().await.unwrap() {
                let _ = inbound.send(tokio::spawn(talk(stream, seed)));
                seed += 1;
            }
        }
    });

    let all = async {
        for talk in talks {
            assert_eq!(talk.await.unwrap(), BYTES);
        }
        for _ in 0..STREAMS {
            let talk = inbound_talks.recv().await.unwrap();
            assert_eq!(talk.await.unwrap(), BYTES);
        }
    };
    tokio::time::timeout(Duration::from_secs(10), all)
        .await
        .expect("the streams are stuck");
    conn_a.abort();
    conn_b.abort();
}
