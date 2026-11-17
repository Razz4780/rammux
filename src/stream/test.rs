use std::{
    num::{NonZeroU16, NonZeroU32},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Context,
    time::Duration,
};

use bytes::Bytes;
use futures::{FutureExt, SinkExt, StreamExt};

use crate::{
    StreamId,
    buffer::Data,
    config::RammuxConfig,
    error::{StreamError, StreamViolation},
    stream::{
        ConnFlow, FinState, RammuxDuplex, RxFlow,
        handle::StreamHandle,
        test_support::{Flag, Selectors},
    },
    wire::header::ControlFlags,
};

const CONFIG: RammuxConfig = RammuxConfig {
    frame_limit: NonZeroU16::new(8).unwrap(),
    max_inbound_streams: 4,
    max_outbound_streams: 4,
    local_recv_window: NonZeroU32::new(12).unwrap(),
    remote_recv_window: NonZeroU32::new(14).unwrap(),
    local_recv_pool: 32,
    remote_recv_pool: 32,
    advance_grant: 0.0,
    advance_budget: 0.0,
    ping_interval: Duration::from_secs(1),
    inbound_frame_timeout: Duration::from_secs(1),
};

/// Starts an outbound stream on a connection with the given receive pool.
fn new_stream(rx_pool: u32) -> (StreamHandle, Selectors, RammuxDuplex) {
    let (handle, credit, data, duplex) = super::new(StreamId::from_be_bytes([0, 0]), true, &CONFIG);
    let mut selectors = Selectors::new(RxFlow::new(rx_pool), ConnFlow::new(0));
    selectors.credit.push(credit);
    selectors.data.push(data);
    (handle, selectors, duplex)
}

/// Hands the parts of an inbound frame to a stream, with shared state of its own.
fn received_alone(
    handle: &StreamHandle,
    credit: Option<NonZeroU32>,
    data: Data,
    flags: ControlFlags,
) -> Result<FinState, StreamViolation> {
    handle.received(
        credit,
        data,
        flags,
        &mut ConnFlow::new(0),
        &mut RxFlow::new(0),
    )
}

#[tokio::test]
async fn rammux_duplex_drop_closes_both_and_aborts_writing() {
    let (_, mut selectors, duplex) = new_stream(0);
    drop(duplex);
    // `SYN` rides on the first frame, whichever direction builds it.
    let (update, fin_state) = selectors.credit.next().await.unwrap();
    assert_eq!(
        update.flags,
        ControlFlags {
            fin_read: true,
            fin_write: false,
            syn: true,
            abort: false,
            reset: false,
        }
    );
    assert_eq!(fin_state, FinState::default());
    let (update, fin_state) = selectors.data.next().await.unwrap();
    assert_eq!(
        update.flags,
        ControlFlags {
            fin_read: false,
            fin_write: true,
            syn: false,
            abort: true,
            reset: false,
        }
    );
    assert_eq!(
        fin_state,
        FinState {
            sent: true,
            received: false
        }
    );
    // Both tasks are done.
    assert!(selectors.credit.is_empty());
    assert!(selectors.data.is_empty());
}

#[tokio::test]
async fn rammux_sink_drop_aborts_writing() {
    let (_, mut selectors, duplex) = new_stream(0);
    let _stream = duplex.into_split().1;
    assert!(selectors.credit.next().now_or_never().is_none());
    let (update, fin_state) = selectors.data.next().await.unwrap();
    assert_eq!(
        update.flags,
        ControlFlags {
            fin_read: false,
            fin_write: true,
            syn: true,
            abort: true,
            reset: false,
        }
    );
    assert_eq!(
        fin_state,
        FinState {
            sent: false,
            received: false
        }
    );
}

#[tokio::test]
async fn rammux_stream_drop_closes_reading() {
    let (_, mut selectors, duplex) = new_stream(0);
    let _sink = duplex.into_split().0;
    assert!(selectors.data.next().now_or_never().is_none());
    let (update, fin_state) = selectors.credit.next().await.unwrap();
    assert_eq!(
        update.flags,
        ControlFlags {
            fin_read: true,
            fin_write: false,
            syn: true,
            abort: false,
            reset: false,
        }
    );
    assert_eq!(
        fin_state,
        FinState {
            sent: false,
            received: false
        }
    );
}

const NO_FINS: ControlFlags = ControlFlags {
    syn: false,
    fin_read: false,
    fin_write: false,
    abort: false,
    reset: false,
};
const FIN_WRITE: ControlFlags = ControlFlags {
    fin_write: true,
    ..NO_FINS
};
const FIN_READ: ControlFlags = ControlFlags {
    fin_read: true,
    ..NO_FINS
};
const RESET: ControlFlags = ControlFlags {
    fin_write: true,
    reset: true,
    ..NO_FINS
};

fn bytes(n: usize) -> Data {
    Data::copy_from_slice(&vec![b'a'; n])
}

#[tokio::test]
async fn credit_is_returned_at_the_threshold() {
    // The base window is 12, so the threshold is 6.
    let (handle, mut selectors, mut duplex) = new_stream(0);
    selectors
        .received(&handle, None, bytes(8), NO_FINS)
        .unwrap();
    assert_eq!(
        selectors.rx_pool_used(),
        0,
        "the base window is not the pool"
    );
    duplex.next().await.unwrap().unwrap();
    // The whole 8 bytes were read, the threshold is passed.
    let (update, _) = selectors.credit.next().await.unwrap();
    assert_eq!(update.credit, 8);
    assert!(selectors.credit.next().now_or_never().is_none());

    // Reads below the threshold do not produce credit.
    for _ in 0..5 {
        selectors
            .received(&handle, None, bytes(1), NO_FINS)
            .unwrap();
        duplex.next().await.unwrap().unwrap();
        assert!(selectors.credit.next().now_or_never().is_none());
    }
    selectors
        .received(&handle, None, bytes(1), NO_FINS)
        .unwrap();
    duplex.next().await.unwrap().unwrap();
    let (update, _) = selectors.credit.next().await.unwrap();
    assert_eq!(update.credit, 6);
}

#[tokio::test]
async fn credit_is_not_sent_for_unread_data() {
    let (handle, mut selectors, mut duplex) = new_stream(0);
    selectors
        .received(&handle, None, bytes(12), NO_FINS)
        .unwrap();
    assert!(selectors.credit.next().now_or_never().is_none());
    duplex.next().await.unwrap().unwrap();
    assert_eq!(selectors.credit.next().await.unwrap().0.credit, 12);
}

#[tokio::test]
async fn base_window_and_pool_are_respected() {
    // B = 12 and the pool is 32.
    let (handle, mut selectors, _duplex) = new_stream(32);
    for _ in 0..(12 + 32) {
        selectors
            .received(&handle, None, bytes(1), NO_FINS)
            .unwrap();
    }
    assert_eq!(selectors.rx_pool_used(), 32);
    selectors
        .received(&handle, None, bytes(1), NO_FINS)
        .unwrap_err();
}

#[tokio::test]
async fn the_pool_is_shared_between_streams() {
    let (handle_1, mut selectors, _duplex_1) = new_stream(10);
    let (handle_2, credit_2, data_2, _duplex_2) =
        super::new(StreamId::from_be_bytes([0, 2]), true, &CONFIG);
    selectors.credit.push(credit_2);
    selectors.data.push(data_2);

    // The first stream takes 8 of the pool.
    selectors
        .received(&handle_1, None, bytes(12 + 8), NO_FINS)
        .unwrap();
    // The second stream has its base window regardless.
    selectors
        .received(&handle_2, None, bytes(12), NO_FINS)
        .unwrap();
    assert_eq!(selectors.rx_pool_used(), 8);
    // But only 2 bytes of the pool are left.
    selectors
        .received(&handle_2, None, bytes(2), NO_FINS)
        .unwrap();
    selectors
        .received(&handle_2, None, bytes(1), NO_FINS)
        .unwrap_err();
}

#[tokio::test]
async fn credit_returns_the_pool_share() {
    let (handle, mut selectors, mut duplex) = new_stream(32);
    selectors
        .received(&handle, None, bytes(12 + 10), NO_FINS)
        .unwrap();
    assert_eq!(selectors.rx_pool_used(), 10);
    duplex.next().await.unwrap().unwrap();
    let (update, _) = selectors.credit.next().await.unwrap();
    assert_eq!(update.credit, 22);
    assert_eq!(selectors.rx_pool_used(), 0);
}

#[tokio::test]
async fn fin_read_releases_the_pool_share_and_data_after_it_is_discarded() {
    let (handle, mut selectors, duplex) = new_stream(32);
    selectors
        .received(&handle, None, bytes(12 + 10), NO_FINS)
        .unwrap();
    assert_eq!(selectors.rx_pool_used(), 10);
    drop(duplex);

    // The data that arrives before FIN_READ is written still counts.
    selectors
        .received(&handle, None, bytes(5), NO_FINS)
        .unwrap();
    assert_eq!(selectors.rx_pool_used(), 15);
    let (update, _) = selectors.credit.next().await.unwrap();
    assert!(update.flags.fin_read);
    assert_eq!(update.credit, 0);
    assert_eq!(selectors.rx_pool_used(), 0);

    // After it, the data is discarded without a fuss, however much of it.
    for _ in 0..10 {
        selectors
            .received(&handle, None, bytes(8), NO_FINS)
            .unwrap();
    }
    assert_eq!(selectors.rx_pool_used(), 0);
    assert!(!matches!(
        selectors.credit.next().now_or_never(),
        Some(Some(_))
    ));
}

#[tokio::test]
async fn data_after_fin_write_is_a_violation() {
    let (handle, mut selectors, _duplex) = new_stream(0);
    selectors
        .received(&handle, None, bytes(1), FIN_WRITE)
        .unwrap();
    selectors
        .received(&handle, None, bytes(1), NO_FINS)
        .unwrap_err();
}

#[tokio::test]
async fn pool_is_returned_when_the_reader_drains_after_fin_write() {
    let (handle, mut selectors, mut duplex) = new_stream(32);
    selectors
        .received(&handle, None, bytes(12 + 10), FIN_WRITE)
        .unwrap();
    assert_eq!(selectors.rx_pool_used(), 10);
    duplex.next().await.unwrap().unwrap();
    // The reader is done, so FIN_READ zeroes the counter.
    let (update, _) = selectors.credit.next().await.unwrap();
    assert!(update.flags.fin_read || update.credit > 0);
    while let Some(Some((_update, _))) = selectors.credit.next().now_or_never() {}
    assert_eq!(selectors.rx_pool_used(), 0);
}

#[tokio::test]
async fn remote_receive_window_is_respected() {
    let (handle, mut selectors, mut duplex) = new_stream(0);
    for _ in 0..CONFIG.remote_recv_window.get() {
        duplex.feed(Bytes::from_static(b"a")).await.unwrap();
        assert!(duplex.flush().now_or_never().is_none());
        let (update, fin_state) = selectors.data.next().await.unwrap();
        assert_eq!(update.data.len(), 1);
        assert_eq!(
            fin_state,
            FinState {
                sent: false,
                received: false
            }
        );
        duplex.flush().await.unwrap();
    }

    duplex.feed(Bytes::from_static(b"a")).await.unwrap();
    assert!(duplex.flush().now_or_never().is_none());
    assert!(selectors.data.next().now_or_never().is_none());
    received_alone(&handle, NonZeroU32::new(8), Data::default(), NO_FINS).unwrap();
    let (update, fin_state) = selectors.data.next().await.unwrap();
    assert_eq!(update.data.len(), 1);
    assert_eq!(
        fin_state,
        FinState {
            sent: false,
            received: false
        }
    );
    duplex.flush().await.unwrap();
}

#[tokio::test]
async fn fin_read_closes_writing() {
    let (handle, _selectors, mut duplex) = new_stream(0);
    received_alone(&handle, None, Data::default(), FIN_READ).unwrap();
    duplex.send(Bytes::from_static(b"a")).await.unwrap_err();
}

#[tokio::test]
async fn fin_write_closes_reading() {
    let (handle, _selectors, mut duplex) = new_stream(0);
    received_alone(&handle, None, Data::default(), FIN_WRITE).unwrap();
    assert!(duplex.next().await.is_none());
}

#[tokio::test]
async fn fin_write_with_nothing_left_to_read_sends_fin_read() {
    let (handle, mut selectors, _duplex) = new_stream(0);
    // The task waits for something to send.
    assert!(selectors.credit.next().now_or_never().is_none());
    selectors
        .received(&handle, None, Data::default(), FIN_WRITE)
        .unwrap();
    let (update, _) = selectors
        .credit
        .next()
        .now_or_never()
        .expect("FIN_WRITE woke the credit task")
        .unwrap();
    assert!(update.flags.fin_read);
}

#[tokio::test]
async fn parts_of_a_frame_are_processed_in_protocol_order() {
    // DATA is processed before FIN_WRITE.
    let (handle, _selectors, mut duplex) = new_stream(0);
    received_alone(&handle, None, Data::copy_from_slice(b"a"), FIN_WRITE).unwrap();
    assert_eq!(
        duplex.next().await.unwrap().unwrap(),
        Bytes::from_static(b"a")
    );
    assert!(duplex.next().await.is_none());

    // WINDOW is processed before FIN_READ.
    let (handle, mut selectors, mut duplex) = new_stream(0);
    duplex.feed(Bytes::from_static(b"12345678")).await.unwrap();
    assert_eq!(selectors.data.next().await.unwrap().0.data.len(), 8);
    selectors
        .received(&handle, NonZeroU32::new(8), Data::default(), FIN_READ)
        .unwrap();
    // And a WINDOW after FIN_READ is a violation.
    selectors
        .received(&handle, NonZeroU32::new(8), Data::default(), NO_FINS)
        .unwrap_err();
}

#[tokio::test]
async fn a_rejected_write_does_not_lose_the_stream() {
    let (_handle, mut selectors, duplex) = new_stream(0);
    let (mut sink, _stream) = duplex.into_split();
    sink.start_send_unpin(Bytes::from_static(b"a")).unwrap();
    // Not ready: the first write is not sent yet.
    sink.start_send_unpin(Bytes::from_static(b"b")).unwrap_err();
    drop(sink);

    let mut fin_write = None;
    while let Some(Some((update, _))) = selectors.data.next().now_or_never() {
        if update.flags.fin_write {
            fin_write = Some(update.flags);
        }
    }
    let flags = fin_write.expect("dropping the sink sends FIN_WRITE");
    assert!(flags.abort, "a sink dropped without close() aborts");
}

#[tokio::test]
async fn a_reset_discards_the_unread_data_and_returns_the_pool_share() {
    let (handle, mut selectors, mut duplex) = new_stream(32);
    selectors
        .received(&handle, None, bytes(12 + 10), NO_FINS)
        .unwrap();
    assert_eq!(selectors.rx_pool_used(), 10);
    // Data in the frame of the reset is discarded with the rest.
    selectors.received(&handle, None, bytes(5), RESET).unwrap();

    // `FIN_READ` goes out at once, though the application read nothing.
    let (update, _) = selectors
        .credit
        .next()
        .now_or_never()
        .flatten()
        .expect("FIN_READ is due");
    assert!(update.flags.fin_read);
    assert_eq!(update.credit, 0);
    assert_eq!(selectors.rx_pool_used(), 0);

    assert_eq!(duplex.next().await, Some(Err(StreamError::PeerReset)));
    assert_eq!(duplex.next().await, None);
}

#[tokio::test]
async fn a_reset_wakes_a_reader_that_waits() {
    let (handle, _selectors, mut duplex) = new_stream(0);
    let flag = Arc::new(Flag(AtomicBool::new(false)));
    let waker = futures::task::waker(flag.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(duplex.poll_next_unpin(&mut cx).is_pending());

    received_alone(&handle, None, Data::default(), RESET).unwrap();
    assert!(flag.0.load(Ordering::SeqCst));
    assert_eq!(duplex.next().await, Some(Err(StreamError::PeerReset)));
}

#[tokio::test]
async fn a_reset_of_a_stream_that_is_not_read_any_more_changes_nothing() {
    let (handle, mut selectors, duplex) = new_stream(0);
    let (_sink, stream) = duplex.into_split();
    drop(stream);
    let (update, _) = selectors.credit.next().await.unwrap();
    assert!(update.flags.fin_read);

    selectors
        .received(&handle, None, Data::default(), RESET)
        .unwrap();
    assert!(selectors.credit.next().now_or_never().flatten().is_none());
}

#[tokio::test]
async fn a_reset_is_a_fin_write() {
    // Nothing follows it.
    let (handle, _selectors, _duplex) = new_stream(0);
    received_alone(&handle, None, Data::default(), RESET).unwrap();
    received_alone(&handle, None, bytes(1), NO_FINS).unwrap_err();
    let (handle, _selectors, _duplex) = new_stream(0);
    received_alone(&handle, None, Data::default(), RESET).unwrap();
    received_alone(&handle, None, Data::default(), FIN_WRITE).unwrap_err();

    // And it does not follow another one, even while the application still reads the data.
    let (handle, _selectors, _duplex) = new_stream(0);
    received_alone(&handle, None, bytes(1), FIN_WRITE).unwrap();
    received_alone(&handle, None, Data::default(), RESET).unwrap_err();
}

/// Returns the updates that the sending direction of the stream yields now.
fn data_updates(selectors: &mut Selectors) -> Vec<(usize, ControlFlags)> {
    let mut updates = Vec::new();
    while let Some(Some((update, _))) = selectors.data.next().now_or_never() {
        updates.push((update.data.len(), update.flags));
    }
    updates
}

#[tokio::test]
async fn rammux_sink_reset_sends_reset_and_drops_the_data() {
    let (_, mut selectors, duplex) = new_stream(0);
    let (mut sink, _stream) = duplex.into_split();
    sink.feed(Bytes::from_static(b"abc")).await.unwrap();
    sink.reset();
    assert_eq!(
        data_updates(&mut selectors),
        [(0, ControlFlags { syn: true, ..RESET })]
    );
    assert!(selectors.data.is_empty());
}

#[tokio::test]
async fn rammux_duplex_reset_resets_writing_and_stops_reading() {
    let (_, mut selectors, duplex) = new_stream(0);
    duplex.reset();
    let (update, _) = selectors.credit.next().await.unwrap();
    assert_eq!(
        update.flags,
        ControlFlags {
            syn: true,
            ..FIN_READ
        }
    );
    assert_eq!(data_updates(&mut selectors), [(0, RESET)]);
}

#[tokio::test]
async fn a_reset_during_a_close_drops_the_rest_of_the_data() {
    let (_, mut selectors, duplex) = new_stream(0);
    let (mut sink, _stream) = duplex.into_split();
    // More than the window of 14 bytes: 6 bytes wait for credit.
    sink.feed(Bytes::from(vec![b'a'; 20])).await.unwrap();
    assert!(sink.close().now_or_never().is_none());
    let syn = ControlFlags {
        syn: true,
        ..NO_FINS
    };
    assert_eq!(data_updates(&mut selectors), [(8, syn), (6, NO_FINS)]);

    sink.reset();
    assert_eq!(data_updates(&mut selectors), [(0, RESET)]);
    assert!(selectors.data.is_empty());
}

#[tokio::test]
async fn a_reset_after_a_close_does_nothing() {
    let (_, mut selectors, duplex) = new_stream(0);
    let (mut sink, _stream) = duplex.into_split();
    sink.feed(Bytes::from_static(b"abc")).await.unwrap();
    assert!(sink.close().now_or_never().is_none());
    let clean = ControlFlags {
        syn: true,
        ..FIN_WRITE
    };
    assert_eq!(data_updates(&mut selectors), [(3, clean)]);
    sink.close().await.unwrap();

    sink.reset();
    assert!(selectors.data.is_empty());
}

#[tokio::test]
async fn a_reset_after_the_peer_stopped_reading_sends_a_plain_fin_write() {
    let (handle, mut selectors, duplex) = new_stream(0);
    let (sink, _stream) = duplex.into_split();
    selectors
        .received(&handle, None, Data::default(), FIN_READ)
        .unwrap();
    sink.reset();
    assert_eq!(
        data_updates(&mut selectors),
        [(
            0,
            ControlFlags {
                syn: true,
                ..FIN_WRITE
            }
        )]
    );
}

#[tokio::test]
async fn a_reset_after_the_end_of_a_close_went_out_does_nothing() {
    let (_, mut selectors, duplex) = new_stream(0);
    let (mut sink, _stream) = duplex.into_split();
    sink.feed(Bytes::from_static(b"abc")).await.unwrap();
    assert!(sink.close().now_or_never().is_none());
    let clean = ControlFlags {
        syn: true,
        ..FIN_WRITE
    };
    // The connection takes the end of the stream. The application does not look at its close again.
    assert_eq!(data_updates(&mut selectors), [(3, clean)]);

    sink.reset();
    assert!(selectors.data.is_empty());
}

#[tokio::test]
async fn a_reset_stays_a_reset_when_the_peer_stops_reading_first() {
    let (handle, mut selectors, duplex) = new_stream(0);
    let (sink, _stream) = duplex.into_split();
    sink.reset();
    // `FIN_READ` of the peer comes before the connection sends anything.
    selectors
        .received(&handle, None, Data::default(), FIN_READ)
        .unwrap();
    assert_eq!(
        data_updates(&mut selectors),
        [(0, ControlFlags { syn: true, ..RESET })]
    );
}

#[tokio::test]
async fn a_reset_that_crosses_fin_read_gets_no_second_one() {
    let (handle, mut selectors, duplex) = new_stream(32);
    let (_sink, stream) = duplex.into_split();
    selectors
        .received(&handle, None, bytes(12 + 10), NO_FINS)
        .unwrap();
    assert_eq!(selectors.rx_pool_used(), 10);
    // The application stops reading, and the reset arrives before `FIN_READ` goes out.
    drop(stream);
    selectors
        .received(&handle, None, Data::default(), RESET)
        .unwrap();

    let mut fin_reads = 0;
    while let Some(Some((update, _))) = selectors.credit.next().now_or_never() {
        fin_reads += u32::from(update.flags.fin_read);
        assert_eq!(update.credit, 0);
    }
    assert_eq!(fin_reads, 1);
    assert_eq!(selectors.rx_pool_used(), 0);
}
