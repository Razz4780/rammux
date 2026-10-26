use std::{
    num::{NonZeroU16, NonZeroU32},
    time::Duration,
};

use async_selector::selector::Selector;
use bytes::Bytes;
use futures::{FutureExt, SinkExt, StreamExt};

use crate::{
    StreamId,
    buffer::Data,
    config::{RammuxConfig, TransitControl},
    global_pool::GlobalPool,
    header::ControlFlags,
    stream::{FinState, RammuxDuplex, handle::StreamHandle, updates::StreamUpdates},
};

const CONFIG: RammuxConfig = RammuxConfig {
    frame_limit: NonZeroU16::new(8).unwrap(),
    max_inbound_streams: 4,
    max_outbound_streams: 4,
    local_recv_window: NonZeroU32::new(12).unwrap(),
    remote_recv_window: NonZeroU32::new(14).unwrap(),
    local_recv_pool: 32,
    remote_recv_pool: 32,
    transit_control: TransitControl::Off,
    ping_bytes: NonZeroU32::new(32).unwrap(),
    queue_target_bytes: NonZeroU32::new(64).unwrap(),
    ping_interval: Duration::from_secs(1),
    inbound_frame_timeout: Duration::from_secs(1),
};

fn new_stream(
    global: GlobalPool,
) -> (
    StreamHandle,
    Selector<StreamUpdates, GlobalPool>,
    RammuxDuplex,
) {
    let (handle, updates, duplex) = super::new(StreamId::from_be_bytes([0, 0]), true, &CONFIG);
    let mut selector = Selector::new(global);
    selector.push(updates);
    (handle, selector, duplex)
}

#[tokio::test]
async fn rammux_duplex_drop_closes_both() {
    let (_, mut selector, duplex) = new_stream(pool(0));
    drop(duplex);
    let (update, fin_state) = selector.next().await.unwrap();
    assert_eq!(
        update.flags,
        ControlFlags {
            fin_read: true,
            fin_write: true,
            syn: true
        }
    );
    assert_eq!(
        fin_state,
        FinState {
            sent: true,
            received: false
        }
    );
}

#[tokio::test]
async fn rammux_sink_drop_closes_writing() {
    let (_, mut selector, duplex) = new_stream(pool(0));
    let _stream = duplex.into_split().1;
    let (update, fin_state) = selector.next().await.unwrap();
    assert_eq!(
        update.flags,
        ControlFlags {
            fin_read: false,
            fin_write: true,
            syn: true
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
    let (_, mut selector, duplex) = new_stream(pool(0));
    let _sink = duplex.into_split().0;
    let (update, fin_state) = selector.next().await.unwrap();
    assert_eq!(
        update.flags,
        ControlFlags {
            fin_read: true,
            fin_write: false,
            syn: true
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

fn pool(rx_pool: u32) -> GlobalPool {
    GlobalPool::new(rx_pool, 0)
}

fn bytes(n: usize) -> Data {
    Data::copy_from_slice(&vec![b'a'; n])
}

#[tokio::test]
async fn credit_is_returned_at_the_threshold() {
    // The base window is 12, so the threshold is 6.
    let (mut handle, mut selector, mut duplex) = new_stream(pool(0));
    handle
        .received(None, bytes(8), false, false, selector.strategy_mut())
        .unwrap();
    assert_eq!(
        selector.strategy().rx_used,
        0,
        "the base window is not the pool"
    );
    duplex.next().await.unwrap();
    // The whole 8 bytes were read, the threshold is passed.
    let (update, _) = selector.next().await.unwrap();
    assert_eq!(update.window_update, 8);
    assert!(selector.next().now_or_never().is_none());

    // Reads below the threshold do not produce credit.
    for _ in 0..5 {
        handle
            .received(None, bytes(1), false, false, selector.strategy_mut())
            .unwrap();
        duplex.next().await.unwrap();
        assert!(selector.next().now_or_never().is_none());
    }
    handle
        .received(None, bytes(1), false, false, selector.strategy_mut())
        .unwrap();
    duplex.next().await.unwrap();
    let (update, _) = selector.next().await.unwrap();
    assert_eq!(update.window_update, 6);
}

#[tokio::test]
async fn credit_is_not_sent_for_unread_data() {
    let (mut handle, mut selector, mut duplex) = new_stream(pool(0));
    handle
        .received(None, bytes(12), false, false, selector.strategy_mut())
        .unwrap();
    assert!(selector.next().now_or_never().is_none());
    duplex.next().await.unwrap();
    assert_eq!(selector.next().await.unwrap().0.window_update, 12);
}

#[tokio::test]
async fn base_window_and_pool_are_respected() {
    // B = 12 and the pool is 32.
    let (mut handle, mut selector, _duplex) = new_stream(pool(32));
    for _ in 0..(12 + 32) {
        handle
            .received(None, bytes(1), false, false, selector.strategy_mut())
            .unwrap();
    }
    assert_eq!(selector.strategy().rx_used, 32);
    handle
        .received(None, bytes(1), false, false, selector.strategy_mut())
        .unwrap_err();
}

#[tokio::test]
async fn the_pool_is_shared_between_streams() {
    let (mut handle_1, mut selector, _duplex_1) = new_stream(pool(10));
    let (handle_2, updates_2, _duplex_2) =
        super::new(StreamId::from_be_bytes([0, 2]), true, &CONFIG);
    selector.push(updates_2);
    let mut handle_2 = handle_2;

    // The first stream takes 8 of the pool.
    handle_1
        .received(None, bytes(12 + 8), false, false, selector.strategy_mut())
        .unwrap();
    // The second stream has its base window regardless.
    handle_2
        .received(None, bytes(12), false, false, selector.strategy_mut())
        .unwrap();
    assert_eq!(selector.strategy().rx_used, 8);
    // But only 2 bytes of the pool are left.
    handle_2
        .received(None, bytes(2), false, false, selector.strategy_mut())
        .unwrap();
    handle_2
        .received(None, bytes(1), false, false, selector.strategy_mut())
        .unwrap_err();
}

#[tokio::test]
async fn credit_returns_the_pool_share() {
    let (mut handle, mut selector, mut duplex) = new_stream(pool(32));
    handle
        .received(None, bytes(12 + 10), false, false, selector.strategy_mut())
        .unwrap();
    assert_eq!(selector.strategy().rx_used, 10);
    duplex.next().await.unwrap();
    let (update, _) = selector.next().await.unwrap();
    assert_eq!(update.window_update, 22);
    assert_eq!(selector.strategy().rx_used, 0);
}

#[tokio::test]
async fn fin_read_releases_the_pool_share_and_data_after_it_is_discarded() {
    let (mut handle, mut selector, duplex) = new_stream(pool(32));
    handle
        .received(None, bytes(12 + 10), false, false, selector.strategy_mut())
        .unwrap();
    assert_eq!(selector.strategy().rx_used, 10);
    drop(duplex);

    // The data that arrives before FIN_READ is written still counts.
    handle
        .received(None, bytes(5), false, false, selector.strategy_mut())
        .unwrap();
    assert_eq!(selector.strategy().rx_used, 15);
    let (update, fin_state) = selector.next().await.unwrap();
    assert!(update.flags.fin_read);
    assert_eq!(update.window_update, 0);
    assert!(fin_state.sent);
    assert_eq!(selector.strategy().rx_used, 0);

    // After it, the data is discarded without a fuss, however much of it.
    for _ in 0..10 {
        handle
            .received(None, bytes(8), false, false, selector.strategy_mut())
            .unwrap();
    }
    assert_eq!(selector.strategy().rx_used, 0);
    assert!(!matches!(selector.next().now_or_never(), Some(Some(_))));
}

#[tokio::test]
async fn data_after_fin_write_is_a_violation() {
    let (mut handle, mut selector, _duplex) = new_stream(pool(0));
    handle
        .received(None, bytes(1), true, false, selector.strategy_mut())
        .unwrap();
    handle
        .received(None, bytes(1), false, false, selector.strategy_mut())
        .unwrap_err();
}

#[tokio::test]
async fn pool_is_returned_when_the_reader_drains_after_fin_write() {
    let (mut handle, mut selector, mut duplex) = new_stream(pool(32));
    handle
        .received(None, bytes(12 + 10), true, false, selector.strategy_mut())
        .unwrap();
    assert_eq!(selector.strategy().rx_used, 10);
    duplex.next().await.unwrap();
    // The reader is done, so FIN_READ zeroes the counter.
    let (update, _) = selector.next().await.unwrap();
    assert!(update.flags.fin_read || update.window_update > 0);
    while let Some(Some((_update, _))) = selector.next().now_or_never() {}
    assert_eq!(selector.strategy().rx_used, 0);
}

#[tokio::test]
async fn remote_receive_window_is_respected() {
    let (mut handle, mut selector, mut duplex) = new_stream(pool(0));
    for _ in 0..CONFIG.remote_recv_window.get() {
        duplex.feed(Bytes::from_static(b"a")).await.unwrap();
        assert!(duplex.flush().now_or_never().is_none());
        let (update, fin_state) = selector.next().await.unwrap();
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
    assert!(selector.next().now_or_never().is_none());
    handle
        .received(
            NonZeroU32::new(8),
            Data::default(),
            false,
            false,
            &mut GlobalPool::default(),
        )
        .unwrap();
    let (update, fin_state) = selector.next().await.unwrap();
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
    let (mut handle, _selector, mut duplex) = new_stream(pool(0));
    handle
        .received(
            None,
            Data::default(),
            false,
            true,
            &mut GlobalPool::default(),
        )
        .unwrap();
    duplex.send(Bytes::from_static(b"a")).await.unwrap_err();
}

#[tokio::test]
async fn fin_write_closes_reading() {
    let (mut handle, _selector, mut duplex) = new_stream(pool(0));
    handle
        .received(
            None,
            Data::default(),
            true,
            false,
            &mut GlobalPool::default(),
        )
        .unwrap();
    assert!(duplex.next().await.is_none());
}

#[tokio::test]
async fn parts_of_a_frame_are_processed_in_protocol_order() {
    // DATA is processed before FIN_WRITE.
    let (mut handle, _selector, mut duplex) = new_stream(pool(0));
    handle
        .received(
            None,
            Data::copy_from_slice(b"a"),
            true,
            false,
            &mut GlobalPool::default(),
        )
        .unwrap();
    assert_eq!(duplex.next().await.unwrap(), Bytes::from_static(b"a"));
    assert!(duplex.next().await.is_none());

    // WINDOW is processed before FIN_READ.
    let (mut handle, mut selector, mut duplex) = new_stream(pool(0));
    duplex.feed(Bytes::from_static(b"12345678")).await.unwrap();
    assert_eq!(selector.next().await.unwrap().0.data.len(), 8);
    handle
        .received(
            NonZeroU32::new(8),
            Data::default(),
            false,
            true,
            selector.strategy_mut(),
        )
        .unwrap();
    // And a WINDOW after FIN_READ is a violation.
    handle
        .received(
            NonZeroU32::new(8),
            Data::default(),
            false,
            false,
            selector.strategy_mut(),
        )
        .unwrap_err();
}
