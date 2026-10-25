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
    config::RammuxConfig,
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
    ping_bytes: NonZeroU32::new(32).unwrap(),
    ping_interval: Duration::from_secs(1),
    inbound_frame_timeout: Duration::from_secs(1),
    global_recv_window: 32,
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
    let (_, mut selector, duplex) = new_stream(GlobalPool::default());
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
    let (_, mut selector, duplex) = new_stream(GlobalPool::default());
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
    let (_, mut selector, duplex) = new_stream(GlobalPool::default());
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

#[tokio::test(start_paused = true)]
async fn local_receive_window_is_autotuned() {
    let (mut handle, mut selector, mut duplex) = new_stream(GlobalPool {
        available: CONFIG.local_recv_window.get() as usize * 4,
        ..Default::default()
    });

    for _ in 0..5 {
        let data =
            std::iter::repeat_n(b'a', CONFIG.local_recv_window.get() as usize).collect::<Vec<_>>();
        let data = Data::copy_from_slice(&data);
        handle.received(None, data, false, false).unwrap();
        duplex.next().await.unwrap();
        let (update, ..) = selector.next().await.unwrap();
        assert_eq!(update.window_update, CONFIG.local_recv_window.get());
    }

    selector.strategy_mut().rtt = Some(Duration::from_secs(1));
    let mut current_window = CONFIG.local_recv_window.get();

    while selector.strategy().available > 0 {
        tokio::time::advance(Duration::from_millis(100)).await;
        let data = std::iter::repeat_n(b'a', current_window as usize).collect::<Vec<_>>();
        let data = Data::copy_from_slice(&data);
        handle.received(None, data, false, false).unwrap();
        duplex.next().await.unwrap();
        let (update, ..) = selector.next().await.unwrap();
        assert!(update.window_update > current_window);
        current_window = update.window_update;
    }

    while selector.strategy().available < CONFIG.local_recv_window.get() as usize * 4 {
        tokio::time::advance(Duration::from_secs(5)).await;
        let data = std::iter::repeat_n(b'a', current_window as usize).collect::<Vec<_>>();
        let data = Data::copy_from_slice(&data);
        handle.received(None, data, false, false).unwrap();
        duplex.next().await.unwrap();
        let (update, ..) = selector.next().await.unwrap();
        assert!(update.window_update < current_window);
        current_window = update.window_update;
    }
}

#[tokio::test]
async fn local_receive_window_is_respected() {
    let (mut handle, _selector, _duplex) = new_stream(GlobalPool::default());
    for _ in 0..CONFIG.local_recv_window.get() {
        handle
            .received(None, Data::copy_from_slice(b"a"), false, false)
            .unwrap();
    }
    handle
        .received(None, Data::copy_from_slice(b"a"), false, false)
        .unwrap_err();
}

#[tokio::test]
async fn remote_receive_window_is_respected() {
    let (mut handle, mut selector, mut duplex) = new_stream(GlobalPool::default());
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
        .received(NonZeroU32::new(8), Data::default(), false, false)
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
    let (mut handle, _selector, mut duplex) = new_stream(GlobalPool::default());
    handle.received(None, Data::default(), false, true).unwrap();
    duplex.send(Bytes::from_static(b"a")).await.unwrap_err();
}

#[tokio::test]
async fn fin_write_closes_reading() {
    let (mut handle, _selector, mut duplex) = new_stream(GlobalPool::default());
    handle.received(None, Data::default(), true, false).unwrap();
    assert!(duplex.next().await.is_none());
}

#[tokio::test]
async fn parts_of_a_frame_are_processed_in_protocol_order() {
    // DATA is processed before FIN_WRITE.
    let (mut handle, _selector, mut duplex) = new_stream(GlobalPool::default());
    handle
        .received(None, Data::copy_from_slice(b"a"), true, false)
        .unwrap();
    assert_eq!(duplex.next().await.unwrap(), Bytes::from_static(b"a"));
    assert!(duplex.next().await.is_none());

    // WINDOW is processed before FIN_READ.
    let (mut handle, _selector, _duplex) = new_stream(GlobalPool::default());
    handle
        .received(NonZeroU32::new(8), Data::default(), false, true)
        .unwrap();
    // And a WINDOW after FIN_READ is a violation.
    handle
        .received(NonZeroU32::new(8), Data::default(), false, false)
        .unwrap_err();
}
