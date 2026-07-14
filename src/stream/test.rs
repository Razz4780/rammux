use std::{num::NonZeroU32, time::Duration};

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
    frame_limit: NonZeroU32::new(8).unwrap(),
    max_inbound_streams: 4,
    max_outbound_streams: 4,
    local_recv_window: NonZeroU32::new(12).unwrap(),
    remote_recv_window: 14,
    ping_interval: Duration::from_secs(1),
    global_recv_window: 32,
};

fn new_stream() -> (StreamHandle, Selector<StreamUpdates>, RammuxDuplex) {
    let (handle, updates, duplex) = super::new(StreamId::from_be_bytes([0, 0, 0]), true, &CONFIG);
    (handle, Selector::from_iter([updates]), duplex)
}

#[tokio::test]
async fn rammux_duplex_drop_closes_both() {
    let mut global = GlobalPool::default();
    let (_, mut selector, duplex) = new_stream();
    drop(duplex);
    let (update, fin_state) = selector.with_ext(&(), &mut global).next().await.unwrap();
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
    let mut global = GlobalPool::default();
    let (_, mut selector, duplex) = new_stream();
    let _stream = duplex.into_split().1;
    let (update, fin_state) = selector.with_ext(&(), &mut global).next().await.unwrap();
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
    let mut global = GlobalPool::default();
    let (_, mut selector, duplex) = new_stream();
    let _sink = duplex.into_split().0;
    let (update, fin_state) = selector.with_ext(&(), &mut global).next().await.unwrap();
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
    let mut global = GlobalPool {
        rtt: None,
        available: CONFIG.local_recv_window.get() as usize * 4,
    };
    let (mut handle, mut selector, mut duplex) = new_stream();

    for _ in 0..5 {
        let data =
            std::iter::repeat_n(b'a', CONFIG.local_recv_window.get() as usize).collect::<Vec<_>>();
        let data = Data::copy_from_slice(&data);
        handle.received_data(data, false, false).unwrap();
        duplex.next().await.unwrap();
        let (update, ..) = selector.with_ext(&(), &mut global).next().await.unwrap();
        assert_eq!(update.window_update, CONFIG.local_recv_window.get());
    }

    global.rtt = Some(Duration::from_secs(1));
    let mut current_window = CONFIG.local_recv_window.get();

    while global.available > 0 {
        tokio::time::advance(Duration::from_millis(100)).await;
        let data = std::iter::repeat_n(b'a', current_window as usize).collect::<Vec<_>>();
        let data = Data::copy_from_slice(&data);
        handle.received_data(data, false, false).unwrap();
        duplex.next().await.unwrap();
        let (update, ..) = selector.with_ext(&(), &mut global).next().await.unwrap();
        assert!(update.window_update > current_window);
        current_window = update.window_update;
    }

    while global.available < CONFIG.local_recv_window.get() as usize * 4 {
        tokio::time::advance(Duration::from_secs(5)).await;
        let data = std::iter::repeat_n(b'a', current_window as usize).collect::<Vec<_>>();
        let data = Data::copy_from_slice(&data);
        handle.received_data(data, false, false).unwrap();
        duplex.next().await.unwrap();
        let (update, ..) = selector.with_ext(&(), &mut global).next().await.unwrap();
        assert!(update.window_update < current_window);
        current_window = update.window_update;
    }
}

#[tokio::test]
async fn local_receive_window_is_respected() {
    let (mut handle, _selector, _duplex) = new_stream();
    for _ in 0..CONFIG.local_recv_window.get() {
        handle
            .received_data(Data::copy_from_slice(b"a"), false, false)
            .unwrap();
    }
    handle
        .received_data(Data::copy_from_slice(b"a"), false, false)
        .unwrap_err();
}

#[tokio::test]
async fn remote_receive_window_is_respected() {
    let mut global = GlobalPool::default();
    let (mut handle, mut selector, mut duplex) = new_stream();
    for _ in 0..CONFIG.remote_recv_window {
        duplex.feed(Bytes::from_static(b"a")).await.unwrap();
        assert!(duplex.flush().now_or_never().is_none());
        let (update, fin_state) = selector.with_ext(&(), &mut global).next().await.unwrap();
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
    assert!(
        selector
            .with_ext(&(), &mut global)
            .next()
            .now_or_never()
            .is_none()
    );
    handle.received_window_update(8, false, false).unwrap();
    let (update, fin_state) = selector.with_ext(&(), &mut global).next().await.unwrap();
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
    let (mut handle, _selector, mut duplex) = new_stream();
    handle.received_window_update(0, true, false).unwrap();
    duplex.send(Bytes::from_static(b"a")).await.unwrap_err();
}

#[tokio::test]
async fn fin_write_closes_reading() {
    let (mut handle, _selector, mut duplex) = new_stream();
    handle.received_window_update(0, false, true).unwrap();
    assert!(duplex.next().await.is_none());
}
