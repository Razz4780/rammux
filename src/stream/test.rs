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
    stream::{
        FinState, RammuxDuplex,
        handle::StreamHandle,
        updates::{StreamOutput, StreamUpdates},
    },
    transport::StreamFrame,
};

const CONFIG: RammuxConfig = RammuxConfig {
    frame_limit: NonZeroU32::new(8).unwrap(),
    max_inbound_streams: 4,
    max_outbound_streams: 4,
    local_recv_window: NonZeroU32::new(12).unwrap(),
    remote_recv_window: 14,
    global_recv_window: 32,
    local_transit_window: 0,
    remote_transit_window: 0,
    transit_window_max: 4 * 1024 * 1024,
};

/// Credit carried by a `WINDOW_UPDATE` frame.
fn window_update(frame: &StreamFrame) -> u32 {
    match frame {
        StreamFrame::WindowUpdate(update) => update.update,
        StreamFrame::Data(..) => panic!("expected a WINDOW_UPDATE frame"),
    }
}

/// Payload length of a `DATA` frame.
fn payload_len(frame: &StreamFrame) -> usize {
    match frame {
        StreamFrame::Data(data) => data.payload.len(),
        StreamFrame::WindowUpdate(..) => panic!("expected a DATA frame"),
    }
}

fn new_stream(
    global: GlobalPool,
) -> (
    StreamHandle,
    Selector<StreamUpdates, GlobalPool>,
    RammuxDuplex,
) {
    let (handle, updates, duplex) = super::new(StreamId::from_be_bytes([0, 0, 0]), true, &CONFIG);
    let mut selector = Selector::new(global);
    selector.push(updates);
    (handle, selector, duplex)
}

/// Closing both directions at once takes two frames - one carries each
/// fin - and they come out as a pair, back to back.
#[tokio::test]
async fn rammux_duplex_drop_closes_both() {
    let (_, mut selector, duplex) = new_stream(GlobalPool::default());
    drop(duplex);

    let StreamOutput {
        first,
        second,
        fin_state,
    } = selector.next().await.unwrap();
    assert_eq!(
        first.flags(),
        ControlFlags {
            fin_read: true,
            fin_write: false,
            syn: true
        },
        "the window update leads, and carries SYN"
    );
    assert_eq!(
        second.expect("expected the payload frame too").flags(),
        ControlFlags {
            fin_read: false,
            fin_write: true,
            syn: false
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
    let StreamOutput {
        first: update,
        fin_state,
        ..
    } = selector.next().await.unwrap();
    assert_eq!(
        update.flags(),
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
    let StreamOutput {
        first: update,
        fin_state,
        ..
    } = selector.next().await.unwrap();
    assert_eq!(
        update.flags(),
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
        handle
            .received_data(Data::copy_from_slice(&data).into(), false, false)
            .unwrap();
        duplex.next().await.unwrap();
        let StreamOutput { first: update, .. } = selector.next().await.unwrap();
        assert_eq!(window_update(&update), CONFIG.local_recv_window.get());
    }

    selector.strategy_mut().dirty_rtt = Some(Duration::from_secs(1));
    let mut current_window = CONFIG.local_recv_window.get();

    while selector.strategy().available > 0 {
        tokio::time::advance(Duration::from_millis(100)).await;
        let data = std::iter::repeat_n(b'a', current_window as usize).collect::<Vec<_>>();
        handle
            .received_data(Data::copy_from_slice(&data).into(), false, false)
            .unwrap();
        duplex.next().await.unwrap();
        let StreamOutput { first: update, .. } = selector.next().await.unwrap();
        assert!(window_update(&update) > current_window);
        current_window = window_update(&update);
    }

    while selector.strategy().available < CONFIG.local_recv_window.get() as usize * 4 {
        tokio::time::advance(Duration::from_secs(5)).await;
        let data = std::iter::repeat_n(b'a', current_window as usize).collect::<Vec<_>>();
        handle
            .received_data(Data::copy_from_slice(&data).into(), false, false)
            .unwrap();
        duplex.next().await.unwrap();
        let StreamOutput { first: update, .. } = selector.next().await.unwrap();
        assert!(window_update(&update) < current_window);
        current_window = window_update(&update);
    }
}

#[tokio::test]
async fn local_receive_window_is_respected() {
    let (mut handle, _selector, _duplex) = new_stream(GlobalPool::default());
    for _ in 0..CONFIG.local_recv_window.get() {
        handle
            .received_data(Data::copy_from_slice(b"a").into(), false, false)
            .unwrap();
    }
    handle
        .received_data(Data::copy_from_slice(b"a").into(), false, false)
        .unwrap_err();
}

#[tokio::test]
async fn remote_receive_window_is_respected() {
    let (mut handle, mut selector, mut duplex) = new_stream(GlobalPool::default());
    for _ in 0..CONFIG.remote_recv_window {
        duplex.feed(Bytes::from_static(b"a")).await.unwrap();
        assert!(duplex.flush().now_or_never().is_none());
        let StreamOutput {
            first: update,
            fin_state,
            ..
        } = selector.next().await.unwrap();
        assert_eq!(payload_len(&update), 1);
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
    handle.received_window_update(8, false, false).unwrap();
    let StreamOutput {
        first: update,
        fin_state,
        ..
    } = selector.next().await.unwrap();
    assert_eq!(payload_len(&update), 1);
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
    handle.received_window_update(0, true, false).unwrap();
    duplex.send(Bytes::from_static(b"a")).await.unwrap_err();
}

#[tokio::test]
async fn fin_write_closes_reading() {
    let (mut handle, _selector, mut duplex) = new_stream(GlobalPool::default());
    handle.received_window_update(0, false, true).unwrap();
    assert!(duplex.next().await.is_none());
}
