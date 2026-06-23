//! Types for working with virtual Rammux streams.

mod handle;
mod inbound;
mod outbound;
mod public;
mod waker;

pub(crate) use handle::StreamHandle;
pub use public::{RammuxDuplex, RammuxSink, RammuxStream};

use crate::{
    StreamId,
    config::RammuxConfig,
    flow_control::GlobalWindow,
    rr_bus::{Node, RoundRobinBus},
    stream::{inbound::Inbound, outbound::Outbound},
};

pub(crate) fn create(
    id: StreamId,
    syn: bool,
    config: &RammuxConfig,
    global_window: GlobalWindow,
    bus: &RoundRobinBus<StreamHandle>,
) -> (Node<StreamHandle>, RammuxDuplex) {
    let handle = StreamHandle {
        id,
        syn,
        inbound: Inbound::new(config.local_recv_window, global_window),
        outbound: Outbound::new(config.remote_recv_window, config.frame_limit),
    };
    let node = bus.register_node(handle);
    let duplex = RammuxDuplex {
        sink: RammuxSink {
            id,
            node: Some(node.clone()),
        },
        stream: RammuxStream {
            id,
            node: Some(node.clone()),
        },
    };
    (node, duplex)
}

/// Flags describing state of one direction of a Rammux stream.
#[derive(Clone, Copy, Debug, Default)]
struct StateFlags {
    /// Local writer/reader stopped writing/reading.
    local_closed: bool,
    /// `FIN_WRITE`/`FIN_READ` was sent.
    fin_sent: bool,
    /// `FIN_READ`/`FIN_WRITE` was received.
    fin_received: bool,
}

#[cfg(test)]
mod test {
    use std::{
        num::NonZeroU32,
        ops::Not,
        task::{Context, Waker},
        time::Duration,
    };

    use bytes::Bytes;
    use futures::{FutureExt, SinkExt, StreamExt};
    use rstest::rstest;

    use crate::{
        buffer::Data, config::RammuxConfig, flow_control::GlobalWindow, header::ControlFlags,
        rr_bus::MaybeReady, stream::StreamHandle,
    };

    #[tokio::test]
    async fn reader_dropped() {
        let mut config = RammuxConfig::new();
        config.local_recv_window = NonZeroU32::new(16).unwrap();
        let (node, duplex) = super::create(
            rand::random(),
            false,
            &config,
            GlobalWindow::new(0),
            &Default::default(),
        );
        let (_sink, mut stream) = duplex.into_split();
        node.modify(|handle| {
            for _ in 0..4 {
                handle
                    .received_data(Data::copy_from_slice(b"asdf"), false, false)
                    .unwrap();
            }
        });
        assert_eq!(stream.next().await.unwrap(), Bytes::from_static(b"asdf"));
        assert_eq!(stream.next().await.unwrap(), Bytes::from_static(b"asdf"));
        drop(stream);
        let update = node.modify(StreamHandle::read_update);
        assert_eq!(update.data, Bytes::new());
        assert_eq!(update.window_update, 0);
        assert_eq!(
            update.flags,
            ControlFlags {
                fin_read: true,
                ..Default::default()
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn reader_dropped_with_tuned_window() {
        let mut config = RammuxConfig::new();
        config.local_recv_window = NonZeroU32::new(16).unwrap();
        let global_window = GlobalWindow::new(8);
        global_window.update_rtt(Duration::from_millis(100));
        let (node, duplex) = super::create(
            rand::random(),
            false,
            &config,
            global_window.clone(),
            &Default::default(),
        );
        let (_sink, mut stream) = duplex.into_split();
        node.modify(|handle| {
            for _ in 0..4 {
                handle
                    .received_data(Data::copy_from_slice(b"asdf"), false, false)
                    .unwrap();
            }
        });
        tokio::time::advance(Duration::from_millis(10)).await;
        assert_eq!(stream.next().await.unwrap(), Bytes::from_static(b"asdf"));
        assert_eq!(stream.next().await.unwrap(), Bytes::from_static(b"asdf"));
        let update = node.modify(StreamHandle::read_update);
        assert_eq!(update.data, Bytes::new());
        assert_eq!(update.window_update, 16);
        assert_eq!(update.flags, ControlFlags::default());
        assert_eq!(global_window.state().available, 0);
        drop(stream);
        assert_eq!(global_window.state().available, 8);
        let update = node.modify(StreamHandle::read_update);
        assert_eq!(update.data, Bytes::new());
        assert_eq!(update.window_update, 0);
        assert_eq!(
            update.flags,
            ControlFlags {
                fin_read: true,
                ..Default::default()
            }
        );
    }

    #[rstest]
    #[tokio::test]
    async fn fin_write_with_unread_data(#[values(true, false)] reader_dropped_early: bool) {
        let mut config = RammuxConfig::new();
        config.local_recv_window = NonZeroU32::new(16).unwrap();
        let (node, duplex) = super::create(
            rand::random(),
            false,
            &config,
            GlobalWindow::new(0),
            &Default::default(),
        );
        let (_sink, mut stream) = duplex.into_split();
        node.modify(|handle| {
            for _ in 0..4 {
                handle
                    .received_data(Data::copy_from_slice(b"asdf"), false, false)
                    .unwrap();
            }
            handle.received_data(Data::default(), false, true).unwrap();
        });
        assert_eq!(stream.next().await.unwrap(), Bytes::from_static(b"asdf"));
        assert_eq!(stream.next().await.unwrap(), Bytes::from_static(b"asdf"));
        assert_eq!(stream.next().await.unwrap(), Bytes::from_static(b"asdf"));
        let update = node.modify(StreamHandle::read_update);
        assert_eq!(update.data, Bytes::default());
        assert_eq!(update.window_update, 0);
        assert_eq!(update.flags, ControlFlags::default());
        if reader_dropped_early {
            drop(stream);
        } else {
            assert_eq!(stream.next().await.unwrap(), Bytes::from_static(b"asdf"));
            assert_eq!(stream.next().await, None);
        }
        let update = node.modify(StreamHandle::read_update);
        assert_eq!(update.data, Bytes::new());
        assert_eq!(update.window_update, 0);
        assert_eq!(
            update.flags,
            ControlFlags {
                fin_read: true,
                ..Default::default()
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn window_tuning() {
        let mut config = RammuxConfig::new();
        config.local_recv_window = NonZeroU32::new(16).unwrap();
        let global_window = GlobalWindow::new(512);
        global_window.update_rtt(Duration::from_secs(1));
        let (node, duplex) = super::create(
            rand::random(),
            false,
            &config,
            global_window.clone(),
            &Default::default(),
        );
        let (_sink, mut stream) = duplex.into_split();

        let mut transfer_data_chunk = || {
            node.modify(|handle| {
                handle.received_data(Data::copy_from_slice(b"somedatasomedata"), false, false)
            })
            .unwrap();
            assert_eq!(
                stream.next().now_or_never().unwrap().unwrap(),
                Bytes::from_static(b"somedatasomedata")
            );
            let update = node.modify(StreamHandle::read_update);
            assert_eq!(update.data, Bytes::new());
            assert_eq!(update.flags, ControlFlags::default());
        };

        while global_window.state().available > 0 {
            tokio::time::advance(Duration::from_millis(1)).await;
            transfer_data_chunk();
        }

        while global_window.state().available != 512 {
            tokio::time::advance(Duration::from_secs(10)).await;
            transfer_data_chunk();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn restoring_tuned_window_after_fin_write() {
        let mut config = RammuxConfig::new();
        config.local_recv_window = NonZeroU32::new(16).unwrap();
        let global_window = GlobalWindow::new(8);
        global_window.update_rtt(Duration::from_millis(100));
        let (node, duplex) = super::create(
            rand::random(),
            false,
            &config,
            global_window.clone(),
            &Default::default(),
        );

        let (_sink, mut stream) = duplex.into_split();
        node.modify(|handle| {
            for _ in 0..4 {
                handle
                    .received_data(Data::copy_from_slice(b"asdf"), false, false)
                    .unwrap();
            }
        });
        tokio::time::advance(Duration::from_millis(10)).await;
        assert_eq!(stream.next().await.unwrap(), Bytes::from_static(b"asdf"));
        assert_eq!(stream.next().await.unwrap(), Bytes::from_static(b"asdf"));
        assert_eq!(stream.next().await.unwrap(), Bytes::from_static(b"asdf"));
        assert_eq!(stream.next().await.unwrap(), Bytes::from_static(b"asdf"));
        let update = node.modify(StreamHandle::read_update);
        assert_eq!(update.data, Bytes::new());
        assert_eq!(update.window_update, 24);
        assert_eq!(update.flags, ControlFlags::default());
        assert_eq!(global_window.state().available, 0);

        node.modify(|handle| {
            for _ in 0..6 {
                handle
                    .received_data(Data::copy_from_slice(b"asdf"), false, false)
                    .unwrap();
            }
        });
        assert_eq!(stream.next().await.unwrap(), Bytes::from_static(b"asdf"));
        assert!(node.inspect(|handle| handle.is_ready().not()));
        let update = node.modify(StreamHandle::read_update);
        assert_eq!(update.data, Bytes::new());
        assert_eq!(update.window_update, 0);
        assert_eq!(update.flags, ControlFlags::default());

        node.modify(|handle| handle.received_data(Default::default(), false, true))
            .unwrap();
        assert!(node.inspect(|handle| handle.is_ready().not()));
        assert_eq!(global_window.state().available, 4);

        assert_eq!(stream.next().await.unwrap(), Bytes::from_static(b"asdf"));
        assert!(node.inspect(|handle| handle.is_ready().not()));
        let update = node.modify(StreamHandle::read_update);
        assert_eq!(update.data, Bytes::new());
        assert_eq!(update.window_update, 0);
        assert_eq!(update.flags, ControlFlags::default());
        assert_eq!(global_window.state().available, 8);

        for _ in 0..4 {
            assert_eq!(stream.next().await.unwrap(), Bytes::from_static(b"asdf"));
            assert!(node.inspect(|handle| handle.is_ready()).not());
            let update = node.modify(StreamHandle::read_update);
            assert_eq!(update.data, Bytes::new());
            assert_eq!(update.window_update, 0);
            assert_eq!(update.flags, ControlFlags::default(),);
            assert_eq!(global_window.state().available, 8);
        }

        assert_eq!(stream.next().await, None);
        assert!(node.inspect(|handle| handle.is_ready()));
        let update = node.modify(StreamHandle::read_update);
        assert_eq!(update.data, Bytes::new());
        assert_eq!(update.window_update, 0);
        assert_eq!(
            update.flags,
            ControlFlags {
                fin_read: true,
                ..Default::default()
            },
        );
        assert_eq!(global_window.state().available, 8);
    }

    #[tokio::test]
    async fn writer_dropped() {
        let (node, duplex) = super::create(
            rand::random(),
            true,
            &RammuxConfig::new(),
            GlobalWindow::new(0),
            &Default::default(),
        );
        let (sink, _stream) = duplex.into_split();
        drop(sink);

        assert!(node.inspect(|handle| handle.is_ready()));
        let update = node.modify(StreamHandle::read_update);
        assert_eq!(update.data, Bytes::new());
        assert_eq!(update.window_update, 0);
        assert_eq!(
            update.flags,
            ControlFlags {
                syn: true,
                fin_write: true,
                ..Default::default()
            }
        );
        assert!(node.inspect(|handle| handle.is_ready()).not());
    }

    #[tokio::test]
    async fn writer_blocked_on_flushing() {
        let mut config = RammuxConfig::new();
        config.frame_limit = NonZeroU32::new(4).unwrap();
        config.remote_recv_window = 6;
        let (node, duplex) = super::create(
            rand::random(),
            false,
            &config,
            GlobalWindow::new(0),
            &Default::default(),
        );
        let (mut sink, _stream) = duplex.into_split();

        sink.feed(Bytes::from_static(b"abcdefgh")).await.unwrap();

        assert!(node.inspect(|handle| handle.is_ready()));
        let update = node.modify(StreamHandle::read_update);
        assert_eq!(update.data, Bytes::from_static(b"abcd"));
        assert_eq!(update.window_update, 0);
        assert_eq!(update.flags, ControlFlags::default());
        assert!(
            sink.poll_ready_unpin(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert!(sink.flush().now_or_never().is_none());

        assert!(node.inspect(|handle| handle.is_ready()));
        let update = node.modify(StreamHandle::read_update);
        assert_eq!(update.data, Bytes::from_static(b"ef"));
        assert_eq!(update.window_update, 0);
        assert_eq!(update.flags, ControlFlags::default());
        assert!(node.inspect(|handle| handle.is_ready()).not());
        assert!(
            sink.poll_ready_unpin(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert!(sink.flush().now_or_never().is_none());

        node.modify(|handle| handle.received_window_update(2, false, false))
            .unwrap();
        assert!(node.inspect(|handle| handle.is_ready()));
        let update = node.modify(StreamHandle::read_update);
        assert_eq!(update.data, Bytes::from_static(b"gh"));
        assert_eq!(update.window_update, 0);
        assert_eq!(update.flags, Default::default(),);
        assert!(node.inspect(|handle| handle.is_ready()).not());

        sink.feed(Bytes::from_static(b"abcd")).await.unwrap();
        assert!(sink.close().now_or_never().is_none());
        assert!(node.inspect(|handle| handle.is_ready()).not());

        node.modify(|handle| handle.received_window_update(2, false, false))
            .unwrap();
        assert!(node.inspect(|handle| handle.is_ready()));
        let update = node.modify(StreamHandle::read_update);
        assert_eq!(update.data, Bytes::from_static(b"ab"));
        assert_eq!(update.window_update, 0);
        assert_eq!(update.flags, ControlFlags::default());
        assert!(sink.close().now_or_never().is_none());

        node.modify(|handle| handle.received_window_update(2, false, false))
            .unwrap();
        assert!(node.inspect(|handle| handle.is_ready()));
        let update = node.modify(StreamHandle::read_update);
        assert_eq!(update.data, Bytes::from_static(b"cd"));
        assert_eq!(update.window_update, 0);
        assert_eq!(
            update.flags,
            ControlFlags {
                fin_write: true,
                ..Default::default()
            }
        );
        sink.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn writer_notified_on_fin_read(#[values("send", "flush", "close")] action: &str) {
        let mut config = RammuxConfig::new();
        config.frame_limit = NonZeroU32::new(4).unwrap();
        config.remote_recv_window = 6;
        let (node, duplex) = super::create(
            rand::random(),
            false,
            &config,
            GlobalWindow::new(0),
            &Default::default(),
        );
        let (mut sink, _stream) = duplex.into_split();

        sink.feed(Bytes::from_static(b"abcdefgh")).await.unwrap();

        assert!(node.inspect(|handle| handle.is_ready()));
        let update = node.modify(StreamHandle::read_update);
        assert_eq!(update.data, Bytes::from_static(b"abcd"));
        assert_eq!(update.window_update, 0);
        assert_eq!(update.flags, ControlFlags::default());
        assert!(
            sink.poll_ready_unpin(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert!(sink.flush().now_or_never().is_none());

        node.modify(|handle| handle.received_window_update(0, true, false))
            .unwrap();
        assert!(node.inspect(|handle| handle.is_ready()));
        let update = node.modify(StreamHandle::read_update);
        assert_eq!(update.data, Bytes::new());
        assert_eq!(update.window_update, 0);
        assert_eq!(
            update.flags,
            ControlFlags {
                fin_write: true,
                ..Default::default()
            }
        );
        assert!(node.inspect(|handle| handle.is_ready().not() && handle.outbound.is_dead()));
        match action {
            "send" => {
                sink.feed(Bytes::from_static(b"a")).await.unwrap_err();
            },
            "flush" => {
                sink.flush().await.unwrap();
            },
            "close" => {
                sink.close().await.unwrap();
            },
            _ => unreachable!(),
        }
    }
}
