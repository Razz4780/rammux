use std::{
    fmt,
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::time::Instant;

/// Manages flow control for inbound traffic within a single Rammux stream.
///
/// Thanks to the global receive window pool ([`RammuxConfig::global_recv_window`](crate::config::RammuxConfig::global_recv_window)),
/// this struct can resize the receive window above the initial value (minimum guaranteed for each stream).
/// Such resize happens when the stream experiences a high throughput,
/// and requires a borrow from the global pool.
/// The borrow is returned when the throughput decreases or this struct is dropped.
#[derive(Debug)]
pub struct FlowControl(State);

impl FlowControl {
    /// Notifies this struct that `amt` bytes were consumed by the local reader.
    pub fn consumed_bytes(&mut self, amt: u32) {
        match &mut self.0 {
            State::Open {
                current_window,
                consumed,
                ..
            } => {
                if *current_window - *consumed < amt {
                    panic!("consumed more than the window allows");
                }
                *consumed += amt;
            },
            State::HalfOpen { borrowed, global } => {
                let to_return = amt.min(*borrowed);
                if to_return > 0 {
                    global.0.lock().unwrap().available += crate::safe_cast_usize(to_return);
                    *borrowed -= to_return;
                }
            },
            State::Closed => {},
        }
    }

    /// Returns whether this struct is ready to return a non-zero window update from [`Self::get_update`].
    pub fn has_update(&self) -> bool {
        matches!(self.0, State::Open { consumed, current_window, .. } if consumed >= current_window / 2)
    }

    /// Returns a window update to be sent to the peer.
    ///
    /// # Autotuning
    ///
    /// This method attempts to autotune the size of the receive window
    /// to match `1.5 * BDP`.
    /// This includes borrowing from the [`RammuxConfig::global_recv_window`](crate::config::RammuxConfig::global_recv_window)
    /// and returning the borrow if needed.
    ///
    /// Since the initial window size is a guaranteed minimum available to each stream,
    /// the window is never shrunk below that size.
    ///
    /// Also, to mitigate the impact of sudden throughput spikes, the window is never:
    /// 1. Shrunk by more than 25%; OR
    /// 2. Extended by more than 100%.
    ///
    /// # Returns
    ///
    /// Returns a window update to be sent to the peer.
    /// Value `0` means no update.
    ///
    /// Window updates are suspended until at least half of the current window has been depleted.
    /// This prevents frames with insignificant window updates.
    pub fn get_update(&mut self) -> u32 {
        let State::Open {
            initial_window,
            current_window,
            consumed,
            last_update,
            global,
        } = &mut self.0
        else {
            return 0;
        };
        if *consumed < *current_window / 2 {
            return 0;
        }

        let rtt = global.0.lock().unwrap().rtt;
        let next_window = rtt
            .map(|rtt| Self::next_window(*consumed, last_update.elapsed(), rtt))
            .unwrap_or(*current_window);
        let next_window = next_window
            .max(initial_window.get())
            .max(*current_window - *current_window / 4)
            .min(current_window.saturating_mul(2));

        let update = if next_window > *current_window {
            let wanted_borrow = next_window - *current_window;
            let borrow = {
                let mut guard = global.0.lock().unwrap();
                let borrow = u32::try_from(guard.available)
                    .map(|available| available.min(wanted_borrow))
                    .unwrap_or(wanted_borrow);
                guard.available -= crate::safe_cast_usize(borrow);
                borrow
            };
            *current_window += borrow;
            *consumed + borrow
        } else if next_window < *current_window {
            let excess = *current_window - next_window;
            global.0.lock().unwrap().available += crate::safe_cast_usize(excess);
            *current_window = next_window;
            *consumed - excess
        } else {
            *consumed
        };

        *last_update = Instant::now();
        *consumed = 0;
        update
    }

    /// Notifies this struct that the remote writer stopped writing data.
    pub fn writing_closed(&mut self) {
        let State::Open {
            initial_window,
            current_window,
            consumed,
            global,
            ..
        } = &self.0
        else {
            return;
        };
        let mut borrowed = *current_window - initial_window.get();
        let to_return = borrowed.min(*consumed);
        if to_return > 0 {
            global.0.lock().unwrap().available += crate::safe_cast_usize(to_return);
            borrowed -= to_return;
        }
        let global = global.clone();
        self.0 = State::HalfOpen { borrowed, global };
    }

    /// Notifies this struct that the local reader stopped reading data.
    pub fn reading_closed(&mut self) {
        match std::mem::replace(&mut self.0, State::Closed) {
            State::Open {
                initial_window,
                current_window,
                global,
                ..
            } => {
                let borrowed = current_window - initial_window.get();
                if borrowed > 0 {
                    global.0.lock().unwrap().available += crate::safe_cast_usize(borrowed);
                }
            },
            State::HalfOpen { borrowed, global } => {
                if borrowed > 0 {
                    global.0.lock().unwrap().available += crate::safe_cast_usize(borrowed);
                }
            },
            State::Closed => {},
        }
    }

    /// Returns an optimal window size for the given parameters.
    ///
    /// # Casting
    ///
    /// This function produces the next window as [`f64`] and casts it to [`u32`].
    /// This cast can truncate, or produce a 0-sized result if the given durations
    /// are very small/large and the calculated [`f64`] ends up being something like [`f64::NAN`].
    /// However, given that [`Self::consumed_bytes`] applies bounds to window resizes,
    /// this behavior is acceptable.
    #[allow(clippy::cast_possible_truncation)]
    fn next_window(consumed_since_update: u32, time_since_update: Duration, rtt: Duration) -> u32 {
        let new_window = 1.5 * rtt.as_secs_f64() * f64::from(consumed_since_update)
            / time_since_update.as_secs_f64();
        new_window as u32
    }
}

impl Drop for FlowControl {
    fn drop(&mut self) {
        self.reading_closed();
    }
}

enum State {
    /// Data flow is open on both sides.
    ///
    /// As the data is read, we need to keep restoring the window
    /// in order to unblock the writer.
    Open {
        /// Initial receive window, guaranteed by the connection config.
        initial_window: NonZeroU32,
        /// Current receive window (including the already used part).
        ///
        /// This value never drops below `initial_window`.
        current_window: u32,
        /// Bytes of `current_window` consumed by the local reader.
        consumed: u32,
        /// Time of the last window update.
        ///
        /// Start of the stream is treated as the first (implicit) update.
        last_update: Instant,
        /// Reference to the global window, shared by all streams.
        global: GlobalWindow,
    },
    /// Data flow was closed from the writer side.
    ///
    /// As the data is read, we need to return the borrow to the global pool.
    HalfOpen {
        /// Bytes that we have borrowed from the global window.
        borrowed: u32,
        /// Reference to the global window, shared by all streams.
        global: GlobalWindow,
    },
    /// Data flow was closed from the reader side.
    ///
    /// Nothing to do.
    Closed,
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open {
                initial_window,
                current_window,
                consumed,
                last_update,
                ..
            } => f
                .debug_struct("Open")
                .field("initial_window", initial_window)
                .field("current_window", current_window)
                .field("consumed", consumed)
                .field("time_since_last_update", &last_update.elapsed())
                .finish_non_exhaustive(),
            Self::HalfOpen { borrowed, .. } => f
                .debug_struct("HalfOpen")
                .field("borrowed", borrowed)
                .finish_non_exhaustive(),
            Self::Closed => f.write_str("Closed"),
        }
    }
}

#[derive(Clone)]
pub struct GlobalWindow(Arc<Mutex<GlobalWindowState>>);

impl GlobalWindow {
    /// Creates a new global window of the given size.
    pub fn new(size: usize) -> Self {
        Self(Arc::new(Mutex::new(GlobalWindowState {
            rtt: None,
            available: size,
        })))
    }

    /// Updates the round trip time of the connection.
    pub fn update_rtt(&self, rtt: Duration) {
        self.0.lock().unwrap().rtt = Some(rtt);
    }

    /// Returns the current state of the global window.
    pub fn state(&self) -> GlobalWindowState {
        *self.0.lock().unwrap()
    }

    /// Obtains a [`FlowControl`] instance with the initial receive window size.
    ///
    /// The returned instance will use this global pool when resizing the window.
    pub fn flow_control(self, initial_window: NonZeroU32) -> FlowControl {
        FlowControl(State::Open {
            initial_window,
            current_window: initial_window.get(),
            consumed: 0,
            last_update: Instant::now(),
            global: self,
        })
    }
}

/// Inner state of [`GlobalWindow`].
#[derive(Clone, Copy, Debug)]
pub struct GlobalWindowState {
    /// Latest round trip time injected with [`GlobalWindow::update_rtt`].
    pub rtt: Option<Duration>,
    /// Bytes available in the global recv window.
    pub available: usize,
}

#[cfg(test)]
mod test {
    use std::{num::NonZeroU32, time::Duration};

    use rstest::rstest;

    use crate::flow_control::{FlowControl, GlobalWindow};

    #[rstest]
    #[case(128, Duration::from_millis(25), Duration::from_millis(50), 384)]
    #[case(16, Duration::from_millis(50), Duration::from_millis(50), 24)]
    #[case(256, Duration::from_millis(10), Duration::from_millis(50), 1920)]
    #[test]
    fn next_window(
        #[case] consumed_since_update: u32,
        #[case] time_since_update: Duration,
        #[case] rtt: Duration,
        #[case] expected: u32,
    ) {
        let calculated = FlowControl::next_window(consumed_since_update, time_since_update, rtt);
        assert_eq!(calculated, expected);
    }

    #[tokio::test(start_paused = true)]
    async fn grow_and_shrink() {
        let global = GlobalWindow::new(128);
        let mut fc = global.clone().flow_control(NonZeroU32::new(64).unwrap());

        fc.consumed_bytes(32);
        assert_eq!(32, fc.get_update());
        fc.consumed_bytes(64);
        assert_eq!(64, fc.get_update());
        assert_eq!(128, global.0.lock().unwrap().available);

        global.update_rtt(Duration::from_millis(100));

        tokio::time::advance(Duration::from_millis(10)).await;
        fc.consumed_bytes(32);
        assert_eq!(96, fc.get_update());
        assert_eq!(64, global.0.lock().unwrap().available);

        tokio::time::advance(Duration::from_millis(10)).await;
        fc.consumed_bytes(128);
        assert_eq!(192, fc.get_update());
        assert_eq!(0, global.0.lock().unwrap().available);

        tokio::time::advance(Duration::from_millis(10)).await;
        fc.consumed_bytes(192);
        assert_eq!(192, fc.get_update());
        assert_eq!(0, global.0.lock().unwrap().available);

        tokio::time::advance(Duration::from_secs(10)).await;
        fc.consumed_bytes(192);
        assert_eq!(144, fc.get_update());
        assert_eq!(48, global.0.lock().unwrap().available);

        tokio::time::advance(Duration::from_secs(10)).await;
        fc.consumed_bytes(144);
        assert_eq!(108, fc.get_update());
        assert_eq!(84, global.0.lock().unwrap().available);

        tokio::time::advance(Duration::from_secs(10)).await;
        fc.consumed_bytes(108);
        assert_eq!(81, fc.get_update());
        assert_eq!(111, global.0.lock().unwrap().available);

        tokio::time::advance(Duration::from_secs(10)).await;
        fc.consumed_bytes(81);
        assert_eq!(64, fc.get_update());
        assert_eq!(128, global.0.lock().unwrap().available);

        tokio::time::advance(Duration::from_millis(10)).await;
        fc.consumed_bytes(64);
        assert_eq!(128, fc.get_update());
        assert_eq!(64, global.0.lock().unwrap().available);

        drop(fc);
        assert_eq!(128, global.0.lock().unwrap().available);
    }
}
