use std::task::Waker;

/// Slot for a single [`Waker`].
///
/// Requires mutable access.
#[derive(Default)]
pub struct WakerSlot(Option<Waker>);

impl WakerSlot {
    /// Registers the given [`Waker`], possibly replacing the previous one.
    pub fn register(&mut self, waker: &Waker) {
        match self.0.as_mut() {
            Some(prev) if prev.will_wake(waker) => {},
            Some(prev) => prev.clone_from(waker),
            None => self.0 = Some(waker.clone()),
        }
    }

    /// Consumes this slot and wakes up the latest registered [`Waker`].
    pub fn wake(&mut self) {
        if let Some(waker) = self.0.take() {
            waker.wake();
        }
    }
}
