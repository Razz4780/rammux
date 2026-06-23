use std::{
    fmt,
    ops::Not,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use tokio::sync::mpsc;

/// Round robin queue of ready nodes. Node must implement [`MaybeReady`].
///
/// This queue is round robin, because each [`Node`] can only be present in the queue once
/// (even though [`Node`] implements [`Clone`]). The node can only be enqueued again
/// after being dequeued. Enqueuing happens automatically in [`Node::modify`]
/// and [`DequeuedNode::modify`] if the node is ready after the modification.
pub struct RoundRobinBus<T> {
    tx: mpsc::UnboundedSender<Node<T>>,
    rx: mpsc::UnboundedReceiver<Node<T>>,
}

impl<T: MaybeReady> RoundRobinBus<T> {
    /// Registers a new node.
    ///
    /// The returned node is not enqueued, even if it is ready.
    pub fn register_node(&self, value: T) -> Node<T> {
        Node(Arc::new(NodeInner {
            tx: self.tx.downgrade(),
            value: Mutex::new((value, false)),
        }))
    }

    /// Returns the oldest enqueued node.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<DequeuedNode<T>> {
        self.rx
            .poll_recv(cx)
            .map(|node| node.expect("this channel is never closed").0)
            .map(DequeuedNode)
    }
}

impl<T> Default for RoundRobinBus<T> {
    fn default() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self { tx, rx }
    }
}

impl<T> fmt::Debug for RoundRobinBus<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoundRobinBus")
            .field("len", &self.rx.len())
            .finish()
    }
}

/// Node that can be enqueued to [`RoundRobinBus`].
///
/// At any time it may or may not be enqueued.
#[derive(Debug)]
pub struct Node<T>(Arc<NodeInner<T>>);

impl<T: MaybeReady> Node<T> {
    /// Allows for inspecting the value stored in the node.
    ///
    /// Does not ever enqueue the node.
    pub fn inspect<R, F>(&self, with: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        with(&mut self.0.value.lock().unwrap().0)
    }

    /// Allows for modifying the value stored in the node.
    ///
    /// If after modification the node is ready and not in the queue,
    /// it is enqueued.
    pub fn modify<R, F>(&self, with: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.0.value.lock().unwrap();
        let result = with(&mut guard.0);
        if guard.1 || guard.0.is_ready().not() {
            return result;
        }
        guard.1 = true;
        drop(guard);

        if let Some(tx) = self.0.tx.upgrade() {
            let _ = tx.send(self.clone());
        }

        result
    }
}

impl<T> Clone for Node<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// Node that was dequeued from a [`RoundRobinBus`].
#[derive(Debug)]
pub struct DequeuedNode<T>(Arc<NodeInner<T>>);

impl<T: MaybeReady> DequeuedNode<T> {
    /// Allows for modifying the value stored in the node.
    ///
    /// If after the modification the node is ready, it is enqueued.
    pub fn modify<R, F>(self, with: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.0.value.lock().unwrap();
        let result = with(&mut guard.0);
        if guard.0.is_ready().not() {
            guard.1 = false;
            return result;
        }
        drop(guard);

        if let Some(tx) = self.0.tx.upgrade() {
            let _ = tx.send(Node(self.0));
        }

        result
    }
}

#[derive(Debug)]
struct NodeInner<T> {
    tx: mpsc::WeakUnboundedSender<Node<T>>,
    value: Mutex<(T, bool)>,
}

/// Trait for values that can be queued in [`RoundRobinBus`].
pub trait MaybeReady {
    /// Returns whether the value is ready and should be enqueued.
    fn is_ready(&self) -> bool;
}
