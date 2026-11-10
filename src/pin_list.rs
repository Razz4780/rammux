//! An intrusive doubly linked list of [`Arc`]s.
//!
//! The links live in the elements themselves, in a [`Node`]. Linking and unlinking never allocate,
//! and an element is removed in O(1) from wherever it is in the list.
//!
//! The elements are pinned by their [`Arc`]: the list holds a strong reference to every linked element,
//! so an element never moves or goes away while the list points to it.
//!
//! Every list has a unique ID, and a node records the ID of the list it is linked into.
//! A list touches only the links of its own nodes, which makes the API safe:
//! a node of another list is never mistaken for one of ours.

use std::{
    cell::UnsafeCell,
    fmt,
    marker::PhantomData,
    num::NonZeroU64,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

/// An element of a [`PinList`].
///
/// # Safety
///
/// [`Linked::node`] must always return the same [`Node`], and the node must be a part of `self`,
/// so that it lives exactly as long as the element.
pub unsafe trait Linked<L = ()>: Sized {
    /// Returns the node of this element for the lists of the kind `L`.
    fn node(&self) -> &Node<Self>;
}

/// The links of an element of a [`PinList`], embedded in the element.
pub struct Node<T> {
    /// ID of the list the node is linked into, or 0 if it is not linked.
    ///
    /// Only that list reads or writes [`Self::links`].
    owner: AtomicU64,
    links: UnsafeCell<Links<T>>,
}

struct Links<T> {
    prev: Option<NonNull<T>>,
    next: Option<NonNull<T>>,
}

impl<T> Node<T> {
    /// Creates a node that is not linked.
    pub const fn new() -> Self {
        Self {
            owner: AtomicU64::new(0),
            links: UnsafeCell::new(Links {
                prev: None,
                next: None,
            }),
        }
    }
}

impl<T> Default for Node<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> fmt::Debug for Node<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("owner", &self.owner.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

// SAFETY: the links are read only by the list that owns the node, through a shared reference to the list,
// and written only through an exclusive reference to the list. The ownership of a node passes from one list
// to the next with the release store and the acquire compare-exchange of `owner`.
unsafe impl<T: Send + Sync> Send for Node<T> {}
// SAFETY: as above.
unsafe impl<T: Send + Sync> Sync for Node<T> {}

/// An intrusive doubly linked list of [`Arc`]s, see the [module docs](self).
pub struct PinList<T: Linked<L>, L = ()> {
    id: NonZeroU64,
    head: Option<NonNull<T>>,
    tail: Option<NonNull<T>>,
    len: usize,
    /// The list owns a strong reference to every linked element.
    _owns: PhantomData<(Arc<T>, fn() -> L)>,
}

// SAFETY: the list owns `Arc<T>`s, which are `Send` if `T` is `Send + Sync`.
unsafe impl<T: Linked<L> + Send + Sync, L> Send for PinList<T, L> {}
// SAFETY: a shared reference to the list only reads the links and hands out `&T`.
unsafe impl<T: Linked<L> + Send + Sync, L> Sync for PinList<T, L> {}

impl<T: Linked<L>, L> PinList<T, L> {
    /// Creates an empty list.
    pub fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            id: NonZeroU64::new(id).expect("list IDs do not run out"),
            head: None,
            tail: None,
            len: 0,
            _owns: PhantomData,
        }
    }

    /// Returns the number of elements in the list.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the first element.
    pub fn front(&self) -> Option<&T> {
        // SAFETY: a linked element is kept alive by the list.
        self.head.map(|head| unsafe { head.as_ref() })
    }

    /// Returns whether the element is in this list.
    pub fn contains(&self, element: &T) -> bool {
        element.node().owner.load(Ordering::Relaxed) == self.id.get()
    }

    /// Puts the element at the back of the list.
    ///
    /// Returns `false` and does nothing if the element is already in this list.
    ///
    /// # Panics
    ///
    /// Panics if the element is in another list.
    pub fn push_back(&mut self, element: &Arc<T>) -> bool {
        if let Err(owner) = element.node().owner.compare_exchange(
            0,
            self.id.get(),
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            assert_eq!(owner, self.id.get(), "the element is in another list");
            return false;
        }

        let ptr = NonNull::new(Arc::into_raw(Arc::clone(element)).cast_mut())
            .expect("Arc::into_raw never returns null");
        // SAFETY: the element is now owned by this list, and we have exclusive access to the list.
        unsafe {
            *Self::links(ptr) = Links {
                prev: self.tail,
                next: None,
            };
            match self.tail {
                Some(tail) => (*Self::links(tail)).next = Some(ptr),
                None => self.head = Some(ptr),
            }
        }
        self.tail = Some(ptr);
        self.len += 1;
        true
    }

    /// Takes the element out of the list, wherever it is.
    ///
    /// Returns `false` and does nothing if the element is not in this list.
    pub fn remove(&mut self, element: &Arc<T>) -> bool {
        if !self.contains(element) {
            return false;
        }
        // The pointer that the list holds, the one from `Arc::into_raw`, is the one to unlink.
        // SAFETY: the element is in this list, so we may read its links, and so is its neighbour.
        let ptr = unsafe {
            match (*element.node().links.get()).prev {
                Some(prev) => (*Self::links(prev)).next,
                None => self.head,
            }
        }
        .expect("a linked element is reachable from its neighbour");
        // SAFETY: the element is in this list, and we have exclusive access to the list.
        drop(unsafe { self.unlink(ptr) });
        true
    }

    /// Takes the first element out of the list.
    pub fn pop_front(&mut self) -> Option<Arc<T>> {
        let head = self.head?;
        // SAFETY: the head is in this list, and we have exclusive access to the list.
        Some(unsafe { self.unlink(head) })
    }

    /// Returns an iterator over the elements, first to last.
    pub fn iter(&self) -> Iter<'_, T, L> {
        Iter {
            next: self.head,
            _list: PhantomData,
        }
    }

    /// Unlinks the element and returns the strong reference that the list held.
    ///
    /// # Safety
    ///
    /// The element must be in this list, and `ptr` must be the pointer that the list holds.
    unsafe fn unlink(&mut self, ptr: NonNull<T>) -> Arc<T> {
        // SAFETY: the caller guarantees that the element is ours, so are its neighbours,
        // and we have exclusive access to the list.
        unsafe {
            let Links { prev, next } = std::mem::replace(
                &mut *Self::links(ptr),
                Links {
                    prev: None,
                    next: None,
                },
            );
            match prev {
                Some(prev) => (*Self::links(prev)).next = next,
                None => self.head = next,
            }
            match next {
                Some(next) => (*Self::links(next)).prev = prev,
                None => self.tail = prev,
            }
            self.len -= 1;
            ptr.as_ref().node().owner.store(0, Ordering::Release);
            Arc::from_raw(ptr.as_ptr())
        }
    }

    /// Returns the links of an element.
    ///
    /// # Safety
    ///
    /// The element must be alive. The links may be accessed only by the list that owns the node.
    unsafe fn links(ptr: NonNull<T>) -> *mut Links<T> {
        // SAFETY: the caller guarantees that the element is alive.
        unsafe { ptr.as_ref() }.node().links.get()
    }
}

impl<T: Linked<L>, L> Default for PinList<T, L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Linked<L>, L> Drop for PinList<T, L> {
    fn drop(&mut self) {
        while self.pop_front().is_some() {}
    }
}

impl<T: Linked<L> + fmt::Debug, L> fmt::Debug for PinList<T, L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

/// Iterator over the elements of a [`PinList`], see [`PinList::iter`].
pub struct Iter<'a, T, L = ()> {
    next: Option<NonNull<T>>,
    _list: PhantomData<(&'a T, fn() -> L)>,
}

impl<'a, T: Linked<L>, L> Iterator for Iter<'a, T, L> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        let ptr = self.next?;
        // SAFETY: the element is in the list, which is borrowed for `'a`, so it is alive
        // and its links do not change.
        unsafe {
            self.next = (*PinList::<T, L>::links(ptr)).next;
            Some(ptr.as_ref())
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::pin_list::{Linked, Node, PinList};

    #[derive(Debug)]
    struct Element {
        value: u32,
        node: Node<Element>,
    }

    // SAFETY: the node is a field of the element.
    unsafe impl Linked for Element {
        fn node(&self) -> &Node<Self> {
            &self.node
        }
    }

    fn element(value: u32) -> Arc<Element> {
        Arc::new(Element {
            value,
            node: Node::new(),
        })
    }

    fn values(list: &PinList<Element>) -> Vec<u32> {
        list.iter().map(|element| element.value).collect()
    }

    #[test]
    fn push_and_pop_keep_the_order() {
        let mut list = PinList::new();
        let elements = (0..4).map(element).collect::<Vec<_>>();
        for element in &elements {
            assert!(list.push_back(element));
        }
        assert_eq!(values(&list), [0, 1, 2, 3]);
        assert_eq!(list.len(), 4);
        assert_eq!(list.front().unwrap().value, 0);

        let popped = std::iter::from_fn(|| list.pop_front())
            .map(|element| element.value)
            .collect::<Vec<_>>();
        assert_eq!(popped, [0, 1, 2, 3]);
        assert_eq!(list.len(), 0);
        assert!(list.front().is_none());
    }

    #[test]
    fn remove_from_anywhere() {
        let mut list = PinList::new();
        let elements = (0..5).map(element).collect::<Vec<_>>();
        for element in &elements {
            list.push_back(element);
        }

        assert!(list.remove(&elements[2]));
        assert_eq!(values(&list), [0, 1, 3, 4]);
        assert!(list.remove(&elements[0]));
        assert_eq!(values(&list), [1, 3, 4]);
        assert!(list.remove(&elements[4]));
        assert_eq!(values(&list), [1, 3]);
        assert!(!list.remove(&elements[4]), "not in the list anymore");
        assert_eq!(list.len(), 2);

        // A removed element can come back, at the back.
        assert!(list.push_back(&elements[0]));
        assert_eq!(values(&list), [1, 3, 0]);
    }

    #[test]
    fn pushing_twice_does_nothing() {
        let mut list = PinList::new();
        let a = element(1);
        assert!(list.push_back(&a));
        assert!(!list.push_back(&a));
        assert_eq!(values(&list), [1]);
    }

    #[test]
    fn the_list_holds_a_strong_reference() {
        let mut list = PinList::new();
        let a = element(1);
        list.push_back(&a);
        assert_eq!(Arc::strong_count(&a), 2);
        list.remove(&a);
        assert_eq!(Arc::strong_count(&a), 1);

        list.push_back(&a);
        let weak = Arc::downgrade(&a);
        drop(a);
        // The element lives on in the list.
        assert_eq!(list.front().unwrap().value, 1);
        drop(list);
        assert!(
            weak.upgrade().is_none(),
            "dropping the list drops the element"
        );
    }

    #[test]
    fn another_list_does_not_touch_our_elements() {
        let mut ours = PinList::new();
        let mut theirs = PinList::<Element>::new();
        let a = element(1);
        ours.push_back(&a);
        assert!(ours.contains(&a));
        assert!(!theirs.contains(&a));
        assert!(!theirs.remove(&a));
        assert_eq!(values(&ours), [1]);

        ours.remove(&a);
        assert!(theirs.push_back(&a), "a free element can join any list");
    }

    #[test]
    #[should_panic(expected = "in another list")]
    fn pushing_an_element_of_another_list_panics() {
        let mut ours = PinList::new();
        let mut theirs = PinList::new();
        let a = element(1);
        ours.push_back(&a);
        theirs.push_back(&a);
    }

    #[test]
    fn lists_on_different_threads() {
        let elements = (0..64).map(element).collect::<Vec<_>>();
        std::thread::scope(|scope| {
            for chunk in elements.chunks(16) {
                scope.spawn(move || {
                    let mut list = PinList::new();
                    for _ in 0..4 {
                        for element in chunk {
                            list.push_back(element);
                        }
                        for element in chunk.iter().step_by(2) {
                            list.remove(element);
                        }
                        while list.pop_front().is_some() {}
                    }
                });
            }
        });
        assert!(
            elements
                .iter()
                .all(|element| Arc::strong_count(element) == 1)
        );
    }
}
