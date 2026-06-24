use std::{fmt, mem::MaybeUninit, ptr::NonNull};

use bytes::{Buf, Bytes};

/// Fixed size buffer for reading inbound data frames.
///
/// Meant to be transformed into [`Data`] and stored in [`DataList`].
#[derive(Default)]
pub struct Buffer(
    /// If this slice is not empty, it must be at least as long as the size of [`NextLink`].
    /// The first bytes zeroed and reserved for the link.
    Box<[MaybeUninit<u8>]>,
);

impl Buffer {
    /// Creates a new buffer with the given capacity.
    pub fn with_capactiy(capacity: usize) -> Self {
        if capacity == 0 {
            return Default::default();
        }

        let cap = capacity
            .checked_add(std::mem::size_of::<NextLink>())
            .expect("capacity overflow");
        let mut storage = Box::new_uninit_slice(cap);

        let storage_ptr = storage.as_mut_ptr().cast::<NextLink>();
        let stub_ptr = NonNull::from_ref([].as_slice());
        unsafe {
            // SAFETY: we reserved additional memory for the link.
            storage_ptr.cast::<NextLink>().write_unaligned(stub_ptr);
        };

        Self(storage)
    }

    pub fn as_slice(&self) -> &[MaybeUninit<u8>] {
        self.0
            .get(std::mem::size_of::<NextLink>()..)
            .unwrap_or_default()
    }

    pub fn as_mut_slice(&mut self) -> &mut [MaybeUninit<u8>] {
        self.0
            .get_mut(std::mem::size_of::<NextLink>()..)
            .unwrap_or_default()
    }

    /// Assumes that the buffer is fully initialized and returns the data.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the buffer was fully initialized.
    pub unsafe fn assume_init(self) -> Data {
        let storage = unsafe {
            // SAFETY: caller ensures that data space is initialized,
            // and reserved link bytes were initialized in `.with_capacity`.
            self.0.assume_init()
        };
        Data(storage)
    }
}

/// Data extracted from an inbound frame.
///
/// Meant to be stored in [`DataList`].
#[derive(Clone, Default)]
pub struct Data(
    /// If this slice is not empty, it must be at least as long as the size of [`NextLink`],
    /// and the first bytes are reserved for the link.
    Box<[u8]>,
);

impl Data {
    #[cfg(test)]
    pub fn copy_from_slice(slice: &[u8]) -> Self {
        let mut buffer = Buffer::with_capactiy(slice.len());
        buffer.as_mut_slice().write_copy_of_slice(slice);
        unsafe {
            // SAFETY: data space was just initialized,
            // and reserved link bytes were initialized in `Buffer::with_capacity`.
            buffer.assume_init()
        }
    }
}

impl AsRef<[u8]> for Data {
    fn as_ref(&self) -> &[u8] {
        self.0
            .get(std::mem::size_of::<NextLink>()..)
            .unwrap_or_default()
    }
}

impl From<Data> for Bytes {
    fn from(value: Data) -> Self {
        if value.0.is_empty() {
            return Bytes::new();
        }

        // impl From<Box<[u8]>> for Bytes is very cheap.
        // It does not allocate, only moves some pointers.
        let mut bytes = Bytes::from(value.0)
            .try_into_mut()
            .expect("buffer is unique");
        bytes.advance(std::mem::size_of::<NextLink>());
        bytes.freeze()
    }
}

impl fmt::Debug for Data {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl PartialEq for Data {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl Eq for Data {}

/// Link to the next [`Data`] node in the [`DataList`].
///
/// Link is stored in the first bytes of the slice owned by [`Data`],
/// and does not always have to point to a valid node.
/// It is caller's responsibility to ensure that the link is valid
/// when the node is in the [`DataList`], unless as a tail.
type NextLink = NonNull<[u8]>;

/// Forward list of non-empty [`Data`] nodes.
///
/// Thanks to custom [`Data`] implementation, operations on this list are very cheap.
#[derive(Default)]
pub struct DataList {
    head: Option<NonNull<[u8]>>,
    tail: Option<NonNull<[u8]>>,
}

impl DataList {
    /// Pops a node from the front of this list.
    ///
    /// This method is O(1) and very cheap.
    /// It only moves some pointers.
    pub fn pop_front(&mut self) -> Option<Data> {
        let head = self.head?;
        let storage = unsafe {
            // SAFETY: .head was produced from a Box.
            Box::from_raw(head.as_ptr())
        };
        if self.head == self.tail {
            self.head = None;
            self.tail = None;
        } else {
            // If head node was not the tail, it's next link must be initialized.
            let new_head = unsafe {
                // SAFETY: .head was obtained from a non-empty boxed slice from `Data`.
                Self::next(storage.as_ref())
            };
            self.head = Some(new_head);
        }
        Some(Data(storage))
    }

    /// Pushes the given node to the back of this list if the node is not empty.
    /// Otherwise, drops the node.
    ///
    /// This method is O(1) and very cheap.
    /// It only moves some pointers.
    pub fn push_back(&mut self, data: Data) {
        if data.0.is_empty() {
            return;
        }

        let link = NonNull::from_ref(data.0.as_ref());
        let _ = Box::into_raw(data.0);
        match self.tail {
            Some(mut tail) => {
                unsafe {
                    // SAFETY: .tail was obtained from a non-empty boxed slice from `Data`.
                    Self::set_next(tail.as_mut(), link);
                }
                self.tail = Some(link);
            },
            None => {
                self.head = Some(link);
                self.tail = Some(link);
            },
        }
    }

    /// Reads the [`NextLink`] from the given [`Data`] storage.
    ///
    /// # Safety
    ///
    /// `storage` must be long enough to contain [`NextLink`] bytes,
    /// and the first bytes must contain an initialized [`NextLink`].
    unsafe fn next(storage: &[u8]) -> NextLink {
        debug_assert!(storage.len() >= std::mem::size_of::<NextLink>());
        unsafe { storage.as_ptr().cast::<NextLink>().read_unaligned() }
    }

    /// Sets the [`NextLink`] in the given [`Data`] storage.
    ///
    /// # Safety
    ///
    /// `storage` must be long enough to contain [`NextLink`] bytes.
    unsafe fn set_next(storage: &mut [u8], link: NextLink) {
        debug_assert!(storage.len() >= std::mem::size_of::<NextLink>());
        unsafe {
            storage
                .as_mut_ptr()
                .cast::<NextLink>()
                .write_unaligned(link);
        }
    }
}

impl Drop for DataList {
    fn drop(&mut self) {
        while self.pop_front().is_some() {}
    }
}

unsafe impl Send for DataList {}
unsafe impl Sync for DataList {}

#[cfg(test)]
mod test {
    use bytes::Bytes;

    use crate::buffer::{Buffer, Data, DataList};

    #[test]
    fn buffer_list() {
        let chunks: [&[u8]; _] = [b"some", b"chunks", b"of", b"data"];
        let mut list = DataList::default();

        for chunk in chunks {
            let mut buffer = Buffer::with_capactiy(chunk.len());
            let storage = buffer.as_mut_slice();
            assert_eq!(storage.len(), chunk.len());
            storage.write_copy_of_slice(chunk);
            let data = unsafe { buffer.assume_init() };
            assert_eq!(data.as_ref(), chunk);
            list.push_back(data);
        }

        for chunk in chunks {
            let data = list.pop_front().unwrap();
            assert_eq!(Bytes::from(data), chunk);
        }

        assert!(list.pop_front().is_none());
    }

    #[test]
    fn empty_buffer_list() {
        let mut list = DataList::default();
        for _ in 0..2 {
            list.push_back(Data::default());
        }
        assert!(list.pop_front().is_none());
    }
}
