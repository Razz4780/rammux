use std::{fmt, mem::MaybeUninit, ptr::NonNull};

use bytes::{Buf, Bytes};

/// Fixed size buffer for reading inbound data frames.
#[derive(Default)]
pub struct Buffer(Option<Box<[MaybeUninit<u8>]>>);

impl Buffer {
    pub fn with_capactiy(capacity: usize) -> Self {
        if capacity == 0 {
            return Self(None);
        }

        let cap = capacity
            .checked_add(std::mem::size_of::<NextLink>())
            .expect("capacity overflow");
        let storage = Box::new_uninit_slice(cap);
        Self(Some(storage))
    }

    pub fn as_slice(&self) -> &[MaybeUninit<u8>] {
        let Some(storage) = &self.0 else {
            return Default::default();
        };
        unsafe { storage.get_unchecked(std::mem::size_of::<NextLink>()..) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [MaybeUninit<u8>] {
        let Some(storage) = &mut self.0 else {
            return Default::default();
        };
        unsafe { storage.get_unchecked_mut(std::mem::size_of::<NextLink>()..) }
    }

    /// Assumes that the buffer is fully initialized and returns the data.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the buffer was fully initialized.
    pub unsafe fn assume_init(self) -> Data {
        self.0.map(Data).unwrap_or_default()
    }
}

/// Data extracted from an inbound frame.
#[derive(Clone)]
pub struct Data(Box<[MaybeUninit<u8>]>);

impl Data {
    #[cfg(test)]
    pub fn copy_from_slice(slice: &[u8]) -> Self {
        let mut buffer = Buffer::with_capactiy(slice.len());
        unsafe {
            buffer
                .as_mut_slice()
                .assume_init_mut()
                .copy_from_slice(slice);
            buffer.assume_init()
        }
    }
}

impl AsRef<[u8]> for Data {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            self.0
                .get_unchecked(std::mem::size_of::<NextLink>()..)
                .assume_init_ref()
        }
    }
}

impl From<Data> for Bytes {
    fn from(value: Data) -> Self {
        let data = unsafe { value.0.assume_init() };
        // impl From<Box<[u8]>> for Bytes is very cheap.
        // It does not allocate, only moves some pointers.
        let mut bytes = Bytes::from(data).try_into_mut().expect("buffer is unique");
        bytes.advance(std::mem::size_of::<NextLink>());
        bytes.freeze()
    }
}

impl Default for Data {
    fn default() -> Self {
        Self(Box::new_uninit_slice(std::mem::size_of::<NextLink>()))
    }
}

impl PartialEq for Data {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl Eq for Data {}

impl fmt::Debug for Data {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(f)
    }
}

/// Link to the next [`Data`] node in [`DataList`].
///
/// Link is stored in the first bytes of the slice owned by [`Data`],
/// and does not always have to be initialized. It is caller's responsibility to ensure
/// that the link is initialized when needed.
type NextLink = NonNull<[MaybeUninit<u8>]>;

/// Forward list of [`Data`] nodes.
#[derive(Default)]
pub struct DataList {
    head: Option<NonNull<[MaybeUninit<u8>]>>,
    tail: Option<NonNull<[MaybeUninit<u8>]>>,
}

impl DataList {
    /// Pops a node from the front of this list.
    ///
    /// This method is O(1) and very cheap.
    /// It only moves some pointers.
    pub fn pop_front(&mut self) -> Option<Data> {
        let node = Data(unsafe { Box::from_raw(self.head?.as_ptr()) });
        if self.head == self.tail {
            self.head = None;
            self.tail = None;
        } else {
            // If this node was not the tail, it's next link must be initialized.
            self.head = Some(unsafe { Self::next(node.0.as_ptr()) });
        }
        Some(node)
    }

    /// Pushes the given node to the back of this list.
    ///
    /// This method is O(1) and very cheap.
    /// It only moves some pointers.
    pub fn push_back(&mut self, buffer: Data) {
        let link = unsafe { NonNull::new_unchecked(Box::into_raw(buffer.0)) };
        match self.tail {
            Some(tail) => {
                unsafe {
                    Self::set_next(tail.as_ptr().cast(), link);
                }
                self.tail = Some(link);
            },
            None => {
                self.head = Some(link);
                self.tail = Some(link);
            },
        }
    }

    /// Reads the next link from the given [`Data`] storage.
    unsafe fn next(storage: *const MaybeUninit<u8>) -> NextLink {
        unsafe { storage.cast::<NextLink>().read_unaligned() }
    }

    /// Sets the next link in the given [`Data`] storage.
    unsafe fn set_next(storage: *mut MaybeUninit<u8>, link: NextLink) {
        unsafe {
            storage.cast::<NextLink>().write_unaligned(link);
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

    use crate::buffer::{Buffer, DataList};

    #[test]
    fn buffer_list() {
        let chunks: [&[u8]; 4] = [b"some", b"chunks", b"of", b"data"];
        let mut list = DataList::default();

        for chunk in chunks {
            let mut buffer = Buffer::with_capactiy(chunk.len());
            let storage = unsafe { buffer.as_mut_slice().assume_init_mut() };
            assert_eq!(storage.len(), chunk.len());
            storage.copy_from_slice(chunk);
            list.push_back(unsafe { buffer.assume_init() });
        }

        for chunk in chunks {
            let data = list.pop_front().unwrap();
            assert_eq!(Bytes::from(data), chunk);
        }

        assert!(list.pop_front().is_none());
    }
}
