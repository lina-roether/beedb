use std::{
    alloc::{alloc, dealloc, handle_alloc_error, Layout},
    cell::Cell,
    fmt::{self, Debug},
    iter,
    ptr::NonNull,
    slice,
    sync::atomic::{AtomicUsize, Ordering},
};

use parking_lot::{lock_api::RawRwLock as _, RawRwLock};

const PAGE_ALIGN: usize = 8;

const fn page_layout(content_size: usize) -> Layout {
    unsafe { Layout::from_size_align_unchecked(content_size, PAGE_ALIGN) }
}

#[derive(Debug, Default, Clone, Copy)]
#[repr(u8)]
enum PageStatus {
    #[default]
    Clean,
    InUse,
    Dirty,
}

struct PageMeta {
    ref_count: AtomicUsize,
    lock: RawRwLock,
    status: Cell<PageStatus>,
}

impl PageMeta {
    fn new(ref_count: usize, page_status: PageStatus) -> Self {
        Self {
            ref_count: AtomicUsize::new(ref_count),
            lock: RawRwLock::INIT,
            status: Cell::new(page_status),
        }
    }
}

impl Default for PageMeta {
    fn default() -> Self {
        Self::new(0, PageStatus::default())
    }
}

impl Debug for PageMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut page_meta = f.debug_struct("PageMeta");
        page_meta.field("ref_count", &self.ref_count);
        if self.lock.is_locked_exclusive() {
            page_meta.field("lock", &"<locked exclusive>");
        } else if self.lock.is_locked() {
            page_meta.field("lock", &"<locked shared>");
        } else {
            page_meta.field("lock", &"<unlocked>");
        }
        page_meta.field("status", &self.status);
        page_meta.finish()
    }
}

#[derive(Debug)]
pub struct SharedPage<'a> {
    meta: &'a PageMeta,
    pub data: &'a [u8],
}

impl<'a> SharedPage<'a> {
    fn acquire(meta: &'a PageMeta, data: &'a [u8]) -> Self {
        meta.lock.lock_shared();
        meta.ref_count.fetch_add(1, Ordering::Relaxed);
        Self { meta, data }
    }
}

impl<'a> Clone for SharedPage<'a> {
    fn clone(&self) -> Self {
        Self::acquire(self.meta, self.data)
    }
}

impl<'a> Drop for SharedPage<'a> {
    fn drop(&mut self) {
        self.meta.ref_count.fetch_sub(1, Ordering::Relaxed);
        unsafe { self.meta.lock.unlock_shared() };
    }
}

pub struct ExclusivePage<'a> {
    meta: &'a PageMeta,
    pub data: &'a mut [u8],
}

impl<'a> ExclusivePage<'a> {
    fn acquire(meta: &'a PageMeta, data: &'a mut [u8]) -> Self {
        meta.lock.lock_exclusive();
        meta.ref_count.fetch_add(1, Ordering::Relaxed);
        meta.status.set(PageStatus::InUse);
        Self { meta, data }
    }
}

impl<'a> Drop for ExclusivePage<'a> {
    fn drop(&mut self) {
        self.meta.status.set(PageStatus::Dirty);
        self.meta.ref_count.fetch_sub(1, Ordering::Relaxed);
        unsafe { self.meta.lock.unlock_exclusive() };
    }
}

struct PageBuffer {
    length: usize,
    buffer_layout: Layout,
    page_size: usize,
    page_size_padded: usize,
    buffer: NonNull<u8>,
    meta: Box<[Option<PageMeta>]>,
}

impl PageBuffer {
    fn new(page_content_size: usize, length: usize) -> Self {
        let page_layout = page_layout(page_content_size);
        let (buffer_layout, page_size_padded) = page_layout
            .repeat(length)
            .expect("It seems someone thinks they have infinite memory...");

        let buffer = unsafe {
            let ptr = alloc(buffer_layout);
            NonNull::new(ptr).unwrap_or_else(|| handle_alloc_error(buffer_layout))
        };

        let meta = iter::repeat_with(|| None).take(length).collect();

        Self {
            length,
            buffer_layout,
            page_size: page_layout.size(),
            page_size_padded,
            buffer,
            meta,
        }
    }

    #[inline]
    pub fn get_page_shared(&self, index: usize) -> Option<SharedPage> {
        debug_assert!(index < self.length);
        let meta = self.meta[index].as_ref()?;
        let data = unsafe { self.get_page_data(index) };
        Some(SharedPage::acquire(meta, data))
    }

    #[inline]
    pub fn get_page_exclusive(&self, index: usize) -> Option<ExclusivePage> {
        debug_assert!(index < self.length);
        let meta = self.meta[index].as_ref()?;
        let data = unsafe { self.get_page_data(index) };
        Some(ExclusivePage::acquire(meta, data))
    }

    pub fn get_empty_page_mut(&mut self, index: usize) -> Option<&mut [u8]> {
        debug_assert!(index < self.length);
        if self.meta[index].is_some() {
            return None;
        }
        Some(unsafe { self.get_page_data(index) })
    }

    pub fn set_filled(&mut self, index: usize) {
        debug_assert!(index < self.length);
        debug_assert!(self.meta[index].is_none());
        self.meta[index] = Some(PageMeta::default())
    }

    #[allow(clippy::mut_from_ref)]
    #[inline]
    unsafe fn get_page_data(&self, index: usize) -> &mut [u8] {
        slice::from_raw_parts_mut(
            self.buffer.as_ptr().add(index * self.page_size_padded),
            self.page_size,
        )
    }
}

impl Drop for PageBuffer {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.buffer.as_ptr(), self.buffer_layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::*;

    #[test]
    fn page_buffer_construction() {
        let _buffer = PageBuffer::new(69, 420);
    }

    #[test]
    fn try_acquire_empty_page() {
        let buffer = PageBuffer::new(69, 420);
        assert!(buffer.get_page_shared(4).is_none());
        assert!(buffer.get_page_shared(4).is_none());
    }

    #[test]
    fn shared_page_read() {
        let mut buffer = PageBuffer::new(size_of::<u32>(), 4);

        {
            let page_mut = buffer.get_empty_page_mut(3).unwrap();
            page_mut.clone_from_slice(&69_i32.to_ne_bytes());
            buffer.set_filled(3);
        }

        let shared_page = buffer.get_page_shared(3).unwrap();
        assert_eq!(shared_page.data, 69_i32.to_ne_bytes());
    }

    #[test]
    fn exclusive_page_read() {
        let mut buffer = PageBuffer::new(size_of::<u32>(), 4);

        {
            let page_mut = buffer.get_empty_page_mut(3).unwrap();
            page_mut.clone_from_slice(&69_i32.to_ne_bytes());
            buffer.set_filled(3);
        }

        let exclusive_page = buffer.get_page_exclusive(3).unwrap();
        assert_eq!(exclusive_page.data, 69_i32.to_ne_bytes());
    }
}
