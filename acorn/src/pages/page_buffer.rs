use std::{
    alloc::{alloc, dealloc, handle_alloc_error, Layout},
    cell::Cell,
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

    fn get_page_shared(&self, index: usize) -> Option<SharedPage> {
        debug_assert!(index < self.length);
        let meta = self.meta[index].as_ref()?;
        let data = unsafe { self.get_page_data(index) };
        Some(SharedPage::acquire(meta, data))
    }

    fn get_page_exclusive(&self, index: usize) -> Option<ExclusivePage> {
        debug_assert!(index < self.length);
        let meta = self.meta[index].as_ref()?;
        let data = unsafe { self.get_page_data(index) };
        Some(ExclusivePage::acquire(meta, data))
    }

    #[allow(clippy::mut_from_ref)]
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
