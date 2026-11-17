use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering},
};

struct Allocator;

static COUNTS: [AtomicUsize; 32] = [const { AtomicUsize::new(0) }; 32];
static BYTES: [AtomicUsize; 32] = [const { AtomicUsize::new(0) }; 32];

fn class(size: usize) -> usize {
    (usize::BITS - size.saturating_sub(1).leading_zeros()).min(31) as usize
}

fn add(size: usize) {
    COUNTS[class(size)].fetch_add(1, Ordering::Relaxed);
    BYTES[class(size)].fetch_add(size, Ordering::Relaxed);
}

fn remove(size: usize) {
    COUNTS[class(size)].fetch_sub(1, Ordering::Relaxed);
    BYTES[class(size)].fetch_sub(size, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            add(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            add(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        remove(layout.size());
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let replacement = unsafe { System.realloc(pointer, layout, size) };
        if !replacement.is_null() {
            remove(layout.size());
            add(size);
        }
        replacement
    }
}

#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

pub(crate) fn report() {
    for index in 0..COUNTS.len() {
        let bytes = BYTES[index].load(Ordering::Relaxed);
        if bytes >= 1 << 20 {
            eprintln!(
                "native_allocation_class maximum_bytes={} count={} live_bytes={bytes}",
                1usize << index,
                COUNTS[index].load(Ordering::Relaxed)
            );
        }
    }
}
