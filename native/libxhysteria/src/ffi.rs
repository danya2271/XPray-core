//! Stable C ABI for the first bridge milestone.
//!
//! The rings are SPSC: one native producer/consumer pair per ring. The Go
//! adapter must serialize all publishers on its side and must not share a ring
//! between native workers.
#![allow(clippy::missing_safety_doc)]

use std::{
    cell::UnsafeCell,
    ptr,
    sync::atomic::{AtomicBool, AtomicU32, Ordering},
};

const MAX_CAPACITY: usize = 1 << 20;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct XhysEvent {
    pub event_type: u8,
    pub flags: u8,
    pub reserved: u16,
    pub stream_id: u64,
    pub slab_idx: u32,
    pub data_len: u32,
    pub aux: u32,
    pub error_code: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct XhysArenaView {
    pub data: *mut u8,
    pub len: u64,
}

struct Ring {
    head: AtomicU32,
    tail: AtomicU32,
    mask: u32,
    slots: Box<[UnsafeCell<XhysEvent>]>,
}

unsafe impl Send for Ring {}
unsafe impl Sync for Ring {}

impl Ring {
    fn new(capacity: usize) -> Option<Self> {
        if capacity == 0 || !capacity.is_power_of_two() || capacity > MAX_CAPACITY {
            return None;
        }
        let slots = (0..capacity)
            .map(|_| UnsafeCell::new(XhysEvent::default()))
            .collect();
        Some(Self {
            head: AtomicU32::new(0),
            tail: AtomicU32::new(0),
            mask: (capacity - 1) as u32,
            slots,
        })
    }

    fn push(&self, event: XhysEvent) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        if head.wrapping_sub(tail) > self.mask {
            return false;
        }
        // The SPSC contract guarantees that only the producer owns this slot.
        unsafe {
            *self.slots[(head & self.mask) as usize].get() = event;
        }
        self.head.store(head.wrapping_add(1), Ordering::Release);
        true
    }

    fn pop(&self) -> Option<XhysEvent> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        if tail == head {
            return None;
        }
        // The SPSC contract guarantees that the producer published this slot.
        let event = unsafe { *self.slots[(tail & self.mask) as usize].get() };
        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        Some(event)
    }
}

pub struct XhysBridge {
    commands: Ring,
    events: Ring,
    arena: Box<[u8]>,
    closed: AtomicBool,
    event_fd: i32,
}

#[cfg(target_os = "linux")]
fn create_event_fd() -> i32 {
    unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) }
}

#[cfg(not(target_os = "linux"))]
fn create_event_fd() -> i32 {
    -1
}

fn notify(fd: i32) {
    if fd < 0 {
        return;
    }
    #[cfg(target_os = "linux")]
    unsafe {
        let value: u64 = 1;
        let _ = libc::write(
            fd,
            (&value as *const u64).cast(),
            std::mem::size_of_val(&value),
        );
    }
}

fn close_event_fd(fd: i32) {
    if fd < 0 {
        return;
    }
    #[cfg(target_os = "linux")]
    unsafe {
        let _ = libc::close(fd);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn xhys_bridge_create(arena_bytes: u64, ring_capacity: u32) -> *mut XhysBridge {
    let arena_bytes = usize::try_from(arena_bytes).ok();
    let capacity = usize::try_from(ring_capacity).ok();
    let Some(arena_bytes) = arena_bytes else {
        return ptr::null_mut();
    };
    let Some(capacity) = capacity else {
        return ptr::null_mut();
    };
    if arena_bytes == 0 || arena_bytes > (1 << 34) {
        return ptr::null_mut();
    }
    let Some(commands) = Ring::new(capacity) else {
        return ptr::null_mut();
    };
    let Some(events) = Ring::new(capacity) else {
        return ptr::null_mut();
    };
    let Some(arena) = Vec::<u8>::new()
        .try_reserve_exact(arena_bytes)
        .ok()
        .map(|_| vec![0u8; arena_bytes].into_boxed_slice())
    else {
        return ptr::null_mut();
    };
    let bridge = XhysBridge {
        commands,
        events,
        arena,
        closed: AtomicBool::new(false),
        event_fd: create_event_fd(),
    };
    Box::into_raw(Box::new(bridge))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_bridge_destroy(bridge: *mut XhysBridge) {
    if bridge.is_null() {
        return;
    }
    // The caller must stop all producers and consumers before destroy.
    let bridge = unsafe { Box::from_raw(bridge) };
    close_event_fd(bridge.event_fd);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_bridge_arena(
    bridge: *mut XhysBridge,
    view: *mut XhysArenaView,
) -> i32 {
    if bridge.is_null() || view.is_null() {
        return -1;
    }
    let bridge = unsafe { &mut *bridge };
    unsafe {
        *view = XhysArenaView {
            data: bridge.arena.as_mut_ptr(),
            len: bridge.arena.len() as u64,
        };
    }
    0
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_bridge_submit_command(
    bridge: *mut XhysBridge,
    event: *const XhysEvent,
) -> i32 {
    if bridge.is_null() || event.is_null() {
        return -1;
    }
    let bridge = unsafe { &*bridge };
    if bridge.closed.load(Ordering::Acquire) {
        return -2;
    }
    let event = unsafe { *event };
    if bridge.commands.push(event) {
        notify(bridge.event_fd);
        0
    } else {
        1
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_bridge_next_command(
    bridge: *mut XhysBridge,
    event: *mut XhysEvent,
) -> i32 {
    if bridge.is_null() || event.is_null() {
        return -1;
    }
    let bridge = unsafe { &*bridge };
    match bridge.commands.pop() {
        Some(value) => {
            unsafe { *event = value };
            0
        }
        None => {
            if bridge.closed.load(Ordering::Acquire) {
                -2
            } else {
                1
            }
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_bridge_publish_event(
    bridge: *mut XhysBridge,
    event: *const XhysEvent,
) -> i32 {
    if bridge.is_null() || event.is_null() {
        return -1;
    }
    let bridge = unsafe { &*bridge };
    if bridge.closed.load(Ordering::Acquire) {
        return -2;
    }
    let event = unsafe { *event };
    if bridge.events.push(event) {
        notify(bridge.event_fd);
        0
    } else {
        1
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_bridge_next_event(
    bridge: *mut XhysBridge,
    event: *mut XhysEvent,
) -> i32 {
    if bridge.is_null() || event.is_null() {
        return -1;
    }
    let bridge = unsafe { &*bridge };
    match bridge.events.pop() {
        Some(value) => {
            unsafe { *event = value };
            0
        }
        None => {
            if bridge.closed.load(Ordering::Acquire) {
                -2
            } else {
                1
            }
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_bridge_event_fd(bridge: *mut XhysBridge) -> i32 {
    if bridge.is_null() {
        return -1;
    }
    unsafe { (&*bridge).event_fd }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_bridge_dup_event_fd(bridge: *mut XhysBridge) -> i32 {
    if bridge.is_null() {
        return -1;
    }
    #[cfg(target_os = "linux")]
    unsafe {
        libc::dup((&*bridge).event_fd)
    }
    #[cfg(not(target_os = "linux"))]
    {
        -1
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_bridge_close(bridge: *mut XhysBridge) {
    if bridge.is_null() {
        return;
    }
    let bridge = unsafe { &*bridge };
    bridge.closed.store(true, Ordering::Release);
    notify(bridge.event_fd);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_spsc_rings_preserve_order_and_backpressure() {
        let bridge = xhys_bridge_create(4096, 2);
        assert!(!bridge.is_null());
        let first = XhysEvent {
            stream_id: 1,
            ..XhysEvent::default()
        };
        let second = XhysEvent {
            stream_id: 2,
            ..XhysEvent::default()
        };
        let third = XhysEvent {
            stream_id: 3,
            ..XhysEvent::default()
        };
        unsafe {
            assert_eq!(xhys_bridge_submit_command(bridge, &first), 0);
            assert_eq!(xhys_bridge_submit_command(bridge, &second), 0);
            assert_eq!(xhys_bridge_submit_command(bridge, &third), 1);
            let mut got = XhysEvent::default();
            assert_eq!(xhys_bridge_next_command(bridge, &mut got), 0);
            assert_eq!(got.stream_id, 1);
            assert_eq!(xhys_bridge_submit_command(bridge, &third), 0);
            xhys_bridge_close(bridge);
            assert_eq!(xhys_bridge_next_command(bridge, &mut got), 0);
            assert_eq!(got.stream_id, 2);
            assert_eq!(xhys_bridge_next_command(bridge, &mut got), 0);
            assert_eq!(got.stream_id, 3);
            assert_eq!(xhys_bridge_next_command(bridge, &mut got), -2);
            xhys_bridge_destroy(bridge);
        }
    }
}
