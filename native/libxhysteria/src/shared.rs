//! Bounded frame rings with serialized publishers. Only setup/teardown use C.
//!
//! The Go peer uses aligned 32-bit atomics over this little-endian storage ABI.
//! Payload leases outlive descriptor consumption; release returns independent slots.

use crate::h3_transport::Result;
use bytes::Bytes;
use std::{
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};
use tokio::{io::unix::AsyncFd, sync::Notify};

pub const MAGIC: u64 = 0x5848595353484d32;
pub const VERSION: u32 = 6;
pub const CAPACITY: u32 = 8;
pub const MAX_FRAME: usize = 65536 + 4096;
pub const STRIDE: usize = (16 + MAX_FRAME + 63) & !63;
pub const HEADER: usize = 320;
pub const PAYLOAD_SLOTS: usize = 16;
pub const PAYLOAD_SIZE: usize = 16384;
pub const RINGS_BYTES: usize = HEADER + 2 * CAPACITY as usize * STRIDE;
pub const ARENA_BYTES: usize = RINGS_BYTES + 2 * PAYLOAD_SLOTS * PAYLOAD_SIZE;
const PAYLOAD_FLAG: u32 = 1 << 8;

#[repr(C, align(64))]
struct Line {
    value: AtomicU32,
    pad: [u8; 60],
}

#[repr(C, align(64))]
struct Header {
    magic: u64,
    version: u32,
    capacity: u32,
    stride: u32,
    max_frame: u32,
    arena_bytes: u64,
    closed: AtomicU32,
    go_pending: AtomicU32,
    native_pending: AtomicU32,
    space_pending: AtomicU32,
    event_waiting: AtomicU32,
    command_waiting: AtomicU32,
    command_free: AtomicU32,
    event_free: AtomicU32,
    command_head: Line,
    command_tail: Line,
    event_head: Line,
    event_tail: Line,
}

#[repr(C)]
#[derive(Default)]
pub struct View {
    pub data: *mut u8,
    pub len: u64,
    pub notify_fd: i32,
    pub wake_fd: i32,
    pub space_fd: i32,
    pub owner: *mut std::ffi::c_void,
}

pub struct Bridge {
    data: NonNull<u8>,
    go: OwnedFd,
    native: OwnedFd,
    space: OwnedFd,
    event_writer: std::sync::Mutex<()>,
    pub changed: Notify,
}

// The allocation has interior mutability. Each slot has exactly one owner
// established by ring publication and payload leases; peers do not mutate held leases.
unsafe impl Send for Bridge {}
unsafe impl Sync for Bridge {}

fn eventfd() -> io::Result<OwnedFd> {
    let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn signal(fd: i32) {
    let value = 1u64;
    loop {
        let n = unsafe { libc::write(fd, (&value as *const u64).cast(), 8) };
        if n >= 0 || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            // EAGAIN means that this recipient already has a pending wake.
            break;
        }
    }
}

impl Bridge {
    pub fn new() -> io::Result<Arc<Self>> {
        let go = eventfd()?;
        let native = eventfd()?;
        let space = eventfd()?;
        let mapping = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                ARENA_BYTES,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if mapping == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        if unsafe { libc::madvise(mapping, ARENA_BYTES, libc::MADV_NOHUGEPAGE) } != 0 {
            let error = io::Error::last_os_error();
            if !matches!(error.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) {
                unsafe { libc::munmap(mapping, ARENA_BYTES) };
                return Err(error);
            }
        }
        let data = NonNull::new(mapping.cast::<u8>()).unwrap();
        unsafe {
            data.cast::<Header>().as_ptr().write(Header {
                magic: MAGIC,
                version: VERSION,
                capacity: CAPACITY,
                stride: STRIDE as u32,
                max_frame: MAX_FRAME as u32,
                arena_bytes: ARENA_BYTES as u64,
                closed: AtomicU32::new(0),
                go_pending: AtomicU32::new(0),
                native_pending: AtomicU32::new(0),
                space_pending: AtomicU32::new(0),
                event_waiting: AtomicU32::new(0),
                command_waiting: AtomicU32::new(0),
                command_free: AtomicU32::new((1 << PAYLOAD_SLOTS) - 1),
                event_free: AtomicU32::new((1 << PAYLOAD_SLOTS) - 1),
                command_head: Line {
                    value: AtomicU32::new(0),
                    pad: [0; 60],
                },
                command_tail: Line {
                    value: AtomicU32::new(0),
                    pad: [0; 60],
                },
                event_head: Line {
                    value: AtomicU32::new(0),
                    pad: [0; 60],
                },
                event_tail: Line {
                    value: AtomicU32::new(0),
                    pad: [0; 60],
                },
            });
        }
        Ok(Arc::new(Self {
            data,
            go,
            native,
            space,
            event_writer: std::sync::Mutex::new(()),
            changed: Notify::new(),
        }))
    }

    fn header(&self) -> &Header {
        unsafe { &*self.data.cast::<Header>().as_ptr() }
    }

    pub fn view(self: &Arc<Self>) -> io::Result<View> {
        let duplicate = |fd: i32| -> io::Result<OwnedFd> {
            let fd = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(unsafe { OwnedFd::from_raw_fd(fd) })
        };
        let notify = duplicate(self.go.as_raw_fd())?;
        let wake = duplicate(self.native.as_raw_fd())?;
        let space = duplicate(self.space.as_raw_fd())?;
        use std::os::fd::IntoRawFd;
        Ok(View {
            data: self.data.as_ptr(),
            len: ARENA_BYTES as u64,
            notify_fd: notify.into_raw_fd(),
            wake_fd: wake.into_raw_fd(),
            space_fd: space.into_raw_fd(),
            owner: Box::into_raw(Box::new(self.clone())).cast(),
        })
    }

    pub fn closed(&self) -> bool {
        self.header().closed.load(Ordering::Acquire) != 0
    }

    pub fn close(&self) {
        self.header().closed.store(1, Ordering::Release);
        signal(self.go.as_raw_fd());
        signal(self.native.as_raw_fd());
        signal(self.space.as_raw_fd());
        self.changed.notify_waiters();
    }

    pub fn notify_go(&self) {
        if self.header().go_pending.swap(1, Ordering::AcqRel) == 0 {
            signal(self.go.as_raw_fd());
        }
    }

    pub fn notify_space(&self) {
        if self.header().command_waiting.load(Ordering::SeqCst) != 0
            && self.header().command_waiting.swap(0, Ordering::SeqCst) != 0
            && self.header().space_pending.swap(1, Ordering::AcqRel) == 0
        {
            signal(self.space.as_raw_fd());
        }
    }

    pub async fn send(&self, kind: u8, id: u64, data: &[u8]) -> Result<()> {
        self.send_vectored(kind, id, &[data]).await
    }

    pub async fn send_vectored(&self, kind: u8, id: u64, parts: &[&[u8]]) -> Result<()> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            let published = {
                // Serialize only the ring publication, never an async wait.
                // This keeps one producer without an extra task/queue hop.
                let _writer = self.event_writer.lock().unwrap();
                if self.closed() {
                    return Err("Go bridge closed".into());
                }
                self.push_vectored(false, kind, id, parts)?
            };
            self.notify_go();
            if published {
                return Ok(());
            }
            changed.await;
        }
    }

    pub fn notification_fd(&self) -> Result<AsyncFd<OwnedFd>> {
        let fd = unsafe { libc::fcntl(self.native.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error().into());
        }
        // OwnedFd stays alive and its raw descriptor is never replaced while
        // registered with this reactor.
        Ok(unsafe { AsyncFd::register(OwnedFd::from_raw_fd(fd)) }?)
    }

    pub async fn notification(&self, fd: &AsyncFd<OwnedFd>) -> Result<()> {
        loop {
            let mut ready = fd.readable().await?;
            let read = ready.try_io(|fd| {
                let mut value = 0u64;
                let n = unsafe { libc::read(fd.as_raw_fd(), (&mut value as *mut u64).cast(), 8) };
                if n == 8 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            });
            if matches!(read, Ok(Ok(()))) {
                // Acquire all changes published before the coalesced wake,
                // then arm the next one before notifying ring waiters.
                self.header().native_pending.swap(0, Ordering::AcqRel);
                self.changed.notify_waiters();
                return Ok(());
            } else if let Ok(Err(error)) = read {
                return Err(error.into());
            }
        }
    }

    fn slot(&self, commands: bool, index: u32) -> *mut u8 {
        let ring = if commands {
            0
        } else {
            CAPACITY as usize * STRIDE
        };
        unsafe {
            self.data
                .as_ptr()
                .add(HEADER + ring + (index & (CAPACITY - 1)) as usize * STRIDE)
        }
    }

    fn free_slots(&self, commands: bool) -> &AtomicU32 {
        if commands {
            &self.header().command_free
        } else {
            &self.header().event_free
        }
    }

    fn payload_pointer(&self, commands: bool, slot: usize) -> *mut u8 {
        let offset = RINGS_BYTES + (usize::from(!commands) * PAYLOAD_SLOTS + slot) * PAYLOAD_SIZE;
        unsafe { self.data.as_ptr().add(offset) }
    }

    fn acquire_payload(&self, commands: bool) -> Option<usize> {
        let free = self.free_slots(commands);
        let mut available = free.load(Ordering::Acquire);
        while available != 0 {
            let slot = available.trailing_zeros() as usize;
            match free.compare_exchange_weak(
                available,
                available & !(1 << slot),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(slot),
                Err(current) => available = current,
            }
        }
        None
    }

    pub fn pop(self: &Arc<Self>, commands: bool) -> Result<Option<(u8, u64, Bytes)>> {
        let header = self.header();
        let (head, tail) = if commands {
            (&header.command_head.value, &header.command_tail.value)
        } else {
            (&header.event_head.value, &header.event_tail.value)
        };
        let index = tail.load(Ordering::Relaxed);
        let count = head.load(Ordering::Acquire).wrapping_sub(index);
        if count == 0 {
            return Ok(None);
        }
        if count > CAPACITY {
            return Err("corrupt shared ring indices".into());
        }
        let slot = self.slot(commands, index);
        let kind = unsafe { slot.cast::<u32>().read() };
        let len = unsafe { slot.add(4).cast::<u32>().read() } as usize;
        let id = unsafe { slot.add(8).cast::<u64>().read() };
        let leased = kind & PAYLOAD_FLAG != 0;
        let payload_slot = (kind >> 16) as usize;
        if kind & 0xfe00 != 0
            || len > MAX_FRAME
            || (leased
                && (payload_slot >= PAYLOAD_SLOTS
                    || len > PAYLOAD_SIZE
                    || self.free_slots(commands).load(Ordering::Acquire) & (1 << payload_slot)
                        != 0))
            || (!leased && kind > u8::MAX as u32)
        {
            return Err("invalid shared frame descriptor".into());
        }
        let data = if leased {
            Bytes::from_owner(Payload {
                bridge: self.clone(),
                commands,
                slot: payload_slot,
                len,
            })
        } else {
            Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(slot.add(16), len) })
        };
        tail.store(index.wrapping_add(1), Ordering::SeqCst);
        Ok(Some((kind as u8, id, data)))
    }

    pub fn push(&self, commands: bool, kind: u8, id: u64, data: &[u8]) -> Result<bool> {
        let _writer = self.event_writer.lock().unwrap();
        if self.closed() {
            return Err("Go bridge closed".into());
        }
        self.push_vectored(commands, kind, id, &[data])
    }

    fn push_vectored(&self, commands: bool, kind: u8, id: u64, parts: &[&[u8]]) -> Result<bool> {
        let len: usize = parts.iter().map(|part| part.len()).sum();
        if len > MAX_FRAME {
            return Err("oversized shared frame".into());
        }
        let header = self.header();
        let (head, tail) = if commands {
            (&header.command_head.value, &header.command_tail.value)
        } else {
            (&header.event_head.value, &header.event_tail.value)
        };
        let index = head.load(Ordering::Relaxed);
        let mut count = index.wrapping_sub(tail.load(Ordering::Acquire));
        if count > CAPACITY {
            return Err("corrupt shared ring indices".into());
        }
        if count == CAPACITY {
            if !commands {
                header.event_waiting.store(1, Ordering::SeqCst);
                count = index.wrapping_sub(tail.load(Ordering::SeqCst));
            }
            if count == CAPACITY {
                return Ok(false);
            }
        }
        let slot = self.slot(commands, index);
        let payload_slot = if kind == 3 || kind == 4 {
            if len != 0 && len <= PAYLOAD_SIZE {
                self.acquire_payload(commands)
            } else {
                None
            }
        } else {
            None
        };
        let encoded_kind =
            u32::from(kind) | payload_slot.map_or(0, |slot| PAYLOAD_FLAG | ((slot as u32) << 16));
        let payload = if let Some(payload_slot) = payload_slot {
            self.payload_pointer(commands, payload_slot)
        } else {
            unsafe { slot.add(16) }
        };
        unsafe {
            slot.cast::<u32>().write(encoded_kind);
            slot.add(4).cast::<u32>().write(len as u32);
            slot.add(8).cast::<u64>().write(id);
            let mut offset = 0;
            for part in parts {
                std::ptr::copy_nonoverlapping(part.as_ptr(), payload.add(offset), part.len());
                offset += part.len();
            }
        }
        head.store(index.wrapping_add(1), Ordering::Release);
        Ok(true)
    }
}

struct Payload {
    bridge: Arc<Bridge>,
    commands: bool,
    slot: usize,
    len: usize,
}

impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.bridge.payload_pointer(self.commands, self.slot),
                self.len,
            )
        }
    }
}

impl Drop for Payload {
    fn drop(&mut self) {
        self.bridge
            .free_slots(self.commands)
            .fetch_or(1 << self.slot, Ordering::Release);
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_shared_release(owner: *mut std::ffi::c_void) {
    if !owner.is_null() {
        drop(unsafe { Box::from_raw(owner.cast::<Arc<Bridge>>()) });
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.data.as_ptr().cast(), ARENA_BYTES);
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arena_is_zeroed_aligned_and_released_with_last_payload() {
        let bridge = Bridge::new().unwrap();
        assert_eq!(bridge.data.as_ptr() as usize % 64, 0);
        let storage = unsafe { std::slice::from_raw_parts(bridge.data.as_ptr(), ARENA_BYTES) };
        assert!(storage[HEADER..].iter().all(|&value| value == 0));
        let owner = Arc::downgrade(&bridge);
        assert!(bridge.push(true, 3, 1, b"retained").unwrap());
        let (_, _, payload) = bridge.pop(true).unwrap().unwrap();
        drop(bridge);
        assert!(owner.upgrade().is_some());
        assert_eq!(payload.as_ref(), b"retained");
        drop(payload);
        assert!(owner.upgrade().is_none());
    }

    #[test]
    fn fresh_arena_does_not_commit_unused_payload_pages() {
        let bridge = Bridge::new().unwrap();
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let mut resident = vec![0; ARENA_BYTES.div_ceil(page_size)];
        assert_eq!(
            unsafe {
                libc::mincore(
                    bridge.data.as_ptr().cast(),
                    ARENA_BYTES,
                    resident.as_mut_ptr(),
                )
            },
            0
        );
        assert_ne!(resident[0] & 1, 0);
        assert!(
            resident.iter().filter(|&&page| page & 1 != 0).count() <= HEADER.div_ceil(page_size)
        );
    }

    #[test]
    fn rejects_free_out_of_range_and_reserved_payload_leases() {
        for kind in [
            3 | PAYLOAD_FLAG,
            3 | PAYLOAD_FLAG | ((PAYLOAD_SLOTS as u32) << 16),
            3 | (1 << 9),
        ] {
            let bridge = Bridge::new().unwrap();
            let descriptor = bridge.slot(true, 0);
            unsafe {
                descriptor.cast::<u32>().write(kind);
                descriptor.add(4).cast::<u32>().write(1);
            }
            bridge
                .header()
                .command_head
                .value
                .store(1, Ordering::Release);
            assert!(bridge.pop(true).is_err());
            assert_eq!(
                bridge.header().command_tail.value.load(Ordering::Acquire),
                0
            );
        }
    }

    #[test]
    fn payload_leases_support_concurrent_release_and_publication() {
        let bridge = Bridge::new().unwrap();
        let producers: Vec<_> = (0u64..4)
            .map(|producer| {
                let bridge = bridge.clone();
                std::thread::spawn(move || {
                    for sequence in 0..1024 {
                        let id = producer * 1024 + sequence;
                        let payload = id.to_le_bytes();
                        while !bridge.push(false, 3, id, &payload).unwrap() {
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();
        let (sender, receiver) = std::sync::mpsc::sync_channel::<(u64, Bytes)>(PAYLOAD_SLOTS);
        let consumer = std::thread::spawn(move || {
            for (id, payload) in receiver {
                assert_eq!(payload.as_ref(), id.to_le_bytes());
            }
        });
        for _ in 0..4096 {
            loop {
                if let Some((kind, id, payload)) = bridge.pop(false).unwrap() {
                    assert_eq!(kind, 3);
                    sender.send((id, payload)).unwrap();
                    break;
                }
                std::thread::yield_now();
            }
        }
        drop(sender);
        for producer in producers {
            producer.join().unwrap();
        }
        consumer.join().unwrap();
        assert_eq!(
            bridge.header().event_free.load(Ordering::Acquire),
            (1 << PAYLOAD_SLOTS) - 1
        );
    }

    #[test]
    fn leased_payload_survives_descriptor_reuse_and_releases_after_last_clone() {
        let bridge = Bridge::new().unwrap();
        assert!(bridge.push(true, 3, 1, b"owned payload").unwrap());
        let (_, _, payload) = bridge.pop(true).unwrap().unwrap();
        assert_eq!(payload.as_ptr(), bridge.payload_pointer(true, 0));
        let clone = payload.clone();
        for id in 2..40 {
            assert!(bridge.push(true, 3, id, b"replacement").unwrap());
            drop(bridge.pop(true).unwrap().unwrap());
        }
        assert_eq!(payload.as_ref(), b"owned payload");
        drop(payload);
        assert_eq!(bridge.header().command_free.load(Ordering::Acquire) & 1, 0);
        drop(clone);
        assert_ne!(bridge.header().command_free.load(Ordering::Acquire) & 1, 0);
    }

    #[test]
    fn exhausted_payload_pool_falls_back_without_blocking_other_frames() {
        let bridge = Bridge::new().unwrap();
        let mut held = Vec::new();
        for id in 0..PAYLOAD_SLOTS {
            assert!(bridge.push(true, 3, id as u64, b"held").unwrap());
            held.push(bridge.pop(true).unwrap().unwrap().2);
        }
        assert_eq!(bridge.header().command_free.load(Ordering::Acquire), 0);
        assert!(bridge.push(true, 3, 99, b"fallback").unwrap());
        let (_, _, payload) = bridge.pop(true).unwrap().unwrap();
        assert_eq!(payload.as_ref(), b"fallback");
        let address = payload.as_ptr() as usize;
        let start = bridge.data.as_ptr() as usize;
        assert!(address < start || address >= start + ARENA_BYTES);
        assert!(bridge.push(true, 5, 99, b"").unwrap());
        assert_eq!(bridge.pop(true).unwrap().unwrap().0, 5);
        drop(held);
        assert_eq!(
            bridge.header().command_free.load(Ordering::Acquire),
            (1 << PAYLOAD_SLOTS) - 1
        );
    }

    #[test]
    fn payload_owner_keeps_arena_alive_after_bridge_shutdown() {
        let bridge = Bridge::new().unwrap();
        let weak = Arc::downgrade(&bridge);
        assert!(bridge.push(true, 3, 1, b"survives close").unwrap());
        let payload = bridge.pop(true).unwrap().unwrap().2;
        bridge.close();
        drop(bridge);
        assert_eq!(payload.as_ref(), b"survives close");
        assert!(weak.upgrade().is_some());
        drop(payload);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn command_space_notification_requires_a_waiting_writer() {
        let bridge = Bridge::new().unwrap();
        let read = || {
            let mut value = 0u64;
            let count =
                unsafe { libc::read(bridge.space.as_raw_fd(), (&mut value as *mut u64).cast(), 8) };
            (count, value)
        };
        bridge.notify_space();
        assert_eq!(read().0, -1);
        bridge.header().command_waiting.store(1, Ordering::SeqCst);
        bridge.notify_space();
        assert_eq!(read(), (8, 1));
        assert_eq!(bridge.header().command_waiting.load(Ordering::SeqCst), 0);
        bridge.notify_space();
        assert_eq!(read().0, -1);
    }

    #[test]
    fn full_event_ring_arms_its_writer_before_waiting() {
        let bridge = Bridge::new().unwrap();
        for id in 0..CAPACITY {
            assert!(bridge.push(false, 3, u64::from(id), b"payload").unwrap());
        }
        assert_eq!(bridge.header().event_waiting.load(Ordering::SeqCst), 0);
        assert!(!bridge.push(false, 3, 99, b"blocked").unwrap());
        assert_eq!(bridge.header().event_waiting.load(Ordering::SeqCst), 1);
        bridge.pop(false).unwrap().unwrap();
        assert!(bridge.push(false, 3, 99, b"unblocked").unwrap());
    }

    #[tokio::test]
    async fn vectored_payload_publishes_one_frame_without_changing_bytes() {
        let bridge = Bridge::new().unwrap();
        let parts = [b"prefix".as_slice(), b"", b"payload", b"suffix"];
        bridge.send_vectored(3, 17, &parts).await.unwrap();
        let (kind, id, data) = bridge.pop(false).unwrap().unwrap();
        assert_eq!(
            (kind, id, data.as_ref()),
            (3, 17, b"prefixpayloadsuffix".as_slice())
        );
        assert!(bridge.pop(false).unwrap().is_none());
        let oversized = vec![0; MAX_FRAME];
        assert!(
            bridge
                .send_vectored(3, 18, &[&oversized, b"x"])
                .await
                .is_err()
        );
    }

    #[test]
    fn layout_wraparound_and_full_rings() {
        assert_eq!(std::mem::size_of::<Header>(), HEADER);
        assert_eq!(std::mem::offset_of!(Header, closed), 32);
        assert_eq!(std::mem::offset_of!(Header, go_pending), 36);
        assert_eq!(std::mem::offset_of!(Header, native_pending), 40);
        assert_eq!(std::mem::offset_of!(Header, space_pending), 44);
        assert_eq!(std::mem::offset_of!(Header, event_waiting), 48);
        assert_eq!(std::mem::offset_of!(Header, command_waiting), 52);
        assert_eq!(std::mem::offset_of!(Header, command_head), 64);
        assert_eq!(std::mem::offset_of!(Header, command_tail), 128);
        assert_eq!(std::mem::offset_of!(Header, event_head), 192);
        assert_eq!(std::mem::offset_of!(Header, event_tail), 256);
        let b = Bridge::new().unwrap();
        for commands in [false, true] {
            let h = b.header();
            let (head, tail) = if commands {
                (&h.command_head.value, &h.command_tail.value)
            } else {
                (&h.event_head.value, &h.event_tail.value)
            };
            head.store(u32::MAX - 7, Ordering::Relaxed);
            tail.store(u32::MAX - 7, Ordering::Relaxed);
            for id in 0..CAPACITY {
                assert!(b.push(commands, 3, id as u64, b"payload").unwrap());
            }
            assert!(!b.push(commands, 3, 99, b"full").unwrap());
            for id in 0..CAPACITY {
                let (kind, got, payload) = b.pop(commands).unwrap().unwrap();
                assert_eq!(
                    (kind, got, payload.as_ref()),
                    (3, id as u64, b"payload".as_slice())
                );
            }
            assert!(b.pop(commands).unwrap().is_none());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_publishers_preserve_ownership_order_and_wakeups() {
        let bridge = Bridge::new().unwrap();
        let mut producers = tokio::task::JoinSet::new();
        for producer in 0..8u64 {
            let bridge = bridge.clone();
            producers.spawn(async move {
                for sequence in 0..256u64 {
                    let id = producer * 256 + sequence;
                    bridge.send(3, id, &id.to_be_bytes()).await.unwrap();
                }
            });
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut sequences = [0u64; 8];
            for _ in 0..2048 {
                loop {
                    if let Some((kind, id, data)) = bridge.pop(false).unwrap() {
                        let producer = (id / 256) as usize;
                        assert_eq!(kind, 3);
                        assert_eq!(id % 256, sequences[producer]);
                        sequences[producer] += 1;
                        assert_eq!(data.as_ref(), &id.to_be_bytes());
                        bridge.changed.notify_waiters();
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }
            assert_eq!(sequences, [256; 8]);
            while let Some(result) = producers.join_next().await {
                result.unwrap();
            }
        })
        .await
        .unwrap();
    }
}
