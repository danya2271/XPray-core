//! Bounded SPSC frame rings. Only setup/teardown use the C ABI.
//!
//! The Go peer uses aligned 32-bit atomics over this little-endian storage ABI.
//! Payload access is exclusive between head publication and tail release.

use crate::h3_transport::Result;
use bytes::Bytes;
use std::{
    alloc::{Layout, alloc_zeroed, dealloc},
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
pub const VERSION: u32 = 2;
pub const CAPACITY: u32 = 32;
pub const MAX_FRAME: usize = 65536 + 4096;
pub const STRIDE: usize = (16 + MAX_FRAME + 63) & !63;
pub const HEADER: usize = 320;
pub const ARENA_BYTES: usize = HEADER + 2 * CAPACITY as usize * STRIDE;

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
    pad: [u8; 28],
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
}

pub struct Bridge {
    data: NonNull<u8>,
    go: OwnedFd,
    native: OwnedFd,
    pub changed: Notify,
}

// The allocation has interior mutability. Each slot has exactly one owner
// established by the ring indices; no Rust reference spans a Go-owned payload.
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
        let layout = Layout::from_size_align(ARENA_BYTES, 64).unwrap();
        let data = NonNull::new(unsafe { alloc_zeroed(layout) })
            .ok_or_else(|| io::Error::other("native shared arena allocation failed"))?;
        unsafe {
            data.cast::<Header>().as_ptr().write(Header {
                magic: MAGIC,
                version: VERSION,
                capacity: CAPACITY,
                stride: STRIDE as u32,
                max_frame: MAX_FRAME as u32,
                arena_bytes: ARENA_BYTES as u64,
                closed: AtomicU32::new(0),
                pad: [0; 28],
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
            changed: Notify::new(),
        }))
    }

    fn header(&self) -> &Header {
        unsafe { &*self.data.cast::<Header>().as_ptr() }
    }

    pub fn view(&self) -> io::Result<View> {
        let duplicate = |fd: i32| -> io::Result<OwnedFd> {
            let fd = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(unsafe { OwnedFd::from_raw_fd(fd) })
        };
        let notify = duplicate(self.go.as_raw_fd())?;
        let wake = duplicate(self.native.as_raw_fd())?;
        use std::os::fd::IntoRawFd;
        Ok(View {
            data: self.data.as_ptr(),
            len: ARENA_BYTES as u64,
            notify_fd: notify.into_raw_fd(),
            wake_fd: wake.into_raw_fd(),
        })
    }

    pub fn closed(&self) -> bool {
        self.header().closed.load(Ordering::Acquire) != 0
    }

    pub fn close(&self) {
        self.header().closed.store(1, Ordering::Release);
        signal(self.go.as_raw_fd());
        signal(self.native.as_raw_fd());
        self.changed.notify_waiters();
    }

    pub fn notify_go(&self) {
        signal(self.go.as_raw_fd());
    }

    pub async fn notifications(self: Arc<Self>) -> Result<()> {
        let fd = unsafe { libc::fcntl(self.native.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error().into());
        }
        // OwnedFd stays alive and its raw descriptor is never replaced while
        // registered with this reactor.
        let fd = unsafe { AsyncFd::register(OwnedFd::from_raw_fd(fd)) }?;
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
                self.changed.notify_waiters();
                if self.closed() {
                    return Ok(());
                }
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

    pub fn pop(&self, commands: bool) -> Result<Option<(u8, u64, Bytes)>> {
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
        if kind > u8::MAX as u32 || len > MAX_FRAME {
            return Err("invalid shared frame descriptor".into());
        }
        let data = Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(slot.add(16), len) });
        tail.store(index.wrapping_add(1), Ordering::Release);
        Ok(Some((kind as u8, id, data)))
    }

    pub fn push(&self, commands: bool, kind: u8, id: u64, data: &[u8]) -> Result<bool> {
        if data.len() > MAX_FRAME {
            return Err("oversized shared frame".into());
        }
        let header = self.header();
        let (head, tail) = if commands {
            (&header.command_head.value, &header.command_tail.value)
        } else {
            (&header.event_head.value, &header.event_tail.value)
        };
        let index = head.load(Ordering::Relaxed);
        let count = index.wrapping_sub(tail.load(Ordering::Acquire));
        if count > CAPACITY {
            return Err("corrupt shared ring indices".into());
        }
        if count == CAPACITY {
            return Ok(false);
        }
        let slot = self.slot(commands, index);
        unsafe {
            slot.cast::<u32>().write(kind as u32);
            slot.add(4).cast::<u32>().write(data.len() as u32);
            slot.add(8).cast::<u64>().write(id);
            std::ptr::copy_nonoverlapping(data.as_ptr(), slot.add(16), data.len());
        }
        head.store(index.wrapping_add(1), Ordering::Release);
        Ok(true)
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        unsafe {
            dealloc(
                self.data.as_ptr(),
                Layout::from_size_align(ARENA_BYTES, 64).unwrap(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_wraparound_and_full_rings() {
        assert_eq!(std::mem::size_of::<Header>(), HEADER);
        assert_eq!(std::mem::offset_of!(Header, closed), 32);
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
}
