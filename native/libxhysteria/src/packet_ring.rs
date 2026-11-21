//! Single-producer/single-consumer packet frames between trusted processes.
//! Fixed, sealed memfd size; acquire/release indices protect every slot.
//! eventfd wakes are coalesced while the peer is not waiting.

use crate::packet_socket;
use quinn::udp;
use std::{
    io::{self, IoSliceMut},
    mem::size_of,
    net::{IpAddr, SocketAddr},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    ptr::NonNull,
    sync::atomic::{AtomicU32, Ordering},
    task::{Context, Poll, ready},
};
use tokio::io::unix::AsyncFd;

const MAGIC: u64 = 0x31525850414d584e;
const VERSION: u32 = 2;
const DESTINATION: usize = 17;
const DATA_OFFSET: usize = 4 + DESTINATION;
const CAPACITY: u32 = 32;
const HEADER: usize = 192;
const MAX_FRAME: usize = 24 + 65535;
const STRIDE: usize = (4 + MAX_FRAME + 63) & !63;
const LENGTH: usize = HEADER + CAPACITY as usize * STRIDE;
const SEALS: i32 = libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;

#[repr(C, align(64))]
struct Counter {
    value: AtomicU32,
    pad: [u8; 60],
}

#[repr(C, align(64))]
struct Header {
    magic: u64,
    version: u32,
    capacity: u32,
    stride: u32,
    reserved: u32,
    length: u64,
    consumer: AtomicU32,
    read_waiting: AtomicU32,
    write_waiting: AtomicU32,
    closed: AtomicU32,
    pad: [u8; 16],
    head: Counter,
    tail: Counter,
}

const _: () = assert!(size_of::<Header>() == HEADER);

#[derive(Debug)]
struct Mapping(NonNull<u8>);

// All mutation follows the SPSC indices. Neither peer retains pointers after
// releasing its slot. Every shared header field that changes is atomic.
unsafe impl Send for Mapping {}
unsafe impl Sync for Mapping {}

impl Mapping {
    fn map(fd: i32) -> io::Result<Self> {
        let data = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                LENGTH,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if data == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(NonNull::new(data.cast()).unwrap()))
    }

    fn header(&self) -> &Header {
        unsafe { &*self.0.as_ptr().cast::<Header>() }
    }

    fn slot(&self, index: u32) -> *mut u8 {
        unsafe {
            self.0
                .as_ptr()
                .add(HEADER + (index % CAPACITY) as usize * STRIDE)
        }
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.0.as_ptr().cast(), LENGTH);
        }
    }
}

fn eventfd() -> io::Result<OwnedFd> {
    let descriptor = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(descriptor) })
}

fn duplicate(descriptor: i32) -> io::Result<OwnedFd> {
    if descriptor < 3 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid packet ring descriptor",
        ));
    }
    let fd = unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, 3) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn notify(fd: i32) {
    let one = 1u64;
    loop {
        let result = unsafe { libc::write(fd, (&one as *const u64).cast(), 8) };
        if result >= 0 || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            break;
        }
    }
}

fn drain(fd: i32) -> io::Result<()> {
    let mut value = 0u64;
    loop {
        let result = unsafe { libc::read(fd, (&mut value as *mut u64).cast(), 8) };
        if result == 8 {
            continue;
        }
        let error = io::Error::last_os_error();
        match error.kind() {
            io::ErrorKind::Interrupted => continue,
            io::ErrorKind::WouldBlock => return Ok(()),
            _ => return Err(error),
        }
    }
}

fn check_event(fd: &OwnedFd) -> io::Result<()> {
    let link = std::fs::read_link(format!("/proc/self/fd/{}", fd.as_raw_fd()))?;
    if link.as_os_str() != "anon_inode:[eventfd]"
        || unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK == 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "packet ring requires nonblocking eventfds",
        ));
    }
    Ok(())
}

pub struct Sender {
    mapping: Mapping,
    _file: OwnedFd,
    wake: OwnedFd,
    space: AsyncFd<OwnedFd>,
    preserve_on_drop: bool,
}

pub struct Batch<'a> {
    sender: &'a mut Sender,
    head: u32,
    tail: u32,
    initial: u32,
}

impl Batch<'_> {
    #[cfg(test)]
    pub fn push(&mut self, header: &[u8; 24], payloads: &[(SocketAddr, &[u8])]) -> io::Result<()> {
        self.push_with_destination(header, payloads, None)
    }

    pub fn push_with_destination(
        &mut self,
        header: &[u8; 24],
        payloads: &[(SocketAddr, &[u8])],
        destination: Option<IpAddr>,
    ) -> io::Result<()> {
        self.push_parts(header, payloads.iter().map(|p| p.1), destination)
    }

    pub fn push_wire(&mut self, wire: &[u8]) -> io::Result<()> {
        if wire.len() < 24 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.push_wire_with_destination(wire, None)
    }

    pub fn push_wire_with_destination(
        &mut self,
        wire: &[u8],
        destination: Option<IpAddr>,
    ) -> io::Result<()> {
        if wire.len() < 24 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.push_parts(
            wire[..24].try_into().unwrap(),
            std::iter::once(&wire[24..]),
            destination,
        )
    }

    fn push_parts<'a>(
        &mut self,
        header: &[u8; 24],
        payloads: impl Iterator<Item = &'a [u8]> + Clone,
        destination: Option<IpAddr>,
    ) -> io::Result<()> {
        let length = payloads
            .clone()
            .try_fold(24usize, |size, p| size.checked_add(p.len()));
        let Some(length) = length.filter(|size| (24..=MAX_FRAME).contains(size)) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "packet ring frame too large",
            ));
        };
        let h = self.sender.mapping.header();
        if h.closed.load(Ordering::Acquire) != 0 {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "packet ring closed",
            ));
        }
        let mut used = self.tail.wrapping_sub(self.head);
        if used >= CAPACITY {
            // The cached consumer position is conservative within this batch.
            // Refresh only when it would otherwise report a full ring.
            self.head = h.head.value.load(Ordering::Acquire);
            used = self.tail.wrapping_sub(self.head);
        }
        if used > CAPACITY {
            return Err(io::ErrorKind::InvalidData.into());
        }
        if used == CAPACITY {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        unsafe {
            let slot = self.sender.mapping.slot(self.tail);
            slot.cast::<u32>().write(length as u32);
            let mut local = [0u8; DESTINATION];
            match destination {
                Some(IpAddr::V4(ip)) => {
                    local[0] = 4;
                    local[13..].copy_from_slice(&ip.octets());
                }
                Some(IpAddr::V6(ip)) => {
                    local[0] = 6;
                    local[1..].copy_from_slice(&ip.octets());
                }
                None => {}
            }
            std::ptr::copy_nonoverlapping(local.as_ptr(), slot.add(4), DESTINATION);
            std::ptr::copy_nonoverlapping(header.as_ptr(), slot.add(DATA_OFFSET), 24);
            let mut offset = DATA_OFFSET + 24;
            for payload in payloads {
                std::ptr::copy_nonoverlapping(payload.as_ptr(), slot.add(offset), payload.len());
                offset += payload.len();
            }
        }
        self.tail = self.tail.wrapping_add(1);
        Ok(())
    }
}

impl Drop for Batch<'_> {
    fn drop(&mut self) {
        if self.tail == self.initial {
            return;
        }
        let h = self.sender.mapping.header();
        h.tail.value.store(self.tail, Ordering::SeqCst);
        if h.read_waiting.swap(0, Ordering::SeqCst) != 0 {
            notify(self.sender.wake.as_raw_fd());
        }
    }
}

impl Sender {
    /// Reattach an empty ring after the previous producer's death was proven.
    /// This never initializes indices or changes the live consumer's ownership.
    pub fn import_empty(file: i32, wake: i32, space: i32) -> io::Result<Self> {
        if file == wake || file == space || wake == space {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let file = duplicate(file)?;
        let wake = duplicate(wake)?;
        let space = duplicate(space)?;
        check_event(&wake)?;
        check_event(&space)?;
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        let seals = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
        if unsafe { libc::fstat(file.as_raw_fd(), &mut stat) } != 0
            || stat.st_size != LENGTH as libc::off_t
            || seals < 0
            || seals & SEALS != SEALS
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let mapping = Mapping::map(file.as_raw_fd())?;
        let h = mapping.header();
        if h.magic != MAGIC
            || h.version != VERSION
            || h.capacity != CAPACITY
            || h.stride != STRIDE as u32
            || h.length != LENGTH as u64
            || h.reserved != 0
            || h.closed.load(Ordering::Acquire) != 0
            || h.consumer.load(Ordering::Acquire) != 1
            || h.head.value.load(Ordering::Acquire) != h.tail.value.load(Ordering::Acquire)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "recovery requires an open empty packet ring",
            ));
        }
        Ok(Self {
            mapping,
            _file: file,
            wake,
            space: unsafe { AsyncFd::register(space) }?,
            // A rejected recovery must not poison the live receiver's ring.
            preserve_on_drop: true,
        })
    }

    pub fn new() -> io::Result<Self> {
        let fd = unsafe {
            libc::memfd_create(
                c"xhys-packet-ring".as_ptr(),
                libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let file = unsafe { OwnedFd::from_raw_fd(fd) };
        if unsafe { libc::ftruncate(fd, LENGTH as _) } != 0
            || unsafe { libc::fcntl(fd, libc::F_ADD_SEALS, SEALS) } != 0
        {
            return Err(io::Error::last_os_error());
        }
        let mapping = Mapping::map(fd)?;
        unsafe {
            mapping.0.as_ptr().cast::<Header>().write(Header {
                magic: MAGIC,
                version: VERSION,
                capacity: CAPACITY,
                stride: STRIDE as u32,
                reserved: 0,
                length: LENGTH as u64,
                consumer: AtomicU32::new(0),
                read_waiting: AtomicU32::new(0),
                write_waiting: AtomicU32::new(0),
                closed: AtomicU32::new(0),
                pad: [0; 16],
                head: Counter {
                    value: AtomicU32::new(0),
                    pad: [0; 60],
                },
                tail: Counter {
                    value: AtomicU32::new(0),
                    pad: [0; 60],
                },
            });
        }
        Ok(Self {
            mapping,
            _file: file,
            wake: eventfd()?,
            // OwnedFd cannot close or replace the descriptor while registered.
            space: unsafe { AsyncFd::register(eventfd()?) }?,
            preserve_on_drop: false,
        })
    }

    pub fn export(&self) -> io::Result<[OwnedFd; 3]> {
        Ok([
            duplicate(self._file.as_raw_fd())?,
            duplicate(self.wake.as_raw_fd())?,
            duplicate(self.space.as_raw_fd())?,
        ])
    }

    #[cfg(test)]
    fn push(&mut self, header: &[u8; 24], payloads: &[(SocketAddr, &[u8])]) -> io::Result<()> {
        self.batch().push(header, payloads)
    }

    pub fn batch(&mut self) -> Batch<'_> {
        let header = self.mapping.header();
        let head = header.head.value.load(Ordering::Acquire);
        let tail = header.tail.value.load(Ordering::Relaxed);
        Batch {
            sender: self,
            head,
            tail,
            initial: tail,
        }
    }

    pub fn poll_writable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let h = self.mapping.header();
        loop {
            h.write_waiting.store(1, Ordering::SeqCst);
            let used = h
                .tail
                .value
                .load(Ordering::Relaxed)
                .wrapping_sub(h.head.value.load(Ordering::SeqCst));
            if used < CAPACITY || h.closed.load(Ordering::Acquire) != 0 {
                h.write_waiting.store(0, Ordering::Relaxed);
                return Poll::Ready(Ok(()));
            }
            if used > CAPACITY {
                return Poll::Ready(Err(io::ErrorKind::InvalidData.into()));
            }
            let mut ready = ready!(self.space.poll_read_ready(cx))?;
            drain(self.space.as_raw_fd())?;
            ready.clear_ready();
        }
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        if self.preserve_on_drop {
            return;
        }
        self.mapping.header().closed.store(1, Ordering::Release);
        notify(self.wake.as_raw_fd());
    }
}

#[derive(Debug)]
pub struct Receiver {
    mapping: Mapping,
    wake: AsyncFd<OwnedFd>,
    space: OwnedFd,
}

impl Receiver {
    pub fn import(file: i32, wake: i32, space: i32) -> io::Result<Self> {
        if file == wake || file == space || wake == space {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let file = duplicate(file)?;
        let wake = duplicate(wake)?;
        let space = duplicate(space)?;
        check_event(&wake)?;
        check_event(&space)?;
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(file.as_raw_fd(), &mut stat) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let seals = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
        if stat.st_size != LENGTH as libc::off_t || seals < 0 || seals & SEALS != SEALS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "packet ring size must be sealed",
            ));
        }
        let mapping = Mapping::map(file.as_raw_fd())?;
        let h = mapping.header();
        if h.magic != MAGIC
            || h.version != VERSION
            || h.capacity != CAPACITY
            || h.stride != STRIDE as u32
            || h.reserved != 0
            || h.length != LENGTH as u64
            || h.closed.load(Ordering::Acquire) != 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid packet ring header",
            ));
        }
        let wake = unsafe { AsyncFd::register(wake) }?;
        h.consumer
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "packet ring already has a receiver",
                )
            })?;
        Ok(Self {
            mapping,
            wake,
            space,
        })
    }

    fn receive(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        metadata: &mut [udp::RecvMeta],
        local: SocketAddr,
        fence: &std::sync::Mutex<Option<[u8; 16]>>,
    ) -> io::Result<usize> {
        let h = self.mapping.header();
        let mut head = h.head.value.load(Ordering::Relaxed);
        let tail = h.tail.value.load(Ordering::Acquire);
        let pending = tail.wrapping_sub(head);
        if pending > CAPACITY {
            return Err(io::ErrorKind::InvalidData.into());
        }
        if pending == 0 {
            return Ok(0);
        }
        let initial = head;
        let mut count = 0;
        for _ in 0..pending.min(buffers.len().min(metadata.len()) as u32) {
            let slot = self.mapping.slot(head);
            let length = unsafe { slot.cast::<u32>().read() } as usize;
            let destination = unsafe { std::slice::from_raw_parts(slot.add(4), DESTINATION) };
            let destination = match destination[0] {
                0 => None,
                4 if destination[1..13].iter().all(|&byte| byte == 0) => Some(IpAddr::V4(
                    <[u8; 4]>::try_from(&destination[13..]).unwrap().into(),
                )),
                6 => Some(IpAddr::V6(
                    <[u8; 16]>::try_from(&destination[1..]).unwrap().into(),
                )),
                _ => return Err(io::ErrorKind::InvalidData.into()),
            };
            if length == 24 {
                let header = unsafe { std::slice::from_raw_parts(slot.add(DATA_OFFSET), 24) };
                if let Some(token) = packet_socket::drain_marker(header) {
                    *fence
                        .lock()
                        .map_err(|_| io::Error::other("drain fence poisoned"))? = Some(token);
                }
            }
            if (25..=MAX_FRAME).contains(&length) {
                let frame = unsafe { std::slice::from_raw_parts(slot.add(DATA_OFFSET), length) };
                let mut header = frame[..24].try_into().unwrap();
                if let Some((mut address, stride)) =
                    packet_socket::parse_frame_limit(&mut header, length - 24, true, 32)
                    && length - 24 <= buffers[count].len()
                {
                    buffers[count][..length - 24].copy_from_slice(&frame[24..]);
                    if local.is_ipv6()
                        && let IpAddr::V4(ip) = address.ip()
                    {
                        address = SocketAddr::new(ip.to_ipv6_mapped().into(), address.port());
                    }
                    metadata[count] = udp::RecvMeta {
                        addr: address,
                        len: length - 24,
                        stride,
                        ecn: None,
                        dst_ip: destination
                            .or_else(|| (!local.ip().is_unspecified()).then_some(local.ip())),
                    };
                    count += 1;
                }
            }
            head = head.wrapping_add(1);
        }
        if head != initial {
            h.head.value.store(head, Ordering::SeqCst);
            if h.write_waiting.swap(0, Ordering::SeqCst) != 0 {
                notify(self.space.as_raw_fd());
            }
        }
        Ok(count)
    }

    #[cfg(test)]
    pub fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        buffers: &mut [IoSliceMut<'_>],
        metadata: &mut [udp::RecvMeta],
        local: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.poll_recv_fenced(cx, buffers, metadata, local, &std::sync::Mutex::new(None))
    }

    pub fn poll_recv_fenced(
        &mut self,
        cx: &mut Context<'_>,
        buffers: &mut [IoSliceMut<'_>],
        metadata: &mut [udp::RecvMeta],
        local: SocketAddr,
        fence: &std::sync::Mutex<Option<[u8; 16]>>,
    ) -> Poll<io::Result<usize>> {
        if buffers.is_empty() || metadata.is_empty() {
            return Poll::Ready(Err(io::ErrorKind::InvalidInput.into()));
        }
        for _ in 0..32 {
            let count = self.receive(buffers, metadata, local, fence)?;
            if count != 0 {
                return Poll::Ready(Ok(count));
            }
            let h = self.mapping.header();
            h.read_waiting.store(1, Ordering::SeqCst);
            if h.head.value.load(Ordering::Relaxed) != h.tail.value.load(Ordering::SeqCst) {
                h.read_waiting.store(0, Ordering::Relaxed);
                continue;
            }
            if h.closed.load(Ordering::Acquire) != 0 {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            }
            let mut ready = ready!(self.wake.poll_read_ready(cx))?;
            drain(self.wake.as_raw_fd())?;
            ready.clear_ready();
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        let h = self.mapping.header();
        h.read_waiting.store(0, Ordering::Release);
        // Import is one-shot: do not let a new process consume an orphaned
        // ring. Crash recovery requires an explicit ownership protocol.
        h.consumer.store(2, Ordering::Release);
        h.closed.store(1, Ordering::Release);
        notify(self.space.as_raw_fd());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn restored_producer_preserves_live_consumer_and_rejects_pending_or_closed_ring() {
        let mut sender = Sender::new().unwrap();
        let files = sender.export().unwrap();
        let mut receiver = import(&sender);
        let address: SocketAddr = "127.0.0.1:443".parse().unwrap();
        let payload = [0x71; 1200];
        let header = packet_socket::encode_header(address, payload.len()).unwrap();
        sender.push(&header, &[(address, &payload)]).unwrap();
        assert!(
            Sender::import_empty(
                files[0].as_raw_fd(),
                files[1].as_raw_fd(),
                files[2].as_raw_fd()
            )
            .is_err()
        );
        let mut buffer = [0u8; 2048];
        let mut buffers = [IoSliceMut::new(&mut buffer)];
        let mut metadata = [udp::RecvMeta::default()];
        poll_fn(|cx| receiver.poll_recv(cx, &mut buffers, &mut metadata, address))
            .await
            .unwrap();
        assert_eq!(&buffers[0][..1200], &payload);
        // Model SIGKILL: it does not run the producer's closing destructor.
        sender.preserve_on_drop = true;
        drop(sender);
        let restore = || {
            Sender::import_empty(
                files[0].as_raw_fd(),
                files[1].as_raw_fd(),
                files[2].as_raw_fd(),
            )
        };
        let mut recovered = restore().unwrap();
        assert_eq!(
            recovered.mapping.header().consumer.load(Ordering::Acquire),
            1
        );
        drop(recovered);
        // Rejected/aborted recovery must not mark the existing ring closed.
        recovered = restore().unwrap();
        recovered.push(&header, &[(address, &payload)]).unwrap();
        poll_fn(|cx| receiver.poll_recv(cx, &mut buffers, &mut metadata, address))
            .await
            .unwrap();
        assert_eq!(&buffers[0][..1200], &payload);
        recovered
            .mapping
            .header()
            .closed
            .store(1, Ordering::Release);
        assert!(restore().is_err());
    }
    use std::future::poll_fn;

    fn import(sender: &Sender) -> Receiver {
        let descriptors = sender.export().unwrap();
        Receiver::import(
            descriptors[0].as_raw_fd(),
            descriptors[1].as_raw_fd(),
            descriptors[2].as_raw_fd(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn batch_refreshes_cached_head_after_receiver_progress() {
        let mut sender = Sender::new().unwrap();
        let mut receiver = import(&sender);
        let local = "127.0.0.1:443".parse().unwrap();
        let payload = [0x71; 1200];
        let header = packet_socket::encode_header(local, payload.len()).unwrap();
        for _ in 0..CAPACITY {
            sender.push(&header, &[(local, &payload)]).unwrap();
        }
        let mut batch = sender.batch();
        assert_eq!(
            batch
                .push(&header, &[(local, &payload)])
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        let mut buffer = [0u8; 1200];
        let mut buffers = [IoSliceMut::new(&mut buffer)];
        let mut metadata = [udp::RecvMeta::default()];
        receiver
            .receive(
                &mut buffers,
                &mut metadata,
                local,
                &std::sync::Mutex::new(None),
            )
            .unwrap();
        batch.push(&header, &[(local, &payload)]).unwrap();
        assert_eq!(
            batch
                .push(&header, &[(local, &payload)])
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        drop(batch);
        for _ in 0..CAPACITY {
            assert_eq!(
                receiver
                    .receive(
                        &mut buffers,
                        &mut metadata,
                        local,
                        &std::sync::Mutex::new(None)
                    )
                    .unwrap(),
                1
            );
            assert_eq!(&buffers[0][..1200], &payload);
        }
        assert_eq!(
            receiver
                .receive(
                    &mut buffers,
                    &mut metadata,
                    local,
                    &std::sync::Mutex::new(None)
                )
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn ring_preserves_frames_wraparound_and_backpressure() {
        let mut sender = Sender::new().unwrap();
        let mut receiver = import(&sender);
        let local = "[::1]:443".parse().unwrap();
        let address = "127.0.0.1:123".parse().unwrap();
        // Start near wraparound, when neither peer holds a frame.
        sender
            .mapping
            .header()
            .head
            .value
            .store(u32::MAX - 8, Ordering::Relaxed);
        sender
            .mapping
            .header()
            .tail
            .value
            .store(u32::MAX - 8, Ordering::Relaxed);
        for sequence in 0..CAPACITY {
            let header = packet_socket::encode_header(address, 1200).unwrap();
            sender
                .push(&header, &[(address, &vec![sequence as u8; 1200])])
                .unwrap();
        }
        let header = packet_socket::encode_header(address, 1200).unwrap();
        assert_eq!(
            sender
                .push(&header, &[(address, &[0; 1200])])
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        let mut storage = [[0u8; 2048]; 8];
        let mut meta = [udp::RecvMeta::default(); 8];
        let mut next = 0;
        while next < CAPACITY {
            let mut buffers: Vec<_> = storage.iter_mut().map(|p| IoSliceMut::new(p)).collect();
            let count = poll_fn(|cx| receiver.poll_recv(cx, &mut buffers, &mut meta, local))
                .await
                .unwrap();
            for index in 0..count {
                assert_eq!(meta[index].len, 1200);
                assert_eq!(meta[index].stride, 1200);
                assert!(meta[index].addr.is_ipv6());
                assert!(buffers[index][..1200].iter().all(|b| *b == next as u8));
                next += 1;
            }
        }
        poll_fn(|cx| sender.poll_writable(cx)).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wakeups_survive_empty_full_transitions_and_close() {
        let mut sender = Sender::new().unwrap();
        let mut receiver = import(&sender);
        let address: SocketAddr = "127.0.0.1:123".parse().unwrap();
        let reader = tokio::spawn(async move {
            let mut buffer = [0u8; 32];
            let mut metadata = [udp::RecvMeta::default()];
            for sequence in 0..10000u32 {
                let mut buffers = [IoSliceMut::new(&mut buffer)];
                let count =
                    poll_fn(|cx| receiver.poll_recv(cx, &mut buffers, &mut metadata, address))
                        .await
                        .unwrap();
                assert_eq!(count, 1);
                assert_eq!(
                    u32::from_be_bytes(buffer[..4].try_into().unwrap()),
                    sequence
                );
                if sequence % 127 == 0 {
                    tokio::task::yield_now().await;
                }
            }
            let mut buffers = [IoSliceMut::new(&mut buffer)];
            assert_eq!(
                poll_fn(|cx| receiver.poll_recv(cx, &mut buffers, &mut metadata, address))
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            for sequence in 0..10000u32 {
                let payload = sequence.to_be_bytes();
                let header = packet_socket::encode_header(address, payload.len()).unwrap();
                loop {
                    match sender.push(&header, &[(address, &payload)]) {
                        Ok(()) => break,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            poll_fn(|cx| sender.poll_writable(cx)).await.unwrap();
                        }
                        Err(error) => panic!("{error}"),
                    }
                }
                if sequence % 37 == 0 {
                    tokio::task::yield_now().await;
                }
            }
            drop(sender);
            reader.await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn ring_rejects_wrong_descriptors_and_second_receiver() {
        let sender = Sender::new().unwrap();
        let descriptors = sender.export().unwrap();
        assert!(
            Receiver::import(
                descriptors[0].as_raw_fd(),
                descriptors[0].as_raw_fd(),
                descriptors[2].as_raw_fd()
            )
            .is_err()
        );
        assert!(
            Receiver::import(
                descriptors[1].as_raw_fd(),
                descriptors[2].as_raw_fd(),
                descriptors[0].as_raw_fd()
            )
            .is_err()
        );
        let receiver = import(&sender);
        assert!(
            Receiver::import(
                descriptors[0].as_raw_fd(),
                descriptors[1].as_raw_fd(),
                descriptors[2].as_raw_fd()
            )
            .is_err()
        );
        drop(receiver);
        assert!(
            Receiver::import(
                descriptors[0].as_raw_fd(),
                descriptors[1].as_raw_fd(),
                descriptors[2].as_raw_fd()
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn ring_accepts_32_segments_and_skips_invalid_frames() {
        let mut sender = Sender::new().unwrap();
        let mut receiver = import(&sender);
        let address: SocketAddr = "127.0.0.1:123".parse().unwrap();
        let data = vec![0x7bu8; 31 * 1200 + 19];
        let mut header = packet_socket::encode_header(address, data.len()).unwrap();
        header[0] = 2;
        header[20..22].copy_from_slice(&1200u16.to_be_bytes());
        header[22..24].copy_from_slice(&(data.len() as u16).to_be_bytes());
        let mut bad = header;
        bad[20..22].copy_from_slice(&1u16.to_be_bytes());
        sender.push(&bad, &[(address, &data)]).unwrap();
        sender.push(&header, &[(address, &data)]).unwrap();
        let mut storage = vec![0u8; 65535];
        let mut buffers = [IoSliceMut::new(&mut storage)];
        let mut metadata = [udp::RecvMeta::default()];
        let count = poll_fn(|cx| receiver.poll_recv(cx, &mut buffers, &mut metadata, address))
            .await
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(metadata[0].len, data.len());
        assert_eq!(metadata[0].stride, 1200);
        assert_eq!(&buffers[0][..data.len()], data);
        assert_eq!(
            sender.mapping.header().head.value.load(Ordering::Acquire),
            sender.mapping.header().tail.value.load(Ordering::Acquire)
        );
    }

    #[tokio::test]
    async fn batch_publishes_partial_success_without_duplicates() {
        let mut sender = Sender::new().unwrap();
        let mut receiver = import(&sender);
        let address = "127.0.0.1:123".parse().unwrap();
        let header = packet_socket::encode_header(address, 4).unwrap();
        {
            let mut batch = sender.batch();
            for sequence in 0..CAPACITY {
                batch
                    .push(&header, &[(address, &sequence.to_be_bytes())])
                    .unwrap();
            }
            assert_eq!(
                batch
                    .push(&header, &[(address, &[0; 4])])
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(
                receiver.mapping.header().tail.value.load(Ordering::Acquire),
                0
            );
        }
        let mut storage = [0; 4];
        let mut metadata = [udp::RecvMeta::default()];
        for sequence in 0..CAPACITY {
            let mut buffers = [IoSliceMut::new(&mut storage)];
            let count = poll_fn(|cx| receiver.poll_recv(cx, &mut buffers, &mut metadata, address))
                .await
                .unwrap();
            assert_eq!(count, 1);
            assert_eq!(u32::from_be_bytes(storage), sequence);
        }
        assert_eq!(
            receiver.mapping.header().head.value.load(Ordering::Acquire),
            CAPACITY
        );
    }

    #[tokio::test]
    async fn dropping_receiver_wakes_blocked_sender() {
        let mut sender = Sender::new().unwrap();
        let receiver = import(&sender);
        let address = "127.0.0.1:123".parse().unwrap();
        let header = packet_socket::encode_header(address, 4).unwrap();
        for _ in 0..CAPACITY {
            sender.push(&header, &[(address, &[0; 4])]).unwrap();
        }
        let closer = tokio::spawn(async move {
            tokio::task::yield_now().await;
            drop(receiver);
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            poll_fn(|cx| sender.poll_writable(cx)).await.unwrap();
            assert_eq!(
                sender
                    .push(&header, &[(address, &[0; 4])])
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            closer.await.unwrap();
        })
        .await
        .unwrap();
    }
}
