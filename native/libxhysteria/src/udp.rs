use crate::protocol::{Error, MAX_ADDRESS, read_varint, varint_len, write_varint};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

pub const MAX_PAYLOAD: usize = 4096;
pub const FRAGMENT_TTL_NS: u64 = 10_000_000_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message<'a> {
    pub session: u32,
    pub packet: u16,
    pub fragment: u8,
    pub count: u8,
    pub address: &'a [u8],
    pub payload: &'a [u8],
}

impl<'a> Message<'a> {
    pub fn decode(input: &'a [u8]) -> Result<Self, Error> {
        if input.len() < 9 {
            return Err(Error::Incomplete);
        }
        let (address_len, prefix) = read_varint(&input[8..])?;
        if address_len == 0 || address_len > MAX_ADDRESS as u64 {
            return Err(Error::Invalid("invalid UDP address length"));
        }
        let start = 8 + prefix;
        let end = start + address_len as usize;
        let address = input.get(start..end).ok_or(Error::Incomplete)?;
        let msg = Self {
            session: u32::from_be_bytes(input[..4].try_into().unwrap()),
            packet: u16::from_be_bytes(input[4..6].try_into().unwrap()),
            fragment: input[6],
            count: input[7],
            address,
            payload: &input[end..],
        };
        msg.validate()?;
        Ok(msg)
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.address.is_empty() || self.address.len() > MAX_ADDRESS {
            return Err(Error::Invalid("invalid UDP address length"));
        }
        if self.payload.is_empty() || self.payload.len() > MAX_PAYLOAD {
            return Err(Error::Invalid("invalid UDP payload length"));
        }
        // Fragment ID has no meaning for an unfragmented Hysteria datagram.
        if self.count == 0 || (self.count != 1 && self.fragment >= self.count) {
            return Err(Error::Invalid("invalid UDP fragment index/count"));
        }
        Ok(())
    }

    pub fn header_len(&self) -> Result<usize, Error> {
        Ok(8 + varint_len(self.address.len() as u64)? + self.address.len())
    }

    pub fn encode(&self, output: &mut Vec<u8>) -> Result<(), Error> {
        self.validate()?;
        output.clear();
        output.extend_from_slice(&self.session.to_be_bytes());
        output.extend_from_slice(&self.packet.to_be_bytes());
        output.extend_from_slice(&[self.fragment, self.count]);
        write_varint(self.address.len() as u64, output)?;
        output.extend_from_slice(self.address);
        output.extend_from_slice(self.payload);
        Ok(())
    }

    pub fn fragment(&self, max_datagram: usize) -> Result<Vec<Self>, Error> {
        Ok(self.fragments(max_datagram)?.collect())
    }

    pub fn fragments(
        &self,
        max_datagram: usize,
    ) -> Result<impl ExactSizeIterator<Item = Self> + DoubleEndedIterator + 'a, Error> {
        self.validate()?;
        let capacity = max_datagram
            .checked_sub(self.header_len()?)
            .filter(|n| *n > 0)
            .ok_or(Error::Invalid("QUIC datagram limit cannot fit header"))?;
        let count = self.payload.len().div_ceil(capacity);
        if count > 255 {
            return Err(Error::Invalid("too many UDP fragments"));
        }
        let message = self.clone();
        Ok(self
            .payload
            .chunks(capacity)
            .enumerate()
            .map(move |(id, payload)| Self {
                fragment: id as u8,
                count: count as u8,
                payload,
                ..message.clone()
            }))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Datagram {
    pub session: u32,
    pub address: Vec<u8>,
    pub payload: Vec<u8>,
}

pub struct DatagramParts {
    pub session: u32,
    pub address: Vec<u8>,
    parts: Vec<Option<Vec<u8>>>,
    bytes: usize,
}

impl DatagramParts {
    pub fn payloads(&self) -> impl ExactSizeIterator<Item = &[u8]> + Clone {
        self.parts.iter().map(|part| part.as_deref().unwrap())
    }

    pub fn into_datagram(self) -> Datagram {
        let mut payload = Vec::with_capacity(self.bytes);
        for part in self.parts {
            payload.extend_from_slice(&part.unwrap());
        }
        Datagram {
            session: self.session,
            address: self.address,
            payload,
        }
    }
}

struct Assembly {
    session: u32,
    packet: u16,
    address: Vec<u8>,
    parts: Vec<Option<Vec<u8>>>,
    received: usize,
    bytes: usize,
    last_seen: u64,
}

/// One instance per QUIC connection; bounded across all of its UDP sessions.
pub struct Defragger {
    slots: Vec<Assembly>,
    limit: usize,
    stats: Option<Arc<ReassemblyDrops>>,
}

#[derive(Debug, Default)]
pub(crate) struct ReassemblyDrops {
    pub(crate) expired: AtomicU64,
    pub(crate) evicted: AtomicU64,
    pub(crate) replaced: AtomicU64,
    pub(crate) oversized: AtomicU64,
}

impl Defragger {
    pub fn new(max_incomplete_packets: usize) -> Result<Self, Error> {
        if max_incomplete_packets == 0 || max_incomplete_packets > 128 {
            return Err(Error::Invalid("invalid UDP reassembly slot limit"));
        }
        Ok(Self {
            slots: Vec::new(),
            limit: max_incomplete_packets,
            stats: None,
        })
    }

    #[cfg(any(feature = "engine", test))]
    pub(crate) fn with_stats(mut self, stats: Option<Arc<ReassemblyDrops>>) -> Self {
        self.stats = stats;
        self
    }

    pub fn pending(&self) -> usize {
        self.slots.len()
    }

    pub fn expire(&mut self, now_ns: u64) {
        let before = self.slots.len();
        self.slots
            .retain(|slot| now_ns.saturating_sub(slot.last_seen) <= FRAGMENT_TTL_NS);
        let expired = before - self.slots.len();
        if expired != 0
            && let Some(stats) = &self.stats
        {
            stats.expired.fetch_add(expired as u64, Ordering::Relaxed);
        }
    }

    pub fn feed(&mut self, now_ns: u64, message: &Message<'_>) -> Result<Option<Datagram>, Error> {
        if message.count == 1 {
            message.validate()?;
            self.expire(now_ns);
            return Ok(Some(Datagram {
                session: message.session,
                address: message.address.into(),
                payload: message.payload.into(),
            }));
        }
        Ok(self
            .feed_parts(now_ns, message)?
            .map(DatagramParts::into_datagram))
    }

    pub fn feed_parts(
        &mut self,
        now_ns: u64,
        message: &Message<'_>,
    ) -> Result<Option<DatagramParts>, Error> {
        message.validate()?;
        self.expire(now_ns);
        if message.count == 1 {
            return Ok(Some(DatagramParts {
                session: message.session,
                address: message.address.into(),
                parts: vec![Some(message.payload.into())],
                bytes: message.payload.len(),
            }));
        }
        let found = self
            .slots
            .iter()
            .position(|s| s.session == message.session && s.packet == message.packet);
        let index = match found {
            Some(i)
                if self.slots[i].address != message.address
                    || self.slots[i].parts.len() != message.count as usize =>
            {
                self.slots.swap_remove(i);
                if let Some(stats) = &self.stats {
                    stats.replaced.fetch_add(1, Ordering::Relaxed);
                }
                None
            }
            other => other,
        };
        let index = index.unwrap_or_else(|| {
            if self.slots.len() == self.limit {
                let oldest = self
                    .slots
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, s)| s.last_seen)
                    .unwrap()
                    .0;
                self.slots.swap_remove(oldest);
                if let Some(stats) = &self.stats {
                    stats.evicted.fetch_add(1, Ordering::Relaxed);
                }
            }
            self.slots.push(Assembly {
                session: message.session,
                packet: message.packet,
                address: message.address.into(),
                parts: vec![None; message.count as usize],
                received: 0,
                bytes: 0,
                last_seen: now_ns,
            });
            self.slots.len() - 1
        });
        let slot = &mut self.slots[index];
        let part = &mut slot.parts[message.fragment as usize];
        if part.is_some() {
            return Ok(None);
        }
        slot.bytes += message.payload.len();
        if slot.bytes > MAX_PAYLOAD {
            self.slots.swap_remove(index);
            if let Some(stats) = &self.stats {
                stats.oversized.fetch_add(1, Ordering::Relaxed);
            }
            return Err(Error::Invalid("UDP assembly exceeds payload limit"));
        }
        *part = Some(message.payload.into());
        slot.received += 1;
        slot.last_seen = now_ns;
        if slot.received != slot.parts.len() {
            return Ok(None);
        }
        let slot = self.slots.swap_remove(index);
        Ok(Some(DatagramParts {
            session: slot.session,
            address: slot.address,
            parts: slot.parts,
            bytes: slot.bytes,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(session: u32, fragment: u8, payload: &[u8]) -> Message<'_> {
        Message {
            session,
            packet: 7,
            fragment,
            count: 2,
            address: b"example.com:53",
            payload,
        }
    }

    #[test]
    fn sessions_with_the_same_packet_id_do_not_collide() {
        let mut d = Defragger::new(8).unwrap();
        assert!(d.feed(0, &msg(1, 0, b"a")).unwrap().is_none());
        assert!(d.feed(0, &msg(2, 0, b"c")).unwrap().is_none());
        assert_eq!(d.feed(1, &msg(1, 1, b"b")).unwrap().unwrap().payload, b"ab");
        assert_eq!(d.feed(1, &msg(2, 1, b"d")).unwrap().unwrap().payload, b"cd");
        assert_eq!(d.pending(), 0);
    }

    #[test]
    fn wire_format_and_fragmentation_round_trip_out_of_order() {
        let payload = vec![0xa5; MAX_PAYLOAD];
        let message = Message {
            count: 1,
            ..msg(1, 0, &payload)
        };
        let fragments = message.fragment(1200).unwrap();
        let mut defrag = Defragger::new(8).unwrap();
        let mut result = None;
        for fragment in fragments.iter().rev() {
            let mut wire = Vec::new();
            fragment.encode(&mut wire).unwrap();
            assert!(wire.len() <= 1200);
            assert_eq!(&wire[..4], &1u32.to_be_bytes());
            result = defrag.feed(1, &Message::decode(&wire).unwrap()).unwrap();
        }
        assert_eq!(result.unwrap().payload, payload);
    }

    #[test]
    fn fragment_iterator_matches_collected_wire_parts() {
        let payload = vec![0xa5; MAX_PAYLOAD];
        let message = Message {
            count: 1,
            ..msg(1, 0, &payload)
        };
        let expected = message.fragment(1200).unwrap();
        let mut fragments = message.fragments(1200).unwrap();
        assert_eq!(fragments.len(), expected.len());
        assert_eq!(fragments.next(), expected.first().cloned());
        assert_eq!(fragments.next_back(), expected.last().cloned());
        assert_eq!(
            fragments.collect::<Vec<_>>(),
            expected[1..expected.len() - 1]
        );
        assert!(message.fragments(1).is_err());
        assert!(
            message
                .fragments(message.header_len().unwrap() + 1)
                .is_err()
        );
    }

    #[test]
    fn completed_parts_retain_compact_fragment_allocations_without_joining() {
        let mut defrag = Defragger::new(8).unwrap();
        let mut source = vec![0x71; 3000];
        assert!(defrag.feed_parts(0, &msg(1, 1, &source)).unwrap().is_none());
        let retained = defrag.slots[0].parts[1].as_ref().unwrap().as_ptr();
        assert_ne!(retained, source.as_ptr());
        source.fill(0);
        assert!(
            defrag
                .feed_parts(1, &msg(1, 1, b"duplicate"))
                .unwrap()
                .is_none()
        );
        let completed = defrag
            .feed_parts(2, &msg(1, 0, b"prefix"))
            .unwrap()
            .unwrap();
        assert_eq!(completed.session, 1);
        assert_eq!(completed.address, b"example.com:53");
        assert_eq!(completed.payloads().len(), 2);
        assert_eq!(completed.payloads().nth(1).unwrap().as_ptr(), retained);
        assert_eq!(completed.payloads().next().unwrap(), b"prefix");
        assert_eq!(completed.payloads().nth(1).unwrap(), &[0x71; 3000]);
        assert_eq!(defrag.pending(), 0);
    }

    #[test]
    fn vectored_reassembly_expires_rejects_oversize_and_preserves_small_datagrams() {
        let mut defrag = Defragger::new(1).unwrap();
        defrag.feed_parts(0, &msg(1, 0, b"expired")).unwrap();
        defrag.expire(FRAGMENT_TTL_NS + 1);
        assert_eq!(defrag.pending(), 0);
        defrag.feed_parts(0, &msg(1, 0, &[0; 3000])).unwrap();
        assert!(defrag.feed_parts(1, &msg(1, 1, &[0; 2000])).is_err());
        assert_eq!(defrag.pending(), 0);
        let message = Message {
            count: 1,
            ..msg(2, 0, b"single")
        };
        let completed = defrag
            .feed_parts(2, &message)
            .unwrap()
            .unwrap()
            .into_datagram();
        assert_eq!(completed.session, 2);
        assert_eq!(completed.payload, b"single");
    }

    #[test]
    fn duplicates_expiration_and_eviction_are_bounded() {
        let stats = Arc::new(ReassemblyDrops::default());
        let mut d = Defragger::new(1).unwrap().with_stats(Some(stats.clone()));
        d.feed(0, &msg(1, 0, b"old")).unwrap();
        d.feed(FRAGMENT_TTL_NS, &msg(1, 0, b"duplicate")).unwrap();
        d.expire(FRAGMENT_TTL_NS + 1);
        assert_eq!(d.pending(), 0);
        d.feed(0, &msg(1, 0, b"old")).unwrap();
        d.feed(1, &msg(2, 0, b"new")).unwrap();
        assert_eq!(d.pending(), 1);
        assert_eq!(
            d.feed(2, &msg(2, 1, b"data")).unwrap().unwrap().payload,
            b"newdata"
        );
        assert_eq!(stats.expired.load(Ordering::Relaxed), 1);
        assert_eq!(stats.evicted.load(Ordering::Relaxed), 1);
        assert_eq!(stats.replaced.load(Ordering::Relaxed), 0);
        assert_eq!(stats.oversized.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn rejects_oversized_assembly_and_bad_metadata() {
        let stats = Arc::new(ReassemblyDrops::default());
        let mut d = Defragger::new(8).unwrap().with_stats(Some(stats.clone()));
        d.feed(0, &msg(1, 0, &[0; 3000])).unwrap();
        assert!(d.feed(1, &msg(1, 1, &[0; 2000])).is_err());
        assert_eq!(d.pending(), 0);
        assert_eq!(stats.oversized.load(Ordering::Relaxed), 1);
        assert!(d.feed(1, &msg(1, 2, b"bad")).is_err());
        d.feed(2, &msg(2, 0, b"original")).unwrap();
        let replacement = Message {
            address: b"other:443",
            ..msg(2, 0, b"replacement")
        };
        assert!(d.feed(3, &replacement).unwrap().is_none());
        assert_eq!(stats.replaced.load(Ordering::Relaxed), 1);
        assert!(Message::decode(&[0; 9]).is_err());
        assert!(msg(1, 0, b"").validate().is_err());
        assert!(msg(1, 0, b"a").fragment(1).is_err());
    }
}
