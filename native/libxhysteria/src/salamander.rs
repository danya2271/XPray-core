use crate::protocol::Error;
use std::sync::OnceLock;

pub const SALT_LEN: usize = 8;

#[derive(Clone)]
pub struct Salamander {
    hash: HashTemplate,
}

#[derive(Clone)]
enum HashTemplate {
    // PSK || salt fits into one BLAKE2b block. Keep the unkeyed input
    // instead of cloning and finalizing a streaming state for every packet.
    SingleBlock { prefix: [u8; 128], psk_len: u8 },
    Streaming(Box<blake2b_simd::State>),
}

fn hash_params() -> &'static blake2b_simd::Params {
    static PARAMS: OnceLock<blake2b_simd::Params> = OnceLock::new();
    PARAMS.get_or_init(|| {
        let mut params = blake2b_simd::Params::new();
        params.hash_length(32);
        params
    })
}

impl Salamander {
    pub fn new(psk: &[u8]) -> Result<Self, Error> {
        if psk.len() < 4 {
            return Err(Error::Invalid(
                "Salamander PSK must have at least four bytes",
            ));
        }
        let hash = if psk.len() <= 128 - SALT_LEN {
            let mut prefix = [0; 128];
            prefix[..psk.len()].copy_from_slice(psk);
            HashTemplate::SingleBlock {
                prefix,
                psk_len: psk.len() as u8,
            }
        } else {
            let mut hash = hash_params().to_state();
            hash.update(psk);
            HashTemplate::Streaming(Box::new(hash))
        };
        Ok(Self { hash })
    }

    fn key(&self, salt: &[u8]) -> blake2b_simd::Hash {
        match &self.hash {
            HashTemplate::SingleBlock { prefix, psk_len } => {
                let mut input = *prefix;
                let start = *psk_len as usize;
                input[start..start + SALT_LEN].copy_from_slice(salt);
                // This remains BLAKE2b-256(PSK || salt), not BLAKE2 keyed mode.
                hash_params().hash(&input[..start + SALT_LEN])
            }
            HashTemplate::Streaming(template) => {
                let mut hash = (**template).clone();
                hash.update(salt);
                hash.finalize()
            }
        }
    }

    fn xor(&self, salt: &[u8], payload: &mut [u8]) {
        let key = self.key(salt);
        Self::xor_key(key.as_bytes(), payload);
    }

    #[cfg(feature = "engine")]
    pub(crate) fn seal_in_place(&self, packet: &mut [u8], salt: &[u8; SALT_LEN]) {
        let (prefix, payload) = packet.split_at_mut(SALT_LEN);
        prefix.copy_from_slice(salt);
        self.xor(prefix, payload);
    }

    fn xor_key(key: &[u8], payload: &mut [u8]) {
        let mask: [u64; 4] = std::array::from_fn(|index| {
            u64::from_ne_bytes(key[index * 8..index * 8 + 8].try_into().unwrap())
        });
        let (chunks, tail) = payload.as_chunks_mut::<32>();
        for chunk in chunks {
            for (word, mask) in chunk.as_chunks_mut::<8>().0.iter_mut().zip(mask) {
                *word = (u64::from_ne_bytes(*word) ^ mask).to_ne_bytes();
            }
        }
        for (byte, mask) in tail.iter_mut().zip(key) {
            *byte ^= mask;
        }
    }

    pub(crate) fn seal_segments(
        &self,
        payload: &[u8],
        stride: usize,
        salts: &[u8],
        output: &mut Vec<u8>,
    ) {
        let stride = stride.max(1);
        let segments = payload.len().div_ceil(stride);
        assert_eq!(salts.len(), segments * SALT_LEN);
        output.clear();
        output.reserve(payload.len() + segments * SALT_LEN);
        for (data, salt) in payload.chunks(stride).zip(salts.as_chunks::<SALT_LEN>().0) {
            let start = output.len();
            output.extend_from_slice(salt);
            self.append_masked(salt, data, output);
            debug_assert_eq!(output.len() - start, data.len() + SALT_LEN);
        }
    }

    pub fn seal(&self, payload: &[u8], output: &mut Vec<u8>) -> Result<(), getrandom::Error> {
        let mut salt = [0; SALT_LEN];
        getrandom::fill(&mut salt)?;
        self.seal_with_salt(payload, salt, output);
        Ok(())
    }

    fn seal_with_salt(&self, payload: &[u8], salt: [u8; SALT_LEN], output: &mut Vec<u8>) {
        output.clear();
        output.reserve(SALT_LEN + payload.len());
        output.extend_from_slice(&salt);
        self.append_masked(&salt, payload, output);
    }

    fn append_masked(&self, salt: &[u8], data: &[u8], output: &mut Vec<u8>) {
        let key = self.key(salt);
        let start = output.len();
        output.extend_from_slice(data);
        Self::xor_key(key.as_bytes(), &mut output[start..]);
    }

    /// Returns the decoded payload without moving it over the salt prefix.
    pub fn open_in_place<'a>(&self, packet: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        if packet.len() <= SALT_LEN {
            return Err(Error::Invalid("truncated Salamander packet"));
        }
        let (salt, payload) = packet.split_at_mut(SALT_LEN);
        self.xor(salt, payload);
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_blake2b_256_vector() {
        // Independently generated with b2sum -l 256 over PSK || salt.
        let sm = Salamander::new(b"test-password").unwrap();
        let mut packet = Vec::new();
        sm.seal_with_salt(&[0; 32], [0, 1, 2, 3, 4, 5, 6, 7], &mut packet);
        let digest: String = packet[8..].iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            digest,
            "d9a10a59b56c49101e97fae27c7ebc0d45cd988b07524dfc0a5c896b41fc1fab"
        );
        assert_eq!(sm.open_in_place(&mut packet).unwrap(), &[0; 32]);
    }

    #[test]
    fn simd_hash_matches_reference_for_long_keys_and_unaligned_payloads() {
        use blake2::{Blake2b, Digest, digest::consts::U32};
        for key_len in [4, 13, 119, 120, 121, 127, 128, 129, 255, 1024] {
            let key: Vec<_> = (0..key_len).map(|i| (i % 251) as u8).collect();
            let sm = Salamander::new(&key).unwrap();
            for len in [1, 15, 16, 31, 32, 33, 1200, 1452] {
                let salt = [0, 1, 2, 3, 4, 5, 6, 7];
                let mut hash = Blake2b::<U32>::new();
                hash.update(&key);
                hash.update(salt);
                let expected_key = hash.finalize();
                let data: Vec<_> = (0..len).map(|i| (i % 251) as u8).collect();
                let mut packet = Vec::new();
                sm.seal_with_salt(&data, salt, &mut packet);
                for i in 0..len {
                    assert_eq!(packet[i + SALT_LEN], data[i] ^ expected_key[i % 32]);
                }
                assert_eq!(sm.open_in_place(&mut packet).unwrap(), data);
            }
        }
    }

    #[test]
    fn single_block_boundary_and_cloned_templates_preserve_every_salt() {
        use blake2::{Blake2b, Digest, digest::consts::U32};
        for key_len in [4, 13, 119, 120, 121, 128, 1024] {
            let key = vec![0xa3; key_len];
            let original = Salamander::new(&key).unwrap();
            let mask = original.clone();
            for index in 0..128u64 {
                let salt = index.wrapping_mul(0x9e3779b97f4a7c15).to_le_bytes();
                let mut reference = Blake2b::<U32>::new();
                reference.update(&key);
                reference.update(salt);
                let expected = reference.finalize();
                assert_eq!(mask.key(&salt).as_bytes(), expected.as_slice());
                assert_eq!(original.key(&salt), mask.key(&salt));
            }
        }
    }

    #[test]
    #[ignore = "manual fixed-work CPU benchmark"]
    fn benchmark_single_packet_hashing() {
        let reference = std::env::var_os("XHYS_HASH_REFERENCE").is_some();
        println!(
            "salamander_bytes={} streaming_state_bytes={}",
            std::mem::size_of::<Salamander>(),
            std::mem::size_of::<blake2b_simd::State>()
        );
        for key_len in [13, 120, 121, 1024] {
            let key = vec![0xa3; key_len];
            let mask = Salamander::new(&key).unwrap();
            let mut template = hash_params().to_state();
            template.update(&key);
            for payload_size in [64, 1200, 1452] {
                let mut packet = vec![0x71; SALT_LEN + payload_size];
                let started = std::time::Instant::now();
                for index in 0..131072u64 {
                    let salt = index.to_le_bytes();
                    let digest = if reference {
                        let mut hash = std::hint::black_box(&template).clone();
                        hash.update(std::hint::black_box(&salt));
                        hash.finalize()
                    } else {
                        mask.key(std::hint::black_box(&salt))
                    };
                    packet[..SALT_LEN].copy_from_slice(&salt);
                    Salamander::xor_key(digest.as_bytes(), &mut packet[SALT_LEN..]);
                    std::hint::black_box(&packet);
                }
                println!(
                    "key_len={key_len} payload_size={payload_size} reference={reference} ns_per_packet={:.3}",
                    started.elapsed().as_nanos() as f64 / 131072.
                );
            }
        }
    }

    #[test]
    fn fresh_salts_and_in_place_round_trip() {
        let sm = Salamander::new(b"password").unwrap();
        let data = vec![0xa5; 1500];
        let mut a = Vec::new();
        let mut b = Vec::new();
        sm.seal(&data, &mut a).unwrap();
        sm.seal(&data, &mut b).unwrap();
        assert_ne!(&a[..8], &b[..8]);
        assert_eq!(sm.open_in_place(&mut a).unwrap(), data);
        assert!(sm.open_in_place(&mut [0; 8]).is_err());
        assert!(Salamander::new(b"abc").is_err());
    }

    #[test]
    fn segmented_packets_have_distinct_salts_and_independent_round_trips() {
        let sm = Salamander::new(b"password").unwrap();
        for len in [1, 31, 32, 33, 1200, 1201, 1200 * 32] {
            let payload: Vec<_> = (0..len).map(|i| (i % 251) as u8).collect();
            let mut wire = Vec::new();
            let mut random = vec![0; payload.len().div_ceil(1200) * SALT_LEN];
            getrandom::fill(&mut random).unwrap();
            sm.seal_segments(&payload, 1200, &random, &mut wire);
            let mut decoded = Vec::new();
            let mut salts = std::collections::HashSet::new();
            for packet in wire.chunks_mut(1200 + SALT_LEN) {
                assert!(salts.insert(packet[..SALT_LEN].to_vec()));
                decoded.extend_from_slice(sm.open_in_place(packet).unwrap());
            }
            assert_eq!(decoded, payload);
        }
    }
}
