use crate::protocol::Error;

pub const SALT_LEN: usize = 8;

#[derive(Clone)]
pub struct Salamander {
    hash: blake2b_simd::State,
}

impl Salamander {
    pub fn new(psk: &[u8]) -> Result<Self, Error> {
        if psk.len() < 4 {
            return Err(Error::Invalid(
                "Salamander PSK must have at least four bytes",
            ));
        }
        let mut hash = blake2b_simd::Params::new().hash_length(32).to_state();
        hash.update(psk);
        Ok(Self { hash })
    }

    fn key(&self, salt: &[u8]) -> blake2b_simd::Hash {
        let mut hash = self.hash.clone();
        hash.update(salt);
        hash.finalize()
    }

    fn xor(&self, salt: &[u8], payload: &mut [u8]) {
        let key = self.key(salt);
        let (chunks, tail) = payload.as_chunks_mut::<32>();
        for chunk in chunks {
            for (byte, mask) in chunk.iter_mut().zip(key.as_bytes()) {
                *byte ^= mask;
            }
        }
        for (byte, mask) in tail.iter_mut().zip(key.as_bytes()) {
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
        let (chunks, tail) = data.as_chunks::<32>();
        for chunk in chunks {
            let mut encoded = *chunk;
            for (byte, mask) in encoded.iter_mut().zip(key.as_bytes()) {
                *byte ^= mask;
            }
            output.extend_from_slice(&encoded);
        }
        let mut encoded = [0u8; 32];
        for ((dest, byte), mask) in encoded.iter_mut().zip(tail).zip(key.as_bytes()) {
            *dest = byte ^ mask;
        }
        output.extend_from_slice(&encoded[..tail.len()]);
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
        for key_len in [4, 120, 127, 128, 129, 255, 1024] {
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
