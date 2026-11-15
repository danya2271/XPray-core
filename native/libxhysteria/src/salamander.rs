use crate::protocol::Error;
use blake2::{Blake2b, Digest, digest::consts::U32};

pub const SALT_LEN: usize = 8;

#[derive(Clone)]
pub struct Salamander {
    psk: Box<[u8]>,
}

impl Salamander {
    pub fn new(psk: &[u8]) -> Result<Self, Error> {
        if psk.len() < 4 {
            return Err(Error::Invalid(
                "Salamander PSK must have at least four bytes",
            ));
        }
        Ok(Self { psk: psk.into() })
    }

    fn xor(&self, salt: &[u8], payload: &mut [u8]) {
        let mut hash = Blake2b::<U32>::new();
        hash.update(&self.psk);
        hash.update(salt);
        let key = hash.finalize();
        for (i, byte) in payload.iter_mut().enumerate() {
            *byte ^= key[i % 32];
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
        output.extend_from_slice(&salt);
        output.extend_from_slice(payload);
        self.xor(&salt, &mut output[SALT_LEN..]);
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
}
