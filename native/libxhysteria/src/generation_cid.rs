use aes::cipher::{BlockDecrypt, BlockEncrypt, KeyInit};
use quinn_proto::{ConnectionId, ConnectionIdGenerator, InvalidCid};
use serde::Deserialize;
use std::time::Duration;

const CID_LENGTH: usize = 16;
const FORMAT: &[u8; 5] = b"NXCI\x01";

#[derive(Clone, Deserialize)]
pub struct Config {
    pub key: [u8; CID_LENGTH],
    pub slot: u8,
}

pub(crate) struct Generator {
    cipher: aes::Aes128,
    slot: u8,
}

impl Generator {
    pub(crate) fn new(config: &Config) -> Self {
        Self {
            cipher: aes::Aes128::new((&config.key).into()),
            slot: config.slot,
        }
    }

    fn encode(&self, nonce: [u8; 10]) -> ConnectionId {
        let mut plain = [0; CID_LENGTH];
        plain[..5].copy_from_slice(FORMAT);
        plain[5] = self.slot;
        plain[6..].copy_from_slice(&nonce);
        self.cipher.encrypt_block((&mut plain).into());
        ConnectionId::new(&plain)
    }
}

impl ConnectionIdGenerator for Generator {
    fn generate_cid(&mut self) -> ConnectionId {
        let mut nonce = [0; 10];
        getrandom::fill(&mut nonce).expect("could not generate an opaque QUIC connection ID");
        self.encode(nonce)
    }

    fn validate(&self, cid: &ConnectionId) -> Result<(), InvalidCid> {
        let mut plain: [u8; CID_LENGTH] = cid.as_ref().try_into().map_err(|_| InvalidCid)?;
        self.cipher.decrypt_block((&mut plain).into());
        if &plain[..5] == FORMAT && plain[5] == self.slot {
            Ok(())
        } else {
            Err(InvalidCid)
        }
    }

    fn cid_len(&self) -> usize {
        CID_LENGTH
    }

    fn cid_lifetime(&self) -> Option<Duration> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_go_wire_format() {
        let mut key = [0; 16];
        key[..3].copy_from_slice(&[1, 2, 3]);
        let mut generator = Generator::new(&Config { key, slot: 12 });
        let cid = generator.encode([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(
            cid.as_ref(),
            &[
                0x9c, 0x45, 0x94, 0xa8, 0x12, 0xf8, 0xdf, 0xfe, 0x10, 0xb5, 0x82, 0xbb, 0xea, 0xbe,
                0xfd, 0xfb
            ]
        );
        assert!(generator.validate(&cid).is_ok());
        assert!(
            Generator::new(&Config { key, slot: 13 })
                .validate(&cid)
                .is_err()
        );
        let random = generator.generate_cid();
        assert_ne!(random, cid);
        assert!(generator.validate(&random).is_ok());
    }
}
