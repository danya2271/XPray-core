use rustls::{Side, SupportedCipherSuite, quic};
use std::{hint::black_box, time::Instant};

fn keys(suite: SupportedCipherSuite, side: Side, id: &[u8]) -> quic::Keys {
    suite
        .tls13()
        .unwrap()
        .quic_suite()
        .unwrap()
        .keys(id, side, quic::Version::V1)
}

fn compatible(sender: SupportedCipherSuite, receiver: SupportedCipherSuite, size: usize) {
    let tx = keys(sender, Side::Client, b"compat01");
    let rx = keys(receiver, Side::Server, b"compat01");
    let header = [0x43u8; 16];
    let mut packet = vec![0x5a; size + 16];
    let tag = tx
        .local
        .packet
        .encrypt_in_place(42, &header, &mut packet[..size])
        .unwrap();
    packet[size..].copy_from_slice(tag.as_ref());
    let mut first = 0x43;
    let mut pn = [0, 0, 0, 42];
    tx.local
        .header
        .encrypt_in_place(&packet[..16], &mut first, &mut pn)
        .unwrap();
    rx.remote
        .header
        .decrypt_in_place(&packet[..16], &mut first, &mut pn)
        .unwrap();
    assert_eq!((first, pn), (0x43, [0, 0, 0, 42]));
    let ciphertext = packet.clone();
    assert_eq!(
        rx.remote
            .packet
            .decrypt_in_place(42, &header, &mut packet)
            .unwrap(),
        vec![0x5a; size]
    );
    packet = ciphertext.clone();
    let mut wrong_header = header;
    wrong_header[0] ^= 1;
    assert!(
        rx.remote
            .packet
            .decrypt_in_place(42, &wrong_header, &mut packet)
            .is_err()
    );
    packet = ciphertext;
    packet[size / 2] ^= 1;
    assert!(
        rx.remote
            .packet
            .decrypt_in_place(42, &header, &mut packet)
            .is_err()
    );
}

fn bench(provider: &str, suite: SupportedCipherSuite, size: usize, repeat: u64) {
    const COUNT: u64 = 262144;
    let id = (repeat * 4096 + size as u64).to_be_bytes();
    let keys = keys(suite, Side::Client, &id);
    let mut packet = vec![0x5a; size + 16];
    let header = [0x43u8; 16];
    let started = Instant::now();
    for pn in 0..COUNT {
        let tag = keys
            .local
            .packet
            .encrypt_in_place(pn, &header, &mut packet[..size])
            .unwrap();
        packet[size..].copy_from_slice(tag.as_ref());
        let mut first = 0x43;
        let mut number = (pn as u32).to_be_bytes();
        keys.local
            .header
            .encrypt_in_place(&packet[..16], &mut first, &mut number)
            .unwrap();
        black_box((first, number));
        keys.local
            .packet
            .decrypt_in_place(pn, &header, &mut packet)
            .unwrap();
        black_box(&packet);
    }
    let elapsed = started.elapsed().as_nanos() as f64 / COUNT as f64;
    assert_eq!(&packet[..size], vec![0x5a; size]);
    println!("repeat={repeat} provider={provider} bytes={size} seal_open_header_ns={elapsed:.3}");
}

fn main() {
    let ring = rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256;
    let aws = rustls::crypto::aws_lc_rs::cipher_suite::TLS13_AES_128_GCM_SHA256;
    for size in [64, 512, 1200, 1452] {
        compatible(ring, aws, size);
        compatible(aws, ring, size);
    }
    eprintln!("Cross-provider AES128-GCM payload/header/tamper checks PASS");
    for repeat in 1..=7 {
        let providers = if repeat % 2 == 1 {
            [("ring", ring), ("aws", aws)]
        } else {
            [("aws", aws), ("ring", ring)]
        };
        for size in [64, 512, 1200, 1452] {
            for (name, suite) in providers {
                bench(name, suite, size, repeat);
            }
        }
    }
}
