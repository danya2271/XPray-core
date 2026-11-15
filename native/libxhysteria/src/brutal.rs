//! Port of Xray's Brutal sampling and token-bucket pacer.
//! All timestamps are monotonic nanoseconds and all rates are bytes/second.

const SECOND: u64 = 1_000_000_000;
const MIN_PACING_DELAY: u64 = 1_000_000;
const MIN_ACK_RATE: f64 = 0.8;
const INITIAL_MTU: u64 = 1280;

#[derive(Clone, Copy, Debug, Default)]
struct Sample {
    second: u64,
    acked: u64,
    lost: u64,
}

#[derive(Clone, Debug)]
pub struct Brutal {
    rate: u64,
    compensation: bool,
    samples: [Sample; 5],
    ack_rate: f64,
    mtu: u64,
    budget_at_send: u64,
    last_send: Option<u64>,
}

impl Brutal {
    pub fn new(bytes_per_second: u64, compensation: bool) -> Option<Self> {
        if bytes_per_second == 0 || bytes_per_second > i64::MAX as u64 / 2 {
            return None;
        }
        Some(Self {
            rate: bytes_per_second,
            compensation,
            samples: [Sample::default(); 5],
            ack_rate: 1.0,
            mtu: INITIAL_MTU,
            budget_at_send: 10 * INITIAL_MTU,
            last_send: None,
        })
    }

    pub fn on_congestion_event(&mut self, now_ns: u64, acked: u64, lost: u64) {
        let second = now_ns / SECOND;
        let slot = &mut self.samples[(second % 5) as usize];
        if slot.second != second {
            *slot = Sample {
                second,
                ..Sample::default()
            };
        }
        slot.acked = slot.acked.saturating_add(acked);
        slot.lost = slot.lost.saturating_add(lost);
        let (acks, losses) = self
            .samples
            .iter()
            .filter(|s| s.second >= second.saturating_sub(5))
            .fold((0u64, 0u64), |(a, l), s| {
                (a.saturating_add(s.acked), l.saturating_add(s.lost))
            });
        self.ack_rate = if !self.compensation || acks.saturating_add(losses) < 50 {
            1.0
        } else {
            (acks as f64 / (acks as f64 + losses as f64)).max(MIN_ACK_RATE)
        };
    }

    pub fn ack_rate(&self) -> f64 {
        self.ack_rate
    }

    pub fn pacing_rate(&self) -> u64 {
        (self.rate as f64 / self.ack_rate) as u64
    }

    pub fn congestion_window(&self, rtt_ns: u64) -> u64 {
        if rtt_ns == 0 {
            return 10240;
        }
        ((self.rate as f64 * (rtt_ns as f64 / SECOND as f64) * 2.0 / self.ack_rate) as u64)
            .max(self.mtu)
    }

    pub fn set_max_datagram_size(&mut self, size: u16) -> bool {
        if size == 0 {
            return false;
        }
        self.mtu = size as u64;
        true
    }

    fn burst_size(&self) -> u64 {
        let timed = (4 * MIN_PACING_DELAY as u128 * self.pacing_rate() as u128 / SECOND as u128)
            .min(u64::MAX as u128) as u64;
        timed.max(10 * self.mtu)
    }

    pub fn budget(&self, now_ns: u64) -> u64 {
        let Some(sent) = self.last_send else {
            return self.burst_size();
        };
        let refill = (self.pacing_rate() as u128 * now_ns.saturating_sub(sent) as u128
            / SECOND as u128)
            .min(u64::MAX as u128) as u64;
        self.budget_at_send
            .saturating_add(refill)
            .min(self.burst_size())
    }

    pub fn on_packet_sent(&mut self, now_ns: u64, bytes: u64) {
        self.budget_at_send = self.budget(now_ns).saturating_sub(bytes);
        self.last_send = Some(now_ns);
    }

    /// None means sending can proceed immediately; Some is an absolute deadline.
    pub fn time_until_send(&self) -> Option<u64> {
        if self.budget_at_send >= self.mtu {
            return None;
        }
        let needed = SECOND as u128 * (self.mtu - self.budget_at_send) as u128;
        let delay = needed
            .div_ceil(self.pacing_rate() as u128)
            .min(u64::MAX as u128) as u64;
        Some(
            self.last_send
                .unwrap_or(0)
                .saturating_add(delay.max(MIN_PACING_DELAY)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loss_compensation_matches_go() {
        for (acked, lost, want) in [(100, 0, 1.0), (80, 20, 0.8), (50, 50, 0.8), (10, 5, 1.0)] {
            for compensation in [false, true] {
                let mut b = Brutal::new(1_000_000, compensation).unwrap();
                b.on_congestion_event(5 * SECOND, acked, lost);
                assert_eq!(b.ack_rate(), if compensation { want } else { 1.0 });
            }
        }
    }

    #[test]
    fn rotates_buckets_and_does_not_reset_too_early() {
        let mut b = Brutal::new(1_000_000, true).unwrap();
        b.on_congestion_event(SECOND, 80, 20);
        b.on_congestion_event(2 * SECOND, 100, 0);
        assert_eq!(b.ack_rate(), 0.9);
        b.on_congestion_event(6 * SECOND, 0, 0);
        assert_eq!(b.ack_rate(), 1.0);
        b.on_congestion_event(8 * SECOND, 1, 0);
        assert_eq!(b.ack_rate(), 1.0);
    }

    #[test]
    fn caps_compensation_and_preserves_rtt_window() {
        let mut b = Brutal::new(1_000_000, true).unwrap();
        b.on_congestion_event(SECOND, 0, 100);
        assert_eq!(b.pacing_rate(), 1_250_000);
        assert_eq!(b.congestion_window(0), 10240);
        assert_eq!(b.congestion_window(100_000_000), 250_000);
        assert_eq!(b.congestion_window(1), INITIAL_MTU);
    }

    #[test]
    fn pacing_burst_and_deadline_round_up() {
        let mut b = Brutal::new(999_999, true).unwrap();
        assert_eq!(b.budget(SECOND), 12800);
        b.on_packet_sent(SECOND, 12800);
        assert_eq!(b.budget(SECOND + 1_000_000), 999);
        assert_eq!(b.time_until_send(), Some(SECOND + 1_280_002));
        assert!(b.set_max_datagram_size(1400));
        assert_eq!(b.congestion_window(1), 1400);
        assert!(!b.set_max_datagram_size(0));
        assert!(Brutal::new(0, true).is_none());
    }
}
