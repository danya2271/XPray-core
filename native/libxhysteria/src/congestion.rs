use crate::brutal::Brutal;
use quinn::congestion::{Controller, ControllerFactory, ControllerMetrics};
use quinn_proto::RttEstimator;
use std::{
    any::Any,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

pub struct Factory {
    pub compensate: bool,
    pub fallback: Arc<dyn ControllerFactory + Send + Sync>,
}

impl ControllerFactory for Factory {
    fn build(self: Arc<Self>, now: Instant, mtu: u16) -> Box<dyn Controller> {
        let mut brutal = Brutal::new(1, self.compensate).unwrap();
        brutal.set_max_datagram_size(mtu);
        Box::new(Hybrid {
            brutal,
            selected: Arc::new(AtomicU64::new(0)),
            active: false,
            compensate: self.compensate,
            mtu,
            epoch: now,
            rtt_ns: 0,
            fallback: self.fallback.clone().build(now, mtu),
        })
    }
}

struct Hybrid {
    brutal: Brutal,
    selected: Arc<AtomicU64>,
    active: bool,
    compensate: bool,
    mtu: u16,
    epoch: Instant,
    rtt_ns: u64,
    fallback: Box<dyn Controller>,
}

impl Hybrid {
    fn nanos(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.epoch)
            .as_nanos()
            .min(u64::MAX as u128) as u64
    }
    fn sync(&mut self) -> u64 {
        let rate = self.selected.load(Ordering::Acquire);
        if rate != 0 {
            if !self.active {
                self.brutal = Brutal::new(rate, self.compensate).unwrap();
                self.brutal.set_max_datagram_size(self.mtu);
            } else {
                self.brutal.set_rate(rate);
            }
        }
        self.active = rate != 0;
        rate
    }
}

impl Controller for Hybrid {
    fn can_send(&self, in_flight: u64, bytes_to_send: u64) -> bool {
        if self.selected.load(Ordering::Acquire) == 0 {
            self.fallback.can_send(in_flight, bytes_to_send)
        } else {
            in_flight <= self.window()
        }
    }
    fn pacing_deadline(&mut self, now: Instant, rtt: Duration) -> Option<Option<Instant>> {
        if self.sync() == 0 {
            return None;
        }
        self.rtt_ns = rtt.as_nanos().min(u64::MAX as u128) as u64;
        let now_ns = self.nanos(now);
        if self.brutal.budget(now_ns) >= self.mtu as u64 {
            return Some(None);
        }
        Some(
            self.brutal
                .time_until_send()
                .map(|n| self.epoch + Duration::from_nanos(n)),
        )
    }
    fn on_packets_lost(&mut self, now: Instant, packets: u64) {
        if self.sync() > 0 {
            self.brutal.on_congestion_event(self.nanos(now), 0, packets);
        }
    }
    fn on_sent(&mut self, now: Instant, bytes: u64, last: u64) {
        self.fallback.on_sent(now, bytes, last);
        if self.sync() > 0 {
            self.brutal.on_packet_sent(self.nanos(now), bytes);
        }
    }
    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        limited: bool,
        rtt: &RttEstimator,
    ) {
        self.fallback.on_ack(now, sent, bytes, limited, rtt);
        self.rtt_ns = rtt.get().as_nanos().min(u64::MAX as u128) as u64;
        if self.sync() > 0 {
            self.brutal.on_congestion_event(self.nanos(now), 1, 0);
        }
    }
    fn on_end_acks(&mut self, now: Instant, flight: u64, limited: bool, last: Option<u64>) {
        self.fallback.on_end_acks(now, flight, limited, last)
    }
    fn on_congestion_event(&mut self, now: Instant, sent: Instant, persistent: bool, bytes: u64) {
        self.fallback
            .on_congestion_event(now, sent, persistent, bytes)
    }
    fn on_mtu_update(&mut self, mtu: u16) {
        self.mtu = mtu;
        self.brutal.set_max_datagram_size(mtu);
        self.fallback.on_mtu_update(mtu)
    }
    fn window(&self) -> u64 {
        let rate = self.selected.load(Ordering::Acquire);
        if rate == 0 {
            self.fallback.window()
        } else {
            self.brutal.window_for_rate(rate, self.rtt_ns)
        }
    }
    fn initial_window(&self) -> u64 {
        self.fallback.initial_window()
    }
    fn metrics(&self) -> ControllerMetrics {
        if self.selected.load(Ordering::Acquire) == 0 {
            return self.fallback.metrics();
        }
        let mut metrics = ControllerMetrics::default();
        metrics.congestion_window = self.window();
        metrics.pacing_rate = Some(self.brutal.pacing_rate().saturating_mul(8));
        metrics
    }
    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(Self {
            brutal: self.brutal.clone(),
            selected: self.selected.clone(),
            active: self.active,
            compensate: self.compensate,
            mtu: self.mtu,
            epoch: self.epoch,
            rtt_ns: self.rtt_ns,
            fallback: self.fallback.clone_box(),
        })
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

pub fn negotiate(connection: &quinn::Connection, mode: &str, local: u64, peer: &str) {
    let rate = match mode {
        "reno" | "bbr" => 0,
        "force-brutal" => local,
        _ => match peer.parse::<u64>() {
            Ok(remote) if local > 0 && remote > 0 => local.min(remote),
            _ => 0,
        },
    };
    if let Ok(controller) = connection
        .congestion_state()
        .into_any()
        .downcast::<Hybrid>()
    {
        controller.selected.store(rate, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_loss_counts_and_custom_pacing_are_independent_of_cwnd() {
        let now = Instant::now();
        let factory = Arc::new(Factory {
            compensate: true,
            fallback: Arc::new(quinn::congestion::NewRenoConfig::default()),
        });
        let mut controller = factory
            .build(now, 1280)
            .into_any()
            .downcast::<Hybrid>()
            .unwrap();
        controller.selected.store(1_000_000, Ordering::Release);
        assert!(controller.can_send(controller.window(), 1280));
        assert!(!controller.can_send(controller.window() + 1, 1280));
        assert_eq!(
            controller.pacing_deadline(now, Duration::from_millis(100)),
            Some(None)
        );
        controller.brutal.on_congestion_event(0, 80, 0);
        controller.on_packets_lost(now, 20);
        assert_eq!(controller.brutal.pacing_rate(), 1_250_000);
        controller.on_sent(now, 12800, 1);
        assert!(
            controller
                .pacing_deadline(now, Duration::from_millis(100))
                .flatten()
                .is_some()
        );
        assert_eq!(controller.window(), 250_000);
    }
}
