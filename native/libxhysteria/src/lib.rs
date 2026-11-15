//! Protocol and congestion core for the native Hysteria backend.
//! Socket integration is opt-in; this crate does not replace Xray registrations.

pub mod brutal;
pub mod ffi;
pub mod protocol;
pub mod salamander;
pub mod udp;

#[cfg(feature = "engine")]
mod congestion;
#[cfg(all(feature = "engine", unix))]
pub mod engine;
#[cfg(feature = "engine")]
mod h3_transport;
#[cfg(feature = "engine")]
pub mod hopping;
#[cfg(feature = "probe")]
pub mod socket;

pub const ABI_VERSION: u32 = 1;

/// Version query only. No network engine lifecycle is exposed yet.
#[unsafe(no_mangle)]
pub extern "C" fn xhys_abi_version() -> u32 {
    ABI_VERSION
}
