//! Protocol and congestion core for the native Hysteria backend.
//! Socket integration is opt-in; this crate does not replace Xray registrations.

pub mod brutal;
pub mod ffi;
pub mod protocol;
pub mod salamander;
pub mod udp;

#[cfg(feature = "allocation-profile")]
mod allocation_profile;

#[cfg(feature = "engine")]
mod congestion;
#[cfg(all(feature = "engine", unix))]
pub mod engine;
#[cfg(feature = "engine")]
mod h3_transport;
#[cfg(feature = "engine")]
pub mod hopping;
#[cfg(feature = "engine")]
mod masquerade;
#[cfg(all(feature = "engine", unix))]
mod packet_socket;
#[cfg(all(feature = "engine", any(target_os = "linux", target_os = "android")))]
mod shared;
#[cfg(feature = "probe")]
pub mod socket;

pub const ABI_VERSION: u32 = 4;

/// Version of the setup/teardown and shared-memory storage ABI.
#[unsafe(no_mangle)]
pub extern "C" fn xhys_abi_version() -> u32 {
    ABI_VERSION
}
