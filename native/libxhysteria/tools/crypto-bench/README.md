# QUIC AES-128-GCM provider comparison

This standalone manual benchmark compares ring and AWS-LC through the same
rustls QUIC packet/header key interfaces. It does not change Xray's dependencies,
TLS configuration or production provider. The checked-in lockfile fixes both
implementations and rustls for reproduction.

```sh
cargo build --locked --release \
  --manifest-path native/libxhysteria/tools/crypto-bench/Cargo.toml
taskset -c 4 native/libxhysteria/tools/crypto-bench/target/release/xhys-crypto-probe
```

Choose one allowed physical CPU for the second command. Finish all builds and
other task benchmarks before measurement. The program checks ring→AWS-LC and
AWS-LC→ring decryption, header protection, and rejection of corrupted payloads
and incorrect associated headers at 64, 512, 1200 and 1452 bytes before timing.

Each observation performs 262,144 cycles, each with one payload encryption,
one payload decryption and one header-protection operation. Keys and payload
storage are created outside timing. Packet numbers increase within each key.
Seven repeats alternate provider order. Synthetic Initial-derived AES keys
exercise the packet algorithms; TLS handshakes are excluded.

`seal_open_header_ns` is the cost of that combined cycle, not the cost of
encryption alone. The benchmark omits socket I/O, packet construction, QUIC
flow control and scheduling. Its result cannot establish whole-transport
throughput, p99, RSS, or an optimum for a different CPU architecture.

## Android arm64

Run on a physical target to compare its implementations. An x86 result or an
emulator run cannot select the faster provider on that ARM CPU. Record the
device/CPU, OS, chosen core and power/thermal conditions with the output.

Cross-build using an installed Android NDK, replacing the first path:

```sh
crypto_ndk=/absolute/path/to/android-ndk
crypto_toolchain="$crypto_ndk/toolchains/llvm/prebuilt/linux-x86_64/bin"
env CC="$crypto_toolchain/aarch64-linux-android26-clang" \
  CXX="$crypto_toolchain/aarch64-linux-android26-clang++" \
  AR="$crypto_toolchain/llvm-ar" \
  CC_aarch64_linux_android="$crypto_toolchain/aarch64-linux-android26-clang" \
  AR_aarch64_linux_android="$crypto_toolchain/llvm-ar" \
  CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="$crypto_toolchain/aarch64-linux-android26-clang" \
  cargo build --locked --release --target aarch64-linux-android \
  --manifest-path native/libxhysteria/tools/crypto-bench/Cargo.toml
```

With the intended device connected:

```sh
adb push native/libxhysteria/tools/crypto-bench/target/aarch64-linux-android/release/xhys-crypto-probe /data/local/tmp/
adb shell chmod 755 /data/local/tmp/xhys-crypto-probe
adb shell /data/local/tmp/xhys-crypto-probe
```

Select one allowed physical core using the target's affinity tool when
reproducing the pinned measurements. No particular core number is optimal
across devices. Compiling the executable does not validate its speed or
device runtime behavior.
