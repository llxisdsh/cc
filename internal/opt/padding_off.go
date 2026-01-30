//go:build cc_disable_padding || (!cc_enable_padding && !(arm64 || loong64 || mips64 || mips64le || ppc64 || ppc64le || riscv64 || s390x))

package opt

// Padding_ controls cache line padding for Map's internal counter.
// Disabled on: 32-bit (limited memory), amd64 (good HW prefetch), wasm (no physical cache).
// Only specific 64-bit architectures enable padding to prevent false sharing.
const Padding_ = 0
