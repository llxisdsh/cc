//go:build cc_disable_padding || (!cc_enable_padding && (amd64 || 386 || arm || mips || mipsle || wasm))

package opt

// Padding_ controls cache line padding for Map's internal counter.
// Disabled by default on amd64 as a size/performance trade-off, and on
// 32-bit architectures and wasm where padding is usually wasteful.
const Padding_ = 0
