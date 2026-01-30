//go:build cc_enable_padding || (!cc_disable_padding && !(386 || arm || mips || mipsle || amd64 || wasm))

package opt

// Padding_ controls cache line padding for Map's internal counter.
// Enabled on other 64-bit architectures to prevent false sharing.
const Padding_ = 1
