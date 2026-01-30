//go:build cc_disable_padding || (!cc_enable_padding && (386 || arm || mips || mipsle || amd64 || wasm))

package opt

// Padding_ controls cache line padding for Map's internal counter.
// Disabled on 32-bit (386/arm/mips), amd64 (good HW prefetch), and wasm (no physical cache).
const Padding_ = 0
