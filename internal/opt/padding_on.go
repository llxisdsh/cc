//go:build !cc_disable_padding && (cc_enable_padding || !(amd64 || 386 || arm || mips || mipsle || wasm))

package opt

// Padding_ controls cache line padding for Map's internal counter.
// Enabled by default on architectures where padding is the safer choice to
// prevent false sharing. Use cc_disable_padding to force it off.
const Padding_ = 1
