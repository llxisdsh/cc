//go:build cc_enable_padding || (!cc_disable_padding && (arm64 || loong64 || mips64 || mips64le || ppc64 || ppc64le || riscv64 || s390x))

package opt

// Padding_ controls cache line padding for Map's internal counter.
// Enabled on specific 64-bit architectures to prevent false sharing.
const Padding_ = 1
