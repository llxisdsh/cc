//go:build cc_disable_padding || (!cc_enable_padding && !(arm64 || loong64 || mips64 || mips64le || ppc64 || ppc64le || riscv64 || s390x))

package opt

const Padding_ = 0
