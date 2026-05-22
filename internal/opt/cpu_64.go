//go:build amd64 || arm64 || ppc64 || ppc64le || mips64 || mips64le || riscv64 || s390x || wasm

package opt

// HashPrime is the 64-bit Golden Ratio mixing constant.
const HashPrime = 0x9e3779b97f4a7c15
