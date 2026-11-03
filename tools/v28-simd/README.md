# V28 SIMD backend

This branch targets Go 1.28 and calls the native
`simd.Mask8s.ToBits(dst []uint) int` API directly. It also requires
`simd.MaxMaskBitsLen`. The probing loop lives in `V28Map.Load`; there is no
project-level forwarding helper or Go 1.27 compatibility overlay.

The target API is based on [CL 855881](https://go.dev/cl/855881). Its inclusion
in Go 1.28, final signature, and compiler support for SIMD in generic methods
must be verified against the released toolchain. This branch is prepared for
that release, not a claim that the release has already been validated.

## Build selection

- Go 1.28+, `GOEXPERIMENT=simd`, and a non-race build select the V28 backend.
- Older Go versions, builds without the experiment, and race builds expose
  `V28Map` as an alias of `Map`; they do not exercise V28.
- The root `go.mod` remains at Go 1.24 for those fallback builds.
- The SIMD CI job uses Go `1.28.x` and native Go commands. It requires that
  toolchain to be available; no temporary SDK or overlay is installed by CI.

## Before merging into main

1. Verify that released Go 1.28 provides the expected `ToBits` API and can
   compile the direct generic `Load` method.
2. Run ordinary tests, then enable the SIMD experiment and run the full suite
   and focused V28 tests. The following commands assume the Go 1.28 toolchain:

   ```powershell
   $env:GOEXPERIMENT = 'nosimd'
   go test ./...
   $env:GOEXPERIMENT = 'simd'
   go build ./...
   go test ./...
   go test -run '^TestV28' -count=10 ./...
   ```

3. Run V28 on amd64 and arm64, and check supported vector widths and
   hit/miss benchmarks. Race tests validate only the Map fallback.
4. Revisit the root README's Experimental label after validating V28 itself.
   A Go release does not by itself make the library or its SIMD dependency
   stable. Keep any upstream experiment requirement explicit.

If the API and compiler behavior match these expectations, no further V28
source migration is planned before merging. Otherwise, adjust or defer the
merge. If SIMD becomes available without the experiment, remove
`goexperiment.simd` from the V28/test build constraints and
`!goexperiment.simd` from the fallback constraint together, and update CI.

## SIMD width

Vector width follows Go's portable SIMD feature selection, not just the
largest vector instruction supported by the CPU. The previously tested Go
1.27.3 requires both AVX2 and VPCLMULQDQ to select 256 bits by default. The
Threadripper 3970X supports AVX2 but not VPCLMULQDQ, so that toolchain selects
128 bits. Recheck the policy when Go 1.28 is released.

`GODEBUG=simd=+256` bypasses the full-feature check in that tested toolchain
and works for V28's AVX2-only byte operations. It affects the whole process
and does not make missing instructions available. V28 does not force this
setting; do not request widths or operations unsupported by the CPU.

V28 currently requires a power-of-two vector width from 16 to 64 bytes whose
mask fits in one native uint word. Supporting wider future vectors is
separate work; retain the layout guard. For js/wasm, use a cache-line override
such as `-tags=cc_cachelinesize_64` for this repository's configuration.
