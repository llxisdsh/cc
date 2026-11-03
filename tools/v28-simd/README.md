# Temporary V28 SIMD support

V28 calls the proposed `simd.Mask8s.ToBits(dst []uint) int` API directly.
Until it is available in the installed Go toolchain, use `v28_simd.ps1` from
the repository root. It enables `GOEXPERIMENT=simd` and adds a temporary
standard-library overlay. It does not change GOROOT or persistent Go settings.

```powershell
.\v28_simd.ps1
.\v28_simd.ps1 -GoArgs @('test', '-run=^TestV28', '-count=10', '.')
.\v28_simd.ps1 -GoArgs @('test', '-run=^$', '-bench=BenchmarkV28MapStringLoad', '-benchmem', '.')
```

The `.go.txt` templates backport only the Mask8s subset of
[CL 855881](https://go.dev/cl/855881), revision
`b15462f6b2d877b259bf375ef5b6604d0f3fce69`: the maximum-size constant, method
declaration, emulation, hardware bridge, and arm64/wasm extraction. Method
bodies match that revision; filenames and comments are adapted for the overlay.
Other mask element sizes are deliberately not backported. Go's BSD license is
included here. These templates are not compiled as packages in this repository.

`Load` forwards to the generic `v28Load` function because Go 1.27.3's SIMD
rewriter reports `missing Types entry: Load@simd0(key)` when that body is a
generic method. The public API and version checks are unchanged.

The target is Go 1.27.3; the temporary SIMD CI job stays on `1.27.x`.
A toolchain already containing `src/simd/maskbits.go` uses its own
implementation instead; select one with `-Go 'path/to/go.exe'`.
Normal builds without the SIMD experiment, and race builds, keep the existing
Map fallback. SIMD builds on unpatched Go need the wrapper/overlay.

SIMD width is selected by `simd`, including its `GODEBUG` setting. Go 1.27.3
requires both AVX2 and VPCLMULQDQ to select 256 bits by default. The
Threadripper 3970X supports AVX2 but not VPCLMULQDQ, so the default portable
width is 128 bits. This is Go's package-wide feature policy, not a ToBits
restriction. `GODEBUG=simd=256` fails that feature check; `GODEBUG=simd=+256`
explicitly bypasses it. The latter can exercise V28's AVX2-only byte operations,
but affects the whole process and must not be used to assume support for every
SIMD instruction on that CPU. V28 does not override this setting itself.

V28 currently requires a power-of-two vector width from 16 to 64 bytes whose
mask fits in one native uint word. Supporting wider future vectors is separate
work; keep the layout guard when migrating toolchains.

For js/wasm, include a cache-line override such as
`-tags=cc_cachelinesize_64` in `-GoArgs` for this repository's existing
cache-line configuration.

## Moving to Go 1.28

This is a migration plan, not a guarantee that Go 1.28 will include the API.
First verify that the released toolchain provides `Mask8s.ToBits([]uint) int`
and `simd.MaxMaskBitsLen` with the expected semantics. If it does, the current
probing code and the `Load`/`v28Load` arrangement can stay unchanged.

1. Change `go1.27` to `go1.28` in the build constraints of `v28_map.go`,
   `v28_simd_go127.go`, `v28_map_test.go`, `v28_simd_test.go`, and
   `v28_map_bench_test.go`. Retain `!race && goexperiment.simd` while the
   official package still requires the experiment.
2. Change the fallback constraint in `v28_map_fallback.go` to
   `race || !goexperiment.simd || !go1.28`. Older Go versions, non-SIMD builds,
   and race builds will still expose V28Map as an alias of Map, not run V28.
3. Optionally rename `v28_simd_go127.go` to `v28_simd_go128.go` and remove its
   temporary-overlay comment. The build constraint, not this filename,
   enforces the minimum Go version.
4. Set the SIMD job in `.github/workflows/go.yml` to Go `1.28.x`, replace the
   wrapper calls with `go build -v ./...` and
   `go test -v -run '^TestV28' ./...`, and remove the wrapper-only `shell: pwsh`
   settings. Keep `GOEXPERIMENT=simd` if the official package requires it.
5. Once native-toolchain tests pass, delete `v28_simd.ps1` and this
   `tools/v28-simd/` directory. No changes to GOROOT are needed.
6. Update the root README to describe the 1.28 minimum for the V28 backend.
   The root `go.mod` can remain at Go 1.24 because older builds use Map.

If SIMD becomes available without the experiment, remove `goexperiment.simd`
from the V28/test constraints and `!goexperiment.simd` from the fallback
constraint together. Do not infer that change merely from the Go version.

Before merging into main, run ordinary and SIMD tests, exercise the supported
widths, and run V28 on amd64 and arm64. Race tests only cover the Map fallback.
Removing V28's Experimental label is a separate library support decision;
keep any upstream SIMD experiment requirement explicit in the README.
