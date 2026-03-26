package cc

import (
	"math/bits"
	"reflect"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/llxisdsh/cc/internal/opt"
)

// ============================================================================
// Private Constants
// ============================================================================

const (
	bitSize       = 32 << (^uint(0) >> 63) // 32 or 64
	maxInt        = 1<<(bitSize-1) - 1     // maxInt32 or maxInt64
	cacheLineSize = opt.CacheLineSize_     // size of a cache line in bytes
)

const (
	// opByteIdx reserves the highest byte of meta for extended status flags
	opByteIdx    = 7
	opLockMask   = uint64(1) << (opByteIdx*8 + 7)
	opNextMask   = uint64(1) << (opByteIdx*8 + 6)
	metaDataMask = uint64(0x00ffffffffffffff)

	// entriesPerBucket defines the number of per-bucket entry pointers.
	// Computed at compile time to avoid padding while packing buckets
	// tightly within cache lines.
	//
	// Calculation:
	//   ptrSize  = sizeof(unsafe.Pointer)
	//   overhead = 8(meta) + ptrSize(next)
	//   target   = min(CacheLineSize, base)
	//   base     = 32 on 32-bit, 64 on 64-bit
	//   entries  = min(7, (target - overhead) / ptrSize)
	//
	// Rationale:
	//   - 64-bit: bucket size becomes 64B → 1/2/4 buckets per
	//     64/128/256B cache line, with no per-bucket padding.
	//   - 32-bit: bucket size becomes 32B → 2/4/8 buckets per
	//     64/128/256B cache line, also without padding.
	//
	// Example outcomes (cacheLineSize → entriesPerBucket):
	//   64bit: 32B → 2; 64B → 6; 128B → 6; 256B → 6
	//   32bit: 32B → 5; 64B → 5; 128B → 5; 256B → 5
	pointerSize    = int(unsafe.Sizeof(unsafe.Pointer(nil)))
	bucketOverhead = int(unsafe.Sizeof(struct {
		meta uint64
		next unsafe.Pointer
	}{}))
	maxBucketBytes   = min(int(cacheLineSize), 32+32*(pointerSize/8))
	entriesPerBucket = min(opByteIdx, (maxBucketBytes-bucketOverhead)/pointerSize)

	// Metadata constants for bucket entry management
	metaEmpty uint64 = 0
	metaMask  uint64 = 0x8080808080808080 >>
		(64 - min(entriesPerBucket*8, 64))

	// h2 byte format: [1-bit: non-empty flag][7-bit: entropy]
	h2Empty  = 0
	h2Bits   = 7
	h2TopBit = 1 << h2Bits // 0x80, non-empty marker
)

const (
	// shrinkFraction: shrink table when occupancy < 1/shrinkFraction
	shrinkFraction = 8
	// loadFactor: resize table when occupancy > loadFactor
	loadFactor = 0.75
	// minTableLen: minimum number of buckets
	minTableLen = 32
	// minBucketsPerCPU: threshold for parallel resizing
	minBucketsPerCPU = 4
	// asyncThreshold: threshold for asynchronous resize
	asyncThreshold = 128 * 1024
	// resizeOverPartition: over-partition factor to reduce resize tail latency
	resizeOverPartition = 8
)

type mapRebuildHint uint8

const (
	mapNoHint mapRebuildHint = iota
	mapGrowHint
	mapShrinkHint
	mapRebuildAllowWritersHint
	mapRebuildBlockWritersHint
)

type computeOp uint8

const (
	cancelOp computeOp = iota
	updateOp
	deleteOp
)

const (
	computeInit           uint8 = 1 << iota // auto-init table if nil
	computeIgnoreHint                       // skip rebuild cooperation
	computeSkipIfFound                      // fast path: skip lock if key found
	computeSkipIfNotFound                   // fast path: skip lock if key not found
)

var maxProcs_ = runtime.GOMAXPROCS(0)

//go:nosplit
func maxProcs() int {
	return maxProcs_
}

// ============================================================================
// Private struct definitions
// ============================================================================

// counterStripe represents a striped counter to reduce contention.
//
// Padding strategy:
//   - amd64: Padding is omitted by default — performance is identical
//     with or without padding on this architecture.
//   - Other 64‑bit arches: Padding is automatically enabled by default
//     to prevent false sharing.
//   - 32‑bit arches: Padding is automatically disabled to save memory.
//
// Manual override via build options:
//   - cc_enable_padding – force padding on any architecture.
//   - cc_disable_padding – force disable padding on any architecture.
//
// When padding is active, each stripe occupies a full cache line.
// This consumes a small amount of extra memory, but significantly reduces
// performance loss due to false sharing.
type counterStripe struct {
	_ [(opt.CacheLineSize_ - unsafe.Sizeof(struct {
		c uintptr
	}{})%opt.CacheLineSize_) % opt.CacheLineSize_ * opt.Padding_]byte
	c uintptr // Counter value, accessed atomically
}

// ============================================================================
// Utility Functions
// ============================================================================

// calcParallelism calculates the number of goroutines for parallel processing.
//
// Parameters:
//   - items: Number of items to process.
//   - threshold: Minimum threshold to enable parallel processing.
//   - number of available CPU cores
//
// Returns:
//   - chunks: Suggested degree of parallelism (number of goroutines).
//
//go:nosplit
func calcParallelism(items, threshold, cpus int) int {
	if items <= threshold {
		return 1
	}
	chunks := min(items/threshold, cpus)
	// chunkSz = (items + chunks - 1) / chunks
	return chunks
}

// calcTableLen computes the bucket count for the table
// return value must be a power of 2
//
//go:nosplit
func calcTableLen(capacity int) int {
	tableLen := minTableLen
	const minThreshold = int(float64(minTableLen*entriesPerBucket) * loadFactor)
	if capacity >= minThreshold {
		const invFactor = 1.0 / (float64(entriesPerBucket) * loadFactor)
		// +entriesPerBucket-1 is used to compensate for calculation
		// inaccuracies
		tableLen = nextPowOf2(
			int(float64(capacity+entriesPerBucket-1) * invFactor),
		)
	}
	return tableLen
}

// calcSizeLen computes the size count for the table
// return value must be a power of 2
//
//go:nosplit
func calcSizeLen(tableLen, cpus int) int {
	return nextPowOf2(min(cpus, tableLen>>10))
}

// nextPowOf2 calculates the smallest power of 2 that is greater than or equal
// to n.
// Compatible with both 32-bit and 64-bit systems.
//
//go:nosplit
func nextPowOf2(v int) int {
	if v <= 0 {
		return 1
	}
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	if bitSize == 64 {
		v |= v >> 32
	}
	v++
	return v
}

// noescape hides a pointer from escape analysis. noescape is
// the identity function, but escape analysis doesn't think the
// output depends on the input.  noescape is inlined and currently
// compiles down to zero instructions.
// USE CAREFULLY!
//
//go:nosplit
//go:nocheckptr
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	//nolint:all
	//goland:noinspection ALL
	return unsafe.Pointer(x ^ 0)
}

//go:nosplit
//go:nocheckptr
func noEscape[T any](p *T) *T {
	return (*T)(noescape(unsafe.Pointer(p)))
}

// ============================================================================
// SWAR Utilities
// ============================================================================

// intHash computes the hash for integer keys.
// The switch on unsafe.Sizeof is evaluated at compile time for each generic instantiation.
// The Go compiler (since 1.18) will perform Dead Code Elimination (DCE) to remove
// unreachable branches, resulting in a zero-cost abstraction (no runtime branch).
//
//go:nosplit
func intHash[K any](ptr unsafe.Pointer) uintptr {
	switch unsafe.Sizeof(*(*K)(nil)) {
	case 8:
		if bitSize == 64 {
			return *(*uintptr)(ptr)
		} else {
			v := *(*uint64)(ptr)
			return uintptr(v>>32) ^ uintptr(v)
		}
	case 4:
		if bitSize == 32 {
			return *(*uintptr)(ptr)
		} else {
			return uintptr(*(*uint32)(ptr))
		}
	case 2:
		return uintptr(*(*uint16)(ptr))
	case 1:
		return uintptr(*(*uint8)(ptr))
	default:
		return 0
	}
}

// h1 extracts the bucket index from a hash value.
// The hash format is unified: [high bits: bucket index] [low h2Bits: h2 entropy]
// This allows branch-free extraction for all key types.
//
//go:nosplit
func h1(h uintptr) int {
	return int(h) >> h2Bits
}

//go:nosplit
func h1IntKey(h uintptr) int {
	return int(h) / entriesPerBucket
}

// h2 extracts the byte-level hash for in-bucket lookups.
// Uses the low h2Bits of the hash value.
//
//go:nosplit
func h2(h uintptr) uint8 {
	return uint8(h) | h2TopBit
}

// broadcast replicates a byte value across all bytes of an uint64.
//
//go:nosplit
func broadcast(b uint8) uint64 {
	return 0x101010101010101 * uint64(b)
}

// firstMarkedByteIndex finds the index of the first marked byte in an uint64.
// It uses the trailing zeros count to determine the position of the first set
// bit, then converts that bit position to a byte index (dividing by 8).
//
// Parameters:
//   - w: A uint64 value with bits set to mark specific bytes
//
// Returns:
//   - The index (0-7) of the first marked byte in the uint64
//
//go:nosplit
func firstMarkedByteIndex(w uint64) int {
	return bits.TrailingZeros64(w) >> 3
}

// markZeroBytes implements SWAR (SIMD Within A Register) byte search.
// It may produce false positives (e.g., for 0x0100), so results should be
// verified. Returns an uint64 with the most significant bit of each byte set if
// that byte is zero.
//
// Notes:
//   - This SWAR algorithm identifies byte positions containing zero values.
//   - The operation (w - 0x0101010101010101) triggers underflow for zero-value
//     bytes, causing their most significant bit (MSB) to flip to 1.
//   - The subsequent &^ operation isolates the MSB markers specifically for
//     bytes, that were originally zero.
//   - Finally, & metaMask filters to only consider relevant data slots,
//     using the mask-defined marker bits (MSB of each byte).
//
//go:nosplit
func markZeroBytes(w uint64) uint64 {
	return (w - 0x0101010101010101) &^ w & metaMask
}

// setByte sets the byte at index idx in the uint64 w to the value b.
// Returns the modified uint64 value.
//
//go:nosplit
func setByte(w uint64, b uint8, idx int) uint64 {
	shift := idx << 3
	return (w &^ (0xff << shift)) | (uint64(b) << shift)
}

// ============================================================================
// Slice Utilities
// ============================================================================

// unsafeSlice provides semi-ergonomic limited slice-like functionality
// without bounds checking for fixed sized slices.
type unsafeSlice[T any] struct {
	ptr unsafe.Pointer
}

func makeUnsafeSlice[T any](len int) unsafeSlice[T] {
	return unsafeSlice[T]{ptr: unsafe.Pointer(unsafe.SliceData(make([]T, len)))}
}

//go:nosplit
func toUnsafeSlice[T any](s []T) unsafeSlice[T] {
	return unsafeSlice[T]{ptr: unsafe.Pointer(unsafe.SliceData(s))}
}

//go:nosplit
func (s unsafeSlice[T]) At(i int) *T {
	return (*T)(unsafe.Add(s.ptr, unsafe.Sizeof(*new(T))*uintptr(i)))
}

// ============================================================================
// Locker Utilities
// ============================================================================

// noCopy may be added to structs which must not be copied
// after the first use.
//
// See https://golang.org/issues/8005#issuecomment-190753527
// for details.
//
// Note that it must not be embedded, due to the Lock and Unlock methods.
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

func delay(spins *int) {
	if runtime_canSpin(*spins) {
		*spins++
		runtime_doSpin()
		return
	}
	*spins = 0
	// prioritizeLowLatency: lower latency over CPU usage.
	// Uses runtime.Gosched() for faster retries, unlike time.Sleep()
	// which increases throughput but results in slightly higher tail latency.
	const prioritizeLowLatency = true
	if prioritizeLowLatency {
		runtime.Gosched()
	} else {
		// time.Sleep with non-zero duration (≈Millisecond level) works
		// effectively as backoff under high concurrency.
		// The 500µs duration is derived from Facebook/folly's implementation:
		// https://github.com/facebook/folly/blob/main/folly/synchronization/detail/Sleeper.h
		time.Sleep(500 * time.Microsecond)
	}
}

// nolint:all
//
//go:linkname runtime_canSpin sync.runtime_canSpin
//goland:noinspection ALL
func runtime_canSpin(i int) bool

// nolint:all
//
//go:linkname runtime_doSpin sync.runtime_doSpin
//goland:noinspection ALL
func runtime_doSpin()

// ============================================================================
// Hash Utilities
// ============================================================================

type (
	// HashFunc is the function to hash a value of type K.
	HashFunc func(ptr unsafe.Pointer, seed uintptr) uintptr
	// EqualFunc is the function to compare two values of type V.
	EqualFunc func(ptr unsafe.Pointer, other unsafe.Pointer) bool
)

//go:nosplit
func defaultHasher[K comparable, V any]() (
	keyHash HashFunc,
	valEqual EqualFunc,
	intKey bool,
) {
	keyHash, valEqual = defaultHasherUsingBuiltIn[K, V]()

	switch any(*new(K)).(type) {
	case uint, int, uintptr,
		uint64, int64,
		uint32, int32,
		uint16, int16,
		uint8, int8:
		return keyHash, valEqual, true
	default:
		// for types like integers
		kType := reflect.TypeFor[K]()
		switch kType.Kind() {
		case reflect.Uint, reflect.Int, reflect.Uintptr,
			reflect.Int64, reflect.Uint64,
			reflect.Int32, reflect.Uint32,
			reflect.Int16, reflect.Uint16,
			reflect.Int8, reflect.Uint8:
			return keyHash, valEqual, true
		default:
			return keyHash, valEqual, false
		}
	}
}

// defaultHasherUsingBuiltIn gets Go's built-in hash and equality functions
// for the specified types using reflection.
//
// This approach provides direct access to the type-specific functions without
// the overhead of switch statements, resulting in better performance.
//
// Notes:
//   - This implementation relies on Go's internal type representation
//   - It should be verified for compatibility with each Go version upgrade
//
//go:nosplit
func defaultHasherUsingBuiltIn[K comparable, V any]() (
	keyHash HashFunc,
	valEqual EqualFunc,
) {
	mapType := iTypeOf((map[K]V)(nil)).MapType()
	return mapType.Hasher, mapType.Elem.Equal
}

type (
	iTFlag   uint8
	iKind    uint8
	iNameOff int32
)

// TypeOff is the offset to a type from moduledata.types.  See resolveTypeOff in
// runtime.
type iTypeOff int32

type iType struct {
	Size_       uintptr
	PtrBytes    uintptr // number of (prefix) bytes in the type that can contain pointers
	Hash        uint32  // hash of type; avoids computation in hash tables
	TFlag       iTFlag  // extra type information flags
	Align_      uint8   // alignment of variable with this type
	FieldAlign_ uint8   // alignment of struct field with this type
	Kind_       iKind   // enumeration for C
	// function for comparing objects of this type
	// (ptr to object A, ptr to object B) -> ==?
	Equal func(unsafe.Pointer, unsafe.Pointer) bool
	// GCData stores the GC type data for the garbage collector.
	// Normally, GCData points to a bitmask that describes the
	// ptr/nonptr fields of the type. The bitmask will have at
	// least PtrBytes/ptrSize bits.
	// If the TFlagGCMaskOnDemand bit is set, GCData is instead a
	// **byte and the pointer to the bitmask is one dereference away.
	// The runtime will build the bitmask if needed.
	// (See runtime/type.go:getGCMask.)
	// Note: multiple types may have the same value of GCData,
	// including when TFlagGCMaskOnDemand is set. The types will, of course,
	// have the same pointer layout (but not necessarily the same size).
	GCData    *byte
	Str       iNameOff // string form
	PtrToThis iTypeOff // type for pointer to this type, may be zero
}

//go:nosplit
func (t *iType) MapType() *iMapType {
	return (*iMapType)(unsafe.Pointer(t))
}

type iMapType struct {
	iType
	Key   *iType
	Elem  *iType
	Group *iType // internal type representing a slot group
	// function for hashing keys (ptr to key, seed) -> hash
	Hasher func(unsafe.Pointer, uintptr) uintptr
}

//go:nosplit
func iTypeOf(a any) *iType {
	eface := *(*iEmptyInterface)(unsafe.Pointer(&a))
	// Types are either static (for compiler-created types) or
	// heap-allocated but always reachable (for reflection-created
	// types, held in the central map). So there is no need to
	// escape types. noescape here help avoid unnecessary escape
	// of v.
	return (*iType)(noescape(unsafe.Pointer(eface.Type)))
}

type iEmptyInterface struct {
	Type *iType
	Data unsafe.Pointer
}

// ============================================================================
// Atomic Utilities
// ============================================================================

// isTSO_ detects TSO architectures; on TSO, plain reads/writes are safe for
// pointers and native word-sized integers
//
//goland:noinspection GoBoolExpressions
const isTSO_ = runtime.GOARCH == "amd64" ||
	runtime.GOARCH == "386" ||
	runtime.GOARCH == "s390x"

//goland:noinspection GoBoolExpressions
const noRaceTSO_ = !opt.Race_ && isTSO_

// loadPtr loads a pointer atomically on non-TSO architectures.
// On TSO architectures, it performs a plain pointer load.
//
//nolint:unused
//go:nosplit
func loadPtr(addr *unsafe.Pointer) unsafe.Pointer {
	if noRaceTSO_ {
		return *addr
	} else {
		return atomic.LoadPointer(addr)
	}
}

// storePtr stores a pointer atomically on non-TSO architectures.
// On TSO architectures, it performs a plain pointer store.
//
//nolint:unused
//go:nosplit
func storePtr(addr *unsafe.Pointer, val unsafe.Pointer) {
	if noRaceTSO_ {
		*addr = val
	} else {
		atomic.StorePointer(addr, val)
	}
}

//nolint:unused
//go:nosplit
func loadUint64(addr *uint64) uint64 {
	if noRaceTSO_ && bitSize == 64 {
		return *addr
	} else {
		return atomic.LoadUint64(addr)
	}
}

//nolint:unused
//go:nosplit
func storeUint64(addr *uint64, val uint64) {
	if noRaceTSO_ && bitSize == 64 {
		*addr = val
	} else {
		atomic.StoreUint64(addr, val)
	}
}

//nolint:unused
//go:nosplit
func loadUint32(addr *uint32) uint32 {
	if noRaceTSO_ {
		return *addr
	} else {
		return atomic.LoadUint32(addr)
	}
}

//nolint:unused
//go:nosplit
func storeUint32(addr *uint32, val uint32) {
	if noRaceTSO_ {
		*addr = val
	} else {
		atomic.StoreUint32(addr, val)
	}
}

//nolint:unused
//go:nosplit
func loadUintptr(addr *uintptr) uintptr {
	if noRaceTSO_ {
		return *addr
	} else {
		return atomic.LoadUintptr(addr)
	}
}

//nolint:unused
//go:nosplit
func storeUintptr(addr *uintptr, val uintptr) {
	if noRaceTSO_ {
		*addr = val
	} else {
		atomic.StoreUintptr(addr, val)
	}
}

// loadUint64Fast performs a non-atomic read, safe only when the caller holds
// a relevant lock or is within a seqlock read window.
//
//nolint:unused
//go:nosplit
func loadUint64Fast(addr *uint64) uint64 {
	if opt.Race_ {
		return atomic.LoadUint64(addr)
	} else {
		return *addr
	}
}

// storeUint64Fast performs a non-atomic write, safe only for thread-private or
// not-yet-published memory locations.
//
//nolint:unused
//go:nosplit
func storeUint64Fast(addr *uint64, val uint64) {
	if opt.Race_ {
		atomic.StoreUint64(addr, val)
	} else {
		*addr = val
	}
}

//nolint:unused
//go:nosplit
func loadUint32Fast(addr *uint32) uint32 {
	if opt.Race_ {
		return atomic.LoadUint32(addr)
	} else {
		return *addr
	}
}

//nolint:unused
//go:nosplit
func storeUint32Fast(addr *uint32, val uint32) {
	if opt.Race_ {
		atomic.StoreUint32(addr, val)
	} else {
		*addr = val
	}
}

//nolint:unused
//go:nosplit
func loadUintptrFast(addr *uintptr) uintptr {
	if opt.Race_ {
		return atomic.LoadUintptr(addr)
	} else {
		return *addr
	}
}

//nolint:unused
//go:nosplit
func storeUintptrFast(addr *uintptr, val uintptr) {
	if opt.Race_ {
		atomic.StoreUintptr(addr, val)
	} else {
		*addr = val
	}
}

// Concurrency variable access rules
// 1. If a variable has atomic writes outside locks:
//    - Must use atomic loads AND stores inside locks
//    - Example:
//      var value int32
//      func update() {
//          atomic.StoreInt32(&value, 1) // external atomic write
//      }
//      func lockedOp() {
//          mu.Lock()
//          defer mu.Unlock()
//          v := atomic.LoadInt32(&value) // internal atomic load
//          atomic.StoreInt32(&value, v+1) // internal atomic store
//      }
//
// 2. If a variable only has atomic reads outside locks:
//    - Only need atomic stores inside locks (atomic loads not required)
//    - Example:
//      func read() int32 {
//          return atomic.LoadInt32(&value) // external atomic read
//      }
//      func lockedOp() {
//          mu.Lock()
//          defer mu.Unlock()
//          // Normal read sufficient (lock guarantees visibility)
//          v := value
//          // But writes need atomic store:
//          atomic.StoreInt32(&value, 42)
//      }
//
// 3. If a variable has no external access:
//    - No atomic operations needed inside locks
//    - Normal reads/writes sufficient (lock provides full protection)
//    - Example:
//      func lockedOp() {
//          mu.Lock()
//          defer mu.Unlock()
//          value = 42 // normal write
//          v := value // normal read
//      }
