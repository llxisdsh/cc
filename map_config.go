package cc

import (
	"unsafe"
)

// ============================================================================
// Configuration
// ============================================================================

// MapConfig defines configurable options for Map initialization.
// This structure contains all the configuration parameters that can be used
// to customize the behavior and performance characteristics of a Map
// instance.
type MapConfig struct {
	// keyHash specifies a custom hash function for keys.
	// If nil, the built-in hash function will be used.
	// Custom hash functions can improve performance for specific key types
	// or provide better hash distribution.
	keyHash HashFunc

	// valEqual specifies a custom equality function for values.
	// If nil, the built-in equality comparison will be used.
	// This is primarily used for compare-and-swap operations.
	// Note: Using Compare* methods with non-comparable value types
	// will panic if valEqual is nil.
	valEqual EqualFunc

	// capacity provides an estimate of the expected number of entries.
	// This is used to pre-allocate the underlying hash table with appropriate
	// capacity, reducing the need for resizing during initial population.
	// If zero or negative, the default minimum capacity will be used.
	// The actual capacity will be rounded up to the next power of 2.
	capacity int

	// autoShrink controls whether the map can automatically shrink
	// when the load factor falls below the shrink threshold.
	// When false (default), the map will only grow and never shrink,
	// which provides better performance but may use more memory.
	// When true, the map will shrink when occupancy < 1/shrinkFraction.
	autoShrink bool
}

// WithCapacity configuring new Map instance with capacity enough
// to hold cap entries. The capacity is treated as the minimal
// capacity, meaning that the underlying hash table will never shrink
// to a smaller capacity. If cap is zero or negative, the value
// is ignored.
func WithCapacity(cap int) func(*MapConfig) {
	return func(c *MapConfig) {
		if cap > 0 {
			c.capacity = cap
		}
	}
}

// WithAutoShrink configures automatic map shrinking when the load factor
// falls below the threshold (default: 1/shrinkFraction).
// Disabled by default to prioritize performance.
func WithAutoShrink() func(*MapConfig) {
	return func(c *MapConfig) {
		c.autoShrink = true
	}
}

// WithKeyHasher sets a custom key hashing function for the map.
// This allows you to optimize hash distribution for specific key types
// or implement custom hashing strategies.
//
// Parameters:
//   - keyHash: custom hash function that takes a key and seed,
//     returns hash value. Pass nil to use the default built-in hasher
//
// Usage:
//
//	m := NewMap[string, int](WithKeyHasher(myCustomHashFunc))
//
// Use cases:
//   - Optimize hash distribution for specific data patterns
//   - Implement case-insensitive string hashing
//   - Custom hashing for complex key types
//   - Performance tuning for known key distributions
func WithKeyHasher[K comparable](
	keyHash func(key K, seed uintptr) uintptr,
) func(*MapConfig) {
	return func(c *MapConfig) {
		if keyHash != nil {
			c.keyHash = func(pointer unsafe.Pointer, u uintptr) uintptr {
				return keyHash(*(*K)(pointer), u)
			}
		}
	}
}

// WithKeyHasherUnsafe sets a low-level unsafe key hashing function.
// This is the high-performance version that operates directly on memory
// pointers. Use this when you need maximum performance and are comfortable with
// unsafe operations.
//
// Parameters:
//   - hs: unsafe hash function that operates on raw unsafe.Pointer.
//     The pointer points to the key data in memory.
//     Pass nil to use the default built-in hasher
//
// Usage:
//
//	// Basic unsafe hasher
//	unsafeHasher := func(ptr unsafe.Pointer, seed uintptr) uintptr {
//		// Cast ptr to your key type and implement hashing
//		key := *(*string)(ptr)
//		return uintptr(len(key)) // example hash
//	}
//	m := NewMap[string, int](WithKeyHasherUnsafe(unsafeHasher))
//
// See [hashUint64] for reference implementation.
//
// Notes:
//   - You must correctly cast unsafe.Pointer to the actual key type
//   - Incorrect pointer operations will cause crashes or memory corruption
//   - Only use if you understand Go's unsafe package
func WithKeyHasherUnsafe(
	hs HashFunc,
) func(*MapConfig) {
	return func(c *MapConfig) {
		c.keyHash = hs
	}
}

// WithValueEqual sets a custom value equality function for the map.
// This is essential for CompareAndSwap and CompareAndDelete operations
// when working with non-comparable value types or custom equality logic.
//
// Parameters:
//   - valEqual: custom equality function that compares two values
//     Pass nil to use the default built-in comparison (for comparable types)
//
// Usage:
//
//	// For custom structs with specific equality logic
//	EqualFunc := func(a, b MyStruct) bool {
//		return a.ID == b.ID && a.Name == b.Name
//	}
//	m := NewMap[string, MyStruct](WithValueEqual(EqualFunc))
//
// Use cases:
//   - Custom equality for structs (compare specific fields)
//   - Case-insensitive string comparison
//   - Floating-point comparison with tolerance
//   - Deep equality for slices/maps
//   - Required for non-comparable types (slices, maps, functions)
func WithValueEqual[V any](
	valEqual func(val, val2 V) bool,
) func(*MapConfig) {
	return func(c *MapConfig) {
		if valEqual != nil {
			c.valEqual = func(val unsafe.Pointer, val2 unsafe.Pointer) bool {
				return valEqual(*(*V)(val), *(*V)(val2))
			}
		}
	}
}

// WithValueEqualUnsafe sets a low-level unsafe value equality function.
// This is the high-performance version that operates directly on memory
// pointers. Use this when you need maximum performance and are comfortable with
// unsafe operations.
//
// Parameters:
//   - eq: unsafe equality function that operates on raw unsafe.Pointer
//     Both pointers point to value data in memory
//     Pass nil to use the default built-in comparison
//
// Usage:
//
//	unsafeEqual := func(ptr, other unsafe.Pointer) bool {
//		// Cast pointers to your value type and implement comparison
//		val1 := *(*MyStruct)(ptr)
//		val2 := *(*MyStruct)(other)
//		return val1.ID == val2.ID // example comparison
//	}
//	m := NewMap[string, MyStruct](WithValueEqualUnsafe(unsafeEqual))
//
// Notes:
//   - You must correctly cast unsafe.Pointer to the actual value type
//   - Both pointers must point to valid memory of the same type
//   - Incorrect pointer operations will cause crashes or memory corruption
//   - Only use if you understand Go's unsafe package
func WithValueEqualUnsafe(eq EqualFunc) func(*MapConfig) {
	return func(c *MapConfig) {
		c.valEqual = eq
	}
}

// WithBuiltInHasher returns a MapConfig option that explicitly sets the
// built-in hash function for the specified type.
//
// This option is useful when you want to explicitly use Go's built-in hasher
// instead of any optimized variants. It ensures that the map uses the same
// hashing strategy as Go's native map implementation.
//
// Performance characteristics:
// - Provides consistent performance across all key sizes
// - Uses Go's optimized internal hash functions
// - Guaranteed compatibility with future Go versions
// - May be slower than specialized hashers for specific use cases
//
// Usage:
//
//	m := NewMap[string, int](WithBuiltInHasher[string]())
func WithBuiltInHasher[T comparable]() func(*MapConfig) {
	return func(c *MapConfig) {
		c.keyHash = GetBuiltInHasher[T]()
	}
}

// GetBuiltInHasher returns Go's built-in hash function for the specified type.
// This function provides direct access to the same hash function that Go's
// built-in map uses internally, ensuring optimal performance and compatibility.
//
// The returned hash function is type-specific and optimized for the given
// comparable type T. It uses Go's internal type representation to access
// the most efficient hashing implementation available.
//
// Usage:
//
//	hashFunc := GetBuiltInHasher[string]()
//	m := NewMap[string, int](WithKeyHasherUnsafe(GetBuiltInHasher[string]()))
func GetBuiltInHasher[T comparable]() HashFunc {
	keyHash, _ := defaultHasherUsingBuiltIn[T, struct{}]()
	return keyHash
}

// IHashFunc defines a custom hash function interface for key types.
// Key types implementing this interface can provide their own hash computation,
// serving as an alternative to WithKeyHasher for type-specific optimization.
//
// This interface is automatically detected during Map initialization and
// takes
// precedence over the default built-in hasher but is overridden by explicit
// WithKeyHasher configuration.
//
// Usage:
//
//	type UserID struct {
//		ID int64
//		Tenant string
//	}
//
//	func (u *UserID) HashFunc(seed uintptr) uintptr {
//		return uintptr(u.ID) ^ seed
//	}
type IHashFunc interface {
	HashFunc(seed uintptr) uintptr
}

// IEqualFunc defines a custom equality comparison interface for value types.
// Value types implementing this interface can provide their own equality logic,
// serving as an alternative to WithValueEqual for type-specific comparison.
//
// This interface is automatically detected during Map initialization and is
// essential for non-comparable value types or custom equality semantics.
// It takes precedence over the default built-in comparison but is overridden
// by explicit WithValueEqual configuration.
//
// Usage:
//
//	type UserProfile struct {
//		Name string
//		Tags []string // slice makes this non-comparable
//	}
//
//	func (u *UserProfile) EqualFunc(other UserProfile) bool {
//		return u.Name == other.Name && slices.EqualFunc(u.Tags, other.Tags)
//	}
type IEqualFunc[T any] interface {
	EqualFunc(other T) bool
}

//go:nosplit
func parseKeyInterface[K comparable]() (keyHash HashFunc) {
	var k *K
	if _, ok := any(k).(IHashFunc); ok {
		keyHash = func(ptr unsafe.Pointer, seed uintptr) uintptr {
			return any((*K)(ptr)).(IHashFunc).HashFunc(seed)
		}
	}
	return keyHash
}

//go:nosplit
func parseValueInterface[V any]() (valEqual EqualFunc) {
	var v *V
	if _, ok := any(v).(IEqualFunc[V]); ok {
		valEqual = func(ptr unsafe.Pointer, other unsafe.Pointer) bool {
			return any((*V)(ptr)).(IEqualFunc[V]).EqualFunc(*(*V)(other))
		}
	}
	return valEqual
}
