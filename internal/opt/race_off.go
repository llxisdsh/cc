//go:build !race

package opt

import (
	_ "unsafe" // for linkname
)

const Race_ = false

// Sema is a zero-allocation semaphore optimized for performance.
// In !race mode, it is a direct wrapper around runtime.semacquire/semrelease.
type Sema uint32

func (s *Sema) Acquire() {
	runtime_semacquire((*uint32)(s))
}

func (s *Sema) Release() {
	runtime_semrelease((*uint32)(s), false, 0)
}

//go:linkname runtime_semacquire sync.runtime_Semacquire
func runtime_semacquire(s *uint32)

//go:linkname runtime_semrelease sync.runtime_Semrelease
func runtime_semrelease(s *uint32, handoff bool, skipframes int)
