package cc

import (
	"sync/atomic"

	"github.com/llxisdsh/cc/internal/opt"
)

// WaitGroup is a reusable WaitGroup.
// Unlike sync.WaitGroup, it can be reused immediately after the previous batch of tasks is done,
// without waiting for all Wait() calls to return.
//
// Limitations:
// - Max tasks: 2^22 - 1 (approx 4 million)
// - Max waiters: 2^22 - 1 (approx 4 million)
// - Generations: 2^20 (approx 1 million, then wraps)
type WaitGroup struct {
	_ noCopy
	// state stores:
	// - Generation (20 bits)
	// - Waiter Count (22 bits)
	// - Task Counter (22 bits)
	state atomic.Uint64

	// sema is a double-buffered semaphore to prevent signal stealing.
	sema [2]opt.Sema
}

const (
	taskBits   = 22
	waiterBits = 22
	genBits    = 20

	taskMask   = (1 << taskBits) - 1
	waiterMask = ((1 << waiterBits) - 1) << taskBits
	genMask    = ((1 << genBits) - 1) << (taskBits + waiterBits)

	waiterUnit = 1 << taskBits
)

// Add adds delta, which may be negative, to the WaitGroup counter.
// If the counter becomes zero, all goroutines blocked on Wait are released.
// If the counter goes negative, Add panics.
func (wg *WaitGroup) Add(delta int) {
	var spins int
	for {
		state := wg.state.Load()
		cnt := int32(state & taskMask)

		newCnt := int64(cnt) + int64(delta)
		if newCnt < 0 {
			panic("cc: negative WaitGroup counter")
		}
		if newCnt >= (1 << taskBits) {
			panic("cc: WaitGroup counter overflow")
		}

		next := state
		// Update task count
		next &^= taskMask
		next |= uint64(newCnt)

		var waitersToRelease uint64
		var semaGen uint64

		// If we transition from >0 to 0, we finish the current generation.
		if newCnt == 0 && cnt > 0 {
			gen := (state & genMask) >> (taskBits + waiterBits)
			newGen := (gen + 1) & ((1 << genBits) - 1)

			// Update generation
			next &^= genMask
			next |= newGen << (taskBits + waiterBits)

			// Capture waiters and clear them
			waiters := (state & waiterMask) >> taskBits
			waitersToRelease = waiters
			semaGen = gen

			next &^= waiterMask
		}

		if wg.state.CompareAndSwap(state, next) {
			if waitersToRelease > 0 {
				semaPtr := &wg.sema[semaGen%2]
				for i := 0; i < int(waitersToRelease); i++ {
					semaPtr.Release()
				}
			}
			return
		}
		delay(&spins)
	}
}

// Done decrements the WaitGroup counter by one.
func (wg *WaitGroup) Done() {
	wg.Add(-1)
}

// Wait blocks until the WaitGroup counter is zero.
func (wg *WaitGroup) Wait() {
	if wg.TryWait() {
		return
	}
	wg.slowWait()
}

func (wg *WaitGroup) slowWait() {
	for {
		state := wg.state.Load()
		cnt := state & taskMask
		if cnt == 0 {
			return
		}

		waiters := (state & waiterMask) >> taskBits
		if waiters == (1<<waiterBits)-1 {
			panic("cc: WaitGroup waiter overflow")
		}

		// Increment waiter count
		next := state + waiterUnit

		if wg.state.CompareAndSwap(state, next) {
			gen := (state & genMask) >> (taskBits + waiterBits)
			wg.sema[gen%2].Acquire()
			return
		}
	}
}

// Go calls f in a new goroutine and adds that task to the WaitGroup.
// When f returns, the task is removed from the WaitGroup.
func (wg *WaitGroup) Go(f func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		f()
	}()
}

// Count returns the current number of active tasks.
// Note: This is an approximate value as it can change concurrently.
func (wg *WaitGroup) Count() int {
	return int(wg.state.Load() & taskMask)
}

// Waiters returns the current number of goroutines waiting on Wait().
// Note: This is an approximate value as it can change concurrently.
func (wg *WaitGroup) Waiters() int {
	return int((wg.state.Load() & waiterMask) >> taskBits)
}

// TryWait returns true if the WaitGroup counter is zero.
// It does not block.
func (wg *WaitGroup) TryWait() bool {
	return wg.Count() == 0
}
