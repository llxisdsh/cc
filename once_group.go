package cc

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync/atomic"
)

// OnceGroupResult holds the results of Do, so they can be passed on a channel.
type OnceGroupResult[V any] struct {
	Val    V
	Err    error
	Shared bool
}

// call represents an in-flight or completed OnceGroup.Do call
type call[V any] struct {
	latch     Latch
	val       V
	err       error
	dups      int32
	chans     []chan<- OnceGroupResult[V]
	completed int32
}

// OnceGroup represents a class of work and forms a namespace in
// which units of work can be executed with duplicate suppression.
type OnceGroup[K comparable, V any] struct {
	m Map[K, *call[V]]
}

// Do execute and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
// The return value shared indicates whether v was given to multiple callers.
func (g *OnceGroup[K, V]) Do(
	key K,
	fn func() (V, error),
) (V, error, bool) {
	primary := &call[V]{}
	c, loaded := g.m.LoadOrStore(key, primary)
	if loaded {
		// mark duplication under lock to ensure shared=true visibility
		_, _ = g.m.Compute(key, func(it *MapEntry[K, *call[V]]) {
			if it.Loaded() {
				atomic.AddInt32(&it.Value().dups, 1)
			}
		})

		c.latch.Wait()
		var e *panicError
		if errors.As(c.err, &e) {
			panic(e)
		} else if errors.Is(c.err, errGoexit) {
			runtime.Goexit()
		}
		return c.val, c.err, true
	}

	// Primary executes with panic/Goexit semantics compatible with x/sync/singleflight.
	g.doCall(c, key, fn)
	shared := atomic.LoadInt32(&c.dups) > 0 || len(c.chans) > 1
	return c.val, c.err, shared
}

// DoChan is like Do but returns a channel that will receive the
// results when they are ready.
//
// The returned channel will not be closed.
func (g *OnceGroup[K, V]) DoChan(
	key K,
	fn func() (V, error),
) <-chan OnceGroupResult[V] {
	ch := make(chan OnceGroupResult[V], 1)
	c0 := &call[V]{
		chans: append(
			make([]chan<- OnceGroupResult[V], 0, runtime.GOMAXPROCS(0)),
			ch,
		),
	}
	c, loaded := g.m.LoadOrStore(key, c0)
	if loaded {
		// Duplicates: if already completed, send immediately; otherwise wait on wg and send
		if atomic.LoadInt32(&c.completed) == 1 {
			shared := atomic.LoadInt32(&c.dups) > 0 || len(c.chans) > 1
			ch <- OnceGroupResult[V]{Val: c.val, Err: c.err, Shared: shared}
			return ch
		}
		// Mark duplication for shared flag visibility
		_, _ = g.m.Compute(key, func(it *MapEntry[K, *call[V]]) {
			if it.Loaded() {
				atomic.AddInt32(&it.Value().dups, 1)
			}
		})
		go func(c *call[V], ch chan<- OnceGroupResult[V]) {
			c.latch.Wait()
			var e *panicError
			switch {
			case errors.As(c.err, &e):
				go panic(e)
				select {}
			case errors.Is(c.err, errGoexit):
				return
			default:
				shared := atomic.LoadInt32(&c.dups) > 0 || len(c.chans) > 1
				ch <- OnceGroupResult[V]{Val: c.val, Err: c.err, Shared: shared}
			}
		}(c, ch)
		return ch
	}
	go g.doCall(c, key, fn)
	return ch
}

// Forget tells the group to stop tracking a key. Future calls
// to Do for this key will invoke the function rather than waiting for
// an existing call to complete.
func (g *OnceGroup[K, V]) Forget(key K) {
	g.m.Delete(key)
}

// ForgetUnshared deletes the key only if no duplicates joined.
func (g *OnceGroup[K, V]) ForgetUnshared(key K) bool {
	deleted := false
	_, _ = g.m.Compute(
		key,
		func(it *MapEntry[K, *call[V]]) {
			if it.Loaded() && atomic.LoadInt32(&it.Value().dups) == 0 {
				it.Delete()
				deleted = true
			}
		},
	)
	return deleted
}

// doCall runs fn with panic/Goexit semantics and broadcasts results.
func (g *OnceGroup[K, V]) doCall(
	c *call[V],
	key K,
	fn func() (V, error),
) {
	normalReturn := false
	recovered := false

	defer func() {
		// Mark Goexit if the goroutine terminated without normal return
		// and without a recovered panic.
		if !normalReturn && !recovered {
			c.err = errGoexit
		}

		// Complete the call and remove the key atomically.
		c.latch.Open()
		atomic.StoreInt32(&c.completed, 1)

		var chs []chan<- OnceGroupResult[V]
		_, _ = g.m.Compute(
			key,
			func(it *MapEntry[K, *call[V]]) {
				if it.Loaded() && it.Value() == c {
					chs = append(chs, it.Value().chans...)
				}
			},
		)
		if len(chs) == 0 {
			chs = c.chans
		}

		// After wg.Done, duplicates in Do() will wake and re-panic/goexit.
		var e *panicError
		switch {
		case errors.As(c.err, &e):
			// Match x/sync: ensure panic is unrecoverable and visible.
			if len(chs) > 0 {
				//goland:noinspection All
				go panic(e)
				select {}
			} else {
				panic(e)
			}
		case errors.Is(c.err, errGoexit):
			// Primary goroutine already Goexit'ed; nothing to do here.
		default:
			// Normal return: notify DoChan waiters.
			shared := atomic.LoadInt32(&c.dups) > 0 || len(c.chans) > 1
			for _, ch := range chs {
				ch <- OnceGroupResult[V]{Val: c.val, Err: c.err, Shared: shared}
			}
		}
	}()

	// Distinguish panic from Goexit via double-defer with inner wrapper,
	// matching the structure of the official implementation.
	func() {
		defer func() {
			if !normalReturn {
				// Only recover when not a normal return, so we can
				// differentiate panic vs Goexit.
				if r := recover(); r != nil {
					c.err = newPanicError(r)
				}
			}
		}()

		c.val, c.err = fn()
		normalReturn = true
	}()

	if !normalReturn {
		recovered = true
	}
}

// -------------------------
// Panic/Goexit handling
// -------------------------

// panicError mirrors the type used by x/sync/singleflight.
// A panicError is an arbitrary value recovered from a panic
// with the stack trace during the execution of given function.
type panicError struct {
	value any
	stack []byte
}

// Error implements error interface.
func (p *panicError) Error() string {
	return fmt.Sprintf("%v\n\n%s", p.value, p.stack)
}

// Unwrap returns the underlying error value, if any.
func (p *panicError) Unwrap() error {
	if err, ok := p.value.(error); ok {
		return err
	}
	return nil
}

func newPanicError(v any) error {
	stack := debug.Stack()
	// Trim first line "goroutine N [status]:" which can be misleading.
	if line := bytes.IndexByte(stack, '\n'); line >= 0 {
		stack = stack[line+1:]
	}
	return &panicError{value: v, stack: stack}
}

var errGoexit = errors.New("runtime.Goexit was called")
