package cc

import (
	"context"
	"runtime"
	"sync/atomic"
	"time"
)

// Wait executes fn and waits for it to return or for the context to be canceled.
// It is useful for making blocking calls (that don't support Context) cancellable.
//
// Usage:
//
//	cc.Wait(ctx, wg.Wait)
func Wait(ctx context.Context, fn func()) error {
	return Do(ctx, func() error {
		fn()
		return nil
	})
}

// WaitTimeout executes fn and waits for it to return or for the timeout to elapse.
// It returns context.DeadlineExceeded if the timeout occurs.
//
// Usage:
//
//	if err := cc.WaitTimeout(time.Second, wg.Wait); err != nil {
//	    // timed out
//	}
func WaitTimeout(timeout time.Duration, fn func()) error {
	return DoTimeout(timeout, func() error {
		fn()
		return nil
	})
}

// Do execute fn and waits for it to return or for the context to be canceled.
// It returns fn's error if it completes, or ctx.Err() if canceled.
//
// Usage:
//
//	err := cc.Do(ctx, func() error {
//	    return expensiveOp()
//	})
//
// Notes: The function fn must return eventually. If fn blocks indefinitely
// (e.g. deadlock), the internal goroutine will leak even if the context is canceled.
func Do(ctx context.Context, fn func() error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Buffer size 1 to prevent goroutine leak if ctx cancels
	done := make(chan error, 1)
	go func() {
		done <- fn()
	}()

	select {
	case <-ctx.Done():
		select {
		case err := <-done:
			return err
		default:
			return ctx.Err()
		}
	case err := <-done:
		return err
	}
}

// DoTimeout executes fn and waits for it to return or for the timeout to elapse.
// It returns fn's error if it completes, or context.DeadlineExceeded if timed out.
//
// Notes: The function fn must return eventually. If fn blocks indefinitely
// (e.g. deadlock), the internal goroutine will leak even if the timeout expires.
func DoTimeout(timeout time.Duration, fn func() error) error {
	if timeout <= 0 {
		return context.DeadlineExceeded
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return Do(ctx, fn)
}

// Repeat executes the action periodically with the given interval until the context is canceled.
// It stops immediately if the action returns an error.
// The first execution happens immediately.
//
// Usage:
//
//	cc.Repeat(ctx, time.Second, func(ctx context.Context) error {
//	    // do something periodically
//	    return nil
//	})
func Repeat(ctx context.Context, interval time.Duration, action func(context.Context) error) error {
	if interval <= 0 {
		return action(ctx)
	}

	// Run immediately first
	if err := action(ctx); err != nil {
		return err
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := action(ctx); err != nil {
				return err
			}
		}
	}
}

// Parallel executes n copies of the action concurrently.
// It blocks until all actions complete or the context is canceled.
// Returns the first error encountered, if any.
//
// If n <= 0, it defaults to GOMAXPROCS(0).
//
// Usage:
//
//	cc.Parallel(ctx, 10, func(ctx context.Context, i int) error {
//	    // process item i
//	    return nil
//	})
func Parallel(ctx context.Context, n int, action func(context.Context, int) error) error {
	if n <= 0 {
		n = runtime.GOMAXPROCS(0)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Create a cancellable context to stop other goroutines on error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg WaitGroup // Use cc.WaitGroup for efficiency
	wg.Add(n)

	// We use a channel to collect the first error.
	// Buffer size 1 is enough because we only care about the first one.
	errCh := make(chan error, 1)
	var failed atomic.Bool

	for i := range n {
		go func(idx int) {
			defer wg.Done()

			// If an error already occurred, skip execution
			if failed.Load() {
				return
			}
			// Check context before starting action (double check)
			if ctx.Err() != nil {
				return
			}

			if err := action(ctx, idx); err != nil {
				if failed.CompareAndSwap(false, true) {
					select {
					case errCh <- err:
					default:
					}
					cancel() // Cancel other workers
				}
			}
		}(i)
	}

	// Wait for completion or context cancel
	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-ctx.Done():
		select {
		case err := <-errCh:
			return err
		default:
			return ctx.Err()
		}
	case err := <-errCh:
		return err
	case <-waitDone:
		// Check if any error was reported just before completion
		select {
		case err := <-errCh:
			return err
		default:
			return nil
		}
	}
}
