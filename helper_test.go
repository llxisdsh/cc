package cc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHelperWait(t *testing.T) {
	// Case 1: Success
	err := Wait(context.Background(), func() {
		// do nothing
	})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}

	// Case 2: Context Cancel
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = Wait(ctx, func() {
		time.Sleep(time.Hour)
	})
	if err != context.Canceled {
		t.Errorf("expected Canceled, got %v", err)
	}
}

func TestHelperWaitTimeout(t *testing.T) {
	// Case 1: Success
	err := WaitTimeout(time.Second, func() {
		// do nothing
	})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}

	// Case 2: Timeout
	err = WaitTimeout(10*time.Millisecond, func() {
		time.Sleep(100 * time.Millisecond)
	})
	if err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestHelperDo(t *testing.T) {
	// Case 1: Success with value
	err := Do(context.Background(), func() error {
		return nil
	})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}

	// Case 2: Error propagation
	expectedErr := errors.New("fail")
	err = Do(context.Background(), func() error {
		return expectedErr
	})
	if err != expectedErr {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}

	// Case 3: Context Cancel
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = Do(ctx, func() error {
		time.Sleep(time.Hour)
		return nil
	})
	if err != context.Canceled {
		t.Errorf("expected Canceled, got %v", err)
	}
}

func TestHelperDoTimeout(t *testing.T) {
	// Case 1: Success
	err := DoTimeout(time.Second, func() error {
		return nil
	})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}

	// Case 2: Timeout
	err = DoTimeout(10*time.Millisecond, func() error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	if err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}

	// Case 3: Error propagation
	expectedErr := errors.New("fail")
	err = DoTimeout(time.Second, func() error {
		return expectedErr
	})
	if err != expectedErr {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
}

func TestRepeat(t *testing.T) {
	// Case 1: Run 3 times then cancel
	ctx, cancel := context.WithCancel(context.Background())
	var count int32

	err := Repeat(ctx, 10*time.Millisecond, func(ctx context.Context) error {
		val := atomic.AddInt32(&count, 1)
		if val == 3 {
			cancel()
		}
		return nil
	})

	if err != context.Canceled {
		t.Errorf("expected Canceled, got %v", err)
	}
	if count < 3 {
		t.Errorf("expected at least 3 runs, got %d", count)
	}

	// Case 2: Error stops repetition
	expectedErr := errors.New("stop")
	count = 0
	err = Repeat(context.Background(), 10*time.Millisecond, func(ctx context.Context) error {
		atomic.AddInt32(&count, 1)
		return expectedErr
	})

	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
	if count != 1 {
		t.Errorf("expected 1 run, got %d", count)
	}
}

func TestParallel(t *testing.T) {
	// Case 1: Success all
	n := 10
	var count int32
	err := Parallel(context.Background(), n, func(ctx context.Context, i int) error {
		atomic.AddInt32(&count, 1)
		return nil
	})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if count != int32(n) {
		t.Errorf("expected %d, got %d", n, count)
	}

	// Case 2: Error propagation
	expectedErr := errors.New("fail")
	err = Parallel(context.Background(), n, func(ctx context.Context, i int) error {
		if i == 5 {
			return expectedErr
		}
		return nil
	})
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	// Case 3: Context cancel
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately
	err = Parallel(ctx, n, func(ctx context.Context, i int) error {
		time.Sleep(time.Hour)
		return nil
	})
	if err != context.Canceled {
		t.Errorf("expected Canceled, got %v", err)
	}
}

func TestParallelStress(t *testing.T) {
	// High iteration count to catch the race
	iterations := 10000
	expectedErr := errors.New("worker error")

	var contextCanceledCount int32
	var successCount int32

	var wg sync.WaitGroup
	wg.Add(iterations)

	for i := 0; i < iterations; i++ {
		go func() {
			defer wg.Done()
			// We use a clean background context so only Parallel's internal cancellation is at play
			err := Parallel(context.Background(), 5, func(ctx context.Context, idx int) error {
				if idx == 0 {
					return expectedErr
				}
				return nil
			})

			if err == nil {
				t.Error("expected error, got nil")
			} else if errors.Is(err, context.Canceled) {
				atomic.AddInt32(&contextCanceledCount, 1)
			} else if errors.Is(err, expectedErr) {
				atomic.AddInt32(&successCount, 1)
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}

	wg.Wait()

	if contextCanceledCount > 0 {
		t.Errorf("Failed: Got context.Canceled %d times out of %d. The race condition is present.", contextCanceledCount, iterations)
	} else {
		t.Logf("Success: Got expected error %d times. No context.Canceled observed.", successCount)
	}
}
