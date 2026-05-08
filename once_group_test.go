package cc

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOnceGroup_DoDuplicates(t *testing.T) {
	var g OnceGroup[string, int]

	var calls int32
	key := "same"
	n := 64

	var wg sync.WaitGroup
	wg.Add(n)
	sharedCount := int32(0)
	start := make(chan struct{})
	fnBlock := make(chan struct{})
	for range n {
		go func() {
			defer wg.Done()
			<-start
			v, err, shared := g.Do(key, func() (int, error) {
				atomic.AddInt32(&calls, 1)
				<-fnBlock
				return 42, nil
			})
			if err != nil || v != 42 {
				t.Errorf("bad result: %v, %v", v, err)
			}
			if shared {
				atomic.AddInt32(&sharedCount, 1)
			}
		}()
	}
	close(start)

	// Give all goroutines ample time to hit LoadOrStore before we release fn.
	// This prevents a premature deletion of the key which would cause a new
	// fn execution and a duplicate call on slow/heavily loaded machines.
	time.Sleep(100 * time.Millisecond)
	close(fnBlock)
	wg.Wait()

	if calls != 1 {
		t.Fatalf("fn executed %d times, want 1", calls)
	}
	// sharedCount should be 64, meaning 1 primary (which has shared=true because others joined)
	// and 63 duplicates.
	if sharedCount != int32(n) {
		t.Fatalf("shared=%d, want %d", sharedCount, n)
	}
}

func TestOnceGroup_DoChan(t *testing.T) {
	var g OnceGroup[string, string]
	key := "dup"
	n := 32

	primaryStarted := make(chan struct{})
	releasePrimary := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(n)

	go func() {
		defer wg.Done()
		ch := g.DoChan(key, func() (string, error) {
			close(primaryStarted)
			<-releasePrimary
			return "ok", nil
		})
		r := <-ch
		if r.Err != nil || r.Val != "ok" {
			t.Errorf("bad: %v, %v", r.Val, r.Err)
		}
		if !r.Shared {
			t.Errorf("expected shared=true")
		}
	}()

	var joined atomic.Int32
	for i := 1; i < n; i++ {
		go func() {
			defer wg.Done()
			<-primaryStarted
			ch := g.DoChan(key, func() (string, error) { return "ok", nil })
			joined.Add(1)
			r := <-ch
			if r.Err != nil || r.Val != "ok" {
				t.Errorf("bad: %v, %v", r.Val, r.Err)
			}
			if !r.Shared {
				t.Errorf("expected shared=true")
			}
		}()
	}

	for joined.Load() < int32(n-1) {
		runtime.Gosched()
	}
	close(releasePrimary)
	wg.Wait()
}

func TestOnceGroup_Forget(t *testing.T) {
	var g OnceGroup[string, any]
	key := "work"

	firstStarted := make(chan struct{})
	unblockFirst := make(chan struct{})
	firstFinished := make(chan struct{})

	// First in-flight call: signal when registered, then block until unblocked.
	go func() {
		_, _, _ = g.Do(key, func() (any, error) {
			close(firstStarted)
			<-unblockFirst
			close(firstFinished)
			return 1, nil
		})
	}()

	// Ensure first call has registered in the group before Forget.
	<-firstStarted
	g.Forget(key)

	// Second call should not join the first; it must run independently.
	v, err, shared := g.Do(key, func() (any, error) { return 2, nil })
	if err != nil || v.(int) != 2 || shared {
		t.Fatalf("unexpected: v=%v err=%v shared=%v", v, err, shared)
	}

	// Let the first call complete.
	close(unblockFirst)
	<-firstFinished
}

func TestOnceGroup_ForgetUnshared(t *testing.T) {
	var g OnceGroup[string, any]
	key := "k"

	// No duplicates: call in-flight, should forget.
	block := make(chan struct{})
	ch := g.DoChan(key, func() (any, error) {
		<-block
		return 1, nil
	})
	if !g.ForgetUnshared(key) {
		t.Fatalf("ForgetUnshared should succeed without dups")
	}
	close(block)
	<-ch

	// With duplicates: should not forget.
	block = make(chan struct{})
	ch = g.DoChan(key, func() (any, error) {
		<-block
		return 2, nil
	})
	_ = g.DoChan(key, func() (any, error) { return 0, nil })
	if g.ForgetUnshared(key) {
		t.Fatalf("ForgetUnshared should fail with dups")
	}
	close(block)
	<-ch
}

// Panic should propagate to all Do callers (including duplicates).
func TestOnceGroup_Do_Panic(t *testing.T) {
	var g OnceGroup[string, any]
	key := "panic"
	n := 16

	var wg sync.WaitGroup
	wg.Add(n)
	panics := int32(0)
	start := make(chan struct{})
	fnBlock := make(chan struct{})
	for range n {
		go func() {
			defer wg.Done()
			defer func() {
				if recover() != nil {
					atomic.AddInt32(&panics, 1)
				}
			}()
			<-start
			_, _, _ = g.Do(key, func() (any, error) {
				<-fnBlock
				panic("boom")
			})
		}()
	}
	close(start)
	// Give all goroutines ample time to hit LoadOrStore before we panic.
	time.Sleep(50 * time.Millisecond)
	close(fnBlock)
	wg.Wait()
	if panics != int32(n) {
		t.Fatalf("expected %d panics, got %d", n, panics)
	}
}

// Goexit should propagate to all Do callers.
func TestOnceGroup_Do_Goexit(t *testing.T) {
	var g OnceGroup[string, any]
	key := "goexit"
	n := 16

	var wg sync.WaitGroup
	wg.Add(n)
	exited := int32(0)
	start := make(chan struct{})
	fnBlock := make(chan struct{})
	for range n {
		go func() {
			defer wg.Done()
			// runtime.Goexit executes deferred funcs.
			defer atomic.AddInt32(&exited, 1)
			<-start
			_, _, _ = g.Do(key, func() (any, error) {
				<-fnBlock
				runtime.Goexit()
				return nil, nil
			})
		}()
	}
	close(start)
	// Give all goroutines ample time to hit LoadOrStore before we exit.
	time.Sleep(50 * time.Millisecond)
	close(fnBlock)
	wg.Wait()
	if exited != int32(n) {
		t.Fatalf("expected %d goexits, got %d", n, exited)
	}
}

// ============================================================================
// Compatibility test (x/sync/singleflight)
// ============================================================================

type (
	// Group = xsf.Group
	Group    = OnceGroup[string, any]
	errValue struct{}
)

func (err *errValue) Error() string {
	return "error value"
}

func TestPanicErrorUnwrap(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name             string
		panicValue       any
		wrappedErrorType bool
	}{
		{
			name:             "panicError wraps non-error type",
			panicValue:       &panicError{value: "string value"},
			wrappedErrorType: false,
		},
		{
			name:             "panicError wraps error type",
			panicValue:       &panicError{value: new(errValue)},
			wrappedErrorType: false,
		},
	}

	for _, tc := range testCases {
		//nolint:modernize
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var recovered any

			group := &Group{}

			func() {
				defer func() {
					recovered = recover()
					t.Logf("after panic(%#v) in group.Do, recovered %#v", tc.panicValue, recovered)
				}()

				_, _, _ = group.Do(tc.name, func() (any, error) {
					panic(tc.panicValue)
				})
			}()

			if recovered == nil {
				t.Fatal("expected a non-nil panic value")
			}

			err, ok := recovered.(error)
			if !ok {
				t.Fatalf("recovered non-error type: %T", recovered)
			}

			if !errors.Is(err, new(errValue)) && tc.wrappedErrorType {
				t.Errorf("unexpected wrapped error type %T; want %T", err, new(errValue))
			}
		})
	}
}

func TestDo(t *testing.T) {
	var g Group
	v, err, _ := g.Do("key", func() (any, error) {
		return "bar", nil
	})
	if got, want := fmt.Sprintf("%v (%T)", v, v), "bar (string)"; got != want {
		t.Errorf("Do = %v; want %v", got, want)
	}
	if err != nil {
		t.Errorf("Do error = %v", err)
	}
}

func TestDoErr(t *testing.T) {
	var g Group
	someErr := errors.New("some error")
	v, err, _ := g.Do("key", func() (any, error) {
		return nil, someErr
	})
	if !errors.Is(err, someErr) {
		t.Errorf("Do error = %v; want someErr %v", err, someErr)
	}
	if v != nil {
		t.Errorf("unexpected non-nil value %#v", v)
	}
}

func TestDoDupSuppress(t *testing.T) {
	var g Group
	var wg1, wg2 sync.WaitGroup
	c := make(chan string, 1)
	var calls int32
	fn := func() (any, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			// First invocation.
			wg1.Done()
		}
		v := <-c
		c <- v // pump; make available for any future calls

		time.Sleep(10 * time.Millisecond) // let more goroutines enter Do

		return v, nil
	}

	const n = 10
	wg1.Add(1)
	for range n {
		wg1.Add(1)
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			wg1.Done()
			v, err, _ := g.Do("key", fn)
			if err != nil {
				t.Errorf("Do error: %v", err)
				return
			}
			if s, _ := v.(string); s != "bar" {
				t.Errorf("Do = %T %v; want %q", v, v, "bar")
			}
		}()
	}
	wg1.Wait()
	// At least one goroutine is in fn now and all of them have at
	// least reached the line before the Do.
	c <- "bar"
	wg2.Wait()
	if got := atomic.LoadInt32(&calls); got <= 0 || got >= n {
		t.Errorf("number of calls = %d; want over 0 and less than %d", got, n)
	}
}

// Test that singleflight behaves correctly after Forget called.
// See https://github.com/golang/go/issues/31420
func TestForget(t *testing.T) {
	var g Group

	var (
		firstStarted  = make(chan struct{})
		unblockFirst  = make(chan struct{})
		firstFinished = make(chan struct{})
	)

	go func() {
		_, _, _ = g.Do("key", func() (i any, e error) {
			close(firstStarted)
			<-unblockFirst
			close(firstFinished)
			return
		})
	}()
	<-firstStarted
	g.Forget("key")

	unblockSecond := make(chan struct{})
	secondResult := g.DoChan("key", func() (i any, e error) {
		<-unblockSecond
		return 2, nil
	})

	close(unblockFirst)
	<-firstFinished

	thirdResult := g.DoChan("key", func() (i any, e error) {
		return 3, nil
	})

	close(unblockSecond)
	<-secondResult
	r := <-thirdResult
	if r.Val != 2 {
		t.Errorf("We should receive result produced by second call, expected: 2, got %d", r.Val)
	}
}

func TestDoChan(t *testing.T) {
	var g Group
	ch := g.DoChan("key", func() (any, error) {
		return "bar", nil
	})

	res := <-ch
	v := res.Val
	err := res.Err
	if got, want := fmt.Sprintf("%v (%T)", v, v), "bar (string)"; got != want {
		t.Errorf("Do = %v; want %v", got, want)
	}
	if err != nil {
		t.Errorf("Do error = %v", err)
	}
}

// Test singleflight behaves correctly after Do panic.
// See https://github.com/golang/go/issues/41133
func TestPanicDo(t *testing.T) {
	var g Group
	fn := func() (any, error) {
		panic("invalid memory address or nil pointer dereference")
	}

	const n = 5
	waited := int32(n)
	panicCount := int32(0)
	done := make(chan struct{})
	for range n {
		go func() {
			defer func() {
				if err := recover(); err != nil {
					t.Logf("Got panic: %v\n%s", err, debug.Stack())
					atomic.AddInt32(&panicCount, 1)
				}

				if atomic.AddInt32(&waited, -1) == 0 {
					close(done)
				}
			}()

			_, _, _ = g.Do("key", fn)
		}()
	}

	select {
	case <-done:
		if panicCount != n {
			t.Errorf("Expect %d panic, but got %d", n, panicCount)
		}
	case <-time.After(time.Second):
		t.Fatalf("Do hangs")
	}
}

func TestGoexitDo(t *testing.T) {
	var g Group
	fn := func() (any, error) {
		runtime.Goexit()
		return nil, nil
	}

	const n = 5
	waited := int32(n)
	done := make(chan struct{})
	for range n {
		go func() {
			var err error
			defer func() {
				if err != nil {
					t.Errorf("Error should be nil, but got: %v", err)
				}
				if atomic.AddInt32(&waited, -1) == 0 {
					close(done)
				}
			}()
			_, err, _ = g.Do("key", fn)
		}()
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("Do hangs")
	}
}

func executable(t testing.TB) string {
	exe, err := os.Executable()
	if err != nil {
		t.Skipf("skipping: test executable not found")
	}

	// Control case: check whether exec.Command works at all.
	// (For example, it might fail with a permission error on iOS.)
	cmd := exec.Command(exe, "-test.list=^$")
	cmd.Env = []string{}
	if err := cmd.Run(); err != nil {
		t.Skipf("skipping: exec appears not to work on %s: %v", runtime.GOOS, err)
	}

	return exe
}

func TestPanicDoChan(t *testing.T) {
	if os.Getenv("TEST_PANIC_DOCHAN") != "" {
		defer func() {
			_ = recover()
		}()

		g := new(Group)
		ch := g.DoChan("", func() (any, error) {
			panic("Panicking in DoChan")
		})
		<-ch
		t.Fatalf("DoChan unexpectedly returned")
	}

	t.Parallel()

	cmd := exec.Command(executable(t), "-test.run="+t.Name(), "-test.v")
	cmd.Env = append(os.Environ(), "TEST_PANIC_DOCHAN=1")
	out := new(bytes.Buffer)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	err := cmd.Wait()
	t.Logf("%s:\n%s", strings.Join(cmd.Args, " "), out)
	if err == nil {
		t.Errorf("Test subprocess passed; want a crash due to panic in DoChan")
	}
	if bytes.Contains(out.Bytes(), []byte("DoChan unexpectedly")) {
		t.Errorf("Test subprocess failed with an unexpected failure mode.")
	}
	if !bytes.Contains(out.Bytes(), []byte("Panicking in DoChan")) {
		t.Errorf("Test subprocess failed, but the crash isn't caused by panicking in DoChan")
	}
}

func TestPanicDoSharedByDoChan(t *testing.T) {
	if os.Getenv("TEST_PANIC_DOCHAN") != "" {
		blocked := make(chan struct{})
		unblock := make(chan struct{})

		g := new(Group)
		go func() {
			defer func() {
				_ = recover()
			}()
			_, _, _ = g.Do("", func() (any, error) {
				close(blocked)
				<-unblock
				panic("Panicking in Do")
			})
		}()

		<-blocked
		ch := g.DoChan("", func() (any, error) {
			panic("DoChan unexpectedly executed callback")
		})
		close(unblock)
		<-ch
		t.Fatalf("DoChan unexpectedly returned")
	}

	t.Parallel()

	cmd := exec.Command(executable(t), "-test.run="+t.Name(), "-test.v")
	cmd.Env = append(os.Environ(), "TEST_PANIC_DOCHAN=1")
	out := new(bytes.Buffer)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	err := cmd.Wait()
	t.Logf("%s:\n%s", strings.Join(cmd.Args, " "), out)
	if err == nil {
		t.Errorf("Test subprocess passed; want a crash due to panic in Do shared by DoChan")
	}
	if bytes.Contains(out.Bytes(), []byte("DoChan unexpectedly")) {
		t.Errorf("Test subprocess failed with an unexpected failure mode.")
	}
	if !bytes.Contains(out.Bytes(), []byte("Panicking in Do")) {
		t.Errorf("Test subprocess failed, but the crash isn't caused by panicking in Do")
	}
}

func ExampleGroup() {
	g := new(Group)

	block := make(chan struct{})
	res1c := g.DoChan("key", func() (any, error) {
		<-block
		return "func 1", nil
	})
	res2c := g.DoChan("key", func() (any, error) {
		<-block
		return "func 2", nil
	})
	close(block)

	res1 := <-res1c
	res2 := <-res2c

	// Results are shared by functions executed with duplicate keys.
	fmt.Println("Shared:", res2.Shared)
	// Only the first function is executed: it is registered and started with "key",
	// and doesn't complete before the second function is registered with a duplicate key.
	fmt.Println("EqualFunc results:", res1.Val.(string) == res2.Val.(string))
	fmt.Println("OnceGroupResult:", res1.Val)

	// Output:
	// Shared: true
	// EqualFunc results: true
	// OnceGroupResult: func 1
}
