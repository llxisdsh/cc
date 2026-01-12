package cc

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRWLock_Basic(t *testing.T) {
	var a int
	var rw RWLock
	rw.Lock()
	a = 1
	rw.Unlock()
	rw.RLock()
	_ = a
	rw.RUnlock()
}

func TestRWLock_ReadersAndWriters(t *testing.T) {
	var rw RWLock
	var readers int32
	var writers int32

	const loops = 1000
	readerN := runtime.GOMAXPROCS(0)
	writerN := 2

	var wg sync.WaitGroup
	wg.Add(readerN + writerN)

	for range readerN {
		go func() {
			defer wg.Done()
			for range loops {
				rw.RLock()
				n := atomic.AddInt32(&readers, 1)
				if atomic.LoadInt32(&writers) != 0 {
					t.Errorf("reader observed active writer")
					rw.RUnlock()
					return
				}
				if n <= 0 {
					t.Errorf("invalid reader count")
					rw.RUnlock()
					return
				}
				atomic.AddInt32(&readers, -1)
				rw.RUnlock()
			}
		}()
	}

	for range writerN {
		go func() {
			defer wg.Done()
			for range loops {
				rw.Lock()
				if atomic.AddInt32(&writers, 1) != 1 {
					t.Errorf("multiple writers active")
					rw.Unlock()
					return
				}
				if atomic.LoadInt32(&readers) != 0 {
					t.Errorf("writer observed active readers")
					rw.Unlock()
					return
				}
				atomic.AddInt32(&writers, -1)
				rw.Unlock()
			}
		}()
	}

	wg.Wait()
}

func TestRWLock32_Basic(t *testing.T) {
	var a int
	var rw RWLock32
	rw.Lock()
	a = 1
	rw.Unlock()
	rw.RLock()
	_ = a
	rw.RUnlock()
}

func TestRWLock32_ReadersAndWriters(t *testing.T) {
	var rw RWLock32
	var readers int32
	var writers int32

	const loops = 1000
	readerN := runtime.GOMAXPROCS(0)
	writerN := 2

	var wg sync.WaitGroup
	wg.Add(readerN + writerN)

	for range readerN {
		go func() {
			defer wg.Done()
			for range loops {
				rw.RLock()
				n := atomic.AddInt32(&readers, 1)
				if atomic.LoadInt32(&writers) != 0 {
					t.Errorf("reader observed active writer")
					rw.RUnlock()
					return
				}
				if n <= 0 {
					t.Errorf("invalid reader count")
					rw.RUnlock()
					return
				}
				atomic.AddInt32(&readers, -1)
				rw.RUnlock()
			}
		}()
	}

	for range writerN {
		go func() {
			defer wg.Done()
			for range loops {
				rw.Lock()
				if atomic.AddInt32(&writers, 1) != 1 {
					t.Errorf("multiple writers active")
					rw.Unlock()
					return
				}
				if atomic.LoadInt32(&readers) != 0 {
					t.Errorf("writer observed active readers")
					rw.Unlock()
					return
				}
				atomic.AddInt32(&writers, -1)
				rw.Unlock()
			}
		}()
	}

	wg.Wait()
}

func TestRWLock_TryLock(t *testing.T) {
	var rw RWLock

	// TryLock should succeed on free lock
	if !rw.TryLock() {
		t.Error("TryLock should succeed on free lock")
	}
	rw.Unlock()

	// TryRLock should succeed on free lock
	if !rw.TryRLock() {
		t.Error("TryRLock should succeed on free lock")
	}
	rw.RUnlock()

	// TryLock should fail when read locked
	rw.RLock()
	if rw.TryLock() {
		t.Error("TryLock should fail when read locked")
	}
	rw.RUnlock()

	// TryRLock should fail when write locked
	rw.Lock()
	if rw.TryRLock() {
		t.Error("TryRLock should fail when write locked")
	}
	rw.Unlock()
}

func TestRWLock32_TryLock(t *testing.T) {
	var rw RWLock32

	// TryLock should succeed on free lock
	if !rw.TryLock() {
		t.Error("TryLock should succeed on free lock")
	}
	rw.Unlock()

	// TryRLock should succeed on free lock
	if !rw.TryRLock() {
		t.Error("TryRLock should succeed on free lock")
	}
	rw.RUnlock()

	// TryLock should fail when read locked
	rw.RLock()
	if rw.TryLock() {
		t.Error("TryLock should fail when read locked")
	}
	rw.RUnlock()

	// TryRLock should fail when write locked
	rw.Lock()
	if rw.TryRLock() {
		t.Error("TryRLock should fail when write locked")
	}
	rw.Unlock()
}
