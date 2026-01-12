package cc

import (
	"errors"
	"runtime"
	"sync/atomic"
)

var ErrPoolClosed = errors.New("cc: pool closed")

// WorkerPool uses the Reusable WaitGroup and robust worker logic.
type WorkerPool struct {
	jobs      chan func()
	wg        WaitGroup // cc.WaitGroup
	maxWorker int32
	workers   atomic.Int32
	active    atomic.Int32 // number of tasks currently being processed
	closed    bool
	mu        RWLock
	// OnPanic is called when a worker panics.
	// If nil, the panic is ignored.
	OnPanic func(r any)
}

func NewWorkerPool(workers int, queueSize int) *WorkerPool {
	return &WorkerPool{
		jobs:      make(chan func(), queueSize),
		maxWorker: int32(workers),
	}
}

func (p *WorkerPool) Submit(task func()) error {
	// Protected by RLock to ensure we don't send on closed channel
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrPoolClosed
	}

	p.jobs <- task
	p.mu.RUnlock()

	// Lazy start workers
	if p.workers.Load() < p.maxWorker {
		if p.workers.Add(1) <= p.maxWorker {
			p.startWorker()
		} else {
			p.workers.Add(-1)
		}
	}

	return nil
}

func (p *WorkerPool) startWorker() {
	p.wg.Go(func() {
		defer func() {
			p.workers.Add(-1)
			if r := recover(); r != nil {
				// Restart worker if it panicked and pool is not closed
				// Current strategy: allow worker to die, next Submit will lazy start it.
				if p.OnPanic != nil {
					p.OnPanic(r)
				}
			}
		}()

		for task := range p.jobs {
			p.active.Add(1)
			func() {
				defer func() {
					p.active.Add(-1)
					if r := recover(); r != nil {
						// Recover from task panic so worker can continue.
						if p.OnPanic != nil {
							p.OnPanic(r)
						}
					}
				}()
				task()
			}()
		}
	})
}

// Close gracefully shuts down the pool.
// It waits for all submitted tasks to complete.
func (p *WorkerPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.jobs)
	p.mu.Unlock()

	p.wg.Wait()
}

// Wait blocks until all submitted tasks complete, without closing the pool.
// This is useful for batch synchronization while keeping the pool alive.
func (p *WorkerPool) Wait() {
	for {
		if len(p.jobs) == 0 && p.active.Load() == 0 {
			return
		}
		runtime.Gosched()
	}
}

// Pending returns the number of tasks waiting in the queue.
func (p *WorkerPool) Pending() int {
	return len(p.jobs)
}

// Running returns the number of currently active workers.
func (p *WorkerPool) Running() int {
	return int(p.workers.Load())
}
