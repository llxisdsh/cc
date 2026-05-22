# **C**oncurrent **C**ore

[![Go Reference](https://pkg.go.dev/badge/github.com/llxisdsh/cc.svg)](https://pkg.go.dev/github.com/llxisdsh/cc)

**Concurrent Core for Go** — a lightweight, high-performance toolkit designed for critical paths where latency and allocation matter.

## Installation

```bash
go get github.com/llxisdsh/cc
```

## Core Components

### 🚀 Concurrent Maps

State-of-the-art concurrent map implementations, delivering extreme performance in a lightweight package.

| Component       | Description                                                                      | Ideal Use Case                                                          |
|-----------------|----------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| **`Map`**       | **Lock-free reads**, fine-grained write locking. Drop-in `sync.Map` replacement. | General purpose, mixed R/W workloads.                                   |
| **`FlatMap`**   | **Seqlock-based**, inline open-addressing. Heavily optimized for cold starts.    | Cache-sensitive, extremely low tail latency.                            |
| **`SkipMap`**   | **Ordered map**, providing lock-free concurrent lookups, reads, and iteration.   | Highly concurrent ordered data access.                                  |
| **`FunnelMap`** | **Highly robust**, utilizes SkipMap for collisions and PLocal for size tracking. | Extreme resilience against poor hash distributions without degradation. |
| **`OFHTMap`**   | **Experimental**, optimistic open-addressing with inline key/value storage.      | Lowest GC overhead for allocation-sensitive workloads.                  |
| **`DWHTMap`**   | **Experimental**, fully lock-free open-addressing with DWCAS slot publication.   | Low-latency reads and writes under high concurrency.                    |

> **Note**: `Map` and `FlatMap` are streamlined versions of the high-performance [**llxisdsh/pb**](https://github.com/llxisdsh/pb) project. For comprehensive benchmarks (throughput, tail latency, memory usage, cold starts) and advanced architectural details, please refer to the [`benchmark`](./benchmark) directory or the upstream repository.

### ⚡ Processor Local

- **`PLocal[T]`**: Processor-local storage. Shards data by P (GOMAXPROCS) to minimize lock contention. Ideal for high-throughput counters or temporary buffers.

### 🧵 Execution Patterns

Tools to manage task execution and flow.

- **`WorkerPool`**: Robust, high-performance worker pool with zero-allocation on happy path.
- **`OnceGroup[K, V]`**: Coalesces duplicate requests (singleflight). \~20× faster than `singleflight` with panic propagation.

### 🔒 Synchronization Primitives

Atomic, low-overhead coordination tools built on runtime semaphores.

| Primitive             | Metaphor            | Behavior                                                                 | Key Usage                               |
|:----------------------|:--------------------|:-------------------------------------------------------------------------|:----------------------------------------|
| **`Latch`**           | **One-time Door**   | Starts closed. Once `Open()`, stays open forever.                        | Initialization, Shutdown signal.        |
| **`Gate`**            | **Manual Door**     | `Open()`/`Close()`/`Pulse()`. Supports broadcast wakeups.                | Pausing/Resuming, Cond-like signals.    |
| **`Rally`**           | **Meeting Point**   | `Meet(n)` waits until n parties arrive, then releases all.               | CyclicBarrier, MapReduce stages.        |
| **`Phaser`**          | **Dynamic Barrier** | Dynamic party registration with split-phase `Arrive()`/`AwaitAdvance()`. | Java-style Phaser, Pipeline stages.     |
| **`Epoch`**           | **Milestone**       | `WaitAtLeast(n)` blocks until counter reaches n. No thundering herd.     | Phase coordination, Version gates.      |
| **`Barter`**          | **Exchanger**       | Two goroutines swap values at a sync point.                              | Producer-Consumer handoff.              |
| **`RWLock`**          | **Read-Write Lock** | Spin-based R/W lock, writer-preferred.                                   | Low-latency, writer-priority.           |
| **`TicketLock`**      | **Ticket Queue**    | FIFO spin-lock with ticket algorithm.                                    | Fair mutex, Latency-sensitive paths.    |
| **`BitLock`**         | **Bit Lock**        | Spins on a specific bit mask.                                            | Fine-grained, memory-constrained locks. |
| **`SeqLock`**         | **Sequence Lock**   | Optimistic reads with version counting.                                  | Tear-free snapshots, Read-heavy.        |
| **`FairSemaphore`**   | **FIFO Queue**      | Strict FIFO ordering for permit acquisition.                             | Anti-starvation scenarios.              |
| **`TicketLockGroup`** | **Keyed Lock**      | Per-key locking with auto-cleanup.                                       | User/Resource isolation.                |
| **`RWLockGroup`**     | **Keyed R/W Lock**  | Per-key R/W locking with auto-cleanup.                                   | Config/Data partitioning.               |
| **`WaitGroup`**       | **Reusable WG**     | Supports `TryWait()` & `Waiters()`. Reusable immediately.                | Batch processing.                       |

> **Design Philosophy**: Minimal footprint, direct `runtime_semacquire` integration. Most primitives are zero-alloc on hot paths.

### 🛠️ Concurrency Helpers

Generic helpers to add Timeout and Context cancellation support to any blocking operation, plus tools for periodic and parallel execution.

- **`Wait` / `WaitTimeout`**: Add Context cancellation or Timeouts to blocking functions.
- **`Do`**: Execute functions returning errors with Context support.
- **`Repeat`**: Run actions periodically until Context is cancelled or an error occurs.
- **`Parallel`**: Execute N tasks concurrently with fail-fast error handling.

## Quick Start

### Concurrent Map

```go
package main

import "github.com/llxisdsh/cc"

func main() {
    // 1. Standard Map (Lock-free reads, sync.Map compatible)
    var m cc.Map[string, int]
    m.Store("foo", 1)

    // 2. FlatMap (Seqlock-based, inline open-addressing)
    fm := cc.NewFlatMap[string, int](cc.WithCapacity(1000))
    fm.Store("bar", 2)

    // 3. SkipMap (Lock-free, ordered concurrent skip list)
    sm := cc.NewSkipMap[string, int]()
    sm.Store("baz", 3)

    // 4. FunnelMap (High-throughput, extreme collision resilience)
    funnel := cc.NewFunnelMap[string, int]()
    funnel.Store("qux", 4)

    // 5. Compute (Atomic Read-Modify-Write)
    // Safe, lock-free coordination for complex state changes
    m.Compute("foo", func(e *cc.MapEntry[string, int]) {
        if e.Loaded() {
            // Atomically increment if exists
            e.Update(e.Value() + 1)
        } else {
            // Initialize if missing
            e.Update(1)
        }
    })

    // 6. Rebuild (Atomic transaction)
    // Safe, Multiple operations as single atomic transaction
    m.Rebuild(func(r *cc.MapRebuild[string, int]) {
        r.Store("new", 1)
        r.Delete("old")
        r.Compute("counter", func(e *cc.MapEntry[string, int]) {
            e.Update(e.Value() + 1)
        })
    })
}
```

### PLocal (Processor-Local Storage)

```go
// 1. Scalable Counter (PLocalCounter)
var c cc.PLocalCounter
// High throughput: Writes are sharded by P, avoiding global lock contention
c.Add(1)         // Scalable: No global lock
sum := c.Value() // Aggregates across all Ps

// 2. Multi-counter P-local slots (PLocalCounterN)
var cn cc.PLocalCounterN
// Each P-local shard packs multiple counters into one cache line,
// so tracking several counters does not use more cache-line slots than PLocalCounter.
cn.Add(0, 1)
cn.Add(1, 2)
total0 := cn.Value(0)

// 3. Generic PLocal
var p cc.PLocal[*bytes.Buffer]
// Run fn pinned to current P with local shard
p.With(func(buf **bytes.Buffer) {
    if *buf == nil { *buf = new(bytes.Buffer) }
    (*buf).WriteString("data")
})
```

### WorkerPool

```go
// Create a pool with 10 workers and queue size of 100
wp := cc.NewWorkerPool(10, 100)

// Optional: Handle panics from workers
wp.OnPanic = func(r any) {
    log.Printf("Worker panicked: %v", r)
}

// Submit non-blocking task (blocks if queue full)
wp.Submit(func() {
    process()
})

// Wait for all tasks to complete without closing
wp.Wait()

// Graceful shutdown
wp.Close()
```

### OnceGroup

```go
var g cc.OnceGroup[string, string]
// Coalesce duplicate requests
val, err, shared := g.Do("key", func() (string, error) {
    return "expensive-op", nil
})
```

### Concurrency Helpers

```go
// Wait: Add Context cancellation to a blocking function
err := cc.Wait(ctx, func() {
    wg.Wait()
})

// WaitTimeout: Add Timeout to a blocking function
if err := cc.WaitTimeout(time.Second, wg.Wait); err != nil {
    // timed out
}

// Do: Execute a function that returns error, with Context support
err := cc.Do(ctx, func() error {
    return complexOp()
})

// Repeat: Run action periodically until Context cancelled or error
cc.Repeat(ctx, 5*time.Second, func(ctx context.Context) error {
    return reloadConfig()
})

// Parallel: Execute N tasks concurrently (fail-fast on error)
err := cc.Parallel(ctx, 10, func(ctx context.Context, i int) error {
    return processItem(i)
})
```

### Primitives Gallery

#### 1. Coordination

```go
// Latch: One-shot signal (e.g., init finished)
var l cc.Latch
go func() { l.Open() }()
l.Wait() // Blocks until Open()

// Gate: Reusable stop/go barrier
var g cc.Gate
g.Open()   // All waiters pass
g.Close()  // Future waiters block
g.Pulse()  // Wake current waiters only, remain closed

// Rally: Cyclic barrier for N parties
var r cc.Rally
r.Meet(3)  // Blocks until 3 goroutines arrive

// WaitGroup: Reusable
var wg cc.WaitGroup
wg.Go(func() { /* work */ })
// Add timeout support via cc.WaitTimeout
err := cc.WaitTimeout(time.Second, wg.Wait)
```

#### 2. Advanced Locking

```go
// RWLock: Writer-preferred R/W lock (avoids writer starvation)
var rw cc.RWLock
rw.Lock() // Higher priority than RLock

// TicketLock: Fair mutex (FIFO), no starvation
var mu cc.TicketLock
mu.Lock()
defer mu.Unlock()

// BitLock: Memory-efficient lock using a single bit in uint64
var state uint64
const lockBit = 1 << 63
cc.BitLockUint64(&state, lockBit) // Spins until bit 63 is 0, then sets it
cc.BitUnlockUint64(&state, lockBit)

// SeqLock: Tear-free snapshots for read-heavy small data
var sl cc.SeqLock
var slot cc.SeqLockSlot[string]
cc.SeqLockWrite(&sl, &slot, "data") // Writer
val := cc.SeqLockRead(&sl, &slot)   // Reader (optimistic, no blocking)
```

#### 3. Keyed Locks (Auto-cleanup)

```go
// Lock by key (string, int, etc.) without memory leaks
var locks cc.TicketLockGroup[string]

locks.Lock("user:123")
// Critical section for user:123
locks.Unlock("user:123")
```

#### 4. Specialized

```go
// Phaser: Dynamic barrier (Java-style)
p := cc.NewPhaser()
p.Register()
phase := p.ArriveAndAwaitAdvance()

// Epoch: Wait for counter to reach target (e.g., version waits)
var e cc.Epoch
e.WaitAtLeast(5) // Blocks until e.Add() reaches 5

// Barter: Exchanger for 2 goroutines
b := cc.NewBarter[string]()
// G1: b.Exchange("ping") -> returns "pong"
// G2: b.Exchange("pong") -> returns "ping"
```

