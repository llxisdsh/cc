# **C**oncurrent **C**ore

[![Go Reference](https://pkg.go.dev/badge/github.com/llxisdsh/cc.svg)](https://pkg.go.dev/github.com/llxisdsh/cc)

**Concurrent Core for Go** — a lightweight, high-performance toolkit designed for critical paths where latency and allocation matter.

## Installation

```bash
go get github.com/llxisdsh/cc
```

## Core Components

### 🚀 Map & FlatMap

State-of-the-art concurrent map implementations, streamlined from [**llxisdsh/pb**](https://github.com/llxisdsh/pb).

| Component     | Description                                                                      | Ideal Use Case                         |
|---------------|----------------------------------------------------------------------------------|----------------------------------------|
| **`Map`**     | **Lock-free reads**, fine-grained write locking. Drop-in `sync.Map` replacement. | General purpose, mixed R/W workloads.  |
| **`FlatMap`** | **Seqlock-based**, open-addressing with inline storage.                          | Read-heavy, cache-sensitive scenarios. |

> **Note**: These components retain the core high-performance logic of `llxisdsh/pb` but are packaged here for lightweight integration. For comprehensive benchmarks and advanced architectural details, please refer to the [upstream repository](https://github.com/llxisdsh/pb).

### ⚡ OnceGroup

Generic, high-performance duplicate suppression (singleflight).

- **Generic**: `OnceGroup[K comparable, V any]`.
- **Robust**: Preserves `panic` and `Goexit` semantics (unlike `x/sync/singleflight`).
- **Fast**: ~20× faster than `singleflight` for same-key operations with near-zero allocations.

### 🔒 Synchronization Primitives

Atomic, low-overhead coordination tools built on runtime semaphores.

| Primitive | Metaphor | Behavior | Key Usage |
|:---|:---|:---|:---|
| **`Latch`** | **One-time Door** | Starts closed. Once `Open()`, stays open forever. | Initialization, Shutdown signal. |
| **`Gate`** | **Manual Door** | `Open()`/`Close()`/`Pulse()`. Supports broadcast wakeups. | Pausing/Resuming, Cond-like signals. |
| **`Rally`** | **Meeting Point** | `Meet(n)` waits until n parties arrive, then releases all. | CyclicBarrier, MapReduce stages. |
| **`Phaser`** | **Dynamic Barrier** | Dynamic party registration with split-phase `Arrive()`/`AwaitAdvance()`. | Java-style Phaser, Pipeline stages. |
| **`Epoch`** | **Milestone** | `WaitAtLeast(n)` blocks until counter reaches n. No thundering herd. | Phase coordination, Version gates. |
| **`Barter`** | **Exchanger** | Two goroutines swap values at a sync point. | Producer-Consumer handoff. |
| **`RWLock`** | **Read-Write Lock** | Spin-based R/W lock, writer-preferred. | Low-latency, writer-priority. |
| **`TicketLock`** | **Ticket Queue** | FIFO spin-lock with ticket algorithm. | Fair mutex, Latency-sensitive paths. |
| **`BitLock`** | **Bit Lock** | Spins on a specific bit mask. | Fine-grained, memory-constrained locks. |
| **`SeqLock`** | **Sequence Lock** | Optimistic reads with version counting. | Tear-free snapshots, Read-heavy. |
| **`FairSemaphore`**| **FIFO Queue** | Strict FIFO ordering for permit acquisition. | Anti-starvation scenarios. |
| **`TicketLockGroup`** | **Keyed Lock** | Per-key locking with auto-cleanup. | User/Resource isolation. |
| **`RWLockGroup`** | **Keyed R/W Lock** | Per-key R/W locking with auto-cleanup. | Config/Data partitioning. |
| **`WaitGroup`** | **Reusable WG** | Supports `WaitTimeout` & `TryWait`. Reusable immediately. | Batch processing, Time-bounded waits. |

> **Design Philosophy**: Minimal footprint, direct `runtime_semacquire` integration. Most primitives are zero-alloc on hot paths.

## Quick Start

### Concurrent Map

```go
package main

import "github.com/llxisdsh/cc"

func main() {
    // 1. Standard Map (Lock-free reads, sync.Map compatible)
    var m cc.Map[string, int]
    m.Store("foo", 1)

    // 2. FlatMap (Seqlock-based, inline storage)
    fm := cc.NewFlatMap[string, int](cc.WithCapacity(1000))
    fm.Store("bar", 2)

    // 3. Compute (Atomic Read-Modify-Write)
    // Safe, lock-free coordination for complex state changes
    m.Compute("foo", func(e *cc.Entry[string, int]) {
        if e.Loaded() {
            // Atomically increment if exists
            e.Update(e.Value() + 1)
        } else {
            // Initialize if missing
            e.Update(1)
        }
    })
}
```

### OnceGroup

```go
var g cc.OnceGroup[string, string]
// Coalesce duplicate requests
val, err, shared := g.Do("key", func() (string, error) {
    return "expensive-op", nil
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

// WaitGroup: Reusable with Timeout
var wg cc.WaitGroup
wg.Go(func() { /* work */ })
wg.WaitTimeout(time.Second) // Returns true if done, false if timed out
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
