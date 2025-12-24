const translations = {
    en: {
        "nav.features": "Features",
        "nav.showcase": "Explore",
        "hero.title": "Concurrent Core for Go",
        "hero.subtitle": "A lightweight, high-performance toolkit designed for critical paths where latency and allocation matter.",
        "hero.getStarted": "API Reference",
        "hero.github": "Get Started (GitHub)",
        "features.title": "Core Components",
        "features.subtitle": "Engineered for extreme performance and reliability.",
        "features.map.title": "Map & FlatMap",
        "features.map.desc": "State-of-the-art concurrent maps. 10x the throughput and 50% less memory than sync.Map for latency-critical paths.",
        "features.once.title": "High-Level Primitives",
        "features.once.desc": "Orchestrate complex tasks with OnceGroup, WaitGroup, and Fair Semaphores for peak efficiency.",
        "features.locks.title": "Advanced Primitives",
        "features.locks.desc": "Latch, Gate, Rally, Phaser, Epoch, and more. Atomic coordination tools built directly on runtime semaphores.",
        "features.plocal.title": "PLocal Storage",
        "features.plocal.desc": "Processor-local storage (PLS) that shards data by GOMAXPROCS to eliminate lock contention in high-concurrency scenarios.",
        "primitives.title": "Synchronization Gallery",
        "primitives.subtitle": "A rich set of primitives for every concurrency pattern.",
        "primitives.coordination": "Coordination",
        "primitives.locking": "Locking",
        "primitives.advanced": "Advanced",
        "showcase.title": "Built for Performance",
        "knowledge.title": "Deep Dive Center",
        "knowledge.subtitle": "Understand the soul of concurrent programming through vivid analogies and rigorous principles.",
        "doc.nav.map": "Map & FlatMap",
        "doc.nav.once": "OnceGroup",
        "doc.nav.gate": "Gate & Latch",
        "doc.nav.phaser": "Phaser & Rally",
        "doc.nav.locks": "Basic Locks",
        "doc.nav.plocal": "PLocal",
        "doc.nav.advanced": "Advanced Primitives",
        "primitives.specialized": "Specialized",
        "gallery.latch": "One-shot signal",
        "gallery.gate": "Reusable barrier",
        "gallery.rally": "Cyclic barrier",
        "gallery.ticket": "Fair FIFO lock",
        "gallery.rw": "Writer-preferred",
        "gallery.bit": "Memory efficient",
        "gallery.once": "Smart singleflight",
        "gallery.wg": "Reusable tracker",
        "gallery.lg": "Dynamic key-locks",
        "gallery.plocal": "Per-processor storage",
        "gallery.sem": "FIFO permits",
        "gallery.epoch": "Version gates",
        "gallery.seqlock": "Wait-free optimistic-read",
        "perf.title": "Speed That Matters",
        "perf.subtitle": "Performance comparison of cc.Map against standard library sync.Map.",
        "perf.label.throughput": "Throughput",
        "perf.label.latency": "P999 Latency",
        "perf.label.memory": "Memory Usage",
        "perf.map.throughput": "10x Increase",
        "perf.map.latency": "95% Reduction",
        "perf.map.mem": "50% Savings",
        "game.win.title": "Achievement Unlocked!",
        "game.win.subtitle": "The world now has one more bored soul."
    },
    zh: {
        "nav.features": "核心特性",
        "nav.showcase": "深度探索",
        "hero.title": "Concurrent Core for Go",
        "hero.subtitle": "专为关键路径设计的轻量级、高性能并发工具包，关注极致延迟与零内存分配。",
        "hero.getStarted": "API 参考",
        "hero.github": "快速开始 (GitHub)",
        "features.title": "核心组件",
        "features.subtitle": "为极致性能和可靠性而生。",
        "features.map.title": "Map & FlatMap",
        "features.map.desc": "最前沿的并发 Map 实现。相比 sync.Map 提供 10 倍吞吐量并节省 50% 内存，专为关键路径设计。",
        "features.once.title": "高级调度原语",
        "features.once.desc": "利用 OnceGroup、WaitGroup 和公平信号量协调复杂任务，实现极致的协作效率。",
        "features.locks.title": "高级并发原语",
        "features.locks.desc": "包含 Latch, Gate, Rally, Phaser, Epoch 等。基于 Go 运行时信号量的原子级协作工具。",
        "features.plocal.title": "PLocal 处理器本地存储",
        "features.plocal.desc": "按处理器核心 (P) 分片存储数据，彻底消除高并发场景下的全局锁竞争。",
        "primitives.title": "并发原语画廊",
        "primitives.subtitle": "为各种并发模式提供丰富的原语支持。",
        "primitives.coordination": "协同交互",
        "primitives.locking": "锁机制",
        "primitives.advanced": "高级组件",
        "showcase.title": "为性能而生",
        "knowledge.title": "深度探索中心",
        "knowledge.subtitle": "通过生动的比喻和严谨的原理，领悟并发编程的灵魂。",
        "doc.nav.map": "Map 与内存布局",
        "doc.nav.once": "请求去重 OnceGroup",
        "doc.nav.gate": "门控 Gate/Latch",
        "doc.nav.phaser": "分层同步 Phaser/Rally",
        "doc.nav.locks": "基础轻量锁",
        "doc.nav.plocal": "PLocal 本地存储",
        "doc.nav.advanced": "高级并发原语",
        "primitives.specialized": "专家级组件",
        "gallery.latch": "单次触发信号",
        "gallery.gate": "可复用闸门",
        "gallery.rally": "循环屏障",
        "gallery.ticket": "公平 FIFO 锁",
        "gallery.rw": "写优先读写锁",
        "gallery.bit": "位级极简锁",
        "gallery.once": "智能单飞去重",
        "gallery.wg": "可复用任务追踪",
        "gallery.lg": "动态 Key 级锁",
        "gallery.plocal": "处理器本地存储",
        "gallery.sem": "公平信号量",
        "gallery.epoch": "分代版本栅栏",
        "gallery.seqlock": "免等待乐观读锁",
        "perf.title": "追求极致的性能",
        "perf.subtitle": "cc.Map 与标准库 sync.Map 的性能基准测试对比。",
        "perf.label.throughput": "吞吐能力",
        "perf.label.latency": "P999 延迟",
        "perf.label.memory": "内存占用",
        "perf.map.throughput": "增长10倍",
        "perf.map.latency": "减少95% ",
        "perf.map.mem": "节约50%",
        "game.win.title": "成就解锁！",
        "game.win.subtitle": "世界上又多了一个无聊的灵魂。"
    }
};

const docContent = {
    en: {
        map: {
            title: "Map & FlatMap: The Storage Masters",
            desc: "Concurrent Core provides two distinct flavors of maps for different performance profiles.",
            analogy: "<b>The Supermarket vs. The Boutique Warehouse:</b> <br>Standard <code>Map</code> is like a supermarket aisles (buckets) where people check out items. <code>FlatMap</code> is a boutique warehouse where items are packed tightly onto shelves (cache lines) to minimize walking distance.",
            main: "<code>cc.Map</code> is a drop-in <code>sync.Map</code> replacement with <b>10x throughput</b>, <b>1/20th P999 latency</b>, and <b>50% memory</b>. Unique to cc.Map: <code>Compute()</code> for atomic read-modify-write, <code>Entries()</code> for safe iteration with update/delete, and <code>Rebuild()</code> for batch atomic operations. <code>FlatMap</code> provides cache-line aligned inline storage for latency-critical paths.",
            principles: [
                { title: "Sharded Architecture", desc: "Map uses fine-grained bucket sharding with SeqLock per-bucket, allowing massive concurrent reads without global contention." },
                { title: "Seqlock (Sequence Lock)", desc: "Readers check a version number twice. If it didn't change, the read was consistent—avoiding heavy atomic write-barriers." },
                { title: "False Sharing Mitigation", desc: "Structures are padded to CPU cache lines (64/128 bytes) to prevent cores from fighting over the same memory line." }
            ],
            examples: [
                {
                    title: "Atomic Read-Modify-Write (Compute)",
                    code: `<span class="token keyword">var</span> m cc.Map[<span class="token type">string</span>, <span class="token type">int</span>]
<span class="token comment">// Compute: atomic increment without Load+Store race</span>
m.Compute(<span class="token string">"counter"</span>, <span class="token keyword">func</span>(e *cc.MapEntry[<span class="token type">string</span>, <span class="token type">int</span>]) {
    <span class="token keyword">if</span> e.Loaded() {
        e.Update(e.Value() + <span class="token number">1</span>)
    } <span class="token keyword">else</span> {
        e.Update(<span class="token number">1</span>) <span class="token comment">// Initialize if not exists</span>
    }
})`
                },
                {
                    title: "Safe Iteration (Entries)",
                    code: `<span class="token comment">// Update or Delete during iteration—safe and atomic</span>
<span class="token keyword">for</span> e := <span class="token keyword">range</span> m.Entries() {
    <span class="token keyword">if</span> shouldRemove(e.Key()) {
        e.Delete()
    } <span class="token keyword">else</span> {
        e.Update(transform(e.Value()))
    }
}`
                },
                {
                    title: "Batch Atomic Operations (Rebuild)",
                    code: `<span class="token comment">// Multiple operations as single atomic transaction</span>
m.Rebuild(<span class="token keyword">func</span>(r *cc.MapRebuild[<span class="token type">string</span>, <span class="token type">int</span>]) {
    r.Store(<span class="token string">"new"</span>, <span class="token number">1</span>)
    r.Delete(<span class="token string">"old"</span>)
    r.Compute(<span class="token string">"counter"</span>, <span class="token keyword">func</span>(e *cc.MapEntry[<span class="token type">string</span>, <span class="token type">int</span>]) {
        e.Update(e.Value() + <span class="token number">1</span>)
    })
})`
                },
                {
                    title: "Drop-in sync.Map Replacement",
                    code: `<span class="token comment">// 100% API compatible with sync.Map</span>
<span class="token keyword">var</span> m cc.Map[<span class="token type">string</span>, <span class="token type">int</span>]
m.Store(<span class="token string">"key"</span>, <span class="token number">100</span>)
val, ok := m.Load(<span class="token string">"key"</span>)
val, loaded := m.LoadOrStore(<span class="token string">"key"</span>, <span class="token number">200</span>)
m.Delete(<span class="token string">"key"</span>)`
                }
            ]
        },
        plocal: {
            title: "PLocal: Scaling with Cores",
            desc: "Per-processor storage for extreme scalability on multi-core systems.",
            analogy: "<b>The Private Workbench:</b> <br>Instead of all workers sharing one big tool bench (Global Lock) and waiting in line, each worker has their own private workbench (PLocal). They work independently and only combine results at the end.",
            main: "<code>PLocal</code> creates a shard of data for each logical processor (P) in the Go runtime. When you access it via <code>With(fn)</code>, your goroutine is pinned to the current P, ensuring exclusive access to that P's shard without a global lock. <code>PLocalCounter</code> is a specialized implementation for high-throughput counting.",
            principles: [
                { title: "P-Sharding", desc: "Data is split into GOMAXPROCS shards. Access is local to the processor executing the code." },
                { title: "Runtime Pinning", desc: "Uses runtime_procPin to prevent the goroutine from migrating to another processor during execution." },
                { title: "False Sharing Prevention", desc: "Shards are padded to cache lines to prevent cache coherency traffic between cores." }
            ],
            examples: [
                {
                    title: "Scalable Counter (PLocalCounter)",
                    code: `<span class="token keyword">var</span> c cc.PLocalCounter
<span class="token comment">// Add is lock-free relative to other processors</span>
c.Add(<span class="token number">1</span>)
<span class="token comment">// Aggregates across all processors when read</span>
sum := c.Value()`
                },
                {
                    title: "Generic PLocal Usage",
                    code: `<span class="token keyword">var</span> p cc.PLocal[*bytes.Buffer]
<span class="token comment">// Run fn pinned to current P with local shard</span>
p.With(<span class="token keyword">func</span>(buf **bytes.Buffer) {
    <span class="token keyword">if</span> *buf == nil { *buf = <span class="token keyword">new</span>(bytes.Buffer) }
    (*buf).WriteString(<span class="token string">"data"</span>)
})`
                }
            ]
        },
        advanced: {
            title: "Advanced Concurrency Primitives",
            desc: "A suite of sophisticated primitives for complex orchestration and performance-critical safety.",
            analogy: "<b>The Orchestral Conductor:</b> <br>While basic locks are like simple traffic lights, these primitives are the conductor of an orchestra, ensuring multiple sections work in perfect harmony, handling failures gracefully, and optimizing for the fairest resource distribution.",
            main: "This section covers high-level tools designed for sophisticated coordination. <code>OnceGroup</code> handles request coalescing with full panic/Goexit propagation. <code>WaitGroup</code> is <b>fully reusable</b>—unlike sync.WaitGroup, it can start a new batch immediately after the previous batch completes without waiting for Wait() calls to return. <code>FairSemaphore</code> guarantees strict FIFO permit acquisition. <code>Epoch</code> provides version-based coordination without thundering herd problems. <code>LockGroups</code> offer dynamic, auto-cleanup key-based locking.",
            principles: [
                { title: "Panic & Goexit Propagation", desc: "OnceGroup correctly propagates panics and Goexits to ALL waiters, a critical safety guarantee that standard singleflights often fail to achieve." },
                { title: "Instant Reusability", desc: "WaitGroup can be reused immediately after Done()—no need to wait for Wait() calls to return. Double-buffered semaphores prevent signal stealing across generations." },
                { title: "Anti-Thundering Herd", desc: "Epoch uses an ordered waiter list to wake only those whose target is met, avoiding the broadcast storms of condition variables." },
                { title: "Dynamic Lock Cleanup", desc: "LockGroups use reference counting to automatically remove unused locks from memory when no goroutines are waiting." },
                { title: "Strict FIFO Fairness", desc: "FairSemaphore guarantees permits are granted in the exact order of arrival, preventing 'barging' and starvation." }
            ],
            examples: [
                {
                    title: "Reusable WaitGroup (Key Difference)",
                    code: `<span class="token keyword">var</span> wg cc.WaitGroup
<span class="token comment">// Unlike sync.WaitGroup: reuse immediately after batch completes</span>
<span class="token keyword">for</span> batch := <span class="token keyword">range</span> batches {
    <span class="token keyword">for</span> _, task := <span class="token keyword">range</span> batch {
        wg.Go(<span class="token keyword">func</span>() { process(task) })
    }
    wg.Wait() <span class="token comment">// Instantly reusable for next batch!</span>
}
<span class="token comment">// Introspection: Count() and Waiters() for debugging</span>
fmt.Printf(<span class="token string">"Tasks: %d, Waiters: %d"</span>, wg.Count(), wg.Waiters())`
                },
                {
                    title: "Async Singleflight (DoChan)",
                    code: `<span class="token keyword">var</span> g cc.OnceGroup[<span class="token type">string</span>, *User]
<span class="token comment">// DoChan: returns channel immediately for async result</span>
ch := g.DoChan(<span class="token string">"user-123"</span>, <span class="token keyword">func</span>() (*User, <span class="token type">error</span>) {
    <span class="token keyword">return</span> fetchUser(<span class="token string">"123"</span>)
})
doOtherWork() <span class="token comment">// Continue working while loading...</span>
result := <-ch <span class="token comment">// Receive result when ready</span>
user, err, shared := result.Val, result.Err, result.Shared`
                },
                {
                    title: "Cache Invalidation (Forget)",
                    code: `<span class="token keyword">var</span> cache cc.OnceGroup[<span class="token type">string</span>, *Data]
data, err, _ := cache.Do(<span class="token string">"key"</span>, loadFromDB)
<span class="token comment">// Invalidate: next call will re-execute the function</span>
cache.Forget(<span class="token string">"key"</span>)
<span class="token comment">// ForgetUnshared: only invalidate if no duplicates joined</span>
<span class="token keyword">if</span> cache.ForgetUnshared(<span class="token string">"key"</span>) { log.Println(<span class="token string">"Removed"</span>) }`
                },
                {
                    title: "Version Coordination (Epoch)",
                    code: `<span class="token keyword">var</span> epoch cc.Epoch
<span class="token comment">// Waiter: block until version reaches target (no thundering herd!)</span>
<span class="token keyword">go</span> <span class="token keyword">func</span>() {
    epoch.WaitAtLeast(<span class="token number">5</span>)
    fmt.Println(<span class="token string">"Version 5 reached!"</span>)
}()
epoch.Add(<span class="token number">5</span>) <span class="token comment">// Publisher: advance version, wakes only relevant waiters</span>`
                },
                {
                    title: "Non-Blocking TryAcquire",
                    code: `sem := cc.NewFairSemaphore(<span class="token number">5</span>)
<span class="token comment">// TryAcquire: non-blocking, returns false immediately if unavailable</span>
<span class="token keyword">if</span> sem.TryAcquire(<span class="token number">1</span>) {
    <span class="token keyword">defer</span> sem.Release(<span class="token number">1</span>)
    doWork()
} <span class="token keyword">else</span> {
    queueForLater() <span class="token comment">// Fallback when permits unavailable</span>
}`
                },
                {
                    title: "Dynamic Key Locking (Auto-Cleanup)",
                    code: `<span class="token keyword">var</span> users cc.TicketLockGroup[<span class="token type">string</span>]
<span class="token comment">// Lock arbitrary keys—no pre-allocation, auto-cleanup when unlocked</span>
users.Lock(<span class="token string">"user-123"</span>)
updateUser(<span class="token string">"123"</span>)
users.Unlock(<span class="token string">"user-123"</span>) <span class="token comment">// Lock automatically removed from memory</span>

<span class="token comment">// RWLockGroup for shared reads on arbitrary keys</span>
<span class="token keyword">var</span> configs cc.RWLockGroup[<span class="token type">string</span>]
configs.RLock(<span class="token string">"db"</span>); readConfig(); configs.RUnlock(<span class="token string">"db"</span>)`
                }
            ]
        },
        gate: {
            title: "Gate & Latch: Flow Control",
            desc: "Simplified state management for goroutine coordination.",
            analogy: "<b>The Toll Booth:</b> <br>A <code>Gate</code> can be opened (all pass), closed (stop here), or pulsed (one batch passes). A <code>Latch</code> is a one-way exit gate; once it's kicked open, it stays open forever.",
            main: "<code>Gate</code> provides reusable Open/Close/Pulse coordination with <code>IsOpen()</code> fast-path check. <code>Latch</code> is for one-shot initialization signals—once Open(), it stays open forever. Both are built on Go's internal <code>runtime_semacquire</code> for zero-allocation signaling.",
            principles: [
                { title: "Double-Buffered Sema", desc: "Uses two separate semaphores to prevent 'signal stealing' during rapid state transitions." },
                { title: "Generation Counting", desc: "Ensures that calls to Pulse() only wake up waiters that were present at the moment of the pulse." },
                { title: "Fast-Path Check", desc: "IsOpen() provides a non-blocking state query for hot paths that need to avoid blocking." }
            ],
            examples: [
                {
                    title: "Basic Gate Control (Open/Close)",
                    code: `<span class="token keyword">var</span> gate cc.Gate
<span class="token comment">// Coordinator: control worker flow</span>
gate.Open()  <span class="token comment">// All waiters pass immediately</span>
<span class="token comment">// ... workers process ...</span>
gate.Close() <span class="token comment">// Future Wait() calls block</span>

<span class="token comment">// Fast-path check without blocking</span>
<span class="token keyword">if</span> gate.IsOpen() { doQuickWork() }`
                },
                {
                    title: "Broadcast Without Keeping Open (Pulse)",
                    code: `<span class="token keyword">var</span> cond cc.Gate
<span class="token comment">// Pulse: wake ALL current waiters, but stay Closed for future ones</span>
<span class="token comment">// This is like sync.Cond.Broadcast() but safer</span>
<span class="token keyword">go</span> <span class="token keyword">func</span>() { cond.Wait(); handleUpdate() }()
<span class="token keyword">go</span> <span class="token keyword">func</span>() { cond.Wait(); handleUpdate() }()
cond.Pulse() <span class="token comment">// Both wake up, future Wait() still blocks</span>`
                },
                {
                    title: "One-Shot Initialization (Latch)",
                    code: `<span class="token keyword">var</span> ready cc.Latch
<span class="token comment">// Worker goroutines wait for initialization</span>
<span class="token keyword">go</span> <span class="token keyword">func</span>() { ready.Wait(); useConfig() }()
<span class="token keyword">go</span> <span class="token keyword">func</span>() { ready.Wait(); useConfig() }()

loadConfig()
ready.Open() <span class="token comment">// All waiters released, future Wait() returns immediately</span>
<span class="token comment">// Open() is idempotent—safe to call multiple times</span>`
                }
            ]
        },
        phaser: {
            title: "Phaser & Rally: Group Coordination",
            desc: "Coordinate multiple goroutines across stages of work.",
            analogy: "<b>The Multi-Stage Relay:</b> <br>A <code>Phaser</code> is like a relay race where runners can join or leave mid-race. <code>Rally</code> is a meeting point where everyone must arrive before anyone can leave for the next leg.",
            main: "These primitives handle collective synchronization. <code>Rally</code> is a cyclic barrier for fixed batches, while <code>Phaser</code> provides dynamic participation management for complex multi-phase workloads.",
            principles: [
                { title: "Flexible Party Count", desc: "Phaser tracks total participants and arrivals using a single atomic 64-bit state." },
                { title: "Cyclic Reuse", desc: "Rally automatically resets its internal state once all parties have transitioned, allowing it to be used in loops." }
            ],
            examples: [
                {
                    title: "Dynamic Party Management",
                    code: `<span class="token keyword">p</span> := cc.NewPhaser()\np.Register() <span class="token comment">// Dynamically add a participant</span>\n\n<span class="token keyword">go</span> <span class="token keyword">func</span>() {\n    <span class="token keyword">defer</span> p.ArriveAndDeregister() <span class="token comment">// Signal completion and leave</span>\n    doWork()\n}()`
                },
                {
                    title: "Cyclic Synchronization (Rally)",
                    code: `<span class="token comment">// Batch processor waiting for exactly 10 workers</span>\n<span class="token keyword">var</span> b cc.Rally\n\n<span class="token keyword">for</span> i := <span class="token number">0</span>; i < <span class="token number">10</span>; i++ {\n    <span class="token keyword">go</span> <span class="token keyword">func</span>() {\n        doPreWork()\n        b.Meet(<span class="token number">10</span>) <span class="token comment">// All wait here for 10th worker</span>\n        doBatchWork()\n    }()\n}`
                },
                {
                    title: "Multi-Phase Tracking",
                    code: `<span class="token comment">// Wait for specific future stage</span>\nphase := p.Arrive()\nprepareNextStage()\np.AwaitAdvance(phase) <span class="token comment">// Blocks until current phase ends</span>`
                }
            ]
        },
        locks: {
            title: "Basic Locks: The Building Blocks",
            desc: "Ultra-lightweight, efficient locking mechanisms for granular control.",
            analogy: "<b>The Key Holder:</b> <br>Basic locks are individual keys for specific lockers. They are simple, fast, and do one thing perfectly: ensure only one person (or group of readers) has access at a time.",
            main: "When <code>sync.Mutex</code> is too heavy, Concurrent Core offers specialized alternatives. <code>BitLock</code> consumes zero extra bytes, <code>TicketLock</code> ensures strict FIFO, and <code>SeqLock</code> provides wait-free optimistic reads for massively concurrent read-heavy data.",
            principles: [
                { title: "FIFO Fairness", desc: "TicketLock works like a deli counter, ensuring that the person who arrived first gets the lock first, preventing starvation." },
                { title: "Bit-Level Density", desc: "BitLock uses CAS operations to manage a lock state inside a single bit, perfect for millions of small objects." }
            ],
            examples: [
                {
                    title: "Zero-Memory Locking (BitLock)",
                    code: `<span class="token keyword">type</span> Resource <span class="token keyword">struct</span> {\n    State uint64 <span class="token comment">// Bit 63 stores the lock</span>\n}\n\n<span class="token keyword">var</span> r Resource\ncc.BitLock(&r.State, <span class="token number">63</span>)\n<span class="token comment">// perform task...</span>\ncc.BitUnlock(&r.State, <span class="token number">63</span>)`
                },
                {
                    title: "Fair FIFO Access (TicketLock)",
                    code: `<span class="token comment">// Guarantees absolute order of entry</span>\n<span class="token keyword">var</span> mu cc.TicketLock\nmu.Lock()\n<span class="token keyword">defer</span> mu.Unlock()`
                },
                {
                    title: "Wait-Free Reads (SeqLock)",
                    code: `<span class="token comment">// Ultra-low latency optimistic reads</span>\n<span class="token keyword">var</span> sl cc.SeqLock\n<span class="token keyword">var</span> slot cc.SeqLockSlot[Data]\n\n<span class="token comment">// Writer</span>\ncc.SeqLockWrite(&sl, &slot, newData)\n\n<span class="token comment">// Reader (Wait-free, retry handled internally)</span>\ndata := cc.SeqLockRead(&sl, &slot)`
                }
            ]
        },
    },
    zh: {
        map: {
            title: "Map 与 FlatMap：存储大师",
            desc: "Concurrent Core 提供了两种针对不同性能曲线的 Map 实现。",
            analogy: "<b>超市 vs 精品仓库：</b> <br>标准 <code>Map</code> 就像超市货架（桶），多人在不同通道结账。<code>FlatMap</code> 则像一个精品仓库，货物紧密排列（缓存线对齐），以最大限度缩短搬运工（CPU）的走动距离。",
            main: "<code>cc.Map</code> 是 <code>sync.Map</code> 的掉入式替代品，提供 <b>10 倍吞吐量</b>、<b>1/20 P999 延迟</b>、<b>50% 内存占用</b>。cc.Map 独有功能: <code>Compute()</code> 原子读-改-写、<code>Entries()</code> 安全迭代并更新/删除、<code>Rebuild()</code> 批量原子操作。<code>FlatMap</code> 提供缓存线对齐的内联存储，适用于延迟敏感的极致场景。",
            principles: [
                { title: "分片架构", desc: "Map 使用细粒度桶分片，每个桶配备独立 SeqLock，实现海量并发读取而无全局竞争。" },
                { title: "Seqlock (序列锁)", desc: "读取者双重检查版本号确保一致性，完全避免笨重的原子写屏障。" },
                { title: "伪共享消除", desc: "结构体通过填充与 CPU 缓存线（64/128 字节）对齐，防止核心争抢同一内存行。" }
            ],
            examples: [
                {
                    title: "原子读-改-写 (Compute)",
                    code: `<span class="token keyword">var</span> m cc.Map[<span class="token type">string</span>, <span class="token type">int</span>]
<span class="token comment">// Compute: 原子递增，避免 Load+Store 竞态</span>
m.Compute(<span class="token string">"counter"</span>, <span class="token keyword">func</span>(e *cc.MapEntry[<span class="token type">string</span>, <span class="token type">int</span>]) {
    <span class="token keyword">if</span> e.Loaded() {
        e.Update(e.Value() + <span class="token number">1</span>)
    } <span class="token keyword">else</span> {
        e.Update(<span class="token number">1</span>) <span class="token comment">// 不存在则初始化</span>
    }
})`
                },
                {
                    title: "安全迭代 (Entries)",
                    code: `<span class="token comment">// 迭代期间更新或删除——安全且原子</span>
<span class="token keyword">for</span> e := <span class="token keyword">range</span> m.Entries() {
    <span class="token keyword">if</span> shouldRemove(e.Key()) {
        e.Delete()
    } <span class="token keyword">else</span> {
        e.Update(transform(e.Value()))
    }
}`
                },
                {
                    title: "批量原子操作 (Rebuild)",
                    code: `<span class="token comment">// 多个操作作为单一原子事务</span>
m.Rebuild(<span class="token keyword">func</span>(r *cc.MapRebuild[<span class="token type">string</span>, <span class="token type">int</span>]) {
    r.Store(<span class="token string">"new"</span>, <span class="token number">1</span>)
    r.Delete(<span class="token string">"old"</span>)
    r.Compute(<span class="token string">"counter"</span>, <span class="token keyword">func</span>(e *cc.MapEntry[<span class="token type">string</span>, <span class="token type">int</span>]) {
        e.Update(e.Value() + <span class="token number">1</span>)
    })
})`
                },
                {
                    title: "sync.Map 掉入式替代",
                    code: `<span class="token comment">// 100% API 兼容 sync.Map</span>
<span class="token keyword">var</span> m cc.Map[<span class="token type">string</span>, <span class="token type">int</span>]
m.Store(<span class="token string">"key"</span>, <span class="token number">100</span>)
val, ok := m.Load(<span class="token string">"key"</span>)
val, loaded := m.LoadOrStore(<span class="token string">"key"</span>, <span class="token number">200</span>)
m.Delete(<span class="token string">"key"</span>)`
                }
            ]
        },
        plocal: {
            title: "PLocal: 随核心数线性扩展",
            desc: "为多核系统设计的每处理器独立存储，实现极致的水平扩展能力。",
            analogy: "<b>私人工作台：</b> <br>传统的全局锁就像所有工人都挤在一个大工作台上工作，必须排队等待。PLocal 就像给每个工人分配了独立的私人工作台，大家各自干活，互不干扰，最后再汇总结果。",
            main: "<code>PLocal</code> 为 Go 运行时的每个逻辑处理器 (P) 创建一个数据分片。当你通过 <code>With(fn)</code> 访问时，当前 Goroutine 会被固定（Pin）在当前的 P 上，从而无需全局锁即可安全访问该分片。<code>PLocalCounter</code> 是专为高吞吐计数设计的特化实现。",
            principles: [
                { title: "P 级分片", desc: "数据按 GOMAXPROCS 分片，访问仅限于当前执行代码的处理器。" },
                { title: "运行时 Pinning", desc: "使用 runtime_procPin 防止 Goroutine 在执行期间迁移到其他处理器。" },
                { title: "伪共享消除", desc: "结构体通过填充与 CPU 缓存线对齐，防止核心间因争抢同一缓存行而产生的性能损耗。" }
            ],
            examples: [
                {
                    title: "高扩展计数器 (PLocalCounter)",
                    code: `<span class="token keyword">var</span> c cc.PLocalCounter
<span class="token comment">// Add 相对于其他处理器是无锁的</span>
c.Add(<span class="token number">1</span>)
<span class="token comment">// 读取时聚合所有处理器的值</span>
sum := c.Value()`
                },
                {
                    title: "通用 PLocal 用法",
                    code: `<span class="token keyword">var</span> p cc.PLocal[*bytes.Buffer]
<span class="token comment">// 在当前 P 上运行 fn，使用本地分片</span>
p.With(<span class="token keyword">func</span>(buf **bytes.Buffer) {
    <span class="token keyword">if</span> *buf == nil { *buf = <span class="token keyword">new</span>(bytes.Buffer) }
    (*buf).WriteString(<span class="token string">"data"</span>)
})`
                }
            ]
        },
        advanced: {
            title: "高级并发原语",
            desc: "一组专为复杂调度和性能关键路径设计的精密原语。",
            analogy: "<b>交响乐指挥：</b> <br>如果基础锁是简单的红绿灯，这些原语就是交响乐团的指挥，确保各个声部协同工作，优雅处理异常，并优化资源分配的绝对公平性。",
            main: "该部分涵盖了专为复杂协作设计的高级工具。<code>OnceGroup</code> 处理请求合并，自动传播 panic/Goexit。<code>WaitGroup</code> 是<b>完全可重用</b>的——与 sync.WaitGroup 不同，前一批任务完成后可立即启动新批次，无需等待所有 Wait() 调用返回。<code>FairSemaphore</code> 保证严格 FIFO 许可获取。<code>Epoch</code> 提供基于版本的协调，无惊群效应。<code>LockGroups</code> 提供动态、自动清理的 Key 级锁。",
            principles: [
                { title: "异常自动传播", desc: "OnceGroup 能将 panic 和 Goexit 正确传播给所有等待者，这是许多 singleflight 实现无法企及的稳健性。" },
                { title: "动态锁清理", desc: "TicketLockGroup 和 RWLockGroup 在没有协程等待时会自动从内存中释放未使用的锁对象。" },
                { title: "严格 FIFO 公平性", desc: "FairSemaphore 保证许可按照申请顺序发放，彻底消除“冒领”和饥饿现象。" }
            ],
            examples: [
                {
                    title: "可复用 WaitGroup (关键区别)",
                    code: `<span class="token keyword">var</span> wg cc.WaitGroup
<span class="token comment">// 与 sync.WaitGroup 不同: 批次完成后可立即复用</span>
<span class="token keyword">for</span> batch := <span class="token keyword">range</span> batches {
    <span class="token keyword">for</span> _, task := <span class="token keyword">range</span> batch {
        wg.Go(<span class="token keyword">func</span>() { process(task) })
    }
    wg.Wait() <span class="token comment">// 立即可用于下一批次！</span>
}
<span class="token comment">// 内省: Count() 返回活跃任务数，Waiters() 返回阻塞的 Wait() 调用数</span>
fmt.Printf(<span class="token string">"Tasks: %d, Waiters: %d"</span>, wg.Count(), wg.Waiters())`
                },
                {
                    title: "异步单飞 (DoChan)",
                    code: `<span class="token keyword">var</span> g cc.OnceGroup[<span class="token type">string</span>, *User]
<span class="token comment">// DoChan: 立即返回通道，异步获取结果</span>
ch := g.DoChan(<span class="token string">"user-123"</span>, <span class="token keyword">func</span>() (*User, <span class="token type">error</span>) {
    <span class="token keyword">return</span> fetchUser(<span class="token string">"123"</span>)
})
doOtherWork() <span class="token comment">// 加载期间继续其他工作...</span>
result := <-ch <span class="token comment">// 结果就绪时接收</span>
user, err, shared := result.Val, result.Err, result.Shared`
                },
                {
                    title: "缓存失效 (Forget)",
                    code: `<span class="token keyword">var</span> cache cc.OnceGroup[<span class="token type">string</span>, *Data]
data, err, _ := cache.Do(<span class="token string">"key"</span>, loadFromDB)
<span class="token comment">// Forget: 使缓存失效，下次调用将重新执行函数</span>
cache.Forget(<span class="token string">"key"</span>)
<span class="token comment">// ForgetUnshared: 仅在没有其他等待者时失效</span>
<span class="token keyword">if</span> cache.ForgetUnshared(<span class="token string">"key"</span>) { log.Println(<span class="token string">"已移除"</span>) }`
                },
                {
                    title: "版本协调 (Epoch)",
                    code: `<span class="token keyword">var</span> epoch cc.Epoch
<span class="token comment">// 等待者: 阻塞直到版本达到目标 (无惊群效应!)</span>
<span class="token keyword">go</span> <span class="token keyword">func</span>() {
    epoch.WaitAtLeast(<span class="token number">5</span>)
    fmt.Println(<span class="token string">"版本 5 已到达!"</span>)
}()
epoch.Add(<span class="token number">5</span>) <span class="token comment">// 发布者: 推进版本，仅唤醒相关等待者</span>`
                },
                {
                    title: "非阻塞 TryAcquire",
                    code: `sem := cc.NewFairSemaphore(<span class="token number">5</span>)
<span class="token comment">// TryAcquire: 非阻塞，许可不可用时立即返回 false</span>
<span class="token keyword">if</span> sem.TryAcquire(<span class="token number">1</span>) {
    <span class="token keyword">defer</span> sem.Release(<span class="token number">1</span>)
    doWork()
} <span class="token keyword">else</span> {
    queueForLater() <span class="token comment">// 许可不可用时的回退逻辑</span>
}`
                },
                {
                    title: "动态 Key 锁 (自动清理)",
                    code: `<span class="token keyword">var</span> users cc.TicketLockGroup[<span class="token type">string</span>]
<span class="token comment">// 对任意 Key 加锁——无需预分配，解锁时自动清理</span>
users.Lock(<span class="token string">"user-123"</span>)
updateUser(<span class="token string">"123"</span>)
users.Unlock(<span class="token string">"user-123"</span>) <span class="token comment">// 锁自动从内存中移除</span>

<span class="token comment">// RWLockGroup 支持共享读</span>
<span class="token keyword">var</span> configs cc.RWLockGroup[<span class="token type">string</span>]
configs.RLock(<span class="token string">"db"</span>); readConfig(); configs.RUnlock(<span class="token string">"db"</span>)`
                }
            ]
        },
        gate: {
            title: "Gate 与 Latch: 流量控制",
            desc: "用于协程间协作的简化状态管理。",
            analogy: "<b>收费站：</b> <br><code>Gate</code>（门控）可以打开（全体通过）、关闭（全体拦截）或脉冲（放行一批）。<code>Latch</code>（闩锁）则是一个单向出口，一旦踢开，就永远保持开启状态。",
            main: "<code>Gate</code> 提供可复用的 Open/Close/Pulse 协调，带有 <code>IsOpen()</code> 快速路径检查。<code>Latch</code> 用于一次性初始化信号——一旦 Open()，永远保持开启。两者均基于 Go 内部的 <code>runtime_semacquire</code> 构建，实现零分配信号通知。",
            principles: [
                { title: "双缓冲信号量", desc: "使用两个独立的信号量，防止在状态快速切换时发生'信号冒领'。" },
                { title: "代际计数", desc: "确保 Pulse() 调用只唤醒在脉冲发生瞬间确实在等待的协程。" },
                { title: "快速路径检查", desc: "IsOpen() 提供非阻塞的状态查询，适用于热路径避免阻塞。" }
            ],
            examples: [
                {
                    title: "基础 Gate 控制 (Open/Close)",
                    code: `<span class="token keyword">var</span> gate cc.Gate
<span class="token comment">// 协调者: 控制工作者流量</span>
gate.Open()  <span class="token comment">// 所有等待者立即通过</span>
<span class="token comment">// ... 工作者处理任务 ...</span>
gate.Close() <span class="token comment">// 后续 Wait() 调用将阻塞</span>

<span class="token comment">// 非阻塞快速路径检查</span>
<span class="token keyword">if</span> gate.IsOpen() { doQuickWork() }`
                },
                {
                    title: "广播但不保持开启 (Pulse)",
                    code: `<span class="token keyword">var</span> cond cc.Gate
<span class="token comment">// Pulse: 唤醒所有当前等待者，但对后续协程保持关闭</span>
<span class="token comment">// 类似 sync.Cond.Broadcast() 但更安全</span>
<span class="token keyword">go</span> <span class="token keyword">func</span>() { cond.Wait(); handleUpdate() }()
<span class="token keyword">go</span> <span class="token keyword">func</span>() { cond.Wait(); handleUpdate() }()
cond.Pulse() <span class="token comment">// 两者都被唤醒，后续 Wait() 仍阻塞</span>`
                },
                {
                    title: "一次性初始化 (Latch)",
                    code: `<span class="token keyword">var</span> ready cc.Latch
<span class="token comment">// 工作协程等待初始化完成</span>
<span class="token keyword">go</span> <span class="token keyword">func</span>() { ready.Wait(); useConfig() }()
<span class="token keyword">go</span> <span class="token keyword">func</span>() { ready.Wait(); useConfig() }()

loadConfig()
ready.Open() <span class="token comment">// 所有等待者释放，后续 Wait() 立即返回</span>
<span class="token comment">// Open() 是幂等的——可安全多次调用</span>`
                }
            ]
        },
        phaser: {
            title: "Phaser 与 Rally: 团队协调",
            desc: "跨越工作阶段协调多个协程。",
            analogy: "<b>多阶段接力赛：</b> <br><code>Phaser</code> 就像一场可以中途加入或退出的接力赛。<code>Rally</code>（集合）则是一个约定的集合点，每个人都必须到达后才能一起进入下一阶段。",
            main: "这些原语处理集体同步。<code>Rally</code> 是用于固定批次的循环屏障，而 <code>Phaser</code> 则为复杂的多阶段负载提供动态的成员管理。",
            principles: [
                { title: "灵活的成员计数", desc: "Phaser 仅通过一个原子级的 64 位状态即可跟踪总参与数 and 已到达数。" },
                { title: "循环复用", desc: "Rally 在所有成员通过后会自动重置内部状态，使其可以方便地在循环中使用。" }
            ],
            examples: [
                {
                    title: "动态成员管理",
                    code: `<span class="token keyword">p</span> := cc.NewPhaser()\np.Register() <span class="token comment">// 动态添加参与者</span>\n\n<span class="token keyword">go</span> <span class="token keyword">func</span>() {\n    <span class="token keyword">defer</span> p.ArriveAndDeregister() <span class="token comment">// 信号完成并退出团队</span>\n    doWork()\n}()`
                },
                {
                    title: "循环同步 (Rally)",
                    code: `<span class="token comment">// 批处理器等待正好 10 个工作协程</span>\n<span class="token keyword">var</span> b cc.Rally\n\n<span class="token keyword">for</span> i := <span class="token number">0</span>; i < <span class="token number">10</span>; i++ {\n    <span class="token keyword">go</span> <span class="token keyword">func</span>() {\n        doPreWork()\n        b.Meet(<span class="token number">10</span>) <span class="token comment">// 所有人都在此等待第 10 个成员</span>\n        doBatchWork()\n    }()\n}`
                },
                {
                    title: "多阶段追踪",
                    code: `<span class="token comment">// 等待特定的未来阶段</span>\nphase := p.Arrive()\nprepareNextStage()\np.AwaitAdvance(phase) <span class="token comment">// 阻塞直至当前阶段结束</span>`
                }
            ]
        },
        locks: {
            title: "基础轻量锁：核心构建块",
            desc: "极度轻量、高效的锁机制，为您提供最细粒度的并发控制。",
            analogy: "<b>保管箱钥匙：</b> <br>基础锁就像储物柜的独立钥匙。它们简单、快速，只做一件事：确保同一时间只有一个协程（或一组读取者）能访问资源。",
            main: "当 <code>sync.Mutex</code> 显得太重时，Concurrent Core 提供了专门的扩展。<code>BitLock</code> 零额外内存消耗，<code>TicketLock</code> 确保严格 FIFO，而 <code>SeqLock</code> 为高竞争读取场景提供免等待的乐观读支持。",
            principles: [
                { title: "FIFO 公平性", desc: "TicketLock 像熟食店柜台一样工作，确保先到者先得，彻底消除饥饿。" },
                { title: "位级极致密度", desc: "BitLock 利用 CAS 操作在单个位中管理锁状态，是海量细粒度对象的完美选择。" }
            ],
            examples: [
                {
                    title: "零内存占用锁 (BitLock)",
                    code: `<span class="token keyword">type</span> Resource <span class="token keyword">struct</span> {\n    State uint64 <span class="token comment">// 第 63 位存储锁状态</span>\n}\n\n<span class="token keyword">var</span> r Resource\ncc.BitLock(&r.State, <span class="token number">63</span>)\n<span class="token comment">// 处理任务...</span>\ncc.BitUnlock(&r.State, <span class="token number">63</span>)`
                },
                {
                    title: "公平 FIFO 访问 (TicketLock)",
                    code: `<span class="token comment">// 保证绝对的进入顺序</span>\n<span class="token keyword">var</span> mu cc.TicketLock\nmu.Lock()\n<span class="token keyword">defer</span> mu.Unlock()`
                },
                {
                    title: "无锁/免等读取 (SeqLock)",
                    code: `<span class="token comment">// 极致低延迟的最佳实践：乐观读</span>\n<span class="token keyword">var</span> sl cc.SeqLock\n<span class="token keyword">var</span> slot cc.SeqLockSlot[Data]\n\n<span class="token comment">// 写入端</span>\ncc.SeqLockWrite(&sl, &slot, newData)\n\n<span class="token comment">// 读取端 (免等待，内部自动处理冲突重试)</span>\ndata := cc.SeqLockRead(&sl, &slot)`
                }
            ]
        },
    }
};

let currentLang = 'en';

const docOrder = ['map', 'plocal', 'gate', 'phaser', 'locks', 'advanced'];

function showDoc(type) {
    const container = document.getElementById('doc-container');
    const content = docContent[currentLang][type];

    // Update nav buttons
    document.querySelectorAll('.doc-btn').forEach(btn => {
        const btnType = btn.getAttribute('onclick').match(/'([^']+)'/)[1];
        btn.classList.toggle('active', btnType === type);

        // Auto scroll button into view on mobile (horizontal only within doc-nav)
        if (btnType === type && window.innerWidth < 768) {
            const docNav = btn.closest('.doc-nav');
            if (docNav) {
                const btnRect = btn.getBoundingClientRect();
                const navRect = docNav.getBoundingClientRect();
                const scrollLeft = btn.offsetLeft - (navRect.width / 2) + (btnRect.width / 2);
                docNav.scrollTo({ left: scrollLeft, behavior: 'smooth' });
            }
        }
    });

    let principlesHTML = content.principles.map(p => `
    <div class="principle-card">
      <h5><span>⚙️</span> ${p.title}</h5>
      <p>${p.desc}</p>
    </div>
  `).join('');

    let examplesHTML = "";
    if (content.examples) {
        examplesHTML = `
            <div class="examples-section">
                <h4 class="showroom-title">Code Showroom</h4>
                ${content.examples.map(ex => `
                    <div class="example-box">
                        <h5>${ex.title}</h5>
                        <div class="code-block">
                            <pre><code>${ex.code}</code></pre>
                        </div>
                    </div>
                `).join('')}
            </div>
        `;
    }

    container.innerHTML = `
    <div class="doc-content active">
      <div class="doc-main">
        <h3>${content.title}</h3>
        <p>${content.desc}</p>
        <div class="analogy-box">
          <h4>Vivid Analogy</h4>
          <p>${content.analogy}</p>
        </div>
        <p>${content.main}</p>
        ${examplesHTML}
      </div>
      <div class="principle-sidebar">
        <h4 style="margin-bottom: 1rem; font-size: 1.2rem;">Core Principles</h4>
        ${principlesHTML}
      </div>
    </div>
  `;
}


function updateContent() {
    document.querySelectorAll('[data-i18n]').forEach(el => {
        const key = el.getAttribute('data-i18n');
        if (translations[currentLang][key]) {
            el.textContent = translations[currentLang][key];
        }
    });

    // Re-render active doc
    const activeBtn = document.querySelector('.doc-btn.active');
    if (activeBtn) {
        const type = activeBtn.getAttribute('onclick').match(/'([^']+)'/)[1];
        showDoc(type);
    }
}

function toggleLanguage() {
    currentLang = currentLang === 'en' ? 'zh' : 'en';
    updateContent();
    localStorage.setItem('cc_lang', currentLang);
    updateLanguageToggle();
}

// Navbar scroll effect
window.addEventListener('scroll', () => {
    const nav = document.getElementById('navbar');
    if (window.scrollY > 50) {
        nav.classList.add('scrolled');
    } else {
        nav.classList.remove('scrolled');
    }
});

// Performance Chart Animation with 1s Visibility Check
let perfAnimationTimer = null;

const observerOptions = {
    threshold: 0.3,
    rootMargin: '0px 0px -100px 0px'
};

const playPerfAnimation = (container) => {
    const rows = container.querySelectorAll('.chart-row');
    rows.forEach((row, index) => {
        const bar = row.querySelector('.chart-bar.cc');
        if (!bar) return;

        // Reset
        row.classList.remove('active');
        bar.style.transition = 'none';
        bar.style.width = bar.getAttribute('data-base');

        // Force reflow
        void bar.offsetWidth;

        // Animate
        setTimeout(() => {
            row.classList.add('active');
            bar.style.transition = 'width 1.5s cubic-bezier(0.19, 1, 0.22, 1)';
            bar.style.width = bar.getAttribute('data-target');
        }, 600 + index * 1000);
    });
};

const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
        if (entry.isIntersecting) {
            // Start 1s "gaze" timer
            perfAnimationTimer = setTimeout(() => {
                playPerfAnimation(entry.target);
                observer.unobserve(entry.target);
            }, 300);
        } else {
            // Cancel if scrolled away within 0.3s
            if (perfAnimationTimer) {
                clearTimeout(perfAnimationTimer);
                perfAnimationTimer = null;
            }
        }
    });
}, observerOptions);

// Prevent scroll restoration and ensure page starts at top
if ('scrollRestoration' in history) {
    history.scrollRestoration = 'manual';
}

// Force scroll to top on page load (especially for mobile)
window.scrollTo(0, 0);
document.documentElement.scrollTop = 0;
document.body.scrollTop = 0;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    // Ensure we're at the top on DOM ready
    window.scrollTo(0, 0);
    document.documentElement.scrollTop = 0;
    document.body.scrollTop = 0;

    const savedLang = localStorage.getItem('cc_lang');
    if (savedLang && translations[savedLang]) {
        currentLang = savedLang;
    }
    updateContent();
    showDoc('map'); // Default doc

    // Logo "Devour" Mini-game
    const logo = document.querySelector('#gameLogo');
    if (logo) {
        let eatenCount = 0;
        const chars = logo.querySelectorAll('.char:not(.highlight)');
        const highlights = logo.querySelectorAll('.char.highlight');
        const totalChars = chars.length;
        let gameActive = false;

        const resetGame = () => {
            eatenCount = 0;
            gameActive = false;
            logo.classList.remove('game-won');
            chars.forEach(char => {
                char.classList.remove('eaten');
            });
        };

        const checkWin = () => {
            if (eatenCount === totalChars) {
                logo.classList.add('game-won');
            }
        };

        // Click to eat (normal chars)
        chars.forEach(char => {
            char.addEventListener('click', (e) => {
                e.stopPropagation();
                if (!char.classList.contains('eaten')) {
                    char.classList.add('eaten');
                    eatenCount++;
                    checkWin();
                }
            });
        });

        // Click blink effect for highlight chars
        highlights.forEach(char => {
            char.addEventListener('click', (e) => {
                e.stopPropagation();
                char.classList.add('blink');
                setTimeout(() => {
                    char.classList.remove('blink');
                }, 150);
            });
        });

        // Reset on mouse leave
        logo.addEventListener('mouseleave', () => {
            if (gameActive) {
                setTimeout(resetGame, 100);
            }
        });

        logo.addEventListener('mouseenter', () => {
            gameActive = true;
        });
    }

    const perfSection = document.getElementById('performance');
    if (perfSection) {
        observer.observe(perfSection);

        // Click to replay animation
        const chartContainer = perfSection.querySelector('.chart-container');
        if (chartContainer) {
            chartContainer.style.cursor = 'pointer';
            chartContainer.title = 'Click to replay animation';
            chartContainer.addEventListener('click', () => {
                playPerfAnimation(perfSection);
            });
        }
    }
});

function toggleMenu() {
    const navLinks = document.getElementById('navLinks');
    const menuToggle = document.getElementById('menuToggle');
    navLinks.classList.toggle('active');
    menuToggle.classList.toggle('active');

    // Prevent scrolling when menu is open
    document.body.style.overflow = navLinks.classList.contains('active') ? 'hidden' : 'auto';
}

function updateLanguageToggle() {
    const navLinks = document.getElementById('navLinks');
    if (navLinks && navLinks.classList.contains('active')) {
        toggleMenu();
    }
}

// Close mobile menu when a link is clicked
document.querySelectorAll('.nav-links a').forEach(link => {
    link.addEventListener('click', () => {
        const navLinks = document.getElementById('navLinks');
        if (navLinks.classList.contains('active')) {
            toggleMenu();
        }
    });
});

// Final scroll reset after page fully loads (critical for mobile)
window.addEventListener('load', () => {
    setTimeout(() => {
        window.scrollTo(0, 0);
        document.documentElement.scrollTop = 0;
        document.body.scrollTop = 0;
    }, 0);
});
