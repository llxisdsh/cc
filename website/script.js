const translations = {
    en: {
        "nav.features": "Features",
        "nav.showcase": "Showcase",
        "hero.title": "Concurrent Core for Go",
        "hero.subtitle": "A lightweight, high-performance toolkit designed for critical paths where latency and allocation matter.",
        "hero.getStarted": "Go Reference",
        "hero.github": "View on GitHub",
        "features.title": "Core Components",
        "features.subtitle": "Engineered for extreme performance and reliability.",
        "features.map.title": "Map & FlatMap",
        "features.map.desc": "State-of-the-art concurrent maps. 10x the throughput and 50% less memory than sync.Map for latency-critical paths.",
        "features.once.title": "High-Level Primitives",
        "features.once.desc": "Orchestrate complex tasks with OnceGroup, WaitGroup, and Fair Semaphores for peak efficiency.",
        "features.locks.title": "Advanced Primitives",
        "features.locks.desc": "Latch, Gate, Rally, Phaser, Epoch, and more. Atomic coordination tools built directly on runtime semaphores.",
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
        "gallery.sem": "FIFO permits",
        "gallery.epoch": "Version gates",
        "gallery.seqlock": "Wait-free optimistic-read",
        "perf.title": "Speed That Matters",
        "perf.subtitle": "Performance comparison of cc.Map against standard library sync.Map.",
        "perf.label.throughput": "Throughput",
        "perf.label.latency": "P999 Latency",
        "perf.label.memory": "Memory Usage",
        "perf.map.throughput": "10x Faster",
        "perf.map.latency": "1/20 Delay",
        "perf.map.mem": "50% Lower"
    },
    zh: {
        "nav.features": "核心特性",
        "nav.showcase": "代码展示",
        "hero.title": "Concurrent Core for Go",
        "hero.subtitle": "专为关键路径设计的轻量级、高性能 Go 并发工具包，关注极致延迟与零内存分配。",
        "hero.getStarted": "查看文档",
        "hero.github": "在 GitHub 上查看",
        "features.title": "核心组件",
        "features.subtitle": "为极致性能和可靠性而生。",
        "features.map.title": "Map & FlatMap",
        "features.map.desc": "最前沿的并发 Map 实现。相比 sync.Map 提供 10 倍吞吐量并节省 50% 内存，专为关键路径设计。",
        "features.once.title": "高级调度原语",
        "features.once.desc": "利用 OnceGroup、WaitGroup 和公平信号量协调复杂任务，实现极致的协作效率。",
        "features.locks.title": "高级并发原语",
        "features.locks.desc": "包含 Latch, Gate, Rally, Phaser, Epoch 等。基于 Go 运行时信号量的原子级协作工具。",
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
        "gallery.sem": "公平信号量",
        "gallery.epoch": "分代版本栅栏",
        "gallery.seqlock": "免等待乐观读锁",
        "perf.title": "追求极致的性能",
        "perf.subtitle": "cc.Map 与标准库 sync.Map 的性能基准测试对比。",
        "perf.label.throughput": "吞吐能力",
        "perf.label.latency": "P999 延迟",
        "perf.label.memory": "内存占用",
        "perf.map.throughput": "提升 10 倍",
        "perf.map.latency": "降低至 1/20",
        "perf.map.mem": "降低 50%"
    }
};

const docContent = {
    en: {
        map: {
            title: "Map & FlatMap: The Storage Masters",
            desc: "Concurrent Core provides two distinct flavors of maps for different performance profiles.",
            analogy: "<b>The Supermarket vs. The Boutique Warehouse:</b> <br>Standard <code>Map</code> is like a supermarket aisles (buckets) where people check out items. <code>FlatMap</code> is a boutique warehouse where items are packed tightly onto shelves (cache lines) to minimize walking distance.",
            main: "Standard <code>Map</code> is a drop-in <code>sync.Map</code> replacement designed for extreme concurrent paths. Our benchmarks show <b>10x throughput</b> and <b>1/20th the P999 latency</b> while consuming only <b>50% of the memory</b> compared to <code>sync.Map</code>. <code>FlatMap</code> takes it even further with a cache-line aligned inline storage, making it the choice for latency-critical paths.",
            principles: [
                { title: "Seqlock (Sequence Lock)", desc: "Readers check a version number twice. If it didn't change, the read was consistent. This avoids heavy atomic write-barriers for readers." },
                { title: "False Sharing Mitigation", desc: "Data structures are padded to sync with CPU cache lines (64/128 bytes) to prevent cores from fighting over the same memory line." },
                { title: "Memory Allocation", desc: "Reduced pointers and flat memory layouts lead to 50% less overhead and lower GC pressure." }
            ],
            examples: [
                {
                    title: "Atomic Iteration (Entries)",
                    code: `<span class="token comment">// Update or Delete entries during high-perf iteration</span>\n<span class="token keyword">for</span> e := <span class="token keyword">range</span> m.Entries() {\n    <span class="token keyword">if</span> e.Key() % <span class="token number">2</span> == <span class="token number">0</span> {\n        e.Update(e.Value() + <span class="token number">1</span>)\n    } <span class="token keyword">else</span> {\n        e.Delete()\n    }\n}`
                },
                {
                    title: "Batch Atomic Updates (Rebuild)",
                    code: `<span class="token comment">// Perform multiple operations atomically as a batch</span>\nm.Rebuild(<span class="token keyword">func</span>(m *cc.MapRebuild[<span class="token type">string</span>, <span class="token type">int</span>]) {\n    m.Store(<span class="token string">"new_task"</span>, <span class="token number">1</span>)\n    m.Delete(<span class="token string">"expired"</span>)\n    m.Compute(<span class="token string">"counter"</span>, <span class="token keyword">func</span>(e *cc.MapEntry[<span class="token type">string</span>, <span class="token type">int</span>]) {\n        e.Update(e.Value() + <span class="token number">1</span>)\n    })\n})`
                },
                {
                    title: "Drop-in sync.Map Replacement",
                    code: `<span class="token comment">// Fully compatible with standard sync.Map API</span>\n<span class="token keyword">var</span> m cc.Map[<span class="token type">string</span>, <span class="token type">int</span>]\nm.Store(<span class="token string">"health"</span>, <span class="token number">100</span>)\nval, loaded := m.LoadOrStore(<span class="token string">"health"</span>, <span class="token number">200</span>)`
                }
            ]
        },
        advanced: {
            title: "Advanced Concurrency Primitives",
            desc: "A suite of sophisticated primitives for complex orchestration and performance-critical safety.",
            analogy: "<b>The Orchestral Conductor:</b> <br>While basic locks are like simple traffic lights, these primitives are the conductor of an orchestra, ensuring multiple sections work in perfect harmony, handling failures gracefully, and optimizing for the fairest resource distribution.",
            main: "This section covers high-level tools designed for sophisticated coordination. <code>OnceGroup</code> handles request coalescing, <code>WaitGroup</code> tracks task lifecycles, and <code>LockGroups</code> provide fine-grained, dynamic locking for arbitrary keys.",
            principles: [
                { title: "Failure Propagation", desc: "OnceGroup correctly propagates panics and Goexits to all waiters, a feat standard singleflights often fail to achieve." },
                { title: "Dynamic Lock Cleanup", desc: "TicketLockGroup and RWLockGroup automatically remove unused locks from memory when no goroutines are waiting." },
                { title: "Strict FIFO Fairness", desc: "FairSemaphore guarantees that permits are granted in the order of arrival, preventing 'barging' and starvation." }
            ],
            examples: [
                {
                    title: "Smart Singleflight (OnceGroup)",
                    code: `<span class="token keyword">var</span> g cc.OnceGroup[<span class="token type">string</span>, Result]\n\n<span class="token comment">// Subsequent calls wait for the first one and share results</span>\nv, err, shared := g.Do(<span class="token string">"api_key"</span>, <span class="token keyword">func</span>() (Result, <span class="token type">error</span>) {\n    <span class="token keyword">return</span> fetchFromDB()\n})`
                },
                {
                    title: "Modern Task Tracking (WaitGroup)",
                    code: `<span class="token keyword">var</span> wg cc.WaitGroup\n\n<span class="token comment">// Launch managed goroutines directly</span>\nwg.Go(<span class="token keyword">func</span>() { doWork() })\n\n<span class="token comment">// Check if done without blocking</span>\n<span class="token keyword">if</span> wg.TryWait() { finish() }`
                },
                {
                    title: "Starvation-Free Semaphore",
                    code: `<span class="token comment">// Guarantees FIFO even under extreme contention</span>\nsem := cc.NewFairSemaphore(<span class="token number">5</span>)\nsem.Acquire(<span class="token number">1</span>)\n<span class="token keyword">defer</span> sem.Release(<span class="token number">1</span>)`
                },
                {
                    title: "Dynamic Key-Based Locking",
                    code: `<span class="token comment">// Lock on arbitrary strings without pre-allocating locks</span>\n<span class="token keyword">var</span> users cc.TicketLockGroup[<span class="token type">string</span>]\nusers.Lock(<span class="token string">"user-123"</span>)\n<span class="token keyword">defer</span> users.Unlock(<span class="token string">"user-123"</span>)`
                },
                {
                    title: "Shared Key-Based Locking (RW)",
                    code: `<span class="token comment">// Reader-Writer support for arbitrary resources</span>\n<span class="token keyword">var</span> resources cc.RWLockGroup[<span class="token type">int</span>]\nresources.RLock(<span class="token number">42</span>)\n<span class="token keyword">defer</span> resources.RUnlock(<span class="token number">42</span>)`
                }
            ]
        },
        gate: {
            title: "Gate & Latch: Flow Control",
            desc: "Simplified state management for goroutine coordination.",
            analogy: "<b>The Toll Booth:</b> <br>A <code>Gate</code> can be opened (all pass), closed (stop here), or pulsed (one batch passes). A <code>Latch</code> is a one-way exit gate; once it's kicked open, it stays open forever.",
            main: "Built on Go's internal <code>runtime_semacquire</code>, these tools allow zero-allocation signaling. Use a <code>Gate</code> for pause/resume logic and a <code>Latch</code> for initialization or shutdown signals.",
            principles: [
                { title: "Double-Buffered Sema", desc: "Uses two separate semaphores to prevent 'signal stealing' during rapid state transitions." },
                { title: "Generation Counting", desc: "Ensures that calls to Pulse() only wake up waiters that were present at the moment of the pulse." }
            ],
            examples: [
                {
                    title: "Flash Signaling (Pulse)",
                    code: `<span class="token keyword">var</span> g cc.Gate\n\n<span class="token comment">// Pulse wakes current waiters but stays Closed for future ones</span>\n<span class="token keyword">go</span> <span class="token keyword">func</span>() {\n    g.Wait()\n    fmt.Println(<span class="token string">"Woken by flash pulse"</span>)\n}()\n\ng.Pulse()`
                },
                {
                    title: "One-Way Initialization (Latch)",
                    code: `<span class="token keyword">var</span> initialized cc.Latch\n\n<span class="token comment">// Once opened, it never closes again</span>\ninitialized.Open()\n\n<span class="token comment">// High-performance fast path check</span>\n<span class="token keyword">if</span> initialized.Wait() {\n    doFastPath()\n}`
                },
                {
                    title: "Flow Control (Pause/Resume)",
                    code: `<span class="token comment">// Reusable control loop</span>\n<span class="token keyword">for</span> {\n    g.Wait() <span class="token comment">// Blocks if gate is Closed</span>\n    task := queue.Pop()\n    process(task)\n    <span class="token keyword">if</span> queue.Empty() { g.Close() }\n}`
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
                    code: `<span class="token comment">// Batch processor waiting for exactly 10 workers</span>\nb := cc.NewRally(<span class="token number">10</span>)\n\n<span class="token keyword">for</span> i := <span class="token number">0</span>; i < <span class="token number">10</span>; i++ {\n    <span class="token keyword">go</span> <span class="token keyword">func</span>() {\n        doPreWork()\n        b.Meet() <span class="token comment">// All wait here for 10th worker</span>\n        doBatchWork()\n    }()\n}`
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
            main: "标准 <code>Map</code> 是 <code>sync.Map</code> 的高性能掉入式替代品。基准测试显示，在保持完全兼容的同时，它能提供 <b>10 倍的吞吐量</b> 和 <b>1/20 的 P999 延迟</b>，且 <b>内存占用仅为 50%</b>。<code>FlatMap</code> 则通过对齐缓存行的扁平存储，在延迟敏感的极致场景中表现更为卓越。",
            principles: [
                { title: "Seqlock (序列锁)", desc: "读取者通过双重检查版本号来确保一致性，完全避免了笨重的原子写屏障。" },
                { title: "伪共享消除", desc: "数据结构通过填充（Padding）与 CPU 缓存线对齐，防止多个核心争抢同一行内存。" },
                { title: "极简内存布局", desc: "通过减少指针嵌套和扁平化布局，内存开销降低 50%，极大缓解 GC 压力。" }
            ],
            examples: [
                {
                    title: "原子级迭代 (Entries)",
                    code: `<span class="token comment">// 在高性能迭代中安全地更新或删除条目</span>\n<span class="token keyword">for</span> e := <span class="token keyword">range</span> m.Entries() {\n    <span class="token keyword">if</span> e.Key() % <span class="token number">2</span> == <span class="token number">0</span> {\n        e.Update(e.Value() + <span class="token number">1</span>)\n    } <span class="token keyword">else</span> {\n        e.Delete()\n    }\n}`
                },
                {
                    title: "批量原子更新 (Rebuild)",
                    code: `<span class="token comment">// 作为一个批处理原子性地执行多个操作</span>\nm.Rebuild(<span class="token keyword">func</span>(m *cc.MapRebuild[<span class="token type">string</span>, <span class="token type">int</span>]) {\n    m.Store(<span class="token string">"new_task"</span>, <span class="token number">1</span>)\n    m.Delete(<span class="token string">"expired"</span>)\n    m.Compute(<span class="token string">"counter"</span>, <span class="token keyword">func</span>(e *cc.MapEntry[<span class="token type">string</span>, <span class="token type">int</span>]) {\n        e.Update(e.Value() + <span class="token number">1</span>)\n    })\n})`
                },
                {
                    title: "完全兼容 sync.Map",
                    code: `<span class="token comment">// 作为标准库 sync.Map 的掉入式替代品</span>\n<span class="token keyword">var</span> m cc.Map[<span class="token type">string</span>, <span class="token type">int</span>]\nm.Store(<span class="token string">"health"</span>, <span class="token number">100</span>)\nval, loaded := m.LoadOrStore(<span class="token string">"health"</span>, <span class="token number">200</span>)`
                }
            ]
        },
        advanced: {
            title: "高级并发原语",
            desc: "一组专为复杂调度和性能关键路径设计的精密原语。",
            analogy: "<b>交响乐指挥：</b> <br>如果基础锁是简单的红绿灯，这些原语就是交响乐团的指挥，确保各个声部协同工作，优雅处理异常，并优化资源分配的绝对公平性。",
            main: "该部分涵盖了专为复杂协作设计的高级工具。<code>OnceGroup</code> 处理请求合并，<code>WaitGroup</code> 追踪任务生命周期，<code>LockGroups</code> 则为任意 Key 提供细粒度的动态加锁支持。",
            principles: [
                { title: "异常自动传播", desc: "OnceGroup 能将 panic 和 Goexit 正确传播给所有等待者，这是许多 singleflight 实现无法企及的稳健性。" },
                { title: "动态锁清理", desc: "TicketLockGroup 和 RWLockGroup 在没有协程等待时会自动从内存中释放未使用的锁对象。" },
                { title: "严格 FIFO 公平性", desc: "FairSemaphore 保证许可按照申请顺序发放，彻底消除“冒领”和饥饿现象。" }
            ],
            examples: [
                {
                    title: "智能单飞去重 (OnceGroup)",
                    code: `<span class="token keyword">var</span> g cc.OnceGroup[<span class="token type">string</span>, Result]\n\n<span class="token comment">// 后续调用将等待第一个任务并共享结果</span>\nv, err, shared := g.Do(<span class="token string">"api_key"</span>, <span class="token keyword">func</span>() (Result, <span class="token type">error</span>) {\n    <span class="token keyword">return</span> fetchFromDB()\n})`
                },
                {
                    title: "现代任务追踪 (WaitGroup)",
                    code: `<span class="token keyword">var</span> wg cc.WaitGroup\n\n<span class="token comment">// 直接启动受管协程</span>\nwg.Go(<span class="token keyword">func</span>() { doWork() })\n\n<span class="token comment">// 非阻塞检查是否完成</span>\n<span class="token keyword">if</span> wg.TryWait() { finish() }`
                },
                {
                    title: "无饥饿公平信号量",
                    code: `<span class="token comment">// 即使在高竞争下也保证严格的 FIFO</span>\nsem := cc.NewFairSemaphore(<span class="token number">5</span>)\nsem.Acquire(<span class="token number">1</span>)\n<span class="token keyword">defer</span> sem.Release(<span class="token number">1</span>)`
                },
                {
                    title: "动态 Key 级锁 (TicketLockGroup)",
                    code: `<span class="token comment">// 对任意字符串进行加锁，无需预分配</span>\n<span class="token keyword">var</span> users cc.TicketLockGroup[<span class="token type">string</span>]\nusers.Lock(<span class="token string">"user-123"</span>)\n<span class="token keyword">defer</span> users.Unlock(<span class="token string">"user-123"</span>)`
                },
                {
                    title: "Key 级读写锁 (RWLockGroup)",
                    code: `<span class="token comment">// 对任意资源提供读写分离锁支持</span>\n<span class="token keyword">var</span> resources cc.RWLockGroup[<span class="token type">int</span>]\nresources.RLock(<span class="token number">42</span>)\n<span class="token keyword">defer</span> resources.RUnlock(<span class="token number">42</span>)`
                }
            ]
        },
        gate: {
            title: "Gate 与 Latch: 流量控制",
            desc: "用于协程间协作的简化状态管理。",
            analogy: "<b>收费站：</b> <br><code>Gate</code>（门控）可以打开（全体通过）、关闭（全体拦截）或脉冲（放行一批）。<code>Latch</code>（闩锁）则是一个单向出口，一旦踢开，就永远保持开启状态。",
            main: "这些工具基于 Go 内部的 <code>runtime_semacquire</code> 构建，实现了零分配信号通知。<code>Gate</code> 适用于暂停/恢复逻辑，<code>Latch</code> 适用于初始化或停机信号。",
            principles: [
                { title: "双缓冲信号量", desc: "使用两个独立的信号量，防止在状态快速切换时发生“信号冒领”。" },
                { title: "代际计数 (Generation)", desc: "确保 Pulse() 调用只唤醒在脉冲发生的瞬间确实在等待的协程。" }
            ],
            examples: [
                {
                    title: "快速脉冲信号 (Pulse)",
                    code: `<span class="token keyword">var</span> g cc.Gate\n\n<span class="token comment">// Pulse 唤醒当前等待者，但对后续协程保持关闭</span>\n<span class="token keyword">go</span> <span class="token keyword">func</span>() {\n    g.Wait()\n    fmt.Println(<span class="token string">"被脉冲信号唤醒"</span>)\n}()\n\ng.Pulse()`
                },
                {
                    title: "单次初始化 (Latch)",
                    code: `<span class="token keyword">var</span> initialized cc.Latch\n\n<span class="token comment">// 一旦开启，永不关闭</span>\ninitialized.Open()\n\n<span class="token comment">// 高性能快路径检查</span>\n<span class="token keyword">if</span> initialized.Wait() {\n    doFastPath()\n}`
                },
                {
                    title: "流程控制 (暂停/恢复)",
                    code: `<span class="token comment">// 可复用的控制循环</span>\n<span class="token keyword">for</span> {\n    g.Wait() <span class="token comment">// 如果闸门关闭则阻塞</span>\n    task := queue.Pop()\n    process(task)\n    <span class="token keyword">if</span> queue.Empty() { g.Close() }\n}`
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
                    code: `<span class="token comment">// 批处理器等待正好 10 个工作协程</span>\nb := cc.NewRally(<span class="token number">10</span>)\n\n<span class="token keyword">for</span> i := <span class="token number">0</span>; i < <span class="token number">10</span>; i++ {\n    <span class="token keyword">go</span> <span class="token keyword">func</span>() {\n        doPreWork()\n        b.Meet() <span class="token comment">// 所有人都在此等待第 10 个成员</span>\n        doBatchWork()\n    }()\n}`
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

const docOrder = ['map', 'gate', 'phaser', 'locks', 'advanced'];

function showDoc(type) {
    const container = document.getElementById('doc-container');
    const content = docContent[currentLang][type];

    // Update nav buttons
    document.querySelectorAll('.doc-btn').forEach(btn => {
        const btnType = btn.getAttribute('onclick').match(/'([^']+)'/)[1];
        btn.classList.toggle('active', btnType === type);

        // Auto scroll button into view on mobile
        if (btnType === type && window.innerWidth < 768) {
            btn.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'center' });
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
                <h4 style="margin-top: 2.5rem; font-size: 1.4rem; border-bottom: 1px solid var(--border); padding-bottom: 0.5rem; color: var(--text);">Code Showroom</h4>
                ${content.examples.map(ex => `
                    <div class="example-box">
                        <h5 style="margin: 1.5rem 0 0.5rem; font-size: 1.1rem; color: #6366f1;">${ex.title}</h5>
                        <div class="code-block glass" style="padding: 1rem; font-size: 0.85rem; border-radius: 8px; border: 1px solid var(--border); background: rgba(0,0,0,0.2);">
                            <pre style="margin: 0; white-space: pre-wrap; font-family: 'Fira Code', 'Cascadia Code', Consolas, monospace; line-height: 1.5; color: #e2e8f0;"><code>${ex.code}</code></pre>
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
        <h4 style="margin-bottom: 1rem; font-size: 1.2rem;">Experimental Principles</h4>
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

// Chart Animation on Scroll
const observerOptions = {
    threshold: 0.2
};

const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
        if (entry.isIntersecting) {
            entry.target.querySelectorAll('.chart-bar').forEach(bar => {
                const finalWidth = bar.style.width;
                bar.style.width = '0';
                setTimeout(() => {
                    bar.style.width = finalWidth;
                }, 100);
            });
            observer.unobserve(entry.target);
        }
    });
}, observerOptions);

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    const savedLang = localStorage.getItem('cc_lang');
    if (savedLang && translations[savedLang]) {
        currentLang = savedLang;
    }
    updateContent();
    showDoc('map'); // Default doc

    const perfSection = document.getElementById('performance');
    if (perfSection) observer.observe(perfSection);
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
