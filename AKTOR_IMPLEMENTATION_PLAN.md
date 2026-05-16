# Aktor: Async Actor System Implementation Plan

## Architecture Overview
- **Hybrid Actor Model**: Direct calls for local, message passing for remote
- **Fully Typed Actors**: Compile-time safety with Protocol Buffers evolution
- **Ephemeral Crawlers**: One actor per URL for maximum parallelism
- **Strong Consistency**: URL deduplication via PostgreSQL
- **Direct Routing**: Location-transparent actor references
- **Kubernetes Discovery**: Service discovery via k8s DNS

## Implementation Status Summary

**Phase 1 Progress: 10/17 complete (59%)**
- ✅ Core infrastructure: Actor traits, ActorRef, ActorContext, spawning, routing
- ✅ Work-stealing reactive architecture with 99.98%+ concurrent processing
- ✅ Ask pattern with timeout handling and futures
- ✅ Actor factories (actor_of, actor_of_args, actor_of_props)
- ✅ TestKit with test probes and expectation helpers
- ⚠️  Partial: Scheduling (one-time only), supervision (enum only), lifecycle monitoring (stubs only)
- ❌ Missing: Derive macros, message transform/dispatch, advanced patterns, behavior management, routers

**Performance Achievements:**
- 4.56M msg/sec throughput (10K actors, 500M messages)
- 99.9986% concurrent processing (only 6,900 messages in-flight at completion)
- Memory stable with reactive work-stealing architecture
- Sub-millisecond message latency

## Implementation Plan

### Phase 1: Core Actor System Foundation
- [x] **1.1** Define core actor traits (`Actor<M>`, `TypedActor<M>`) ✅ *Complete with `Actor<M>` trait*
- [x] **1.2** Implement `ActorRef<T>` with local/remote variants ✅ *Local complete with work-stealing reactive architecture*
- [x] **1.3** Create `ActorContext` for actor lifecycle management ✅ *Core complete: spawn_child, stop_child, children, parent, send_to, select, schedule_once*
- [x] **1.4** Build basic actor spawning and message routing ✅ *Complete with work-stealing queue and reactive scheduling*
- [x] **1.5** Add actor address system (`actor://node/path`) ✅ *Complete with `ActorAddress` and `ActorPath`*
- [ ] **1.6** Implement actor derive macros (`#[actor]`) ❌ *Auto-generate message enums and dispatch*
- [x] **1.7** Build actor factory system (`actor_of`, `actor_of_args`) ✅ *Complete: `actor_of`, `actor_of_args`, `actor_of_props`, `actor_of_args_props`*
- [x] **1.8** Complete ask pattern with futures ✅ *Complete with `ask()`, `ask_ext()`, timeout handling, and context-based responses*
- [ ] **1.9** Add message transform & dispatch (`Receive<T>`) ❌ *Type-safe message handling patterns*
- [x] **1.10** Create test kit for actor testing ✅ *Complete: `ActorTestKit`, `TestProbe`, `TestContext`, expectation helpers*
- [ ] **1.11** Implement supervision strategies ⚠️ *Enum defined (Restart, Stop, Escalate) but no actual behavior implementation*
- [x] **1.12** Build comprehensive actor context ✅ *Complete: children management, parent ref, message sending, actor selection, scheduling, watch/unwatch stubs*
- [ ] **1.13** Implement advanced interaction patterns ❌ *Adapted response, response aggregator, tail chopping*
- [ ] **1.14** Add behavior management ❌ *Dynamic behavior switching, become/unbecome*
- [ ] **1.15** Build routers and dispatchers ❌ *Round-robin, broadcast, custom thread pools*
- [ ] **1.16** Implement timers and scheduling, including recurring timers
- [ ] **1.17** Add lifecycle monitoring ⚠️ *`watch()`/`unwatch()` stubs present, death watch not implemented*

### 🚨 **CRITICAL: Discovered During Crawler Development**
These features are blocking real-world usage and must be implemented immediately:

- [x] **1.18** 🔥 **PRIORITY: Actor System Extensions** - *Shared resources pattern (HTTP clients, DB pools, etc.)* ✅ **COMPLETE**
  - **Why Critical:** Actors must be stateless for serialization/event sourcing, but need access to shared resources (HTTP clients, DB connections)
  - **Crawler Blocker:** Can't hold `reqwest::Client` in actor state (not serializable), need system-wide shared client
  - **Implementation Complete:**
    - ✅ `Extension` trait for defining shared resources
    - ✅ `ExtensionRegistry` with type-safe get/register
    - ✅ `system.register_extension()` and `system.extension::<T>()` APIs
    - ✅ `get_or_create()` for lazy initialization
    - ✅ Full test coverage (9 tests passing)
  - **Usage:**
    ```rust
    // Register shared resource
    system.register_extension(HttpClientExtension::new());

    // Access in any actor
    impl Actor<M> for CrawlerActor {
        fn handle(&mut self, msg: M, ctx: &ActorContext<M>) {
            let client = ctx.system().extension::<HttpClientExtension>();
            let response = client.get(url).send()?;
        }
    }
    ```

- [ ] **1.19** 🔥 **Access Message Sender** - *`ctx.sender()` to reply to caller*
  - **Why Critical:** Actors need to reply to whoever sent a message, not just parent
  - **Crawler Blocker:** CrawlerActor should reply to Frontier (sender), but can only access parent
  - **Current Workaround:** Pass sender in message payload (breaks actor model)
  - **Akka Equivalent:** `sender()` implicit parameter
  - **Implementation:** Store sender `ActorRef` in context during message dispatch

- [ ] **1.20** 🔥 **Actor Self-Termination** - *`ctx.stop(self)` or similar API*
  - **Why Critical:** Ephemeral actors (1 per task) can't clean themselves up
  - **Crawler Blocker:** CrawlerActor processes one URL then should die, but has no way to stop itself
  - **Current Issue:** Dead actors accumulate in `actor_storage` forever → memory leak
  - **Akka Equivalent:** `context.stop(self)` or `PoisonPill`
  - **Implementation:** Actor sets flag, system removes from storage after message processed

- [ ] **1.21** 🔥 **Automatic Cleanup of Stopped Actors**
  - **Why Critical:** Memory leak without it
  - **Current Issue:** Stopped actors remain in `actor_storage` DashMap forever
  - **Implementation:** Remove from storage when actor stopped, deallocate resources

### Phase 2: Message System & Serialization
- [ ] **2.1** Setup Protocol Buffers for message definitions
- [ ] **2.2** Implement message serialization/deserialization
- [ ] **2.3** Create message envelope with routing metadata
- [ ] **2.4** Add message versioning support
- [ ] **2.5** Build type-safe message dispatch

### Phase 3: Supervision & Fault Tolerance
- [ ] **3.1** Implement supervision strategies (Restart, Stop, Escalate)
- [ ] **3.2** Create actor lifecycle hooks (pre_start, post_stop)
- [ ] **3.3** Add death watch mechanism for actor monitoring
- [ ] **3.4** Build let-it-crash error handling
- [ ] **3.5** Implement actor restart with state recovery

### Phase 4: Distribution Layer
- [ ] **4.1** Kubernetes service discovery integration
- [ ] **4.2** Node membership and cluster management
- [ ] **4.3** Consistent hashing for URL-to-node assignment
- [ ] **4.4** Remote actor proxy implementation
- [ ] **4.5** Network transport layer (TCP/HTTP2)

### Phase 5: Data Access Layer
- [ ] **5.1** Abstract storage traits (`Repository<T>`, `Cache<K,V>`)
- [ ] **5.2** PostgreSQL adapter implementation
- [ ] **5.3** Redis adapter for actor state persistence
- [ ] **5.4** URL deduplication service with ACID guarantees
- [ ] **5.5** Storage configuration and connection pooling

### Phase 6: Crawler-Specific Actors
- [ ] **6.1** `CrawlManager` actor for coordination
- [ ] **6.2** `URLFrontier` actor for queue management
- [ ] **6.3** `CrawlerActor` ephemeral implementation
- [ ] **6.4** `PolitenessManager` for domain rate limiting
- [ ] **6.5** `ResultCollector` for crawled content handling

### Phase 7: Performance & Production
- [ ] **7.1** Actor metrics and monitoring
- [ ] **7.2** Backpressure and flow control
- [ ] **7.3** Configuration management
- [ ] **7.4** Graceful shutdown and cleanup
- [ ] **7.5** Load testing and benchmarking

## Technical Specifications

### Core Dependencies
```toml
# Phase 1 Dependencies
tokio = { version = "1.0", features = ["full"] }
async-trait = "0.1"
uuid = { version = "1.0", features = ["v4"] }
thiserror = "2.0"
tracing = "0.1"
futures = "0.3"          # For ask pattern futures
syn = "2.0"              # For derive macros
quote = "1.0"            # For derive macros
proc-macro2 = "1.0"      # For derive macros

# Phase 2+ Dependencies
prost = "0.12"           # Protocol Buffers
sqlx = "0.7"             # PostgreSQL async driver
redis = "0.24"           # Redis async client
kube = "0.87"            # Kubernetes client
consistent-hash = "0.1"  # Consistent hashing
```

### Storage Schema
```sql
-- URL deduplication and frontier
CREATE TABLE urls (
    url_hash VARCHAR(64) PRIMARY KEY,
    url TEXT UNIQUE NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW()
);

-- Actor state persistence
CREATE TABLE actor_state (
    actor_id VARCHAR(128) PRIMARY KEY,
    node_id VARCHAR(64) NOT NULL,
    state_data JSONB,
    updated_at TIMESTAMP DEFAULT NOW()
);
```

### Phase 1 Implementation Examples

#### Actor Derive Macros (1.6)
```rust
// Auto-generates message enum and dispatch
#[actor(String, u32, GetStatus)]
#[derive(Default)]
struct EchoActor {
    count: u32,
}

// Generated automatically:
// enum EchoActorMsg { String(String), U32(u32), GetStatus(GetStatus) }

impl Receive<String> for EchoActor {
    type Msg = EchoActorMsg;
    fn receive(&mut self, ctx: &Context<Self::Msg>, msg: String, sender: Sender) {
        println!("Received: {}", msg);
    }
}
```

#### Actor Factory System (1.7)
```rust
// Simple factory
let echo = system.actor_of::<EchoActor>("echo").unwrap();

// Factory with arguments
let user = system.actor_of_args::<UserActor, _>("user", "john".to_string()).unwrap();

// Props-based creation
let worker = system.actor_of_props(
    Props::new::<WorkerActor>()
        .with_mailbox_size(5000)
        .with_dispatcher("worker-pool"),
    "worker"
).unwrap();
```

#### Ask Pattern (1.8)
```rust
use futures::executor::block_on;

// Send message and await response
let future = ask(&system, &actor, GetStatus, Duration::from_secs(5));
let response: Status = block_on(future)?;

// Or with async/await
let response = ask(&system, &actor, GetStatus, Duration::from_secs(5)).await?;
```

#### Message Transform & Dispatch (1.9)
```rust
// Type-safe message handling
impl Receive<Command> for ProcessorActor {
    type Msg = ProcessorActorMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, cmd: Command, sender: Sender) {
        match cmd {
            Command::Process(data) => self.process(data),
            Command::Stop => ctx.stop(ctx.myself()),
        }
    }
}

// Transform pattern for complex flows
transform! {
    receive_pipeline[HttpRequest] -> ProcessorActor {
        validate -> ValidatedRequest,
        process -> ProcessedResult,
        respond -> HttpResponse
    }
}
```

#### Test Kit (1.10)
```rust
use aktor_testkit::*;

#[test]
fn test_echo_actor() {
    let system = TestActorSystem::new();
    let probe = TestProbe::new(&system);

    let echo = system.actor_of::<EchoActor>("echo").unwrap();

    echo.tell("hello".to_string(), Some(probe.actor_ref()));

    probe.expect_msg_eq("hello");
    probe.expect_no_msg(Duration::from_millis(100));
}
```

#### Advanced Interaction Patterns (1.13)
```rust
// Response Aggregator - collect from multiple actors
let results = ResponseAggregator::new()
    .ask_all(&[worker1, worker2, worker3], ProcessData(data))
    .timeout(Duration::from_secs(10))
    .collect::<Vec<ProcessResult>>()
    .await?;

// Tail Chopping - race multiple actors for fastest response
let result = tail_chop()
    .ask_all(&[fast_actor, backup_actor], GetValue)
    .first_response()
    .timeout(Duration::from_millis(500))
    .await?;

// Adapted Response - convert response types
ctx.ask_adapted(&child, |response: ChildResponse| ParentMsg::ChildDone(response)).await?;
```

#### Behavior Management (1.14)
```rust
impl Actor for StateMachine {
    fn receive(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg) -> Behavior {
        match (self.state, msg) {
            (State::Idle, StartMsg) => {
                self.state = State::Working;
                Behavior::Same
            }
            (State::Working, CompleteMsg) => {
                self.state = State::Idle;
                Behavior::Same
            }
            _ => Behavior::Unhandled
        }
    }
}

// Dynamic behavior switching
ctx.become(working_behavior);  // Switch to new behavior
ctx.unbecome();               // Revert to previous behavior
```

#### Routers and Dispatchers (1.15)
```rust
// Round-robin router
let router = system.router()
    .round_robin()
    .with_routees(5)
    .of::<WorkerActor>("worker-pool");

// Broadcast router
let broadcaster = system.router()
    .broadcast()
    .with_routees(&[actor1, actor2, actor3]);

// Custom dispatcher for blocking operations
let blocking_actor = system.actor_of_props(
    Props::new::<IoActor>()
        .with_dispatcher("blocking-io-dispatcher"),
    "io-worker"
).unwrap();
```

#### Timers and Scheduling (1.16)
```rust
impl Actor for PeriodicActor {
    fn pre_start(&mut self, ctx: &Context<Self::Msg>) -> Result<(), ActorError> {
        // Schedule recurring task
        ctx.timer().schedule_repeatedly(
            Duration::ZERO,           // initial delay
            Duration::from_secs(30),  // interval
            ctx.myself(),
            PeriodicTask
        );

        // Schedule one-time timeout
        ctx.timer().schedule_once(
            Duration::from_secs(60),
            ctx.myself(),
            TimeoutMsg
        );

        Ok(())
    }
}
```

### Actor Message Examples
```rust
// Generated from crawl.proto
#[derive(Clone, PartialEq, prost::Message)]
pub struct CrawlUrlRequest {
    #[prost(string, tag = "1")]
    pub url: String,
    #[prost(int32, tag = "2")]
    pub depth: i32,
    #[prost(int32, tag = "3")]
    pub priority: i32,
}
```

## Success Criteria
- [ ] Spawn 1M+ ephemeral crawler actors
- [ ] Sub-millisecond local actor message delivery
- [ ] Zero-downtime node addition/removal
- [ ] 99.9% URL deduplication accuracy
- [ ] Seamless storage backend switching