# Slow Block Tracer — BAL-first POC Implementation Plan

Sources: [requirements take 2](2026-07-31_slow-block-tracing-requirements-take2.md) ·
[meeting notes with Karim](2026-07-30_Slow-Block-Tracer-PR-Meeting-with-Karim.md) ·
reference PR [#10746](https://github.com/besu-eth/besu/pull/10746) (branch `slow-block-tracer`) ·
[unified spec](https://ethresear.ch/t/a-small-step-towards-data-driven-protocol-decisions-unified-slowblock-metrics-across-clients/23907) ·
glossary [CONTEXT.md](CONTEXT.md) · ADRs [0001](docs/adr/0001-slow-block-timing-critical-path-wall-clock.md),
[0002](docs/adr/0002-slow-block-cache-hits-derived-from-disk-boundary.md).

Scope (per requirements): **BAL-enabled (amsterdam) block import only**. Block production and
pre-amsterdam serial mode are later phases; no forward-compat implementation detail here.

## Decisions (grilling session 2026-07-31)

| # | Decision |
|---|---|
| 1 | `state_reads.*` = **total logical loads** (cache hits included). The requirements-doc example (`state_reads.accounts == cache.account.misses`) is internally inconsistent with the spec text and gets corrected. |
| 2 | `cache.misses` = reads crossing the RocksDB boundary; `hits` = total − misses, clamped ≥ 0 (ADR-0002). |
| 3 | Keep `unique` + `evm` sections (Besu extensions; not in the released spec). Constraint: **no per-op HashSet insertions** — that regression was measured in PR #10746. |
| 4 | Activation: per-block BAL check; auto-activates at the amsterdam fork; startup warning if the network never schedules amsterdam. Node always runs. |
| 5 | Timing: critical-path wall-clock; `state_read_ms` thread-summed, cache time included (ADR-0001). |
| 6 | One `bal` extension section: `state_hash_background_ms`, `tx_exec_background_ms`, `tx_result_wait_ms`, `replayed_txs`, `prefetch{duration_ms, accounts, storage_slots}`. |
| 7 | Total-read counting at an **accumulator read observer** (observer field, not subclass — inheritance is taken by `BonsaiBalWorldStateUpdateAccumulator`). |
| 8 | `evm.*` + `unique.contracts` via a **per-transaction recorder** replacing the hardcoded `NO_TRACING` on the parallel path; merged only for used background results. |
| 9 | Disk reads via a **global storage decorator with thread-pool attribution** (execution → misses; prefetch → prefetch stats; state-root/RPC → excluded). |
| 10 | `state_writes.*` = **write events** from the final BlockAccessList (per-tx change entries); deletes reported as net outcomes. `unique.accounts`/`unique.storage_slots` also from the BAL. |
| 11 | Wiring: public `--slow-block-threshold` → `BalConfiguration` accessor → direct per-block creation in `AbstractBlockProcessor`. Tracer classes in `ethereum/core` (fixes the evm-module Jackson `compileOnly` landmine). No plugin-api changes. |

## Key code facts the design rests on

- **Parallel path is invisible to block-level machinery.** `BalConcurrentTransactionProcessor.runTransaction`
  (`ethereum/core/.../mainnet/parallelization/BalConcurrentTransactionProcessor.java:155-205`) opens a
  throwaway overlaid world state, executes with `OperationTracer.NO_TRACING` (line 193), never commits.
  Writes return via `PartialBlockAccessView`; `getProcessingResult` (207-261) applies them to the block
  accumulator sequentially. `future.get()` at line 220 is the unbounded wait point.
- **BAL loses repeated-read info twice** (per-tx `AccessLocationTracker` sets; block-level
  `AccountBuilder.slotReads` HashSet, `BlockAccessList.java:308`), but **writes are per-transaction lists**
  (`slotWrites` `Map<slot, List<StorageChange>>`, `BlockAccessList.java:307,363-368`; balances/nonces/codes
  309-311, 376-386) — consensus-grade write-event history for free.
- **Accumulator hooks exist and are free to claim**: `onAccountValueLoaded`/`onCodeValueLoaded`/`onStorageValueLoaded`
  (`PathBasedWorldStateUpdateAccumulator.java:310-318`, fired at 335/340, 536, 574) — but they fire only on
  load-misses, and code/storage hooks fire *before* the lazy read runs (time the suppliers at 533-534 and
  563-572 instead; bracket the parent account load at 330).
- **Storage is a process singleton; no per-block seam.** Single wrap point:
  `KeyValueStorageProviderBuilder.java:62`. A decorator must also implement `SnappableKeyValueStorage` and
  wrap snapshots (unchecked downcast at `BonsaiSnapshotWorldStateKeyValueStorage.java:57-64`) and override
  `containsKey` + `startLowPriorityTransaction` (RocksDB overrides the latter for write throttling).
- **Prefetch (`BalPrefetcher.prefetch`, 75-79) only warms the RocksDB block cache** — results discarded,
  counts already computed for its own log (109-115). Prefetched keys cross JNI *again* during execution.
- **State root**: background compute submitted synchronously in `BalStateRootCommitterFactory.forBlock`
  (57-74) on a default 1-thread pool; `.timed()` at `AbstractBlockProcessor.java:242-246` measures the
  persist-time **wait**, not the work; real work is `BlockAccessListStateRootCalculator.computeAsync`
  (48-61). The parallel→serial fallback (`MainnetParallelBlockProcessor.java:152-162`) **re-enters
  `processBlock` and submits a second computation** — instrumentation must tolerate that.
- `traceEndBlock` fires **before** persist (`AbstractBlockProcessor.java:535-539`) → post-persist end hook
  needed (reference PR's `traceEndBlockPersist` pattern).
- Cross-block cache (`FlatDbCacheManager`) is **off by default and not tied to BAL** (hidden
  `--Xbonsai-cross-block-cache-enabled`) — meeting note corrected. If enabled, derived hits still work
  (it sits above RocksDB). Code reads bypass it entirely.

## Implementation steps

Each step compiles and is unit-testable on its own; roughly one commit each.

### 1. CLI + configuration
- `app/.../cli/options/BalConfigurationOptions.java`: add public `--slow-block-threshold` (Long, default
  `-1`), mapped in `toDomainObject()` (53-59). Non-hidden ⇒ add to `app/src/test/resources/everything_config.toml`
  (`BesuCommandTest` asserts presence).
- `ethereum/core/.../mainnet/BalConfiguration.java`: `@Value.Default long getSlowBlockThresholdMs() { return -1; }`
  (+ convenience `isSlowBlockTracingEnabled()`). Already plumbed to `AbstractBlockProcessor` field 92 —
  no new controller-builder setter, no `CommandTestAbstract` stub.
- `ConfigurationOverviewBuilder` line in the startup box (precedent: `setChainPruningEnabled`, 464-472).
- Startup warning when threshold ≥ 0 and the protocol schedule never activates a BAL fork (emit where the
  schedule is available during controller build).

### 2. Metrics model + tracer core (`ethereum/core`, new package `...ethereum.mainnet.slowblock`)
- `SlowBlockMetrics`: mutable per-block aggregate with `mergeTx(...)` for recorder/observer snapshots,
  BAL-derived setters, timing setters, and the derivation logic (hits = total − misses, clamp ≥ 0 + debug
  log on breach).
- `SlowBlockTracer implements OperationTracer` (from `evm`): used on the sequential path — system-call
  phases and replayed transactions — plus block lifecycle (`startBlock`, post-persist `endBlockPersist`
  with threshold gate).
- `SlowBlockJsonLogger`: Jackson `ObjectMapper`, dedicated `LoggerFactory.getLogger("SlowBlock")` at WARN,
  plain-format fallback on serialization failure. Port the JSON shape from the reference tracer, with the
  corrected `state_reads` semantics and the new `bal` section.

### 3. Read Observer
- New interface `AccumulatorReadObserver` (account/storage/code read callbacks + nanos for fall-through
  loads; code callback carries byte size).
- `PathBasedWorldStateUpdateAccumulator`: nullable field + setter; invoke at the three read entry points
  (`loadAccount` 320/323, `getCode` probe 531, `getStorageValueByStorageSlotKey` probe 554) so *every*
  logical read counts (hits and misses), and wrap the lazy suppliers (533-534, 563-572) / parent load (330)
  for `state_read_ms`. Null-check-only cost when disabled; no logic inside `loadAccount` beyond the calls.
- Install: on each per-tx world state's accumulator right after creation in
  `BalConcurrentTransactionProcessor.getWorldStateForTransaction` (82-99), and on the block accumulator in
  `AbstractBlockProcessor` when tracing. (Alternative install seam if accumulator access is awkward:
  a `withSlowBlockTracing` variant on `WorldStateQueryParams` → `applyBlockAccessListOverlay`,
  `BonsaiWorldState.java:114-119`.)

### 4. Per-transaction Recorder
- `SlowBlockTxRecorder implements OperationTracer`: plain int counters via the reference tracer's
  pattern-switch (`SLoadOperation`/`SStoreOperation`/`AbstractCallOperation`/`AbstractCreateOperation` in
  `tracePostExecution`), small per-tx `traceContextEnter` set for `unique.contracts`. **No stack peeks, no
  block-wide sets** — the PR's `tracePreExecution` slot tracking is not carried over (unique slots come
  from the BAL).
- `runTransaction` (line 193): pass recorder instead of `NO_TRACING` when enabled; time the execution body
  (`tx_exec_background_ms`, separating queue delay per `ParallelBlockTransactionProcessor.runAsyncBlock`
  63-77).
- `ParallelizedTransactionContext` (25-55): add recorder + observer snapshot + timing fields.
- `getProcessingResult`: time `future.get()` (220) → `tx_result_wait_ms`; merge context into
  `SlowBlockMetrics` **only when the background result is used**; failed attempts are discarded with their
  world state. Count replays in the `orElseGet` fallback (`MainnetParallelBlockProcessor.java:114`);
  replayed txs execute under the block-level `SlowBlockTracer` — no double counting.

### 5. Disk Read Decorator
- New `ReadMeteredSegmentedKeyValueStorage` implementing `SegmentedKeyValueStorage` +
  `SnappableKeyValueStorage`; `takeSnapshot()` returns a metered `SnappedKeyValueStorage` wrapper;
  override `containsKey` and `startLowPriorityTransaction` pass-throughs.
- Install conditionally at the `KeyValueStorageProviderBuilder.java:62` lambda.
- Attribution: mark `BlockProcessingExecutors` pools via their thread factories (cpu/io/stateRoot);
  import thread marked around `processBlock`. Per-segment `LongAdder`s × reader class; block window =
  deltas read on the import thread (single concurrent import). Mapping: `ACCOUNT_INFO_STATE`→accounts,
  `ACCOUNT_STORAGE_STORAGE`→storage_slots, `CODE_STORAGE`→code (+ bytes for code); `TRIE_BRANCH_STORAGE`
  and other segments excluded from misses; `ioExecutor` reads feed `bal.prefetch`; state-root/unknown
  threads ignored.

### 6. BAL derivation at block end
- After the BAL is finalized (`AbstractBlockProcessor.java:523` area): `unique.accounts` = touched
  addresses; `unique.storage_slots` = write-keys ∪ read-keys; `state_writes` = change-entry counts
  (accounts = balance+nonce+code entries; storage = Σ `slotWrites` list sizes; `code_bytes` from code
  changes); deletes = net outcomes (final zero-value slot writes; accounts removed as empty in
  `applyWritesFromPartialBlockAccessView`, 263-313).

### 7. Timing hooks — provisional pending #10804
- `total_ms`: nanoTime from processBlock start to after persist, end hook in a `finally` (PR pattern).
- `commit_ms` + `state_hash_ms` (wait): the reference PR's single-`persist`-call-with-nullable-tracer
  pattern (`PathBasedWorldState.persist`, computeRoot at 190-191), kept behind one small internal seam
  class so Karim's state-root refactor (#10804, still open) relands cleanly.
- `bal.state_hash_background_ms`: instrument the supplier in `BlockAccessListStateRootCalculator.computeAsync`
  (53-59); last-write-wins to tolerate the fallback double-submission.
- `bal.prefetch`: duration + counts surfaced from `BalPrefetcher` (already computed at 109-115).

### 8. Plugin-tracer coexistence
- In `AbstractBlockProcessor` tracer assembly (234-263): slow-block alone → single object (monomorphic,
  preserving the `NO_TRACING` downcast at 263); plugin import tracer also active → one 2-element composite
  (bimorphic worst case, goal 3). Parallelized txs remain invisible to plugin tracers — pre-existing
  behavior, unchanged and documented.

## Testing & verification

- **Unit**: recorder merge + discard-on-failure; observer counts including lazy-supplier timing; decorator
  thread attribution and snapshot wrapping (regression test that `takeSnapshot()` through the decorator
  doesn't CCE); BAL derivation (write events, net deletes, uniques); hits clamp; threshold gating; JSON
  shape.
- **Acceptance** (dev amsterdam genesis, reusing the PR's `SlowBlockTracerAcceptanceTest` +
  `dev/dev_amsterdam.json`): log appears at threshold 0, absent when disabled; invariants: hits ≥ 0,
  unique ≤ totals, `tx_count` correct, `cache.hits + cache.misses == state_reads` per type.
- **Exact-count accuracy**: crafted deterministic block (contract with known SLOAD/SSTORE/CALL pattern)
  asserting exact `evm.*`, `state_reads.*`, `unique.*`, `state_writes.*` — catches double counting from
  speculative execution or replay.
- **Perf**: block-import comparison baseline vs flag-disabled (dispatch/no-op cost ≈ 0) and disabled vs
  enabled (bounded overhead). Watch specifically for the two known regression shapes: megamorphic tracer
  dispatch and per-op set insertion.
- Pre-commit: `./gradlew spotlessApply` scoped to changed subprojects, then `./gradlew build`.
- Post-POC (deferred): BAL devnet soak alongside Nethermind/Geth comparing per-block output — feeds the
  Besu-vs-Nethermind disk-I/O investigation.

## Risks / open items

- **#10804** refactors persist/state-root — step 7 seams are the only contact surface; expect one rebase.
- **Fallback double-submission** of background root compute: handled last-write-wins; verify in the exact-count test with a forced fallback.
- `unique.contracts` counts context-enters (includes precompile targets?) — decide filter during implementation; flag in PR description.
- Engine-API nuance: newPayload persists trie logs while FCU persists state — affects what `commit_ms` captures per path (reference PR TODO; document, don't solve).
- EVMv2: the PR's stack-peek TODO dissolves (no `tracePreExecution` state reads anymore); the opcode
  pattern-switch still assumes v1 operation classes — note for the EVMv2 migration.
- Raw-vs-unique read comparability across clients (meeting open question): we emit both (totals + unique
  section); worth raising on the ethresear.ch thread along with parallel-client timing semantics (ADR-0001).

## Later phases (pointers only)

Pre-amsterdam serial mode (the reference PR's `StateAccessTracer` approach remains a viable donor),
block production, per-layer cache visibility if ever needed, spec feedback upstream.
