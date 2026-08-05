## Pre-meeting notes shared by Karim

I also listed some metrics I think are importants , we can discuss it during the meeting  
  
1. StateRoot  
  
We already have some metrics via:  

```
final StateRootCommitter stateRootCommitter =
    protocolSpec
        .getStateRootCommitterFactory()
        .forBlock(protocolContext, blockHeader, blockAccessList, worldState.isStorageFrozen())
        .timed(blockProcessingMetrics.stateRootCalculationTimer());
```

With BAL  
  
Time measured is the wait duration when we. need the stateroot during the block processing. StateRoot computation starts in background immediately. When the block needs the StateRoot, if the background computation is already complete, time is zero. If not yet complete, we measure actual wait until it finishes.  
  
Something we can add is to capture the background computation time separately to understand how much work is happening in parallel.  
  
Without BAL  
  
Time measured equals the complete StateRoot generation time, launched synchronously without background processing.  
  
---  
  
2. Get Parallel Transaction result  
  
In `MainnetParallelBlockProcessor.getTransactionProcessingResult()` we have multiple scenarios.  
  
With BAL  
  
Background execution complete: futures[i].get()  
- Time from retrieval attempt until result is available  
- If already done, time is zero  
  
Background not ready  
- Wait for the result  
  
Without BAL  
  
Background execution complete: futures[i].get()  
- Time from retrieval attempt until result is available  
- If already done, time is zero  
  
Background not ready - replay triggered  
- Background computation did not finish in time  
- Time to re-execute transaction synchronously as fallback  
- Also record that replay was needed  
  
Total Time Calculation  

- BAL : background_execution_time + wait_time
- Without BAL :  replay_time if needed

for that you need to measure here :
```
TransactionProcessingResult transactionProcessingResult =
    getTransactionProcessingResult(
        preProcessingContext,
        blockProcessingContext,
        transactionUpdater,
        blobGasPrice,
        miningBeneficiary,
        transaction,
        i,
        blockHashLookup,
        transactionLocationTracker);
```
  
---  
  
3. Transaction execution in background  
  
In `ParallelBlockTransactionProcessor` we need to measure the background execution time. This measures the actual parallel processing duration in the background, independent of when the result is retrieved.  
  
```
futures[i] = CompletableFuture.supplyAsync(
 () -> runTransaction(...)
)
```
  
4. Prefetcher  
We can take some if this values  
- total duration of the prefetch
- number of accounts prefetched
- number of storage slots prefetched
- number of code entries prefetched

```
public CompletableFuture<Void> prefetch(
 final BonsaiWorldState worldState,
 final BlockAccessList blockAccessList,
 final Executor orchestrationExecutor,
 final Executor fetchExecutor)
```

## Key Outcomes

Simon and Karim reviewed Simon's stripped-down Slow Block Tracer PR, identifying significant design issues with how state read/write metrics and cache hit/miss tracking are currently implemented. The core decision reached is to pivot toward a Block Access List (BAL)-first approach for metric collection, using a decorator on `RocksDB KeyValueStorage` to track disk access directly, which is expected to simplify the implementation considerably. 12 Several metric accuracy issues were identified with the current accumulator-based approach, and Karim's upcoming state root refactoring (expected next week) will affect the persist/state hash timing logic. 3

## Decisions Made

- Cache hit/miss strategy: Instead of tracking cache levels individually, record total account reads at the accumulator level and disk reads at the RocksDB decorator level; derive cache hits as the difference. 45
- Pivot to BAL-first: Implement metrics using Block Access List infrastructure first, then adapt for pre-Amsterdam if still needed. 1
- Unique reads/writes limitation acknowledged: Current accumulator only tracks net state changes per block (not intermediate writes); BAL provides per-transaction granularity and is more accurate. 67
- Persist method: Keep a single persist call rather than two overrides; avoid leaking the slow block tracer into the Plugin API until the design stabilizes. 89
- State read timing: Intended to include cache time, not just disk time, per the spec. 10

## Key Technical Findings

- Megamorphic dispatch issue: Prior vibe-coded PR composed too many tracer interfaces, causing virtual dispatch overhead at the operation level and significant performance regression. 1112
- Code cache tracking is at the wrong level: Cache hit/miss for code reads is currently measured above the accumulator, missing the accumulator cache layer; needs to be tracked at both levels or replaced with the disk-read decorator approach. 1314
- Accumulator write accuracy: If an account's value is modified mid-block but reverts to its original value, the accumulator incorrectly counts it as a read, not a write. 715
- Cross-block cache: Sits just above RocksDB, shared across parallel executions and prefetch; not manageable via RocksDB's own block cache and is enabled by default with BAL. 161718
- Account deletion: Cannot occur at block level post-EIP changes; slot deletion (SSTORE to zero) is still valid and trackable. 1920

## Pending Confirmation

- Whether state read timing measurement is at the right stack level (risk of measuring cross-block cache time instead of pure disk access). 21
- How to cleanly wire the tracer into the world state without modifying the persist method — Karim's `prepareWorldState` refactor may provide a cleaner injection point. 2223
- Whether unique contract execution counts can be derived from BAL (read account + read code = likely execution) rather than tracked at opcode level. 24

## Open Questions

- Whether counting raw state reads (vs. unique reads) is meaningful for cross-client comparison, given each client's different caching optimizations. 2526
- Besu is observed to perform significantly more disk I/O than Nethermind; root cause unknown — this metric tooling may help diagnose. 27

## Action Items

- Simon: Explore BAL-first implementation; add decorator on `RocksDB KeyValueStorage` / `KeyValueSnapshot` to track disk reads; derive cache hits from total reads minus disk reads. 128
- Simon: Wait for Karim's state root refactoring PR before finalizing persist/state hash timing hooks - https://github.com/besu-eth/besu/pull/10804
- Simon: Review Miroslav's existing state root timing implementation for potential reuse. 29
- Karim: Merge state root refactoring PR (target: next week if tests pass). 3
- Simon & Karim: Schedule follow-up meeting after BAL exploration to reassess metric placement and accumulator design. 30