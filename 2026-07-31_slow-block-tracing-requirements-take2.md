# Slow Block Tracing Feature 

High level overview https://hackmd.io/dg7rizTyTXuCf2LSa2LsyQ
High level spec https://ethresear.ch/t/a-small-step-towards-data-driven-protocol-decisions-unified-slowblock-metrics-across-clients/23907

The goal of this feature is to output a log in the following format:

```json
{
  "level": "warn",
  "msg": "Slow block",
  "block": {
    "number": 6,
    "hash": "0x001d7cca37d1daf2936327be4c4ccd09ef92c963fadc3db8c8fe32429abefe88",
    "gas_used": 175488873,
    "tx_count": 95
  },
  "timing": {
    "execution_ms": 313.810666,
    "state_read_ms": 47.915243,
    "state_hash_ms": 42.970958,
    "commit_ms": 10.123541,
    "total_ms": 366.905165
  },
  "throughput": {
    "mgas_per_sec": 559.22
  },
  "state_reads": {
    "accounts": 91,
    "storage_slots": 9515,
    "code": 5,
    "code_bytes": 1630
  },
  "state_writes": {
    "accounts": 170,
    "storage_slots": 9515,
    "code": 0,
    "code_bytes": 0,
    "accounts_deleted": 2,
    "storage_slots_deleted": 0,
  },
  "cache": {
    "account": {
      "hits": 380,
      "misses": 91,
      "hit_rate": 80.68
    },
    "storage": {
      "hits": 6603,
      "misses": 9515,
      "hit_rate": 40.97
    },
    "code": {
      "hits": 5,
      "misses": 0,
      "hit_rate": 100.0
    }
  },
  "unique": {
    "accounts": 150,
    "storage_slots": 9510,
    "contracts": 94
  },
  "evm": {
    "sload": 68,
    "sstore": 9532,
    "calls": 22,
    "creates": 3
  }
}
```

Feature toggled by `--slow-block-threshold=<threshold in ms>`:
 - -1 is disabled, the default. 
 - 0 enables for all blocks.
 - >0 is ms threshold which only displays log if the timing/total_ms equals or exceeds it.

## Goals
 1. Fewer moving parts; comprehension over completeness.
 2. Straight-line code; less abstraction (though Karim's feedback is to avoid polluting some of the already complex code such as `PathBasedWorldStateUpdateAccumulator.loadAccount`.
 3. Monomorphic (or at worst bimorphic) EVM hot-path dispatch — a single tracer object reaches EVM.tracePreExecution / tracePostExecution, regardless of whether a plugin tracer is also active.

## Design
- This should be implemented as a "live tracer" (as opposed to via an RPC tracing endpoint).
- Integrate with OperationTracer interface, if appropriate.
- Phase 1 should only attempt to work with BAL enabled, i.e. with a glamsterdam network (amsterdam fork activated) so that we can reuse the BAL object with minimal modifications.
	- Activing amsterdam enables all the BAL features by default, see https://github.com/besu-eth/besu/blob/main/ethereum/core/src/main/java/org/hyperledger/besu/ethereum/mainnet/BalConfiguration.java
- Phase 1 should only attempt to work with block import, not block production, unless you get it for free.
- Future phases may introduce tracing for BAL disabled modes (pre or post glamsterdam) and for block production. Forward compatibility can be considered but I don't want to include implementation details in the plan. Would be happy for it to require a later refactor to get a more focussed solution for the BAL-enabled mode.

## Implementation Suggestions
- For state reads: since application caching an done at multiple levels including bonsai accumulator, code cache and cross-block cache (`FlatDbCacheManager`), the cleanest approach might be to record reads that cross over rocksdb boundary as cache misses (JNI boundary is significant but it may use rocksdb cache underneath, think we can disregard that db-level cache hit though). This could be achieved by decorating `RocksDBColumnarKeyValueStorage` and `RocksDBColumnarKeyValueSnapshot` when creating the world state. Then if we record the total state read attempts, we can derive cache hits as total - cacheMisses.
- BAL tracks unique reads/writes since it represents before/after block state. However, for this tracing mode, we could create a custom version of it that also tracks the intermediate reads/writes.
- `BalConcurrentTransactionProcessor.getWorldStateForTransaction` creates the world state using `withBalOverlay`. Above that you could have `withSlowBlockTracingEnabled` which modifies how the Bal overlay is created.
- For state metrics, could add decorator around `SegmentedKeyValueStorage` (or `RocksDBColumnarKeyValueStorage` and `RocksDBColumnarKeyValueSnapshot` specifically?)
- `PathBasedWorldStateProvider.getFullWorldStateFromCache` - could apply "slow block tracing" worldState here as decoration before or where `applyBlockAccessListOverlay` is called, so that we can use hooks/template pattern on the accumulator methods to avoid complicating the `PathBasedWorldStateUpdateAccumulator.loadAccount` method for example.
- Maybe could reuse `PathBasedWorldStateUpdateAccumulator.onAccountValueLoaded` for state metrics?

The plan should include automated testing and other verification steps.