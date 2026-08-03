# Slow Block Tracing

Glossary for Besu's slow-block tracing feature: per-block execution metrics emitted as a
structured JSON log when block processing time meets a threshold, per the cross-client
unified SlowBlock metrics spec.

## Language

**Slow Block**:
A block whose total processing time (`total_ms`) meets or exceeds the configured threshold.
_Avoid_: bad block, heavy block

**State Read**:
A logical account, storage-slot, or code load during block execution, counted whether it is
served from any cache or from disk. `state_reads.*` totals these.
_Avoid_: disk read, unique read, state access (writes are separate)

**Cache Miss**:
A state read that crosses the RocksDB boundary, i.e. requires DB I/O. RocksDB's internal
block cache is deliberately disregarded.
_Avoid_: accumulator miss, per-layer miss

**Cache Hit**:
Derived, never counted directly: total state reads minus cache misses. Flattens all
application-level cache layers (accumulator, code cache, cross-block cache) into one number
per data type.
_Avoid_: per-layer hit counts

**Per-transaction Recorder**:
A lightweight per-transaction collector (opcode counters, context-enter set) that replaces
NO_TRACING on the parallel path. Merged into the block aggregate only if that transaction's
background result is used; discarded on failure or replay.
_Avoid_: shared tracer, thread-safe tracer

**Read Observer**:
A nullable observer on the world-state accumulator that counts total logical loads (accounts,
slots, code) at read entry points. Each per-transaction BAL accumulator gets its own; absent
entirely when tracing is disabled.
_Avoid_: accumulator subclass (inheritance is taken by BAL)

**Disk Read Decorator**:
The process-global storage decorator counting reads that cross the RocksDB boundary,
partitioned by reader thread pool: execution threads feed cache misses, prefetch threads feed
prefetch stats, background state-root and unknown threads are excluded.
_Avoid_: per-block storage wrapper (no such seam exists)

**Prefetch Read**:
A RocksDB-boundary read issued by the BAL prefetcher to warm RocksDB's block cache. Never a
cache miss: the same key still crosses the boundary again when execution reads it.
_Avoid_: counting prefetch into cache.misses
