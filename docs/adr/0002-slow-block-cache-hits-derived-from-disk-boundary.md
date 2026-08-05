# Slow-block cache hits are derived, not counted: total reads minus RocksDB-boundary reads

Besu has several stacked read caches (accumulator maps, BAL overlay, process-wide code
cache, optional cross-block cache, RocksDB block cache), instrumenting each per block is
invasive, and the reference PR showed per-layer counting lands at the wrong levels. We
decided `cache.{account,storage,code}.misses` counts reads that cross the RocksDB (JNI)
boundary — via a process-global storage decorator with per-segment counters attributed by
reader thread pool — and `hits` is derived as total logical reads (counted by an accumulator
read observer) minus those misses. RocksDB's internal block cache is deliberately ignored: a
read served by it still "required DB I/O" per the spec's definition, which is also why
prefetch warming does not convert execution reads into hits. Reads from the prefetcher, the
background state-root job, and non-block threads (RPC) are excluded from misses by thread
attribution.

## Consequences

- Per-layer cache effectiveness is not visible in the slow-block output; only the flattened
  application-level story is. That matches the cross-client spec's hit/miss definitions.
- If instrumentation drifts (a read path missed by the observer), derived hits can go
  negative; the implementation clamps to zero and logs, treating it as an invariant breach.
