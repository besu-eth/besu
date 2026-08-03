# Slow-block timing fields report critical-path wall-clock; phases do not sum to total

The unified SlowBlock metrics spec defines `timing.*` fields but is silent on parallel
execution, and under BAL Besu executes transactions and computes the state root
concurrently — the phases genuinely overlap. We decided `execution_ms`, `state_hash_ms`,
`commit_ms` and `total_ms` report wall-clock durations as observed on the import thread
(`state_hash_ms` is the *wait* for the background root, often ~0), while `state_read_ms` is
per-read time summed across worker threads, cache time included. Consequently the phase
fields deliberately do not sum to `total_ms` — that is honest accounting for a parallel
client, not a bug. The real background hashing cost is reported separately in the Besu
extension section (`bal.state_hash_background_ms`).

## Considered Options

- Thread-summed everywhere: internally consistent, but `total_ms` would disagree with the
  wall-clock import time operators actually care about.
- Serialized attribution (each instant assigned to exactly one phase so they sum): tidy
  arithmetic that hides parallelism entirely and makes `state_hash_ms` meaningless.
