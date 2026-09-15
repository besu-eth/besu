# Reference Test Execution and Tracing Guide

This document explains how to run Ethereum reference tests in Besu and how to enable JSON tracing during test and block execution. This is useful for debugging EVM behavior, inspecting opcode execution, and verifying correctness against official test vectors.

## Running the Reference Tests

To run the Ethereum reference tests included in the Besu codebase, use the following Gradle task:

```bash
./gradlew referenceTests
```

This will execute the available test suites (such as GeneralStateTests and execution-spec-tests) and validate Besu's EVM behavior.

> **Note:**
> - Out-of-memory (OOM) errors are common due to the size and number of tests. You may need to increase the heap size using `-Xmx` (e.g., `./gradlew referenceTests -Dorg.gradle.jvmargs="-Xmx8g"`)

## Filtering Execution Spec Tests by Hardfork or EIP

Execution-spec-tests are generated with class names that reflect their hardfork and EIP directory structure. This allows targeted test execution using standard Gradle `--tests` filters.

### By hardfork

```bash
# Run all Prague execution spec tests (blockchain + state)
./gradlew referenceTests --tests "*ExecutionSpec*_prague_*"

# Run only Amsterdam state tests
./gradlew referenceTests --tests "*ExecutionSpecStateTest_amsterdam_*"

# Run all Cancun blockchain tests
./gradlew referenceTests --tests "*ExecutionSpecBlockchainTest_cancun_*"
```

### By EIP

```bash
# Run only EIP-7702 tests
./gradlew referenceTests --tests "*eip7702*"

# Run only EIP-4844 blob tests
./gradlew referenceTests --tests "*eip4844*"
```

### By hardfork + EIP

```bash
# Run Prague EIP-2537 BLS precompile tests specifically
./gradlew referenceTests --tests "*_prague_eip2537_*"
```

### Static (legacy) tests

```bash
# Run all static legacy tests
./gradlew referenceTests --tests "*ExecutionSpec*_static_*"

# Run a specific static test category
./gradlew referenceTests --tests "*_static_stCreate2_*"
```

### Generated class name format

Test classes follow the pattern:
```
ExecutionSpec{Blockchain,State}Test_{hardfork}_{eip_or_topic}_{batch_index}
```

For example:
- `ExecutionSpecBlockchainTest_prague_eip7702_set_code_tx_0`
- `ExecutionSpecStateTest_cancun_eip4844_blobs_2`
- `ExecutionSpecBlockchainTest_static_stCreate2_1`
- `ExecutionSpecBlockchainTest_frontier_opcodes_0`

> **Note:** These hardfork/EIP filters apply only to execution-spec-tests. The legacy `GeneralStateReferenceTest` and `BlockchainReferenceTest` classes still use sequential numbering. For those, use the runtime system properties `test.ethereum.state.eips` and `test.ethereum.include` instead.

## Devnet (pre-release) Reference Tests

`referenceTests` runs the released fixtures. The pre-release devnet fixtures — the tarball pinned by
`devnetTarConfig` in `ethereum/referencetests/build.gradle` — are a separate source set with its own
task:

```bash
./gradlew referenceTestsDevnet
```

It takes the same `--tests` filters as `referenceTests`, over classes named
`ExecutionSpecDevnet{Blockchain,State}Test_{hardfork}_{eip_or_topic}_{batch_index}`:

```bash
./gradlew referenceTestsDevnet --tests "*_amsterdam_*"
./gradlew referenceTestsDevnet --tests "*_amsterdam_eip7928_*"
```

This is the only runner that drives the devnet fixtures through the JUnit reference-test harness, so
it is the one to reach for when you need that harness's tracing (see [Enabling JSON
Tracing](#enabling-json-tracing)) — the evmtool runners below do not go through it.

> `referenceTestsDevnet` is **not** a CI gate and is frequently red on `main` while a devnet is in
> flight. Get a baseline on `main` before reading a failure on your branch as your own regression.

## Hive-Equivalent Fixture Runners (evmtool)

The `referenceTests` task above drives **block import** and **state transition** directly. It never
touches the Engine API, so payload schema, JSON-RPC error codes, fork support and the blob schedule
have no coverage there at all. Upstream, that gap is filled by hive's `consume-engine` simulator —
which spins up a Besu container per fixture group and takes hours.

`evmtool` replays the same fixture trees through the same Besu code, in process, in minutes:

| hive | Gradle task | Besu code path |
|------|-------------|----------------|
| `--sim ethereum/eels/consume-engine` | `consumeEngineTests` | `engine_newPayloadVX` + `engine_forkchoiceUpdatedVX` over `blockchain_tests_engine` |
| `--sim ethereum/eels/consume-rlp` | `consumeRlpTests` | RLP block import over `blockchain_tests` |

Both reuse the same devnet fixture download/extract as the reference tests (`extractDevnetFixtures`
— no separate download) and **fail the build on any fixture failure**.

```bash
# Full consume-engine equivalent
./gradlew consumeEngineTests

# Full consume-rlp equivalent
./gradlew consumeRlpTests

# Both, in one command
./gradlew consumeEngineTests consumeRlpTests
```

`consume-rlp` is not redundant with `consume-engine`: `blockchain_tests_engine` has no pre-merge
fork groups, so `for_frontier`, `for_homestead`, `for_tangerinewhistle`, `for_spuriousdragon`,
`for_byzantium`, `for_constantinoplefix`, `for_istanbul`, `for_berlin` and `for_london` exist only
in `blockchain_tests`.

### Translating a hive invocation

The properties are named after the hive flags they stand in for, and are shared by both tasks, so
one command covers both simulators.

| hive flag | Gradle property |
|-----------|-----------------|
| `--sim.limit=<regex>` (with `--sim.limit.exact=false`) | `-PsimLimit=<regex>`, taken verbatim |
| `--sim.parallelism=N` | `-PsimParallelism=N` (default: available processors) |
| *no equivalent* | `-PsimPath=<subdir>` — scope to one fixture subdirectory, e.g. `for_amsterdam` |

So this hive run:

```bash
./hive --sim ethereum/eels/consume-engine --client besu --sim.parallelism=6 \
  --sim.limit='.*(7928|8282).*' --sim.limit.exact=false ...
```

becomes:

```bash
./gradlew consumeEngineTests -PsimParallelism=6 -PsimLimit='.*(7928|8282).*'
```

and swapping the task for `consumeRlpTests` covers `--sim ethereum/eels/consume-rlp`. Quote the
pattern — the shell would otherwise glob it.

`-PsimPath` has no hive counterpart but is worth reaching for: scoping to a fork group is far
cheaper than filtering the whole tree, since a filter still has to read every fixture file.

```bash
./gradlew consumeEngineTests -PsimPath=for_amsterdam -PsimLimit='.*(7928|8282).*'
```

#### How `-PsimLimit` matches

The value is a hive regex and is passed to `evmtool`'s `--test-name-regex` **verbatim** — nothing is
rewritten, nothing is escaped, so a published `--sim.limit` can be copied character for character,
escapes and all. Match semantics agree with hive's too: the EELS simulators apply the filter with
Python's `re.match`, which is anchored at the start of the pytest node id, open at the end and
case-sensitive, and `--test-name-regex` does the same.

That is why the leading and trailing `.*` in the conventional `.*(7928|8282).*` matter: without the
leading one the pattern would have to match from the first character of the node id.

A malformed pattern is compiled before any fixture is read, so it fails immediately with exit 1
rather than silently running nothing. An empty run is an error too: a run that executed no test
never reports success.

The `[`, `]`, `(` and `)` common in pytest node ids are regex metacharacters. `(` and `)` are what
make the `(a|b|c)` alternation work; escape them when you want them literal:

```bash
# WRONG: '[' opens a character class -> rejected before any test runs
./gradlew consumeEngineTests -PsimLimit='.*[fork_Amsterdam.*'
#   Invalid --test-name-regex pattern: Unclosed character class. …

# RIGHT
./gradlew consumeEngineTests -PsimLimit='.*\[fork_Amsterdam.*'
```

`evmtool` also has a `--test-name` option, which is the friendlier form used when driving the binary
by hand: a bare substring, or a `*`/`?` glob in which `.` is a literal. The Gradle tasks never use
it — `-PsimLimit` is always a regex.

> `stateTestsDevnet` used to take `-PstateTestFilter`, `-PstateTestPath` and `-PstateTestWorkers`.
> Those names are gone; the task now shares `-PsimLimit` / `-PsimPath` / `-PsimParallelism` with the
> other runners. Passing an old one fails the build rather than running the whole tree unfiltered.
> Note that `-PsimLimit` is a regex where `-PstateTestFilter` was a substring or a `*?`-glob.

### Reproducing a published hive run

The devnet hive runs published at [hive.ethpandaops.io](https://hive.ethpandaops.io) each pin a
`--sim.limit`. Four tasks carry those filters so that reproducing a run is a task name rather than a
regex copied out of a web UI:

| Task | Mirrors |
|------|---------|
| `consumeEngineTestsGlamsterdam` | the `glamsterdam` group's `consume-engine` run |
| `consumeRlpTestsGlamsterdam` | the same filter, `consume-rlp` |
| `consumeEngineTestsGlamsterdamQuick` | the `glamsterdam-quick` group's `consume-engine` run |
| `consumeRlpTestsGlamsterdamQuick` | the same filter, `consume-rlp` |

```bash
./gradlew consumeEngineTestsGlamsterdam        # the full published sweep
./gradlew consumeEngineTestsGlamsterdamQuick   # the EIP-scoped sweep
```

The `Glamsterdam` filter selects by fork and currently spans more than one, so the task is not an
Amsterdam-only run. The `GlamsterdamQuick` filter selects by EIP number instead, which cuts across
forks. Read the constants for what each covers today.

A preset ignores `-PsimLimit`: overriding half of it would report a number against a scope nobody
can reconstruct. `-PsimParallelism` still applies. Use the plain `consumeEngineTests` /
`consumeRlpTests` tasks when you want your own filter.

> When comparing against the hive UI, note that the figure it shows most prominently is the *pass*
> count, not the number of tests run.

#### Keeping the presets current

Fork names change with every devnet (a transition fork is renamed, added or dropped) and the quick
run's EIP list is edited as EIPs are scheduled in or out. A filter left behind does not fail — it
selects the old set and goes green — so re-read it from the newest published run rather than
assuming. Both live in one place, as `GLAMSTERDAM_SIM_LIMIT` and `GLAMSTERDAM_QUICK_SIM_LIMIT` at
the top of `ethereum/evmtool/build.gradle`.

The suite JSON records the exact invocation under `runMetadata.hiveCommand`. `listing.jsonl` is not
in chronological order, so sort by `start` rather than taking the last line:

```bash
GROUP=glamsterdam          # or glamsterdam-quick
SIM=eels/consume-engine    # or eels/consume-rlp

FILE=$(curl -sS "https://hive.ethpandaops.io/$GROUP/listing.jsonl" \
  | jq -rs --arg sim "$SIM" 'map(select(.name==$sim)) | sort_by(.start) | last | .fileName')

# The suite JSON is far too large to fetch whole; runMetadata precedes testCases, so a range
# request over the head of it is enough
curl -sS --range 0-65535 "https://hive.ethpandaops.io/$GROUP/results/$FILE" \
  | grep -oE '"(--sim\.limit=[^"]*|fixtures=[^"]*)"'
```

That prints both things worth checking:

- **`--sim.limit=…`** — copy it verbatim into the matching constant. It needs no editing: the tasks
  take a hive regex exactly as written (see [How `-PsimLimit` matches](#how--psimlimit-matches)).
- **`fixtures=…`** — the tarball the run used. If its version differs from `devnetTarConfig` the two
  are not selecting from the same test set and counts will not line up; see
  [Fixture version](#fixture-version).

After changing a constant, run the task and sanity-check the test count against the run you copied
from. A filter that matches nothing fails the build rather than reporting success, but a filter that
matches the *wrong* set will happily go green.

### Worked example

Against the pinned fixtures, scoped to one fork group and filtered to a set of EIPs:

```bash
L='.*(7928|8282).*'
./gradlew consumeEngineTests -PsimPath=for_amsterdam -PsimLimit="$L" -PsimParallelism=12
./gradlew consumeRlpTests    -PsimPath=for_amsterdam -PsimLimit="$L" -PsimParallelism=12
```

Running both is worth the extra minute: where they disagree, the difference is Engine API behaviour
rather than block validity — a payload the engine should have rejected with a JSON-RPC error code,
or an `INVALID` whose validation error does not map to the exception the fixture names. Those are
exactly the failures neither `consumeRlpTests` nor `referenceTests` can see, and they are the reason
the engine runner exists.

### Fixture version

Both tasks run against the tarball pinned by `devnetTarConfig` in
`ethereum/referencetests/build.gradle`. A hive run pins its own via `--sim.buildarg fixtures=<url>`,
so check the two match before comparing results. To move the pinned version:

1. Update `version` in the `devnetTarConfig` dependency.
2. Run `./gradlew --write-verification-metadata sha256` — Besu uses dependency verification, and
   without a matching checksum in `gradle/verification-metadata.xml` the extract fails outright.
3. Commit both together.

To try a different tarball without repinning, extract it yourself and point the binary at it (see
below).

### What these tasks do not cover

`state_tests` are the EVM/state-transition-only slice, consumed by no hive simulator and by neither
task above. `stateTestsDevnet` runs them, taking the same `-PsimLimit` / `-PsimParallelism` /
`-PsimPath` properties:

```bash
./gradlew stateTestsDevnet -PsimPath=for_amsterdam
```

Fixture files that cannot be read as a test of the expected kind are reported separately under
"Unreadable" and do **not** count as failures — a fixture this harness cannot build says nothing
about Besu, and counting it as a failure would make a fixture format change look like a regression.

### Running the binary directly

For an ad-hoc run against any fixtures directory — a tarball you extracted yourself, or a single
file — build the `evm` binary once and call the subcommands:

```bash
./gradlew :ethereum:evmtool:installDist
EVM=ethereum/evmtool/build/install/evmtool/bin/evmtool

$EVM engine-test --workers 8 <path-to>/blockchain_tests_engine/    # consume-engine
$EVM block-test  --workers 8 <path-to>/blockchain_tests/           # consume-rlp
```

A directory argument is walked recursively and spread over `--workers` workers. `--test-name-regex`
is the raw form of `-PsimLimit` and takes the same expression (`.*(7928|8282).*`); `--test-name` is
the glob form described above. `--json-array` emits machine-readable results (`[{name, pass, fork,
lastBlockHash, error}]`) and nothing else, so the exit code is what reports an empty or failed run.

A single fixture file can also be piped in as `stdin`, which all three subcommands accept:

```bash
$EVM engine-test stdin < <path-to>/one_fixture.json
```

`engine-test` prints failures and a final summary only; `--verbose` adds a line per test.
`block-test` logs every imported block, so pipe through `grep -v 'Imported in'` for a quiet run.

> The Gradle-extracted fixtures live at
> `ethereum/referencetests/build/execution-spec-devnet-tests/fixtures/`, so you can point the binary
> there after running `extractDevnetFixtures` once.

> **Tip:** if a verbose run makes the terminal flicker (Gradle's animated console repainting as
> output streams), add `--console=plain`.

## Enabling JSON Tracing

Besu supports detailed opcode-level JSON tracing. You can enable it using either a JVM system property or an environment variable.

### Option 1: JVM System Property

```bash
-Dbesu.debug.traceBlocks=true
```

### Option 2: Environment Variable

```bash
export BESU_TRACE_BLOCKS=true
```

This enables a fallback implementation of `BlockAwareOperationTracer` if no plugin is configured. The default tracer used is `BlockAwareJsonTracer`.

JSON trace output does not appear in the console. To view it, open the associated Gradle test report (usually located in `build/reports/tests/test/index.html`) and find the specific test case output.

## Trace Contents

When enabled, tracing includes:

- Opcode execution and names
- Stack state
- Gas remaining and gas cost
- Memory size
- Precompile execution
- Contract creation and call frames
- Transaction lifecycle events (start, prepare, end)
- Exceptional halts

Each traced operation emits structured JSON data representing the EVM state at that point.

## Output Format

The tracer prints a complete JSON trace of each block’s execution to standard output at the end of the block:

```
==== JSON Trace for Block <BLOCK_NUMBER> (<BLOCK_HASH>) ====
<trace entries>
```

Example:

```json
{
  "pc": 0,
  "op": "0x60",
  "opName": "PUSH1",
  "gas": 999999,
  "gasCost": 3,
  "stack": [],
  "memSize": 0,
  "depth": 1,
  "refund": 0
}
```

## Tracer Implementation

The tracer is implemented in:

```
org.hyperledger.besu.ethereum.mainnet.BlockAwareJsonTracer
```

It uses a `StringWriter` and a `StandardJsonTracer` to collect and format execution traces. Output is flushed during the `traceEndBlock(...)` callback.

The `BlockAwareJsonTracer` is enabled automatically when no plugin provides a custom tracer and one of the tracing flags is set:

```java
if (Boolean.getBoolean("besu.debug.traceBlocks")
    || "true".equalsIgnoreCase(System.getenv("BESU_TRACE_BLOCKS"))) {
  return new BlockAwareJsonTracer();
}
```

## Notes

- Tracing is for debugging purposes only and should not be enabled in production environments.
- Trace output can become large, especially for blocks with many transactions.
- Tracing does not affect EVM execution semantics.

## Resources

- [Ethereum Execution Spec Tests (ethereum/execution-spec-tests)](https://github.com/ethereum/execution-spec-tests)
- [Ethereum Reference Tests (ethereum/tests)](https://github.com/ethereum/tests)
- [EVM Opcodes Reference](https://www.evm.codes/)