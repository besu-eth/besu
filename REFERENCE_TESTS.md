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

## Devnet / Pre-release Execution Spec Tests

In addition to the stable execution-spec-tests fixtures, Besu supports a second set of **pre-release (devnet) fixtures** from upstream. These contain tests for upcoming hardforks (e.g., Amsterdam).

### Running devnet tests

```bash
# Run all devnet/pre-release reference tests
./gradlew referenceTestsDevnet

# Run only Amsterdam devnet tests
./gradlew referenceTestsDevnet --tests "*_amsterdam_*"

# Run both stable + devnet
./gradlew referenceTests referenceTestsDevnet
```

The default `referenceTests` task excludes devnet tests, so CI is unaffected.

### Generated class name format

Devnet test classes follow the same pattern as stable ones, but with an `ExecutionSpecDevnet` prefix:

```
ExecutionSpecDevnet{Blockchain,State}Test_{hardfork}_{eip_or_topic}_{batch_index}
```

### Bumping the pre-release version

1. Update the `version` in the `devnetTarConfig` dependency in `ethereum/referencetests/build.gradle`
2. Make any required infrastructure changes (new header fields, etc.)
3. Run `./gradlew --write-verification-metadata sha256` to update checksums
4. Commit all changes together

### Configuration

The devnet fixtures are resolved from the same GitHub Ivy repository as stable fixtures. The dependency is declared separately via the `devnetTarConfig` configuration in `ethereum/referencetests/build.gradle`.

## Fast Engine API & State Test Runners (evmtool)

The `referenceTests*` tasks above generate one JUnit test per fixture and exercise the **block-import** and **state-transition** code paths. In addition, `evmtool` provides two much faster runners that consume the execution-spec-tests fixtures directly (no per-fixture code generation), and crucially `engine-test` exercises the **real Engine API path** (`engine_newPayloadVX` + `engine_forkchoiceUpdatedVX`) — the same code hive's `consume-engine` simulator drives, but locally and in seconds instead of hours.

- **`engine-test`** consumes `blockchain_tests_engine` fixtures. These are a **superset of the blockchain tests for every post-merge fork** (same scenarios, via the Engine API), so for a post-merge devnet they cover everything the block-import path does, plus the Engine API validation on top (payload schema, error codes, blob schedule, strict exception matching).
- **`state-test`** consumes `state_tests` fixtures (the EVM/state-transition-only slice, not covered by `engine-test`).

> There is intentionally no devnet runner for the RLP `blockchain_tests`: on a post-merge devnet `engine-test` already covers those scenarios.

### Gradle tasks

Both tasks **reuse the same devnet fixture download/extract** as `referenceTestsDevnet` (the `extractDevnetFixtures` task — no separate download), run against the extracted fixtures, and **fail the build on any test failure**. By default only failures and a final summary are printed, so the console isn't flooded with per-test output.

```bash
# Run all devnet engine-API tests (blockchain_tests_engine) through the real Engine API
./gradlew :ethereum:evmtool:engineTestsDevnet

# Run all devnet state tests
./gradlew :ethereum:evmtool:stateTestsDevnet
```

Each task accepts optional `-P` properties:

| Property | Applies to | Meaning |
|----------|-----------|---------|
| `-PengineTestWorkers=N` / `-PstateTestWorkers=N` | engine / state | Parallel workers (default: available processors) |
| `-PengineTestPath=<subdir>` / `-PstateTestPath=<subdir>` | engine / state | Scope to a subdirectory, e.g. `for_amsterdam` |
| `-PengineTestFilter=<substr>` / `-PstateTestFilter=<substr>` | engine / state | Only run tests whose node id contains `<substr>` (a `*`/`?` glob is treated as a regex) |

```bash
# Only the Amsterdam-fork engine fixtures
./gradlew :ethereum:evmtool:engineTestsDevnet -PengineTestPath=for_amsterdam

# Hive consume-engine equivalent of --sim.limit '.*fork_(Amsterdam|BPO2ToAmsterdamAtTime15k|Osaka).*'
./gradlew :ethereum:evmtool:engineTestsDevnet \
  -PengineTestFilter='*fork_(Amsterdam|BPO2ToAmsterdamAtTime15k|Osaka)*'

# A subset of state tests, 8 workers
./gradlew :ethereum:evmtool:stateTestsDevnet -PstateTestPath=for_amsterdam -PstateTestWorkers=8
```

### Running the evmtool binary directly

For ad-hoc runs against any fixtures directory (not just the pinned devnet set), build the `evm` binary once and invoke the subcommands:

```bash
./gradlew :ethereum:evmtool:installDist
EVM=ethereum/evmtool/build/install/evmtool/bin/evmtool

# Engine API tests (file or directory; directories are walked recursively)
$EVM engine-test --workers 8 <path-to>/blockchain_tests_engine/

# State tests
$EVM state-test --workers 8 <path-to>/state_tests/
```

Shared flags: `--workers N`, `--run <substr-or-glob>` (alias `--test-name`), `--verbose` (print a line per test — off by default; only failures and a final summary are printed otherwise), and `--json-array` to emit machine-readable results (`[{name, pass, fork, lastBlockHash|stateRoot, error}]`). Both subcommands exit non-zero if any test fails.

> **Tip:** if a `--verbose` run makes the terminal flicker (Gradle's animated console repainting as output streams), add `--console=plain` to the Gradle invocation.

> The gradle-extracted fixtures used by the tasks above live at
> `ethereum/referencetests/build/execution-spec-devnet-tests/fixtures/{blockchain_tests_engine,state_tests}/`,
> so you can point the binary at that directory after running `referenceTestsDevnet` (or `extractDevnetFixtures`) once.

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