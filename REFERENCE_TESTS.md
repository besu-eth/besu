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

## Fast State Test Runner (evmtool)

The `referenceTests*` tasks above generate one JUnit test per fixture. In addition, `evmtool`'s `state-test` subcommand consumes the `state_tests` fixtures directly — no per-fixture code generation — which makes a whole fixture tree a seconds-long run rather than a build.

### Gradle task

`stateTestsDevnet` **reuses the same devnet fixture download/extract** as `referenceTestsDevnet` (the `extractDevnetFixtures` task — no separate download) and **fails the build on any test failure**.

```bash
./gradlew :ethereum:evmtool:stateTestsDevnet
```

| Property | Meaning |
|----------|---------|
| `-PstateTestWorkers=N` | Parallel workers (default: available processors) |
| `-PstateTestPath=<subdir>` | Scope to a subdirectory, e.g. `for_amsterdam` |
| `-PstateTestFilter=<substr>` | Only run tests whose node id matches — see [Filter syntax](#filter-syntax) |

```bash
./gradlew :ethereum:evmtool:stateTestsDevnet -PstateTestPath=for_amsterdam -PstateTestWorkers=8
```

### Filter syntax

`--run` (and the `-PstateTestFilter` property) accepts two forms:

- **No `*` or `?`** — a case-insensitive **substring** match against the node id.
- **Contains `*` or `?`** — a case-insensitive **regex** that must match the *whole* node id. `*` is rewritten to `.*`, `?` to any single character, and `.` is escaped to a literal (node ids contain `.py`). Everything else reaches `java.util.regex`, so alternation and character classes work.

Because the expression is a regex, the `[`, `]`, `(` and `)` that pytest node ids are full of are **metacharacters, not literals**. Escape them to match literally:

```bash
# WRONG: '[' opens a character class -> rejected before any test runs
$EVM state-test --run '*[fork_Amsterdam*' <fixtures>
#   Invalid --run/--test-name pattern '*[fork_Amsterdam*': Unclosed character class. …

# RIGHT: escape it, or just use the substring form
$EVM state-test --run '*\[fork_Amsterdam*' <fixtures>
$EVM state-test --run 'fork_Amsterdam' <fixtures>
```

The pattern is compiled once before any fixture is read, so a malformed expression is an immediate, explicit failure (exit 1) rather than a run that quietly executes nothing. A filter that compiles but matches no test is also an error — an empty run never reports success.

### Running the evmtool binary directly

```bash
./gradlew :ethereum:evmtool:installDist
EVM=./ethereum/evmtool/build/install/evmtool/bin/evmtool

$EVM state-test --workers 8 <path-to>/state_tests/
```

Flags: `--workers N`, `--run <substr-or-regex>`, `--json-array` to emit machine-readable results (`[{name, pass, fork, stateRoot, error}]`), and `--summary-only` to suppress the per-test JSON line that external tooling parses (which remains the default).

The subcommand exits non-zero if any test fails, if no test ran at all, or if the `--run` pattern is malformed. Fixture files that cannot be read as a state test are listed separately under "Unreadable" and do **not** count as failures — they say nothing about Besu. As of the currently pinned `tests-glamsterdam-devnet` fixtures that is 14 `state_tests` files (`test_bad_v_r_s`), which carry a pre-signed transaction in `post[].txbytes` rather than a `secretKey` that `StateTestVersionedTransaction` can sign with. The JUnit `referenceTestsDevnet` gate drops those same fixtures, silently.

> The gradle-extracted fixtures live at `ethereum/referencetests/build/execution-spec-devnet-tests/fixtures/state_tests/`, so you can point the binary there after running `referenceTestsDevnet` (or `extractDevnetFixtures`) once.

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