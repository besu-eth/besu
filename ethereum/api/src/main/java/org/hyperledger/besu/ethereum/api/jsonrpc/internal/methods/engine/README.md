# Engine API methods — architecture and how to extend it

This package implements the [Engine API](https://github.com/ethereum/execution-apis/tree/main/src/engine)
(`engine_*` JSON-RPC methods). It follows a strict versioning pattern, described here so that a new
version of a method — or a brand-new method series — is always added the same way. Read this before
changing anything in this package, in the related parameter/result classes, or in their tests.

## Architecture

Each method series (`engine_newPayloadV*`, `engine_getPayloadV*`, `engine_forkchoiceUpdatedV*`, ...)
is a **sealed class hierarchy mirroring the specification**: version N extends version N−1 and
overrides only what its spec version adds or changes.

- `EngineGetPayloadV1 permits EngineGetPayloadV2`, `... V5 permits EngineGetPayloadV6`, and the
  latest version is `final`. The same applies to the other series.
- All versions extend `ExecutionEngineJsonRpcMethod`, which owns the fork-window validation
  (`minSupportedFork` / `firstUnsupportedFork` constructor arguments, `validateForkSupported`,
  see also `ForkSupportHelper`). Concrete versions never check fork timestamps themselves.
- The JSON data structures are sealed hierarchies too, mirroring the spec versions:
  - request parameters in `..internal.parameters`: `ExecutionPayloadV1..V4`,
    `PayloadAttributesV1..V4`, `ForkchoiceStateV1`, ...
  - results in `..internal.results`: `EngineGetPayloadResultV1..V6`, `BlobsBundleV1/V2`,
    `PayloadStatusV1`, `ForkchoiceUpdatedResultV1`, ...
- A version class overrides narrow, protected hooks of its parent (e.g. `createResponse`,
  `createExecutionPayload`, `validateParameters`, `validatePayloadAttributes`) — it never
  re-implements the request flow.

### Registration and scheduling

`org.hyperledger.besu.ethereum.api.jsonrpc.methods.ExecutionEngineJsonRpcMethods` declares, per
series, which version is active in which fork window via the `VersionScheduler` DSL:

```java
VersionScheduler.startsFromBeginningUntil(EngineGetPayloadV1.class, SHANGHAI)
    .thenAlsoFromBeginning(EngineGetPayloadV2.class)
    .thenFrom(CANCUN, EngineGetPayloadV3.class)
    ...
    .thenFrom(AMSTERDAM, EngineGetPayloadV6.class)
    .build(...);
```

The scheduler instantiates each version with the right `(minSupportedFork, firstUnsupportedFork)`
pair derived from the chain. Method names live in the `RpcMethod` enum;
`engine_exchangeCapabilities` derives the advertised capability list automatically from every
`RpcMethod` entry starting with `engine_`, so there is no separate capabilities list to maintain.

## Test pattern (src/test, same package)

Tests are layered exactly like the production classes: `EngineGetPayloadV6Test extends
EngineGetPayloadV5Test extends ... V1Test`, so **every version class runs all the tests of the
previous versions plus its own**.

- The V1 test class owns the generic scenarios, written against protected hooks:
  `createMethodInstance()`, `getMinSupportedTimestamp()` / `getMaxSupportedTimestamp()` (or
  `getFirstUnsupportedTimestamp()`), payload/attribute builders, fixture customizers, and
  result-assertion hooks such as `assertPayloadResult(Object)` that each version extends with
  `super.assertPayloadResult(result); ...` plus its own checks.
- A version test class contains only: the `createMethodInstance()` override, the method-name test
  override, hook overrides, and tests for behavior introduced in that version.
- A scenario that stops applying at some version is guarded with
  `assumeTrue(someCapabilityHook())` on a boolean/Optional hook the later version overrides —
  **never** `@Disabled` and never an empty test override.
- Fork milestones in unit tests are the fake ones defined by `AbstractScheduledApiTest`
  (Paris=10, Shanghai=20, Cancun=30, Prague=50, Osaka=60, Amsterdam=70, ...).

Acceptance tests are fixture-driven, one directory per fork:
`acceptance-tests/tests/src/acceptanceTest/resources/jsonrpc/engine/<fork>/` containing a
`genesis.json` and `test-cases/` with JSON request/response pairs (see also the
`*AcceptanceTestHelper` classes under `acceptance-tests/.../acceptance/ethereum/`).

## Checklist: add version N+1 to an existing series

Use the commits that introduced the current latest version as the exemplar
(`git log --oneline -- <path to latest version class>`), then:

1. Un-`final` (or extend `permits`) the current latest method class; add
   `EngineFooVN+1 extends EngineFooVN` (`final`), overriding `getName()` and only the hooks the
   spec changes. The compiler enforces the rest of the chain.
2. If the payload/attributes/result shape changes, extend the corresponding sealed hierarchy in
   `..internal.parameters` / `..internal.results` the same way (update `permits` on the parent).
3. Add `ENGINE_FOO_VN+1("engine_fooVN+1")` to `RpcMethod` (this also advertises it via
   `engine_exchangeCapabilities`).
4. Extend the series' `VersionScheduler` chain in `ExecutionEngineJsonRpcMethods` with
   `.thenFrom(<ACTIVATION_FORK>, EngineFooVN+1.class)`.
5. Add `EngineFooVN+1Test extends EngineFooVNTest`: override `createMethodInstance()`, the
   method-name test, the fork-window hooks, and any builder/assertion hooks; add tests only for
   the new behavior. All inherited tests must pass unmodified.
6. Add/extend the acceptance-test fixtures for the activation fork.
7. Update `CHANGELOG.md`.

## Checklist: add a brand-new method series

1. Create `EngineBarV1 extends ExecutionEngineJsonRpcMethod` (sealed once V2 exists), passing the
   fork window to the super constructor; add its parameter/result classes as (future-sealed)
   hierarchies from the start.
2. Register it in `RpcMethod` and in `ExecutionEngineJsonRpcMethods` via `VersionScheduler`
   (`startsFrom(<FORK>, EngineBarV1.class)` or `alwaysActive(...)`).
3. Create `EngineBarV1Test` with all scenarios written against protected hooks from day one, so
   `EngineBarV2Test` can be layered on top later.
4. Add acceptance-test fixtures and a `CHANGELOG.md` entry.

## Definition of done

```
./gradlew :ethereum:api:test --tests "org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.*"
./gradlew :ethereum:api:spotlessApply
```

Both must pass, with no `@Disabled` tests introduced, before the change is complete.
