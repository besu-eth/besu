# Engine API methods — architecture and how to extend it

This package implements the [Engine API](https://github.com/ethereum/execution-apis/tree/main/src/engine)
(`engine_*` JSON-RPC methods). It follows a strict versioning pattern, described here so that a new
version of a method — or a brand-new method series — is always added the same way. Read this before
changing anything in this package, in the related parameter/result classes, or in their tests.

## Migration status

Not every series has been migrated to the pattern below yet — the migration is landing series by
series, each in its own PR, to keep changes reviewable:

| Series | Constructor takes `ConstructorArguments`? | Registered via `VersionScheduler`? |
|---|---|---|
| `engine_forkchoiceUpdatedV*` | Yes | Yes |
| `engine_newPayloadV*` | Yes | Yes |
| `engine_getPayloadV*`, `engine_getBlobsV*`, `engine_getPayloadBodiesBy*`, `engine_exchangeCapabilities`, `engine_preparePayloadDebug`, `engine_getClientVersionV1`, `engine_exchangeTransitionConfigurationV1` | Not yet | Not yet |

The not-yet-migrated series still take their old flat constructor argument list (some via
`ExecutionEngineJsonRpcMethod`'s TRANSITIONAL SHIM constructors, kept until every series has moved
over) and are registered in `ExecutionEngineJsonRpcMethods.create()` via plain `Arrays.asList(...)`
plus manual `protocolSchedule.milestoneFor(FORK).isPresent()` checks. When migrating one of them,
follow the pattern below and update this table.

## Architecture (migrated series)

Each method series (`engine_forkchoiceUpdatedV*`, `engine_newPayloadV*` — see the migration status
table above) is a **sealed class hierarchy mirroring the specification**: version N extends version
N−1 and overrides only what its spec version adds or changes.

- `EngineForkchoiceUpdatedV1 permits EngineForkchoiceUpdatedV2`, `... V3 permits
  EngineForkchoiceUpdatedV4`; `EngineNewPayloadV1 permits EngineNewPayloadV2`, `... V4 permits
  EngineNewPayloadV5`; and the latest version of each is `final`. Future migrated series follow the
  same shape.
- All versions extend `ExecutionEngineJsonRpcMethod`, which owns the fork-window validation
  (`minSupportedFork` / `firstUnsupportedFork` constructor arguments, `validateForkSupported`,
  see also `ForkSupportHelper`). Concrete versions never check fork timestamps themselves.
- Migrated series take a single `ExecutionEngineJsonRpcMethod.ConstructorArguments` record (built
  via the generated `ConstructorArgumentsBuilder`) plus `(minSupportedFork, firstUnsupportedFork)`,
  instead of a bespoke positional argument list per series — this is what lets `VersionScheduler`
  build every version through one shared factory shape (see below). `ConstructorArguments` only
  carries the fields the currently-migrated series need — mark a field `@Nullable` if only some
  migrated series read it (e.g. `ethPeers`/`metricsSystem` are `engine_newPayloadV*`-only) — and
  extend it (and its builder) when migrating a series that needs a field it doesn't have yet.
- The JSON data structures relevant to migrated series are sealed hierarchies too, mirroring the
  spec versions: request parameters in `..internal.parameters` (`ExecutionPayloadV1..V4`,
  `NewPayloadRequestParametersV1..V3`, `ForkchoiceStateV1`, `PayloadAttributesV1..V4`), results in
  `..internal.results` (`PayloadStatusV1`, `ForkchoiceUpdatedResultV1`).
- A version class overrides narrow, protected hooks of its parent (e.g. `createResponse`,
  `createExecutionPayload`, `validateParameters`, `validatePayloadAttributes`) — it never
  re-implements the request flow.

### Registration and scheduling

`org.hyperledger.besu.ethereum.api.jsonrpc.methods.ExecutionEngineJsonRpcMethods` declares, per
migrated series, which version is active in which fork window via the `VersionScheduler` DSL, using
constructor references (not reflection — see `VersionScheduler.EngineMethodFactory`):

```java
VersionScheduler.startsFromBeginningUntil(EngineForkchoiceUpdatedV1::new, SHANGHAI)
    .thenAlsoFromBeginning(EngineForkchoiceUpdatedV2::new)
    .thenFrom(CANCUN, EngineForkchoiceUpdatedV3::new)
    .thenFrom(AMSTERDAM, EngineForkchoiceUpdatedV4::new)
    .build(constructorArguments);
```

The scheduler instantiates each version with the right `(minSupportedFork, firstUnsupportedFork)`
pair derived from the chain. Method names live in the `RpcMethod` enum;
`engine_exchangeCapabilities` derives the advertised capability list automatically from every
`RpcMethod` entry starting with `engine_`, so there is no separate capabilities list to maintain.

## Test pattern (src/test, same package)

Tests are layered exactly like the production classes: `EngineForkchoiceUpdatedV4Test extends
EngineForkchoiceUpdatedV3Test extends ... V1Test`, so **every version class runs all the tests of
the previous versions plus its own**.

- The V1 test class owns the generic scenarios, written against protected hooks:
  `createMethodInstance()`, `getMinSupportedTimestamp()` / `getMaxSupportedTimestamp()`,
  payload/attribute builders, fixture customizers, and result-assertion hooks such as
  `assertPayloadResult(Object)` that each version extends with
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

## Checklist: add version N+1 to an existing migrated series

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
   `.thenFrom(<ACTIVATION_FORK>, EngineFooVN+1::new)`.
5. Add `EngineFooVN+1Test extends EngineFooVNTest`: override `createMethodInstance()`, the
   method-name test, the fork-window hooks, and any builder/assertion hooks; add tests only for
   the new behavior. All inherited tests must pass unmodified.
6. Add/extend the acceptance-test fixtures for the activation fork.
7. Update `CHANGELOG.md`.

## Checklist: add a brand-new method series (migrated pattern)

1. Create `EngineBarV1 extends ExecutionEngineJsonRpcMethod` (sealed once V2 exists), taking
   `ConstructorArguments` plus the fork window in its constructor; add its parameter/result classes
   as (future-sealed) hierarchies from the start.
2. Register it in `RpcMethod` and in `ExecutionEngineJsonRpcMethods` via `VersionScheduler`
   (`startsFrom(<FORK>, EngineBarV1::new)` or `alwaysActive(...)`).
3. Create `EngineBarV1Test` with all scenarios written against protected hooks from day one, so
   `EngineBarV2Test` can be layered on top later.
4. Add acceptance-test fixtures and a `CHANGELOG.md` entry.

## Definition of done

```
./gradlew :ethereum:api:test --tests "org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.*"
./gradlew :ethereum:api:spotlessApply
```

Both must pass, with no `@Disabled` tests introduced, before the change is complete.
