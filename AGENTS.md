# Guidance for AI coding agents working on Besu

Besu is an Ethereum execution client written in Java, built with Gradle. These notes keep
automated changes consistent with the project's conventions; `CONTRIBUTING.md` remains the
authoritative contributor guide.

## Build and test

- Requires **JDK 25+**.
- Build: `./gradlew compileJava compileTestJava`
- Run a focused test set (preferred while iterating):
  `./gradlew :<module>:test --tests "<fully.qualified.ClassOrPattern>"`
  e.g. `./gradlew :ethereum:api:test --tests "org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.*"`
- Format before committing: `./gradlew spotlessApply` (Google Java Format; builds fail on
  unformatted code).

## Conventions

- Every commit must be signed off (DCO): `git commit -s`. See `DCO.md`.
- Add a `CHANGELOG.md` entry for user-visible changes.
- Match the surrounding code style; do not introduce `@Disabled` tests to make a suite pass.

## Area-specific guides — read these BEFORE editing the area

| If you are changing... | Read first |
|---|---|
| Engine API (`engine_*` JSON-RPC methods, their parameters/results, or their tests) | `ethereum/api/src/main/java/org/hyperledger/besu/ethereum/api/jsonrpc/internal/methods/engine/README.md` |

When you add a new architectural pattern that future changes must follow, document it in a
`README.md` next to the code and add a row to this table.
