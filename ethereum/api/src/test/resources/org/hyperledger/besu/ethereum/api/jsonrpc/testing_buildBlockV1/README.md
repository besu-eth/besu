# `testing_buildBlockV1` by-spec fixtures

These files drive `TestingBuildBlockJsonRpcHttpBySpecTest`, a by-spec test for the
`testing_buildBlockV1` JSON-RPC method.

## Layout

- `chain-data/genesis.json` – the genesis the test chain is built on. It fixes the **genesis
  hash** that every block in `blocks.bin` and every `parentBlockHash` in the spec files is bound
  to.
- `chain-data/blocks.bin` – a pre-built chain (RLP-encoded blocks) imported at test setup. It is a
  generated artifact, not hand-edited.
- `NN_*.json` – one spec per file: a JSON-RPC `request`, the expected `response`, and a
  `statusCode`. The successful cases build a block directly on top of the genesis, so their
  responses embed the genesis hash and the resulting `stateRoot` / `blockHash` /
  `blockAccessList`.

## Integrity guard

`chain-data/genesis.json` and `chain-data/blocks.bin` are protected by the
`checkTestingBuildBlockChainData` Gradle task (wired into `check`), mirroring the plugin-API
`checkAPIChanges` guard. If either file changes, the build fails until the `knownHash` in
`ethereum/api/build.gradle` is deliberately updated. This prevents an accidental regeneration (or
silent corruption) of the fixed chain from slipping through unnoticed — a change here is only
correct alongside regenerated spec responses.

## Regenerating the chain

You only need this when the protocol rules that apply to these blocks change — for example
EIP-8282 adding builder deposit/exit system calls to Amsterdam, which required the two builder
predeploys to be added to the genesis (changing the genesis hash) and the chain to be rebuilt.

1. Edit `chain-data/genesis.json` if the rules require new genesis state (e.g. predeploys). This is
   the only file you edit by hand.
2. Run the regeneration task:

   ```
   ./gradlew :ethereum:api:regenerateTestingBuildBlockChainData
   ```

   This rebuilds `chain-data/blocks.bin`, swaps the old genesis hash for the new one across the spec
   files (both `parentBlockHash` request params and the success-case responses), and refreshes the
   spec response bodies (`stateRoot` / `blockHash` / `blockAccessList`, …). The task prints the new
   integrity checksum.
3. Paste the printed checksum into the `knownHash` of the `checkTestingBuildBlockChainData` task in
   `ethereum/api/build.gradle`. This is a deliberate acknowledgement that the fixed chain changed
   (the guard mirrors the plugin-API `checkAPIChanges` guard). If you skip this, `check` fails and
   reports the value to paste as `Calculated`.
4. Run `TestingBuildBlockJsonRpcHttpBySpecTest` (without the update flag) and `check` to confirm
   everything is consistent.

The generator logic lives in `TestingBuildBlockChainDataGenerator` and is invoked by the Gradle
task — you do not edit or run it directly.
