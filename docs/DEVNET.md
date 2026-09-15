# Joining an ethpandaops devnet

Besu can join the devnets run by [ethpandaops](https://github.com/ethpandaops) with a single option. It downloads the genesis file and bootnodes of the devnet and starts as if `--genesis-file` and `--bootnodes` had been given.

---

## Quick start

```bash
besu --devnet glamsterdam-devnet-11 \
  --data-path ./data/glamsterdam-devnet-11 \
  --engine-rpc-enabled \
  --engine-jwt-secret ./jwt.hex
```

Besu is an execution client, so a consensus client configured for the same devnet is still required. The consensus client files (`config.yaml`, `genesis.ssz`, `bootstrap_nodes.txt`) are in the same `metadata` directory described below.

Use a separate `--data-path` per devnet. The downloaded files are cached in the data directory, and a data directory can only ever hold one chain.

---

## Devnet names

A devnet name has the form `<prefix>-<devnet>`. It is split at the first dash:

| `--devnet`              | Repository                                                                          | Directory                     |
|-------------------------|-------------------------------------------------------------------------------------|-------------------------------|
| `glamsterdam-devnet-11` | [ethpandaops/glamsterdam-devnets](https://github.com/ethpandaops/glamsterdam-devnets) | `network-configs/devnet-11` |
| `perf-devnet-3`         | [ethpandaops/perf-devnets](https://github.com/ethpandaops/perf-devnets)             | `network-configs/devnet-3`    |
| `blob-devnet-0`         | [ethpandaops/blob-devnets](https://github.com/ethpandaops/blob-devnets)             | `network-configs/devnet-0`    |
| `fusaka-sepsf-0`        | [ethpandaops/fusaka-devnets](https://github.com/ethpandaops/fusaka-devnets)         | `network-configs/sepsf-0`     |

The available devnets are the directories under `network-configs` in each repository.

---

## What gets downloaded

The files are fetched from `https://raw.githubusercontent.com/ethpandaops/<prefix>-devnets/master/network-configs/<devnet>/metadata/`:

| File                              | Required | Used for                                                    |
|-----------------------------------|----------|-------------------------------------------------------------|
| `genesis.json`                    | yes      | Genesis of the chain                                        |
| `enodes.txt`                      | no       | `enode://` bootnodes (DiscV4)                               |
| `el_enrs.txt`                     | no       | `enr:` bootnodes (DiscV5)                                   |
| `deposit_contract_block_hash.txt` | no       | Expected genesis block hash, checked at startup             |

All other files in the directory belong to the consensus client and are ignored.

---

## Defaults

`--devnet` behaves like `--genesis-file`, so the defaults for custom networks apply:

- **Network ID**: the `chainId` of the genesis.
- **Sync mode**: `FULL`.
- **Genesis hash**: if the devnet publishes `deposit_contract_block_hash.txt`, Besu compares it with the genesis block it computed and refuses to start if they differ.

---

## Caching

The files are cached in `<data-path>/devnet/<name>/`:

```
<data-path>/devnet/glamsterdam-devnet-11/
├── genesis.json
├── deposit_contract_block_hash.txt
├── enodes.txt
└── el_enrs.txt
```

- `genesis.json` and `deposit_contract_block_hash.txt` are only downloaded when they are not cached yet. Once a data directory is initialised, its genesis never changes.
- `enodes.txt` and `el_enrs.txt` are downloaded again on every start. If GitHub cannot be reached, the cached copy is used and a warning is logged.

If a devnet is relaunched with a new genesis, start with a fresh `--data-path`. To only refresh the cached genesis, delete `<data-path>/devnet/<name>/`.

---

## Overrides and conflicts

- `--bootnodes`, `--network-id` and `--discovery-dns-url` still apply and take precedence over the downloaded values.
- `--network` and `--genesis-file` cannot be combined with `--devnet`.

---

## Troubleshooting

| Symptom                                              | Cause                                                                                                                                                             |
|------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Unknown devnet '...', no genesis found at ...`      | The name does not match a repository and directory. Check the `network-configs` directory of the repository.                                                     |
| `Invalid devnet name '...'`                          | Names must be lowercase `<prefix>-<devnet>`.                                                                                                                      |
| `Genesis block hash ... does not match ...`          | Besu computed a different genesis than the devnet. Make sure the Besu version supports the forks of the devnet, and that the cached genesis is not from an earlier launch. |
| `Failed to resolve devnet ...` on first start        | GitHub could not be reached and nothing is cached yet.                                                                                                            |
| No peers                                             | Check the bootnodes logged at startup and that the devnet is still running. Bootnodes can also be set explicitly with `--bootnodes`.                              |
