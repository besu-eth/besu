# bal-devnet-7 — Besu + Lighthouse (Docker)

Run a local **Besu** execution client and **Lighthouse** beacon node connected to [bal-devnet-7](https://bal-devnet-7.ethpandaops.io/).

Network spec: [bal-devnet-7 spec](https://notes.ethereum.org/@ethpandaops/bal-devnet-7)

Besu flags: `FULL` sync, `BONSAI` storage, metrics on port `6060`.

## Prerequisites

- Docker Engine with Compose v2 (`docker compose`)
- `curl`, `openssl`, `jq`
- Outbound internet access
- **Besu from source:** JDK and Gradle when building the local Besu image

## Quick start

```bash
cd docker/devnet7
chmod +x setup.sh

# Start with Besu built from the current repo branch (auto-builds on first run)
./setup.sh start

# Custom data directory
./setup.sh start --data-dir /tmp/bal-devnet7

# Watch Besu import blocks (EL sync progress)
./setup.sh logs besu
```

### Building Besu from the current branch

By default, Besu runs from a **local Docker image** built from this repository (`besu:devnet7-local`). Lighthouse and network genesis/config come from ethPandaOps.

```bash
# Build only
./setup.sh build

# Force rebuild before start
./setup.sh start --build-besu --data-dir /tmp/bal-devnet7

# Use ethPandaOps pre-built Besu instead
./setup.sh start --besu-image ethpandaops/besu:bal-devnet-7
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--data-dir` | `~/.bal-devnet7` | Base host path; Besu data at `<data-dir>/besu` |
| `--besu-image` | `besu:devnet7-local` | Besu Docker image (local branch build) |
| `--lighthouse-image` | `ethpandaops/lighthouse:bal-devnet-7` | Lighthouse Docker image |
| `--build-besu` | off | Build/rebuild local Besu image before `start` |
| `--p2p-host` | `0.0.0.0` | Besu `--p2p-host` |
| `--bootnodes` | bal-devnet-7 Besu enodes | Override EL `--bootnodes` |
| `--fetch-el-bootnodes` | off | Fetch EL bootnodes from inventory API |
| `--use-static-nodes` | off | Enable `--static-nodes-file=/config/static-nodes.json` |
| `--checkpoint-sync-url` | ethPandaOps bootnode | Lighthouse checkpoint sync endpoint |
| `--refresh-config` | off | Re-download genesis/config from ethPandaOps |

## Besu flags (reference-aligned)

| Flag | Value |
|------|-------|
| `--sync-mode` | `FULL` |
| `--data-storage-format` | `BONSAI` |
| `--bonsai-limit-trie-logs-enabled` | `false` |
| `--rpc-http-api` | `ADMIN,DEBUG,ETH,MINER,NET,TRACE,TXPOOL,WEB3` |

## Network parameters

| Field | Value |
|-------|-------|
| Chain ID | `7071885124` |
| EL genesis | `config.bal-devnet-7.ethpandaops.io/el/besu.json` |
| CL genesis | `config.bal-devnet-7.ethpandaops.io/cl/genesis.ssz` |
| Checkpoint sync | `https://checkpoint-sync.bal-devnet-7.ethpandaops.io` |
| CL client | Lighthouse (`ethpandaops/lighthouse:bal-devnet-7`) |

## Exposed ports

| Service | Port | Purpose |
|---------|------|---------|
| Besu | 8545 | HTTP JSON-RPC |
| Besu | 8551 | Engine API (JWT) |
| Besu | 30303 | EL P2P (TCP/UDP) |
| Besu | 6060 | Metrics |
| Lighthouse | 5052 | HTTP REST API |
| Lighthouse | 9000 | CL P2P (TCP/UDP) |
| Lighthouse | 5054 | Metrics |

## Restart Lighthouse without touching Besu

```bash
./setup.sh restart-lighthouse
./setup.sh resync-lighthouse --confirm   # wipe CL data, fresh checkpoint sync
```

## References

- [bal-devnet-7 dashboard](https://bal-devnet-7.ethpandaops.io/)
- [bal-devnet-7 spec](https://notes.ethereum.org/@ethpandaops/bal-devnet-7)
- [ethPandaOps bal-devnets](https://github.com/ethpandaops/bal-devnets)
