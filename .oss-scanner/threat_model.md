# Besu

Besu is an Ethereum execution client written in Java. On public networks it runs alongside a consensus client and serves validators, RPC providers and ordinary node operators. On private networks it also provides QBFT and IBFT 2.0 consensus and node and account permissioning. It processes adversarial peer traffic, transactions, blocks, contract bytecode and JSON-RPC requests. Correct execution and availability are its main security properties, ahead of confidentiality: a node that disagrees with the other clients about a block leaves the canonical chain, and a node that crashes or stalls makes its validator miss duties.

## Where untrusted input enters

Treat everything below as attacker-controlled, in roughly this order of priority:

- **devp2p peers.** Any node can connect and complete the RLPx handshake. This includes RLPx framing and Snappy decompression, the `HELLO` and `STATUS` exchanges, and every eth protocol message: transactions and transaction announcements, block announcements, headers, bodies and receipts. On the default configuration a peer needs no other privilege.
- **Transactions, blocks and EVM bytecode,** whether they come from peers, from JSON-RPC (`eth_sendRawTransaction`), or from the consensus client through the Engine API. This covers typed transaction decoding (including blob and EIP-7702 transactions), validation, transaction pool admission and eviction, block validation and import, and EVM execution, including precompiles.
- **Sync peers.** Responses received during snap sync, full sync and backward sync are untrusted, and so is what peers report about their chain, such as their head or block range. Data accepted during sync must meet the same rules as block import. When the snap server is enabled, the requests it serves are untrusted too.
- **Discovery.** These are discv4 and discv5 UDP packets, and EIP-1459 DNS discovery lists (`--discovery-dns-url`). A DNS list is published by a third party, and the DNS answers serving it are untrusted.
- **JSON-RPC over HTTP and WebSocket, and GraphQL,** when an operator exposes them. The CLI binds to localhost by default, but the official Docker image binds to all interfaces. `eth_`, `net_`, `web3_` and `txpool_` callers are untrusted, as are filter and subscription registrations. `debug_`, `trace_` and `admin_` are off by default and meant for operators. Their resource limits are still in scope, because operators expose them in practice.
- **Engine API.** It is JWT-authenticated and meant for a trusted consensus client. The consensus client is trusted, but the payloads it passes on are not: they are blocks built by other validators or by block builders. Any work Besu does on a payload, before or during validation, is driven by whoever built it. Authentication bypasses are in scope, and so are robustness failures reachable through it. Say when an attack needs control of the consensus client or the block builder.
- **Private networks.** QBFT and IBFT 2.0 messages are untrusted from non-validators. Validators are trusted only up to the fault tolerance of the protocol: one Byzantine validator must not halt the network, mint currency, or break safety. Unlike a public network, a private one can stop finalising blocks altogether, so its liveness is a security property. Node and account permissioning (local allowlist files and on-chain) is a security boundary on these networks, not defence in depth. Private networks often use settings that public networks never do, such as a zero gas price, a block reward, or a custom genesis and fork schedule; treat these as normal configurations there.

## Trust assumptions

Treat these as trusted:
- the operator, the command line and configuration files, the genesis file, and the node's filesystem (keys, the JWT secret, the database);
- plugins loaded with `--plugin-path`, which run with full node privilege by design;
- an authenticated consensus client, apart from the robustness failures above.

Local administrator control, deliberate edits to trusted configuration and an already compromised host are not vulnerabilities by themselves.

The native libraries from `besu-native` (secp256k1, BLS12-381, BN254, KZG, gnark, Arithmetic) live in another repository. Their internals are out of scope here, but the Java code that calls them is in scope, and so is a crash or wrong result in a native library that attacker input reaches, for example through a precompile. Report it against the Java call site. Several operations have both a native and a Java implementation, chosen at runtime; the native one is the default. Any input on which the two give different results is a consensus bug, because two Besu nodes with different settings would disagree about a block.

## Components that matter most

- `ethereum/core`, `evm`, `datatypes`, `crypto`: transaction processing and validation, the world state (Bonsai) and its trie logs, block import, gas accounting, the EVM and precompiles.
- `ethereum/eth`: the eth and snap protocols, peer management, the transaction pool and synchronisation.
- `ethereum/p2p`, `ethereum/rlp`: RLPx, discovery (including DNS discovery) and RLP decoding.
- `ethereum/api`: JSON-RPC, GraphQL, WebSocket subscriptions, filters, authentication and the Engine API.
- `consensus/qbft-core`, `consensus/qbft`, `consensus/ibft`, `consensus/common`, `consensus/merge`.
- `ethereum/trie`, `ethereum/permissioning`.

Lower priority: `ethereum/evmtool`, `ethereum/ethstats`, `ethereum/stratum`, `consensus/clique` and `consensus/ibftlegacy`. These are an operator utility, optional reporting and mining features, and legacy consensus. They are still in scope. Test code (`acceptance-tests/`, `testutil/`, `ethereum/referencetests/`, `**/src/test*`, `**/src/integration-test`, `**/src/jmh`) is out of scope, except where it shows how to build a reproducer.

## Classes we care about

- **Consensus divergence.** Besu accepts a block, transaction or state transition that the other execution clients reject, rejects one they accept, or computes a different state root, receipt or gas result. This also covers two Besu nodes disagreeing with each other: because of different backends or settings, because parallel transaction processing or another world-state optimisation gives a different result from plain sequential execution, or because of the CPU architecture (x86-64 and ARM64 are both supported).
- **Remote crash, hang or exhaustion** from peer or RPC input. This means uncaught exceptions on network threads, unbounded memory, threads, queues, file descriptors or disk/log growth, and work that grows faster than the input.
- **Excess work on valid input.** A block or transaction that obeys every protocol rule and pays its gas can still make Besu spend far more time than its gas represents, for example through the worst case of a data structure, or work that is not cancelled when a block is rejected. This consumes the time budget for `engine_newPayload` and the slot. Gas pays for opcodes, not for Besu's data structures, so gas does not bound it.
- **Transaction pool abuse.** Transactions that can never pay, or that cost the sender little, fill the pool or evict other users' transactions. This also covers checks that diverge between pool admission and block execution.
- **Silent loss of liveness.** A periodic task, a scheduler, the block import pipeline or the consensus event loop stops after an exception or an error while the process and its health checks stay up.
- **Relay-safety.** Besu is more permissive than the other clients and relays data they treat as a protocol violation, so honest peers penalise or disconnect it.
- **Peer-table poisoning and eclipse** through discovery, including DNS discovery.
- **Authentication and authorisation:** Engine API JWT handling, RPC authentication (`/login`, credential files, JWT), and node and account permissioning.
- **Sync accepting incomplete or wrong data,** such as state that passes the cryptographic checks but is incomplete, or blocks and receipts that full import would reject; and peers steering or stalling sync.
- **Secrets in logs or responses:** keys, the JWT secret, credentials.

## Upcoming network upgrades

Code for network upgrades that are scheduled but not yet active on Mainnet (for example the block-level access lists and the eth/71 messages of Amsterdam) is in scope. Report it as you would for an active fork, and say that the fork is not active yet: we rate it on its impact once the fork activates, and fix it before then.

## How to exercise it

The checkout is at `/src`. The image holds the Gradle cache and the build outputs, so use Gradle with `--offline`. The scanner's machine is small, so run one module or one test class at a time, not the whole build:

```
./gradlew --offline :ethereum:eth:test --tests 'org.hyperledger.besu.ethereum.eth.transactions.*'
```

Further notes:
- A test that reproduces a problem is the preferred reproducer. Most components have unit tests with test doubles you can build on: a mock DNS server for DNS discovery, the `ethereum/mock-p2p` peer network for the eth protocol, `BlockchainSetupUtil` and `ExecutionContextTestFixture` for chains and state, and the JSON-RPC handler tests for RPC methods.
- The node is at `build/install/besu/bin/besu`. `--network=dev` no longer exists. For a local chain that produces blocks without a consensus client, run a single-validator QBFT network:

  ```
  cat > /tmp/qbft.json <<'EOF'
  {"genesis":{"config":{"chainId":1337,"londonBlock":0,"shanghaiTime":0,"cancunTime":0,"pragueTime":0,
    "qbft":{"blockperiodseconds":1,"epochlength":30000,"requesttimeoutseconds":4}},
    "gasLimit":"0x1c9c380","difficulty":"0x1",
    "alloc":{"fe3b557e8fb62b89f4916b721be55ceb828dbd73":{"balance":"0xad78ebc5ac6200000"}}},
   "blockchain":{"nodes":{"generate":true,"count":1}}}
  EOF
  build/install/besu/bin/besu operator generate-blockchain-config --config-file=/tmp/qbft.json --to=/tmp/net --private-key-file-name=key
  build/install/besu/bin/besu --genesis-file=/tmp/net/genesis.json --node-private-key-file /tmp/net/keys/*/key \
    --data-path=/tmp/qbft-data --rpc-http-enabled --min-gas-price=0
  ```

  Add other options as the finding needs. The account in `alloc` is funded, and you can set its private key to any test key you control. A finding that needs a proof-of-stake chain is better reproduced with a unit test, since a consensus client is not available offline. A log4j `Could not determine local host name` error at startup is expected without a network and harmless.
- `ethereum/evmtool/build/install/evmtool/bin/evmtool` runs bytecode and state tests directly.
- `testfuzz/` holds Jazzer fuzz targets.

Use local fixtures and local nodes only. Do not contact public peers, DNS discovery lists or live networks.

## How we rate severity

Rate on realistic reachability, default exposure, the privilege needed and the impact. Distinguish a crash of one node from demonstrated network-wide consensus impact. Our published advisories (https://github.com/besu-eth/besu/security/advisories) show how we have rated past reports.

- **Critical:** a consensus split, or wrong execution such as balances or gas, that an unprivileged attacker can trigger on a public network with a default configuration; remote code execution; remote disclosure of the node key or the JWT secret.
- **High:**
  - a single unprivileged peer or transaction, on a default configuration, crashing the node, exhausting its memory or threads, or keeping it from following the chain;
  - a few requests to a namespace enabled by default (`eth_`, `net_`, `web3_`) crashing an exposed node or stopping its block import;
  - a consensus split that needs a non-default setting or a malicious block builder;
  - an Engine API or RPC authentication bypass;
  - a permissioning bypass on a permissioned network;
  - one non-validator, or one Byzantine validator within the fault tolerance, halting QBFT or IBFT finality or breaking its safety.
- **Medium:**
  - denial of service that needs many peers or significant attacker resources;
  - exhaustion through `debug_`, `trace_` or `admin_` methods, or through sustained load on other RPC methods;
  - degraded sync, discovery or transaction propagation that recovers on its own;
  - relay-safety problems;
  - excess work on valid input that is measured but stays within the slot.
- **Low:** problems that need a non-default experimental option, an authenticated operator, or local access; hardening with no demonstrated impact.

A performance difference alone is not a denial of service. Measure attacker cost against the victim's time, memory or CPU, and compare with a control input of the same size. Say which configuration, flags and network type a finding needs.

## Anything to leave alone

- Dependency version bumps or CVEs in libraries, unless you show the vulnerable code is reachable from Besu with attacker input.
- Behaviour that the Ethereum specifications or EIPs require, even if it looks unsafe. Report it as a spec issue only when Besu differs from the specification or from the other clients.
- Missing hardening of the build, the Docker image or CI configuration, unless it lets an attacker change what is built.
- Issues that require a malicious plugin, a modified binary, or write access to the data directory or configuration.

## Reporting

Findings go privately to `security-besu@lists.lfdecentralizedtrust.org`. Do not open public issues or pull requests, and do not publish unpatched details.
