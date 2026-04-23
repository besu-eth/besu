# Node Analysis Reference

Tooling from https://github.com/Consensys/protocols-node-analysis for analyzing burn-in fleets.

## Prerequisites

1. AWS CLI installed
2. Ansible installed: `brew install ansible`
3. Logged into AWS SSO with protocols profile: `aws sso login --profile protocols`
4. Clone the repo: `git clone https://github.com/Consensys/protocols-node-analysis && cd protocols-node-analysis`

## Step 1: Create Hosts File

Filter AWS instances matching the burn-in fleet name (regex supported):

```bash
./create-hosts-file.sh "burn.*<version>-RC1" <version>-RC1-burn-in-hosts.ini
```

The script queries AWS EC2 for matching instances, displays them, and prompts for confirmation before saving.

**WARNING:** Do not store hosts files for long periods. Once nodes are deleted, the IPs are recycled and you may accidentally target a different node.

## Step 2: Run Node Analysis Report

```bash
./run-ansible-playbook.sh <version>-RC1-burn-in-hosts.ini node-analysis-playbook.yml
```

This playbook:
1. Copies `node-analysis.sh` to each remote host and executes it
2. Collects hardware info, build records, log summaries, sync metrics, RocksDB usage, and OOM kill data
3. Generates an HTML report in `docs/` with a Grafana link at the top showing the fleet's metrics since launch

The report summarizes:
- **Hardware information** — CPU, memory, disk specs, cloud instance type
- **Build records** — Besu version and configuration deployed
- **Log summary** — counts of Besu errors, warnings, exceptions; CL errors and warnings
- **Sync metrics** — sync durations formatted as hours and minutes
- **RocksDB usage** — storage layer statistics
- **OOM kills** — any out-of-memory events from dmesg

### Publishing the report

Commit and push the generated report to make it available on the [reports site](https://crispy-train-plzljoq.pages.github.io/):

```bash
git add docs/
git commit -m "Add <version>-RC1 burn-in analysis report"
git push
```

## What to Look For in the Report

### Errors and warnings
- The report identifies which nodes have Besu or CL errors — investigate each one
- Common benign errors can be noted but should still be accounted for
- CL errors use patterns: `ERR`, `CRIT`
- CL warnings use patterns: `WARN`, `WRN`

### Sync times
- Compare sync times across burn-in nodes
- Compare test nodes (new RC) against control nodes (previous release) for regressions

### Performance comparison
- The burn-in fleet has 3 "perf-test" nodes and 3 "perf-control" nodes
- Everything is identical except the Besu version
- Three of each gives a good average and helps avoid anomalies from bad peers
- Use the Grafana link to compare JVM memory, CPU, and disk I/O between test and control

### JVM memory
- Analyze both during sync and after sync
- Look for memory leaks or unexpected growth patterns

### Peer connections
- Verify nodes are maintaining healthy peer counts

## Step 3: Database Integrity Verification (Bonsai Tree Verifier)

**IMPORTANT:** Do not run until all performance analysis is complete. This step shuts down both EL and CL clients before running the verifier.

```bash
./run-ansible-playbook.sh <version>-RC1-burn-in-hosts.ini bonsaitreeverifier-playbook.yml
```

This playbook:
1. Clones and builds the [bela](https://github.com/Consensys/bela) tool on each node
2. Stops Besu and the CL client
3. Runs `bonsaitreeverifier` against the Besu data directory
4. Takes 2-4 hours for mainnet nodes (depending on D4 or D8 instance size)

View results:

```bash
./run-ansible-adhoc.sh <version>-RC1-burn-in-hosts.ini 'tail /tmp/bonsaitreeverifier.log'
```

**Note:** After running this step, warming caches can take several hours/days to stabilize, which impacts performance analysis — this is why it must be done last.

## Optional: Async Profiler

For debugging performance issues (generally not needed for standard release burn-ins):

```bash
# Wall-clock profiling (defaults to 300 seconds, per-thread)
./run-ansible-playbook.sh <version>-RC1-burn-in-hosts.ini async-profiler-wall-playbook.yml

# CPU profiling
./run-ansible-playbook.sh <version>-RC1-burn-in-hosts.ini async-profiler-cpu-playbook.yml
```

Outputs per VM in home directory:
- `.html` — best for human viewers (flame graph)
- `.collapsed` — better for processing with agents

## Running Ad-Hoc Commands

Execute arbitrary commands on all nodes in the fleet:

```bash
./run-ansible-adhoc.sh <version>-RC1-burn-in-hosts.ini '<command>'
```

Example:
```bash
./run-ansible-adhoc.sh <version>-RC1-burn-in-hosts.ini 'tail /tmp/bonsaitreeverifier.log'
```
