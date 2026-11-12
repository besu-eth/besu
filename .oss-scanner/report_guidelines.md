# How we would like reports written

## Layout and length
- One root cause per report. If several call sites share it, list them all in the same report.
- Title: the component, then the problem and its effect in one line, e.g. "eth protocol: X lets a single peer exhaust the heap".
- Sections, in this order: Summary, Affected code, Preconditions, Reproduction, Impact, Suggested fix.
- Keep the Summary to a short paragraph that a maintainer can act on without reading further.

## What every report should state
- The commit the finding was checked on, and the files and lines involved.
- The attacker's starting position: an unauthenticated peer, an RPC caller, a validator, the consensus client, a block builder.
- The configuration needed: the network type (public or private), and any non-default option, RPC namespace or sync mode. For a private network, give the consensus protocol and the settings that matter, such as the gas price and block reward. Say plainly when the default configuration is affected.
- The fork the finding depends on, and whether it is active on Mainnet yet.
- How the attacker's input reaches the faulty code, from the network entry point to the line that fails.
- Measurements for resource exhaustion and slow processing. Give the attacker's cost (messages, bytes, peers, gas), the victim's time, memory or CPU, and a control input of the same size.
- For consensus and relay problems, what the other execution clients (go-ethereum, Nethermind, Reth, Erigon) do with the same input, or what the specification requires, with a reference.
- Whether the finding is a gap in the fix for one of our published advisories, and if so which one.
- What was confirmed by running code, and what comes from reading it only.

## Reproduction
- Prefer a JUnit test in the affected module that fails before a fix and passes after it, using the module's existing test utilities.
- Otherwise give exact steps against a local single-validator QBFT network, as described in the threat model, or another local private network. Never use public networks or peers.

## Style
- Plain, direct language. State facts and measurements without speculation; mark estimates as estimates.
- Explain the severity with the rating in the threat model.
