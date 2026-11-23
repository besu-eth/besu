# Optimistic parallel transaction processing

This document explains how Besu executes the transactions of a block in parallel when the block has no block access list. On mainnet, that is every block before Amsterdam. It covers `OptimisticConcurrentTransactionProcessor`. Blocks with a block access list go through `BalConcurrentTransactionProcessor`, which this document does not cover. It is written for developers who are new to this part of the code.

- [Where the processor runs](#where-the-processor-runs)
- [The example block](#the-example-block)
- [Speculative runs](#speculative-runs)
- [Balance changes](#balance-changes)
- [Same-sender chains](#same-sender-chains)
- [Threads](#threads)
- [Tests](#tests)
- [Where to start reading](#where-to-start-reading)
- [Glossary](#glossary)

## Where the processor runs

When Besu imports a block, through `engine_newPayload`, `blocks import` or sync, `MainnetBlockValidator.validateAndProcessBlock` validates the header against its parent. `AbstractBlockProcessor.processBlock` then does the following, in order:

1. **System calls** before the first transaction: the parent beacon block root (EIP-4788) and the block hash history (EIP-2935).
2. **Starting parallel execution** with `startParallelExecution`. The processor starts running the block's transactions on other threads before the first one is executed.
3. **The transaction loop.** For each transaction in block order: get its result, from the processor or by executing it, then commit it to the block's updater, add up its gas and create its receipt.
4. **After the loop:** the blob gas check, withdrawals, execution requests (EIP-7685) and rewards.
5. **Outputs and persist.** Before anything is persisted, `BlockBodyValidator.validateBody` checks the outputs against the header: gas used, receipts root, logs bloom and requests. `worldState.persist` then computes the state root, compares it with the header's and writes the new state.

The thread that runs this loop is called **block processing** in the rest of this document. Everything else follows one rule:

> After transaction *i*, the block's state must be exactly the state that executing transactions 0 to *i* one after another would leave.

Parallel execution can only save time; it never decides what the block's state is. Whenever a result from another thread might differ from what executing the transaction at its turn would give, block processing executes the transaction itself.

On Bonsai with `--bonsai-parallel-tx-processing-enabled`, which is the default, the block processor is `MainnetParallelBlockProcessor`. On Forest, or with parallel processing off, `MainnetBlockProcessor` executes every transaction in the loop itself. `MainnetParallelBlockProcessor.startParallelExecution` picks a processor for each block:

| Block | Processor |
|---|---|
| No block access list | `OptimisticConcurrentTransactionProcessor`, on EVM v1 and v2 |
| Has a block access list (Amsterdam) | `BalConcurrentTransactionProcessor`, not covered here. With `--Xbal-perfect-parallelization-enabled=false`, these blocks use the optimistic processor as well. |

A block access list (EIP-7928) states which accounts and slots each transaction touches and the values it leaves there, so the BAL processor knows every transaction's inputs in advance. The optimistic processor has no such list. It learns what a transaction touches only by running it.

Both extend `ParallelBlockTransactionProcessor`:

- `runAsyncBlock` starts the work for the block.
- `getProcessingResult` returns the result for the transaction block processing has reached, or nothing, in which case block processing executes the transaction itself.
- `abort` ends the block's work.

## The example block

The rest of this document follows one made-up block of 13 transactions. A mainnet block has about 250 transactions, most of them independent of each other.

| # | Transaction | Sender | What makes it hard |
|---|---|---|---|
| 0 | swap, pool P | A | writes pool P's price |
| 1 | ETH transfer to I | B | raises I's balance |
| 2 | WETH deposit | C | adds to WETH's balance |
| 3 | WETH deposit | F | adds to WETH's balance after tx 2 |
| 4 | swap, pool Q, private | E, nonce n | its sender is unknown when the block arrives |
| 5 | USDC transfer, private | E, nonce n+1 | same sender as tx 4, next nonce |
| 6 | batch call, 8.6 Mgas | G | one long run |
| 7 | swap, pool P | D | reads pool P after tx 0 wrote it |
| 8 | approve token X | J, nonce m | sender known from the transaction pool |
| 9 | swap X, pool R | J, nonce m+1 | same sender as tx 8; needs its approval |
| 10 | swap, pool S | I | its sender received ETH in tx 1 |
| 11 | swap, pool P | H | reads pool P after tx 0 and tx 7 wrote it |
| 12 | swap, pool S | K | reads pool S after tx 10 wrote it |

Private transactions are sent straight to a builder, so the node has not recovered their senders when the block arrives.

The timeline in [Same-sender chains](#same-sender-chains) is schematic. It keeps the order of events and rough proportions, not measured times, and it shows six workers where a node has one per core. Time runs left to right and each row is a thread. The top row is block processing, with the transaction numbers above it.

- **Blue:** a speculative run, and block processing taking its result.
- **Orange:** block processing executes the transaction itself.
- **Grey:** the start and end of the block, sender recovery, and runs that fail their nonce check.
- **Hatched:** a run that is thrown away.

## Speculative runs

`OptimisticConcurrentTransactionProcessor.runAsyncBlock` hands every transaction to the block's worker threads. Each worker runs its transaction alone on the parent state, in an accumulator of its own, and keeps what it read and wrote together with the result (`ParallelizedTransactionContext`). A worker skips a transaction that block processing has already reached.

When block processing reaches a transaction, `getProcessingResult` does three things:

1. If the transaction's run has not finished, block processing executes the transaction itself and the run is thrown away.
2. `TransactionCollisionDetector.hasCollision` checks whether the earlier transactions of the block changed anything the run touched: the nonce, balance or code of an account, a storage slot, or storage that was cleared. A run that touched the coinbase account usually finds its balance changed, since every transaction with a priority fee pays the fee to it. A change to a balance alone is often no collision, as [Balance changes](#balance-changes) explains.
3. Without a collision, block processing **takes** the result: it imports the run's changes into the block's accumulator and credits the transaction's fee to the coinbase account, which the run kept aside.

A reverted or halted result is taken like a successful one when nothing it read has changed. An invalid result is never taken.

A collision means the run read something that has changed since, so block processing executes the transaction itself. The run still helped, because what it read is now in the caches.

## Balance changes

Many transactions touch an account whose balance an earlier transaction of the block changed: a payment to WETH, to the Uniswap v4 pool manager, to the ERC-4337 entry point, or to an account that received ETH earlier in the block. Most of them only add to that balance, or only need it to be large enough for what they spend. Their result still holds when the transaction's balance change is carried over to the balance the block has reached.

**Balance observations.** The EVM reports which balances a transaction depends on. BALANCE, SELFBALANCE and SELFDESTRUCT report through `WorldUpdater.observeBalance`, and so does a value-carrying CALL, CREATE or PAY whose balance is too small. One whose balance is large enough reports by how much it was large enough (`observeSufficientBalance`). Each updater passes the reports on to its parent. Only a worker's accumulator records them (`recordBalanceObservations`), keeping the smallest margin per account.

**When a balance change is carried over.** An earlier transaction may have changed only the balance of an account the run touched, not its nonce or code. That is no collision when all of these hold:

- The account is not the transaction's sender, whose balance pays for the gas.
- The run did not observe the account's balance.
- The account is not empty in the parent state, at the transaction's turn or after the run. Whether an account is empty decides call costs and state clearing.
- The run only added to the balance. If it spent from it, the block's balance plus the smallest margin still covers the balance the run started from, so every spending would still find enough.

When block processing takes such a result, it sets each of these balances to the block's balance plus the run's change, in the state and, when a block access list is being built, in its post balance.

**The coinbase account.** Every transaction with a priority fee pays the fee to the coinbase account, so a run that touched the coinbase account, such as a direct payment to the block's builder, usually finds a changed balance. The same rules apply to it as to any other account. Such a run already holds its own fee, so block processing does not credit the fee again. A zero fee still touches the coinbase account, and state clearing then deletes it if it is empty. A run that deleted the coinbase account always collides, because its result leaves out a coinbase account the transaction touched only for its fee, and the deletion with it.

A chained result, described next, carries no balance change over.

## Same-sender chains

On the parent state, a sender's second transaction in a block fails its nonce check, because the first one has not happened there. Block processing then executes it with nothing in the caches. Chains run the transactions of one sender in order instead.

**Chains.** The transactions of one sender run in order on one worker, each on the state the one before it left (`runChain`). A chain holds at most 64 transactions, and the sender's later ones run alone. Each member's result carries the changes of the members before it, which block processing checks and imports again, so without the cap a chain would cost the square of its length.

**Taking a chained result.** A chained result is taken when, for everything it touched, the block holds at its turn the value the chain started from: the predecessor's value where the predecessor touched it, the parent's otherwise. Storage cleared on either side is a collision, and so is touching the coinbase account. No balance change is carried over.

**Where a chain stops.** A member whose run fails with an error is reported and executed by block processing, and so are the members after it. An invalid member ends the chain as well, because the sender's next transaction would fail its nonce check. So does a member that block processing reaches before it ran: the members after it would run without the state it leaves.

**Warm-ups.** While a chain runs, each later member is also executed once against the parent state on the I/O thread pool (`warmUp`). Its nonce and gas payment checks are relaxed, as its predecessors have not run there, and the result is discarded. What the member reads is then cached by the time its turn comes, in the chain or, if the chain stopped before it, in block processing.

**Unknown senders and late chains.** The transaction pool knows the senders of the transactions it saw, but not of those sent to builders directly. Those transactions run alone at first. Their senders are recovered on the block's workers, eight per task. The recoveries are queued behind the block's first runs, twice as many as there are worker threads, and ahead of the rest. Once every sender is recovered, the unknown transactions that turn out to share a sender run again as a **late chain**, together with that sender's transactions the transaction pool knew (`chainUnknownSenders`). A late chain continues from the last result that already ran on the state before it: the run of the last known transaction before the first unknown one, or the first unknown one's own run. Block processing prefers a late-chained result to the transaction's run alone. A signature that recovers no key leaves its transaction unchained.

![The example block on the optimistic processor with same-sender chains](images/optimistic-processor.svg)

In the figure:

1. The runs start once block processing has made the system calls, and block processing gets to tx 0 right away. Tx 0's run has only just started, so block processing executes tx 0 itself and the run is thrown away.
2. J's two transactions run as one chain on one worker. Tx 9 runs on the state tx 8 left, and block processing takes both results.
3. E's transactions were private. Tx 5's own run fails its nonce check, because tx 4 has not happened on the parent state.
4. Once E is recovered, a late chain runs tx 5 on the result of tx 4's run. Block processing gets to tx 5 before the chain ends, so it executes tx 5 itself and the chain is thrown away. A transaction further into the block leaves a late chain more time.
5. Tx 3's deposit adds to WETH's balance, which tx 2 changed. The deposit does not read that balance, so block processing takes its result and adds the deposit to the balance tx 2 left.
6. The batch call's run is long. Block processing reaches the call before the run ends, throws the run away and executes the call from the start. That alone takes more than a third of the block.
7. Tx 7, 10, 11 and 12 collide, on pool P, the sender's balance, pool P and pool S. Block processing executes them. Tx 10's sender pays for its gas from the balance tx 1 raised, so that change is not carried over.

Without chains, block processing executes tx 9 as well, 8 of the 13 transactions instead of 7. The dashed line marks where that block ends.

## Threads

| Thread | Count | Runs |
|---|---|---|
| block processing | 1 | the transaction loop: takes results and executes the rest |
| `besu-block-cpu-N` | one per core | runs, chains, late chains, sender recovery |
| `besu-block-io-N` | two per core | warm-ups of chain members |

The system properties `besu.block.cpuThreads` and `besu.block.ioThreads` change the counts.

## Tests

- `TransactionCollisionDetectorTest` tests the collision rules, for runs alone and chained runs, and when a balance change is carried over.
- `BalanceObservationTest` in the `evm` module tests that the operations report the balances they depend on.
- `OptimisticTransactionProcessorUnitTest` tests chains, where they stop, sender recovery, late chains and warm-ups.
- `OptimisticParallelBlockProcessorIntegrationTest` imports blocks on a genesis built at runtime and compares the outcome with sequential execution.

```
./gradlew :ethereum:core:test --tests 'org.hyperledger.besu.ethereum.mainnet.parallelization.*'
```

## Where to start reading

1. `AbstractBlockProcessor.processBlock`: the loop and the rule it keeps.
2. `MainnetParallelBlockProcessor`: picking the processor, and falling back to execution.
3. `OptimisticConcurrentTransactionProcessor`: runs, chains, late chains, warm-ups and takes.
4. `TransactionCollisionDetector`: what counts as a collision, for runs alone and chained runs, and which balance changes are carried over.

## Glossary

- **Block processing:** the thread that runs the transaction loop. It alone decides the block's state.
- **Speculative run:** a transaction executed on another thread before block processing reaches it.
- **Take:** block processing using a run's result instead of executing the transaction.
- **Collision:** something a run touched changed in the block before the run's turn, so its result cannot be taken.
- **Carry-over:** taking a result although an earlier transaction changed a balance it touched, by adding the run's change to the block's balance.
- **Chain:** a sender's transactions run in order on one worker, each on the state the one before it left.
- **Late chain:** a chain formed once the senders the transaction pool did not know are recovered, after their transactions already ran alone.
- **Warm-up:** a chain member executed once against the parent state, only so that what it reads is cached.
