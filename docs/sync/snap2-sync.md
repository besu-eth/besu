# SNAP2 sync

This document explains how Besu syncs a new node with `--sync-mode=SNAP2`. SNAP2 downloads the state at a pivot block, as SNAP does, but it keeps that state in step with the chain head by applying the block access lists (EIP-7928) of the blocks the head moves past. So the state never has to be healed at the end. It needs the fork that adds block access lists, Amsterdam. It is written for developers who are new to this part of the code.

- [Where SNAP2 runs](#where-snap2-runs)
- [The example sync](#the-example-sync)
- [The pivot](#the-pivot)
- [The state download](#the-state-download)
- [Pivot moves](#pivot-moves)
- [Storage tries](#storage-tries)
- [The chain](#the-chain)
- [The final build](#the-final-build)
- [Sorted files](#sorted-files)
- [Restarts and stops](#restarts-and-stops)
- [After the sync](#after-the-sync)
- [Threads](#threads)
- [Options](#options)
- [Tests](#tests)
- [Where to start reading](#where-to-start-reading)
- [Glossary](#glossary)

## Where SNAP2 runs

SNAP2 is never the default sync mode; a node gets it with `--sync-mode=SNAP2`, on Bonsai or Forest. `Snap2SyncFactory.snap2OrSnap` syncs with SNAP instead while the fork that adds block access lists is not scheduled or not reached, and while a SNAP sync from an earlier start is unfinished. `Snap2SyncFactory.create` then decides whether a sync is needed at all: on a node that holds nothing but genesis or the checkpoint header, after a sync that did not finish (the file `snap2-sync-in-progress` in the data directory), or on a resync.

`Snap2SyncController` runs the sync:

1. **Preparing the storage.** `SyncStateStorage.prepareForSync` wipes the world state.
2. **Waiting** for a peer that serves snap, and for a chain head from the consensus client.
3. **Picking the pivot** (see [The pivot](#the-pivot)).
4. **An attempt.** `SyncAttempt` downloads the state and the chain, builds the tries and sets the chain head. An attempt that fails with a `RestartException` starts over from step 1, at most 3 times.
5. **Handing over.** The request scheduler goes to the back-fill, which downloads the bodies and receipts below the synced blocks.

Everything the state download writes follows one rule:

> The flat state holds, for every range it covers, the state at the current pivot.

An answer about an older pivot is dropped. A pivot move brings what is already written up to the new pivot before anything is written at it. Bytecodes are the exception: they are stored by their hash, which no pivot changes.

SNAP changes its pivot during the download without applying what changed in between, so the state it writes mixes several blocks, and it heals the trie at the end by asking peers for trie nodes. snap/2 has no trie node messages. It adds `GetBlockAccessLists` instead, which a SNAP2 sync uses to follow the head.

## The example sync

The rest of this document follows one made-up sync. The account key space is cut into 8 ranges here; a real sync cuts it into 65,536.

When the sync starts, the head is block 1,064, so the pivot is block 1,000. While the state downloads, the head reaches 1,096 and the pivot moves to 1,032. The download completes with the head at 1,120, and the final catch-up moves the pivot to 1,056, where the tries are built.

| Account | Range | What happens to it |
|---|---|---|
| A, plain account | 0 | Downloaded at 1,000. Block 1,011 raises its balance. |
| B, plain account | 6 | Not downloaded yet when the pivot moves. Block 1,011 raises its balance too. |
| C, contract, 40 slots | 1 | Its storage and storage trie are complete at 1,000. Block 1,020 changes one slot. |
| D, contract, 3M slots | 2 | Its storage is still downloading in chunks when the pivot moves. Block 1,030 changes two slots. |
| F, contract | 1 | Created in block 1,025, with its code and two slots. |

![The example sync: the state download with one pivot move, the header download, the final steps and the work after the sync](images/snap2-sync.svg)

The figure is schematic. It keeps the order of events, not measured times. Each row is a part of the sync, named after its threads.

- **Blue:** downloads from peers: state, headers, bodies and receipts.
- **Orange:** pivot moves, which apply block access lists.
- **Green:** trie work: storage trie updates and the final build.
- **Grey:** storage work: merging the files the sync wrote.

## The pivot

`Snap2SyncController.selectPivot` takes the head the consensus client reported, through `ForkchoiceHeadTracker`, and picks the block 64 below it, or the block above the floor if the chain is shorter. The floor is the checkpoint block, or genesis. The block after the pivot must have a block access list: before the fork, the sync waits.

The pivot is a `Pivot(epoch, header)`. Each move increments the epoch. Requests are tagged with the epoch they were sent at, and `StateSync.atPivot` writes an answer only if its epoch is still the current one.

## The state download

`StateSync` downloads accounts, storages and bytecodes in parallel. A loop thread, `snap2-state`, decides what to send. It runs whenever an answer comes back, and every 500 ms. Its capacity is a quarter more requests than the scheduler can have in flight on snap peers.

**Account ranges.** The 65,536 account ranges are asked lowest first, so that the state is written in key order. A third of the capacity goes to them. They wait while more than 50,000 storages or bytecodes are waiting, as each account adds work for the others.

**Storages.** Each account with storage becomes a `StorageTask` with its storage root. Small storages are asked 256 to a request. When a storage does not fit in one answer, the rest of it is cut into up to 64 chunks, sized by how dense its slots were so far, and the chunks are asked in parallel.

**Bytecodes.** Code hashes go to a `CodeQueue`, which skips codes that are already stored. They are asked 64 to a request, with whatever capacity the state requests leave.

**Proofs.** Every account or storage range comes with a range proof, which `RangeProof.verify` checks against the pivot's state root or the storage root on a `snap2-worker` thread. A peer whose answer fails is disconnected, and the range is asked again.

**Writing.** Verified answers are written on the `snap2-persist` threads by `StateWriter`. `StateCoverage` records which account ranges are written, and which storages are still pending. The download is complete when every account range is covered, no storage is pending, no bytecode is waiting and nothing is in flight.

**Peers that stop serving.** A peer that answers empty about a pivot that other peers serve is left out of state requests for 30 seconds, doubling with each repeat up to 10 minutes (`StateServers`). After 12 empty answers in a row from 3 peers, the pivot counts as stale: peers have pruned it, so the pivot moves even if the head has not gone far.

In the example, A and C are written at 1,000. D's storage is too large for one answer and downloads in chunks. B's range has not been asked yet.

## Pivot moves

`PivotMoves.maybeMove` runs every 2 seconds. It moves the pivot once the head is 96 blocks ahead of it, or the pivot is stale, to 64 below the head. A move:

1. Fetches the headers from the head down to the pivot, and checks that they descend from the pivot. A chain that reorganised below the pivot starts the attempt over.
2. Fetches the block access lists of the blocks in between with `BalFetcher`, from peers that speak snap/2 or eth/71, and checks each against its header.
3. Stores the lists and the headers. The lists are kept to serve them to peers later.
4. Folds the lists into one `StateDiff`, the last value of every account and slot that changed.
5. Applies the diff with `StateSync.moveTo`, under the write lock that every write of the download holds shared, so no answer is written halfway through. It then increments the epoch.

A move spans at most 128 blocks; a longer one is cut into steps, and the state requests pause until the last step.

`StateDiffApplier` decides what the diff changes, account by account:

- **Outside the covered ranges:** skipped. The account is downloaded later, at the new pivot. That is B.
- **A covered account:** its fields are updated in place. That is A.
- **An account emptied by a block** (EIP-161): removed, with its storage.
- **A storage that is complete:** the slots are written, and the account's storage root becomes unknown until the storage trie gets the change (see [Storage tries](#storage-tries)). That is C, and F, which the diff creates with its code and slots.
- **A storage still pending:** the slots in the part already downloaded are written, and the task is marked stale. The account is asked again at the new pivot to learn its new storage root, and the remaining ranges are asked at the new pivot. That is D.

## Storage tries

The proof of a storage range carries the trie nodes on the edges of the range, and `RangeProof` rebuilds the nodes inside it. The storage trie of a storage downloaded at one root is therefore written while downloading, range by range. Its root node is kept back until every range is in. A storage whose ranges came at two roots, such as D after the move, gives its trie up, and the final build makes it from the slots.

A move that changes a complete storage hands the change to `StorageTrieUpdater`, which applies it to the stored trie and writes the new storage root into the account. It waits until the download ends, so that a storage changed by several moves is updated once, or starts earlier when more than 250,000 slots are waiting. In the example it updates C's trie and makes F's.

## The chain

`SyncChain` downloads the chain alongside the state:

- **Headers** from the first pivot down to genesis, or to the checkpoint with `--snapsync-synchronizer-skip-pre-checkpoint-headers-enabled`, 64 requests at a time (`HeaderBackfill`). Each segment must link to the headers above it. The headers above the first pivot come from the pivot moves.
- **The window:** the bodies and receipts of the 128 blocks up to the final pivot, downloaded once the headers are in, while the tries are built. In the example, blocks 929 to 1,056.

`SyncChain.setChainHead` then makes the final pivot the chain head.

## The final build

Once the state download is complete, `SyncAttempt` runs these steps:

1. **Settle:** the periodic moves stop.
2. **Prepare** the storage for the build (see [Sorted files](#sorted-files)).
3. **Catch up:** one last move to 64 below the head, so the tries are built at a recent block. In the example, 1,033 to 1,056.
4. **Storage trie updates:** the updater applies the moves' changes, and the download's threads stop.
5. **The build:** `WorldStateTrieBuilder` scans the flat state in 256 partitions on `snap2-trie-builder` threads. It keeps every storage trie whose root node is stored and reads no slots for it. For any other account with storage, it builds the trie from the slots and fixes the account's storage root. It then builds the account trie.
6. **The root check:** the built root must be the final pivot's state root, or the attempt starts over.

In the example, the build keeps A's, C's and F's tries and makes D's from its slots.

## Sorted files

On Bonsai, the state is written as sorted files that RocksDB adds as they are, instead of through its write path of memtables and compactions (`SortedStateIngest`). `--Xsnap2-sorted-ingest-enabled=false` turns this off.

**Why.** A random write lands in level 0 and is rewritten on its way down each level of the database, and most of the state is written that way. A file whose keys do not overlap the files already there goes straight to the bottom level and is not rewritten.

**Accounts.** The accounts of a range wait in memory until every range before it is complete, then go into the account file in key order. As ranges are asked lowest first, few wait.

**Storages.** Slots go into the storage file in account order: a storage waits until it is complete and the accounts before it are done. A storage too large to wait gets files of its own. Whatever waits is capped at a twelfth of the heap, between 64 MiB and 2 GiB; past it, the largest waiting storages and ranges go through the regular write path. Under 1 MiB, nothing is worth a file.

**Pivot moves** flush everything that waits, as the diff reads and overwrites the state, so a move writes it through the regular path.

**The bulk load.** During the sync, RocksDB holds back the compaction of the account, slot and trie segments, so that the files stay where they are put. Bytecodes come in no key order, so their values go into blob files and compaction moves only their keys.

**Before the final build,** the slot segment leaves the bulk load, and the build waits until its level 0 is merged. The slots that went through the regular write path pile up there, and the build opens a slot iterator per account, each of which would open every file of level 0.

**The final build** writes the storage tries of each partition as one sorted file, and each partition's account nodes as files of their own. The partitions built at the same time share the same memory cap for the account nodes they hold.

**After the sync,** the bulk load ends: RocksDB merges level 0 down with its limits lifted and 2 extra background jobs, then merges the small files the load left at the bottom level.

## Restarts and stops

An attempt starts over with a `RestartException`, at most 3 times:

- the state download failed,
- the built root differs from the pivot's state root,
- the chain reorganised below the pivot,
- a pivot move failed partway through its diff,
- the checkpoint header is not available.

Each restart wipes the state. A header download that fails for good, for example because the consensus client follows another chain, ends the sync instead.

A stop cancels the attempt, and `SyncStateStorage.abandonSync` ends the bulk load, so that a resync on the same database can start its own. A node restarted during a sync finds `snap2-sync-in-progress` and syncs again from the start.

## After the sync

- **The back-fill** downloads the bodies and receipts below the window, down to the floor or the oldest block the chain pruner keeps, on 8 `snap2-backfill` threads. It pauses while the node is behind the head. `--Xsnap2-backfill-enabled=false` turns it off.
- **Serving:** with `--snapsync-server-enabled` on Bonsai, the node serves snap/2, block access lists included, to peers that sync.

## Threads

| Thread | Count | Runs |
|---|---|---|
| `snap2-sync-N` | as needed | the attempts, the final build's steps, the header pipeline |
| `snap2-control` | 1 | pivot move checks every 2 s, the progress log every 30 s |
| `snap2-scheduler` | 1 | the request scheduler: which peer gets which request |
| `snap2-worker-N` | one per core | decoding answers and checking proofs, applying pivot moves |
| `snap2-state` | 1 | the state download's loop |
| `snap2-persist-N` | one per core | writing verified answers |
| `snap2-storage-trie-N` | a quarter of the cores, 1 to 4 | storage trie updates |
| `snap2-trie-builder-N` | one per core | the final build's partitions |
| `snap2-sorted-files` | 1 | adding sorted files to RocksDB |
| `snap2-window-N` | 4 | the window's bodies and receipts |
| `snap2-backfill-N` | 8 | the back-fill |

## Options

| Option | Default | Effect |
|---|---|---|
| `--sync-mode=SNAP2` | off | syncs with SNAP2 |
| `--Xsnap2-sorted-ingest-enabled` | true | writes the state as sorted files, on Bonsai |
| `--Xsnap2-backfill-enabled` | true | downloads the bodies and receipts below the window after the sync |
| `--snapsync-synchronizer-skip-pre-checkpoint-headers-enabled` | false | downloads the headers only down to the checkpoint |
| `--snapsync-server-enabled` | false | serves snap, and snap/2 on a SNAP2 node, after the sync |

## Tests

- `Snap2SyncControllerTest` and `SyncAttemptTest` run whole syncs on the test thread against `FakeSnapNetwork`, an in-memory snap server with real proofs and block access lists. `PivotMovesTest` and `SyncChainTest` test the pivot moves and the chain.
- `StateSyncTest`, `StateWriterTest` and `StateDiffApplierTest` test the download, the writes and pivot moves.
- `StorageTrieUpdaterTest` and `WorldStateTrieBuilderTest` test the tries, on Bonsai, Bonsai with sorted files and Forest.
- `SortedStateIngestTest` tests the sorted files, and `RocksDBColumnarKeyValueStorageTest` the RocksDB side.

```
./gradlew :ethereum:eth:test --tests 'org.hyperledger.besu.ethereum.eth.sync.snap2.*'
```

## Where to start reading

1. `Snap2SyncController`: pivot selection, attempts and restarts.
2. `SyncAttempt.start`: the order of the steps.
3. `StateSync`: what is requested, when, and how answers are written.
4. `PivotMoves` and `StateDiffApplier`: moving the pivot.
5. `WorldStateTrieBuilder`: the final build.
6. `SortedStateIngest` and `BonsaiSyncStateStorage`: the sorted files.

## Glossary

- **Pivot:** the block whose state the download writes. It trails the head by 64 blocks.
- **Epoch:** the number of the pivot; answers about an older epoch are dropped.
- **Pivot move:** applying the block access lists of the blocks between the pivot and a newer one to the state already written.
- **Stale pivot:** a pivot peers no longer serve, which forces a move.
- **Covered range:** an account range whose accounts are written at the current pivot.
- **Storage task:** a storage still to download, with its root, its covered ranges and its chunks.
- **Window:** the 128 blocks up to the final pivot whose bodies and receipts the sync stores.
- **Back-fill:** downloading the bodies and receipts below the window after the sync.
- **Bulk load:** the time during which RocksDB holds back the compaction of the state segments.
- **Sorted file:** a file of keys in increasing order that RocksDB adds to a segment as it is.
