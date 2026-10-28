/*
 * Copyright contributors to Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import static org.hyperledger.besu.ethereum.eth.sync.snap2.state.StorageTask.CoverResult.ALREADY_COVERED;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.state.StorageTask.CoverResult.COMPLETED;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StorageTask.CoverResult;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.Accounts;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes the verified responses of the state download to the flat state, with the storage trie
 * nodes their proofs give, and records what they cover. The download runs each write at the pivot
 * of its request, exclusive with the pivot moves.
 */
final class StateWriter {

  private static final Logger LOG = LoggerFactory.getLogger(StateWriter.class);

  private final SyncStateStorage storage;
  private final StateCoverage coverage;
  private final AtomicLong accountsWritten = new AtomicLong();
  private final AtomicLong slotsWritten = new AtomicLong();
  private final AtomicLong codesWritten = new AtomicLong();
  private final AtomicLong storageTriesWritten = new AtomicLong();
  private final AtomicLong storageTrieSlots = new AtomicLong();
  // storages complete without their trie, which the final build makes
  private final Set<Bytes32> storageTriesLeft = ConcurrentHashMap.newKeySet();

  StateWriter(final SyncStateStorage storage, final StateCoverage coverage) {
    this.storage = storage;
    this.coverage = coverage;
  }

  /**
   * What an account range wrote.
   *
   * @param end the last key it covers
   * @param done whether it covers the range up to the limit asked
   * @param storageTasks the storages of its accounts
   * @param codeHashes the bytecodes of its accounts
   */
  record AccountsWritten(
      Bytes32 end, boolean done, List<StorageTask> storageTasks, Set<Bytes32> codeHashes) {}

  /** Writes the accounts of a range up to its limit, and covers the range. */
  AccountsWritten accounts(
      final AccountRangeRequest request, final AccountRangeRequest.Result result) {
    final List<Bytes32> hashes = result.hashes();
    // the last account may lie past the limit, as the proof reaches it
    final int count =
        (int) hashes.stream().takeWhile(hash -> hash.compareTo(request.limit) <= 0).count();
    final List<StorageTask> storageTasks = new ArrayList<>();
    final Set<Bytes32> codeHashes = new LinkedHashSet<>();
    write(
        batch -> {
          for (int i = 0; i < count; i++) {
            final Bytes account = result.accounts().get(i);
            batch.putAccount(hashes.get(i), account);
            final PatriciaTrieAccountValue value = Accounts.decode(account);
            if (!value.getStorageRoot().equals(Hash.EMPTY_TRIE_HASH)) {
              storageTasks.add(
                  new StorageTask(hashes.get(i), Bytes32.wrap(value.getStorageRoot().getBytes())));
            }
            if (!value.getCodeHash().equals(Hash.EMPTY)) {
              codeHashes.add(Bytes32.wrap(value.getCodeHash().getBytes()));
            }
          }
        });
    final boolean done = reachesLimit(hashes, result.hasMore(), request.limit);
    final Bytes32 end = coveredEnd(hashes, result.hasMore(), request.limit);
    storageTasks.forEach(coverage::addStorageTask);
    coverage.accounts().add(request.origin, end);
    accountsWritten.addAndGet(count);
    return new AccountsWritten(end, done, storageTasks, codeHashes);
  }

  /**
   * What a storage response wrote.
   *
   * @param served the storages served, the first ones asked
   * @param lastSlot the last slot returned for the last storage, null if none
   * @param lastComplete whether the last storage is complete up to the limit asked
   * @param lastReturned the number of slots returned for the last storage
   */
  record StorageWritten(int served, Bytes32 lastSlot, boolean lastComplete, int lastReturned) {}

  /**
   * Writes the slots of the pending storages served and the trie nodes of their ranges. A storage
   * they complete gets the root node of its trie, or is left to the final build.
   */
  StorageWritten storages(
      final StorageRangesRequest request, final StorageRangesRequest.Result result) {
    final SyncStateStorage.TrieWriter writer = storage.trieWriter();
    try {
      final List<Covered> covered = storeRanges(request, result, writer);
      cover(request.origin, covered, writer);
    } catch (final RuntimeException e) {
      writer.abort();
      throw e;
    }
    final StorageRangesRequest.Slots last = result.accounts().getLast();
    return new StorageWritten(
        result.accounts().size(),
        last.hashes().isEmpty() ? null : last.hashes().getLast(),
        reachesLimit(last.hashes(), !last.complete(), request.limit),
        last.hashes().size());
  }

  /**
   * Writes an account fetched again at the pivot, or removes it if gone. Returns false, writing
   * nothing, once its storage is no longer pending.
   */
  boolean refresh(final StorageTask task, final AccountRangeRequest.Result result) {
    if (!coverage.isPending(task)) {
      return false;
    }
    final Bytes32 hash = task.accountHash();
    if (!result.startsWith(hash)) {
      LOG.debug("Account {} no longer exists at the pivot, dropping it", hash);
      write(batch -> batch.removeAccount(hash));
      dropStorage(task);
      return true;
    }
    final Bytes account = result.accounts().getFirst();
    write(batch -> batch.putAccount(hash, account));
    final Hash storageRoot = Accounts.decode(account).getStorageRoot();
    if (storageRoot.equals(Hash.EMPTY_TRIE_HASH)) {
      dropStorage(task);
    } else {
      task.refreshRoot(Bytes32.wrap(storageRoot.getBytes()));
    }
    return true;
  }

  /** Writes bytecodes. */
  void codes(final Map<Bytes32, Bytes> received) {
    write(batch -> received.forEach(batch::putCode));
    codesWritten.addAndGet(received.size());
  }

  /** Returns the number of accounts written. */
  long accountsWritten() {
    return accountsWritten.get();
  }

  /** Returns the number of slots written. */
  long slotsWritten() {
    return slotsWritten.get();
  }

  /** Returns the number of bytecodes written. */
  long codesWritten() {
    return codesWritten.get();
  }

  /** Returns the number of storage tries written. */
  long storageTriesWritten() {
    return storageTriesWritten.get();
  }

  /** Returns the number of slots in the storage tries written. */
  long storageTrieSlots() {
    return storageTrieSlots.get();
  }

  /** Returns the number of storages complete without their trie. */
  long storageTriesLeft() {
    return storageTriesLeft.size();
  }

  /** Returns whether the storage of an account is complete without its trie. */
  boolean isTrieLeft(final Bytes32 accountHash) {
    return storageTriesLeft.contains(accountHash);
  }

  // writes a batch and commits it, or drops it if a write fails
  private void write(final Consumer<SyncStateStorage.Batch> writes) {
    final SyncStateStorage.Batch batch = storage.batch();
    try {
      writes.accept(batch);
    } catch (final RuntimeException e) {
      batch.abort();
      throw e;
    }
    batch.commit();
  }

  private record Covered(StorageTask task, Bytes32 end, int slots) {}

  // stores the slots of each pending storage served and the trie nodes of its range
  private List<Covered> storeRanges(
      final StorageRangesRequest request,
      final StorageRangesRequest.Result result,
      final SyncStateStorage.TrieWriter writer) {
    final List<Covered> covered = new ArrayList<>();
    write(
        batch -> {
          for (int i = 0; i < result.accounts().size(); i++) {
            final StorageTask task = request.tasks.get(i);
            final StorageRangesRequest.Slots slots = result.accounts().get(i);
            if (coverage.isPending(task)) {
              final int stored = storeSlots(task, slots, request.limit, batch);
              writeTrie(task, slots, writer);
              final Bytes32 end = coveredEnd(slots.hashes(), !slots.complete(), request.limit);
              covered.add(new Covered(task, end, stored));
            }
          }
          // the nodes first: if their commit fails, the slots are dropped too
          writer.commit();
        });
    return covered;
  }

  // the slots up to the limit, returns how many it stored
  private static int storeSlots(
      final StorageTask task,
      final StorageRangesRequest.Slots slots,
      final Bytes32 limit,
      final SyncStateStorage.Batch batch) {
    int stored = 0;
    for (int i = 0; i < slots.hashes().size() && slots.hashes().get(i).compareTo(limit) <= 0; i++) {
      final Bytes32 value = Bytes32.leftPad(RLP.decodeOne(slots.values().get(i)));
      if (!value.isZero()) {
        batch.putStorage(task.accountHash(), slots.hashes().get(i), value);
        stored++;
      }
    }
    return stored;
  }

  // whether no key is left up to the limit; the last key returned may lie past it
  private static boolean reachesLimit(
      final List<Bytes32> keys, final boolean hasMore, final Bytes32 limit) {
    return keys.isEmpty() || !hasMore || keys.getLast().compareTo(limit) >= 0;
  }

  // the end of the range a response covers: the limit once reached, else the last key returned
  private static Bytes32 coveredEnd(
      final List<Bytes32> keys, final boolean hasMore, final Bytes32 limit) {
    return reachesLimit(keys, hasMore, limit) ? limit : keys.getLast();
  }

  // the nodes of a range at the root of its trie, the root node kept until every range is in
  private static void writeTrie(
      final StorageTask task,
      final StorageRangesRequest.Slots slots,
      final SyncStateStorage.TrieWriter writer) {
    if (!task.writesTrieAt(slots.root())) {
      return;
    }
    Optional<Bytes> rootNode = Optional.empty();
    for (final StorageRangesRequest.TrieNode node : slots.nodes()) {
      if (node.location().isEmpty()) {
        rootNode = Optional.of(node.rlp());
      } else {
        writer.storageNode(task.accountHash(), node.location(), node.hash(), node.rlp());
      }
    }
    task.trieWritten(rootNode);
  }

  // covers the stored ranges, then writes the root nodes of the storages they complete
  private void cover(
      final Bytes32 origin, final List<Covered> covered, final SyncStateStorage.TrieWriter writer) {
    final List<StorageTask> completed = new ArrayList<>();
    for (final Covered range : covered) {
      final StorageTask task = range.task();
      final CoverResult outcome = task.cover(origin, range.end(), range.slots());
      if (outcome != ALREADY_COVERED) {
        slotsWritten.addAndGet(range.slots());
      }
      if (outcome == COMPLETED) {
        coverage.removeStorageTask(task);
        completed.add(task);
      }
    }
    writeRootNodes(completed, writer);
  }

  // a completed storage gets the root node of its trie, or is left to the final build
  private void writeRootNodes(
      final List<StorageTask> completed, final SyncStateStorage.TrieWriter writer) {
    final List<StorageTask> withTrie = new ArrayList<>();
    try {
      for (final StorageTask task : completed) {
        final Optional<Bytes> rootNode = task.trieRootNode();
        if (rootNode.isPresent()) {
          writer.storageNode(task.accountHash(), Bytes.EMPTY, task.storageRoot(), rootNode.get());
          withTrie.add(task);
        } else {
          storageTriesLeft.add(task.accountHash());
        }
      }
      writer.commit();
    } catch (final RuntimeException e) {
      // their root nodes may be lost: the final build makes their tries
      completed.forEach(task -> storageTriesLeft.add(task.accountHash()));
      throw e;
    }
    storageTriesWritten.addAndGet(withTrie.size());
    storageTrieSlots.addAndGet(withTrie.stream().mapToLong(StorageTask::slots).sum());
  }

  private void dropStorage(final StorageTask task) {
    storage.deleteStorage(task.accountHash());
    coverage.removeStorageTask(task);
  }
}
