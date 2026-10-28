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
package org.hyperledger.besu.ethereum.eth.sync.snap2.trie;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.Accounts;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.patricia.AscendingCollapsePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.AscendingCollapsePatriciaTrie.Subtree;
import org.hyperledger.besu.ethereum.trie.patricia.AscendingCollapsePatriciaTrie.SubtreeBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the account trie and the storage tries from the flat state, in parallel over the 256
 * first-byte partitions of the account key space, and fixes the storage roots of the flat accounts.
 *
 * <p>A storage trie the download wrote is kept without being read again, once its root node is
 * found: a storage with a known root has its trie unless the download left it, and a storage a
 * pivot move changed has an unknown root until the trie updater updates its trie.
 */
public final class WorldStateTrieBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(WorldStateTrieBuilder.class);
  private static final int PARTITIONS = 256;
  private static final int FIXES_PER_COMMIT = 10_000;
  private static final int SLOTS_PER_CANCEL_CHECK = 65_536;

  private final SyncStateStorage storage;
  private final Predicate<Bytes32> trieLeft;
  private final Supplier<ExecutorService> executors;
  private final int fixesPerCommit;
  private final LongAdder accounts = new LongAdder();
  private final LongAdder slots = new LongAdder();
  private final LongAdder keptStorageTries = new LongAdder();
  private final AtomicBoolean cancelled = new AtomicBoolean();
  private final AtomicReference<Throwable> failure = new AtomicReference<>();

  /**
   * Creates a build.
   *
   * @param trieLeft whether the download completed the storage of an account without its trie; the
   *     partitions call it from several threads
   */
  public WorldStateTrieBuilder(
      final SyncStateStorage storage, final int threads, final Predicate<Bytes32> trieLeft) {
    this(
        storage,
        trieLeft,
        () ->
            Executors.newFixedThreadPool(
                Math.max(1, threads),
                Thread.ofPlatform().name("snap2-trie-builder-", 0).daemon().factory()),
        FIXES_PER_COMMIT);
  }

  /**
   * Creates a build whose partitions run on the executor {@code executors} gives, and commit their
   * fixes {@code fixesPerCommit} at a time.
   */
  @VisibleForTesting
  WorldStateTrieBuilder(
      final SyncStateStorage storage,
      final Predicate<Bytes32> trieLeft,
      final Supplier<ExecutorService> executors,
      final int fixesPerCommit) {
    this.storage = storage;
    this.trieLeft = trieLeft;
    this.executors = executors;
    this.fixesPerCommit = fixesPerCommit;
  }

  /**
   * Result of a build.
   *
   * @param root the state root
   * @param rootNode the encoding of the root node
   * @param accounts the number of accounts
   * @param slots the number of storage slots read to build tries
   * @param keptStorageTries the number of storage tries built while downloading and kept
   */
  public record Result(
      Bytes32 root, Bytes rootNode, long accounts, long slots, long keptStorageTries) {}

  /** Returns the number of accounts processed so far. */
  public long accountsProcessed() {
    return accounts.sum();
  }

  /** Returns the number of slots processed so far. */
  public long slotsProcessed() {
    return slots.sum();
  }

  /** Returns the number of storage tries built while downloading and kept so far. */
  public long keptStorageTries() {
    return keptStorageTries.sum();
  }

  /** Stops a running build, which then throws a {@link CancellationException}. */
  public void cancel() {
    cancelled.set(true);
  }

  /** Builds the tries and writes their nodes; an instance builds once. */
  public Result build() {
    final ExecutorService executor = executors.get();
    final SyncStateStorage.TrieWriter writer = storage.trieWriter();
    try {
      // the sink gets the root node last, at the empty location
      final Bytes[] rootNode = {MerkleTrie.EMPTY_TRIE_NODE};
      final AscendingCollapsePatriciaTrie top =
          new AscendingCollapsePatriciaTrie(
              (location, hash, rlp) -> {
                writer.accountNode(location, hash, rlp);
                if (location.isEmpty()) {
                  rootNode[0] = rlp;
                }
              });
      final List<Future<Optional<Subtree>>> parts = new ArrayList<>(PARTITIONS);
      for (int prefix = 0; prefix < PARTITIONS; prefix++) {
        final int p = prefix;
        parts.add(executor.submit(() -> buildPartition(top, p)));
      }
      for (final Future<Optional<Subtree>> part : parts) {
        part.get().ifPresent(top::insert);
      }
      final Bytes32 root = top.rootHash();
      writer.commit();
      if (cancelled.get()) {
        throw new CancellationException("trie build cancelled");
      }
      return new Result(root, rootNode[0], accounts.sum(), slots.sum(), keptStorageTries.sum());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CancellationException("trie build interrupted");
    } catch (final ExecutionException e) {
      final Throwable first = Objects.requireNonNullElse(failure.get(), e.getCause());
      Throwables.throwIfUnchecked(first);
      throw new IllegalStateException("trie build failed", first);
    } finally {
      // whatever ended the build stops the partitions, and drops the nodes it did not commit
      cancel();
      executor.shutdownNow();
      awaitPartitions(executor);
      writer.abort();
    }
  }

  // no partition writes once the build returned, even when interrupted
  private static void awaitPartitions(final ExecutorService executor) {
    if (!Uninterruptibles.awaitTerminationUninterruptibly(executor, 1, TimeUnit.MINUTES)) {
      LOG.warn("Trie build partitions still running a minute after the build stopped");
    }
  }

  // the accounts of one first byte, as a subtree of the account trie; none if it has no account
  private Optional<Subtree> buildPartition(
      final AscendingCollapsePatriciaTrie top, final int prefix) {
    final byte[] from = new byte[32];
    final byte[] to = RangeManager.MAX_RANGE.toArray();
    from[0] = (byte) prefix;
    to[0] = (byte) prefix;
    final SyncStateStorage.TrieWriter writer = storage.trieWriter();
    final PartitionVisitor visitor =
        new PartitionVisitor(writer, top.subtreeBuilder(writer::accountNode));
    try {
      storage.scan(Bytes32.wrap(from), Bytes32.wrap(to), visitor);
      visitor.finishAccount();
      final Optional<Subtree> subtree =
          visitor.accountTrie.isEmpty()
              ? Optional.empty()
              : Optional.of(visitor.accountTrie.build());
      visitor.commit();
      return subtree;
    } catch (final Throwable e) {
      // the other partitions stop too, and the build reports the first failure
      failure.compareAndSet(null, e);
      cancel();
      visitor.abort();
      throw e;
    }
  }

  private final class PartitionVisitor implements SyncStateStorage.ScanVisitor {
    private final SyncStateStorage.TrieWriter writer;
    private final SubtreeBuilder accountTrie;
    private SyncStateStorage.Batch fixes;
    private int pendingFixes;
    // the account whose slots come next, null once it is finished
    private Bytes32 accountHash;
    private Bytes account;
    private PatriciaTrieAccountValue accountValue;
    private AscendingCollapsePatriciaTrie storageTrie;
    private long partitionSlots;

    private PartitionVisitor(
        final SyncStateStorage.TrieWriter writer, final SubtreeBuilder accountTrie) {
      this.writer = writer;
      this.accountTrie = accountTrie;
    }

    @Override
    public boolean onAccount(final Bytes32 hash, final Bytes rlp) {
      finishAccount();
      if (cancelled.get()) {
        throw new CancellationException("trie build cancelled");
      }
      final PatriciaTrieAccountValue value = Accounts.decode(rlp);
      final Hash storageRoot = value.getStorageRoot();
      // the trie the download wrote is kept if its root node is stored, as the trie updater checks
      if (Accounts.hasKnownStorageTrie(storageRoot)
          && !trieLeft.test(hash)
          && storage.hasStorageTrie(hash, Bytes32.wrap(storageRoot.getBytes()))) {
        accountTrie.insert(hash, rlp);
        accounts.increment();
        keptStorageTries.increment();
        return false;
      }
      accountHash = hash;
      account = rlp;
      accountValue = value;
      return true;
    }

    @Override
    public void onSlot(final Bytes32 slotAccount, final Bytes32 slotHash, final Bytes value) {
      if (!slotAccount.equals(accountHash)) {
        LOG.debug("Dropping slot {} of missing account {}", slotHash, slotAccount);
        fixes().removeStorage(slotAccount, slotHash);
        return;
      }
      if (++partitionSlots % SLOTS_PER_CANCEL_CHECK == 0 && cancelled.get()) {
        throw new CancellationException("trie build cancelled");
      }
      if (storageTrie == null) {
        // made on the first slot, most accounts have none
        final Bytes32 hash = accountHash;
        storageTrie =
            new AscendingCollapsePatriciaTrie(
                (location, nodeHash, node) -> writer.storageNode(hash, location, nodeHash, node));
      }
      storageTrie.insert(slotHash, RLP.encodeOne(value.trimLeadingZeros()));
      slots.increment();
    }

    private void finishAccount() {
      if (accountHash == null) {
        return;
      }
      final Bytes32 storageRoot =
          storageTrie == null ? MerkleTrie.EMPTY_TRIE_NODE_HASH : storageTrie.rootHash();
      Bytes leaf = account;
      if (!accountValue.getStorageRoot().getBytes().equals(storageRoot)) {
        leaf = Accounts.encode(Accounts.withStorageRoot(accountValue, Hash.wrap(storageRoot)));
        fixes().putAccount(accountHash, leaf);
      }
      accountTrie.insert(accountHash, leaf);
      accounts.increment();
      accountHash = null;
      storageTrie = null;
    }

    // the batch for one more fix, committing a full one first
    private SyncStateStorage.Batch fixes() {
      if (pendingFixes == fixesPerCommit) {
        commit();
      }
      if (fixes == null) {
        fixes = storage.batch();
      }
      pendingFixes++;
      return fixes;
    }

    // the nodes before the fixes: a committed root always has its trie
    private void commit() {
      writer.commit();
      final SyncStateStorage.Batch batch = fixes;
      // dropped first, so a failed commit leaves no batch to abort
      fixes = null;
      pendingFixes = 0;
      if (batch != null) {
        batch.commit();
      }
    }

    private void abort() {
      if (fixes != null) {
        fixes.abort();
        fixes = null;
      }
      writer.abort();
    }
  }
}
