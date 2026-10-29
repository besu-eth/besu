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

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.Accounts;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Updates the storage tries written during the download with the slots that pivot moves changed, as
 * a block import does. Each storage gets one update for all its moves, at the end of the download
 * in key order, or earlier past a memory budget, so that each trie is read as little as possible.
 * Until then its account keeps an unknown storage root; a storage without a trie gets its root from
 * the final build.
 */
public final class StorageTrieUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(StorageTrieUpdater.class);
  // changed slots kept in memory, some 200 bytes each, past which updates are applied early
  private static final int MAX_PENDING_SLOTS = 250_000;

  private final SyncStateStorage storage;
  private final Predicate<Runnable> withoutMoves;
  private final Predicate<Bytes32> trieLeft;
  private final int maxPendingSlots;
  private final ExecutorService workers;
  // guarded by this: the updates not started, by account, the slots they change, and the
  // accounts a worker is on
  private final TreeMap<Bytes32, Update> pending = new TreeMap<>();
  private long pendingSlots;
  private final Set<Bytes32> inProgress = new HashSet<>();
  private final AtomicLong updated = new AtomicLong();
  private boolean finishing;
  private boolean stopped;

  /** The slots moves changed in a storage, and the root of the trie to change. */
  private static final class Update {
    private final Bytes32 account;
    // null until the update in progress for the account gives it
    private Bytes32 base;
    private final Map<Bytes32, UInt256> changes = new HashMap<>();

    private Update(final Bytes32 account, final Bytes32 base) {
      this.account = account;
      this.base = base;
    }
  }

  /**
   * Creates the updater.
   *
   * @param withoutMoves runs an action exclusive with pivot moves, false once the sync stopped; the
   *     download writes the account of a downloaded storage only in a move
   * @param trieLeft whether the download completed the storage of an account without its trie
   */
  public StorageTrieUpdater(
      final SyncStateStorage storage,
      final Predicate<Runnable> withoutMoves,
      final Predicate<Bytes32> trieLeft,
      final int threads) {
    this(storage, withoutMoves, trieLeft, threads, MAX_PENDING_SLOTS);
    checkArgument(threads >= 1, "the updates need a worker");
  }

  @VisibleForTesting
  StorageTrieUpdater(
      final SyncStateStorage storage,
      final Predicate<Runnable> withoutMoves,
      final Predicate<Bytes32> trieLeft,
      final int threads,
      final int maxPendingSlots) {
    this.storage = storage;
    this.withoutMoves = withoutMoves;
    this.trieLeft = trieLeft;
    this.maxPendingSlots = maxPendingSlots;
    this.workers =
        Executors.newFixedThreadPool(
            // a pool takes at least one thread, which only starts with a worker
            Math.max(1, threads),
            Thread.ofPlatform().name("snap2-storage-trie-", 0).daemon().factory());
    for (int i = 0; i < threads; i++) {
      workers.execute(this::run);
    }
  }

  /**
   * Records slots a pivot move changed in a downloaded storage, whose account had the given root
   * before the move; moves call it exclusive with the other writes of the download.
   */
  public synchronized void changed(
      final Bytes32 accountHash, final Hash previousRoot, final Map<Bytes32, UInt256> changed) {
    // a storage left to the final build has no trie to change
    if (stopped || trieLeft.test(accountHash)) {
      return;
    }
    Update update = pending.get(accountHash);
    if (update == null) {
      final Bytes32 base;
      if (!Accounts.UNKNOWN_STORAGE_ROOT.equals(previousRoot)) {
        base = Bytes32.wrap(previousRoot.getBytes());
      } else if (inProgress.contains(accountHash)) {
        base = null;
      } else {
        // no trie to change: the final build makes it
        return;
      }
      update = new Update(accountHash.copy(), base);
      pending.put(update.account, update);
    }
    for (final Map.Entry<Bytes32, UInt256> slot : changed.entrySet()) {
      if (update.changes.put(slot.getKey().copy(), slot.getValue()) == null) {
        pendingSlots++;
      }
    }
    if (pendingSlots > maxPendingSlots) {
      notifyAll();
    }
  }

  /** Returns the number of storage tries changed. */
  public long updated() {
    return updated.get();
  }

  /** Returns the number of storages whose changes wait to be applied. */
  public synchronized int pendingStorages() {
    return pending.size();
  }

  /** Returns the number of changed slots waiting to be applied. */
  public synchronized long pendingSlots() {
    return pendingSlots;
  }

  /**
   * Applies the changes still waiting, on this thread too, then stops. As for {@link #stop()}, the
   * caller must not hold what {@code withoutMoves} waits for.
   */
  public void finish() {
    synchronized (this) {
      if (!pending.isEmpty()) {
        LOG.info("SNAP2 applying the pivot moves to {} storage tries", pending.size());
      }
      finishing = true;
      notifyAll();
    }
    try {
      // this thread applies updates beside the workers, so finishing needs none
      applyUpdates(true);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    stop();
  }

  /**
   * Stops, once the updates in progress wrote what they changed. A worker may be waiting in {@code
   * withoutMoves}, so the caller must not hold what it waits for.
   */
  public void stop() {
    cancel();
    workers.shutdownNow();
    try {
      if (!workers.awaitTermination(1, TimeUnit.MINUTES)) {
        LOG.warn("Storage trie updates still running a minute after they were stopped");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  // no update starts or publishes a root from now on
  private synchronized void cancel() {
    stopped = true;
    pending.clear();
    pendingSlots = 0;
    notifyAll();
  }

  /** Applies on this thread an update that can start, returns whether there was one. */
  @VisibleForTesting
  boolean applyNext() {
    final Update update;
    synchronized (this) {
      update = stopped ? null : ready();
    }
    if (update == null) {
      return false;
    }
    apply(update, storage.trieWriter());
    return true;
  }

  private void run() {
    try {
      applyUpdates(false);
    } catch (final InterruptedException e) {
      // stopped while waiting
    }
  }

  // applies updates until the updates stop, or until none is left when draining
  private void applyUpdates(final boolean draining) throws InterruptedException {
    final SyncStateStorage.TrieWriter writer = storage.trieWriter();
    boolean running = true;
    while (running) {
      final Update update = next(draining);
      running = update != null && apply(update, writer);
    }
  }

  // the next update to apply; null once stopped, or once none is left when draining
  private synchronized Update next(final boolean draining) throws InterruptedException {
    while (!stopped) {
      final Update update = ready();
      if (update != null) {
        return update;
      }
      if (draining && pending.isEmpty() && inProgress.isEmpty()) {
        return null;
      }
      wait();
    }
    return null;
  }

  // takes, in key order, the first update of an account no one is on, once updates may start
  private Update ready() {
    if (finishing || pendingSlots > maxPendingSlots) {
      for (final Iterator<Update> updates = pending.values().iterator(); updates.hasNext(); ) {
        final Update update = updates.next();
        if (!inProgress.contains(update.account)) {
          updates.remove();
          pendingSlots -= update.changes.size();
          inProgress.add(update.account);
          return update;
        }
      }
    }
    return null;
  }

  // applies an update, returns false once the updates stopped
  private boolean apply(final Update update, final SyncStateStorage.TrieWriter writer) {
    try {
      process(update, writer);
      return true;
    } catch (final CancellationException e) {
      // the sync stopped: the other updates stop too
      cancel();
      return false;
    } catch (final RuntimeException e) {
      writer.abort();
      LOG.info(
          "SNAP2 left the storage trie of {} to the final build: {}",
          update.account,
          e.getMessage());
      return true;
    } finally {
      release(update.account);
    }
  }

  private synchronized void release(final Bytes32 account) {
    inProgress.remove(account);
    notifyAll();
  }

  private void process(final Update update, final SyncStateStorage.TrieWriter writer) {
    // a storage whose trie was left to the final build, or whose previous update failed
    if (update.base == null || !hasTrie(update.account, update.base)) {
      return;
    }
    final Bytes32 root = change(update, writer);
    publish(update.account, root);
    updated.incrementAndGet();
  }

  private boolean hasTrie(final Bytes32 accountHash, final Bytes32 root) {
    return root.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)
        || storage.hasStorageTrie(accountHash, root);
  }

  // applies the changed slots to the stored trie and commits it, the root node last
  private Bytes32 change(final Update update, final SyncStateStorage.TrieWriter writer) {
    final Bytes32 accountHash = update.account;
    final StoredMerklePatriciaTrie<Bytes32, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) -> storage.storageTrieNode(accountHash, location, hash),
            update.base,
            Function.identity(),
            Function.identity());
    update.changes.forEach(
        (slot, value) -> {
          if (value.isZero()) {
            trie.remove(slot);
          } else {
            trie.put(slot, RLP.encodeOne(value.toMinimalBytes()));
          }
        });
    trie.commit((location, hash, node) -> writer.storageNode(accountHash, location, hash, node));
    writer.commit();
    return trie.getRootHash();
  }

  // gives the root to the next update of the account, or to the account
  private void publish(final Bytes32 accountHash, final Bytes32 root) {
    final boolean running =
        withoutMoves.test(
            () -> {
              final Update next;
              synchronized (this) {
                // a move after the stop left the root unknown, for the final build
                if (stopped) {
                  throw new CancellationException("storage trie updates stopped");
                }
                next = pending.get(accountHash);
                if (next != null) {
                  next.base = root;
                }
              }
              if (next == null) {
                writeRoot(accountHash, root);
              }
            });
    if (!running) {
      throw new CancellationException("storage trie updates stopped");
    }
  }

  // an account a move removed gets no root
  private void writeRoot(final Bytes32 accountHash, final Bytes32 root) {
    storage
        .getAccount(accountHash)
        .map(Accounts::decode)
        .map(account -> Accounts.withStorageRoot(account, Hash.wrap(root)))
        .map(Accounts::encode)
        .ifPresent(encoded -> storage.write(batch -> batch.putAccount(accountHash, encoded)));
  }
}
