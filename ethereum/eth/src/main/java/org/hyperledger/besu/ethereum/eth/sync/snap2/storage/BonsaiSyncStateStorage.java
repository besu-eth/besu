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
package org.hyperledger.besu.ethereum.eth.sync.snap2.storage;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SegmentBulkLoad;
import org.hyperledger.besu.plugin.services.storage.SortedWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sync storage writing straight into the Bonsai flat database and trie node store. */
public final class BonsaiSyncStateStorage extends AbstractSyncStateStorage {

  // the sync writes no trie log, so its updaters get a transaction that holds nothing to release
  private static final KeyValueStorageTransaction NO_TRIE_LOG =
      new KeyValueStorageTransaction() {
        @Override
        public void put(final byte[] key, final byte[] value) {
          throw new UnsupportedOperationException("the sync writes no trie log");
        }

        @Override
        public void remove(final byte[] key) {
          throw new UnsupportedOperationException("the sync writes no trie log");
        }

        @Override
        public void commit() {}

        @Override
        public void rollback() {}

        @Override
        public void close() {}
      };

  // the storage nodes of one account sorted in memory before going to a file
  private static final long MAX_SORTED_STORAGE_BYTES = 32L * 1024 * 1024;

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiSyncStateStorage.class);

  private final BonsaiWorldStateKeyValueStorage bonsai;
  private final long maxSortedStorageBytes;
  private final boolean sortedFlatState;
  private final boolean trieHeldBack;
  private volatile SortedIngestStats stats = new SortedIngestStats();
  private volatile SortedStateIngest ingest;
  private volatile SegmentBulkLoad bulkLoad;

  public BonsaiSyncStateStorage(final BonsaiWorldStateKeyValueStorage bonsai) {
    this(bonsai, false, false);
  }

  /**
   * Creates the storage.
   *
   * @param bonsai the Bonsai storage
   * @param sortedFlatState whether to write the downloaded accounts and slots, and the nodes of the
   *     final trie build, as sorted files, with the compaction of the flat state segments held back
   *     until the sync is complete
   * @param trieHeldBack whether the compaction of the trie node segment waits too
   */
  public BonsaiSyncStateStorage(
      final BonsaiWorldStateKeyValueStorage bonsai,
      final boolean sortedFlatState,
      final boolean trieHeldBack) {
    this(bonsai, MAX_SORTED_STORAGE_BYTES, sortedFlatState, trieHeldBack);
  }

  @VisibleForTesting
  BonsaiSyncStateStorage(
      final BonsaiWorldStateKeyValueStorage bonsai, final long maxSortedStorageBytes) {
    this(bonsai, maxSortedStorageBytes, true, true);
  }

  private BonsaiSyncStateStorage(
      final BonsaiWorldStateKeyValueStorage bonsai,
      final long maxSortedStorageBytes,
      final boolean sortedFlatState,
      final boolean trieHeldBack) {
    super(bonsai.getComposedWorldStateStorage(), ACCOUNT_INFO_STATE, ACCOUNT_STORAGE_STORAGE);
    this.bonsai = bonsai;
    this.maxSortedStorageBytes = maxSortedStorageBytes;
    this.sortedFlatState = sortedFlatState;
    this.trieHeldBack = trieHeldBack;
  }

  @Override
  public Optional<SortedWrites> sortedWrites() {
    return Optional.ofNullable(ingest);
  }

  @Override
  public void deleteStorage(final Bytes32 accountHash) {
    final SortedStateIngest current = ingest;
    if (current != null) {
      current.storageRemoved(accountHash);
    }
    super.deleteStorage(accountHash);
  }

  @Override
  public boolean hasCode(final Bytes32 codeHash) {
    return flat.containsKey(CODE_STORAGE, codeHash.toArrayUnsafe());
  }

  // the writes of Bonsai's own updater, through its flat database strategy
  @Override
  public Batch batch() {
    final BonsaiWorldStateKeyValueStorage.Updater updater = newUpdater();
    return new FlatBatch() {
      @Override
      public void putAccount(final Bytes32 accountHash, final Bytes account) {
        updater.putAccountInfoState(Hash.wrap(accountHash), account);
      }

      @Override
      public void removeAccount(final Bytes32 accountHash) {
        updater.removeAccountInfoState(Hash.wrap(accountHash));
      }

      @Override
      public void putStorage(
          final Bytes32 accountHash, final Bytes32 slotHash, final Bytes32 value) {
        updater.putStorageValueBySlotHash(Hash.wrap(accountHash), Hash.wrap(slotHash), value);
      }

      @Override
      public void removeStorage(final Bytes32 accountHash, final Bytes32 slotHash) {
        updater.removeStorageValueBySlotHash(Hash.wrap(accountHash), Hash.wrap(slotHash));
      }

      // stored by code hash, which prepareForSync requires
      @Override
      public void putCode(final Bytes32 codeHash, final Bytes code) {
        updater.putCode(Hash.ZERO, Hash.wrap(codeHash), code);
      }

      @Override
      protected void commitWrites() {
        updater.commitComposedOnly();
      }

      @Override
      protected void dropWrites() {
        updater.rollback();
      }
    };
  }

  @Override
  public Optional<Bytes> storageTrieNode(
      final Bytes32 accountHash, final Bytes location, final Bytes32 hash) {
    return bonsai.getTrieNode(TrieNodeKey.of(Hash.wrap(accountHash), location), hash);
  }

  @Override
  protected NodeBatch nodeBatch() {
    final BonsaiWorldStateKeyValueStorage.Updater updater = newUpdater();
    return new NodeBatch() {
      @Override
      public void putAccountNode(final Bytes location, final Bytes32 hash, final Bytes rlp) {
        updater.putTrieNode(location, hash, rlp);
      }

      @Override
      public void putStorageNode(
          final Bytes32 accountHash, final Bytes location, final Bytes32 hash, final Bytes rlp) {
        updater.putTrieNode(TrieNodeKey.of(Hash.wrap(accountHash), location), hash, rlp);
      }

      @Override
      public void commit() {
        updater.commitComposedOnly();
      }

      @Override
      public void abort() {
        updater.rollback();
      }
    };
  }

  @Override
  public TrieWriter partitionTrieWriter() {
    // the files hold the nodes under the keys of the default strategy only
    return sortedFlatState
            && bonsai.getTrieNodeStrategy().getClass() == BonsaiTrieNodeStrategy.class
        ? new SortedTrieWriter(trieWriter())
        : trieWriter();
  }

  /**
   * Writes the trie nodes of a partition as sorted files, which skip the compactions of the trie
   * segment. The storage nodes of each account are sorted and added to the file of the partition. A
   * storage too large to sort in memory gets files of its own, one sorted chunk at a time, and the
   * file of the partition ends before it so that the two do not overlap. The account nodes of the
   * partition go into a file of their own at the end. Nothing takes the usual writes: a memtable
   * flushed during the build holds keys of every partition, and every file ingested after it would
   * then stay in level 0.
   */
  private final class SortedTrieWriter implements TrieWriter {
    // the account nodes of a partition are held for as many bytes as this many storage chunks
    private static final int ACCOUNT_NODE_CHUNKS = 8;

    private final TrieWriter usual;
    private final List<SortedNode> nodes = new ArrayList<>();
    private final List<SortedNode> accountNodes = new ArrayList<>();
    private Bytes32 accountHash;
    private long sortedBytes;
    private long accountBytes;
    private boolean tooLarge;
    private SortedWriter file;

    private record SortedNode(byte[] key, byte[] rlp) {}

    private SortedTrieWriter(final TrieWriter usual) {
      this.usual = usual;
    }

    @Override
    public void accountNode(final Bytes location, final Bytes32 hash, final Bytes rlp) {
      if (hash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
        return;
      }
      accountNodes.add(new SortedNode(location.toArrayUnsafe(), rlp.toArrayUnsafe()));
      accountBytes += location.size() + rlp.size();
      if (accountBytes > ACCOUNT_NODE_CHUNKS * maxSortedStorageBytes) {
        writeSorted(accountNodes);
        accountBytes = 0;
      }
    }

    @Override
    public void storageNode(
        final Bytes32 accountHash, final Bytes location, final Bytes32 hash, final Bytes rlp) {
      if (hash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
        return;
      }
      if (!accountHash.equals(this.accountHash)) {
        finishAccount();
        this.accountHash = accountHash.copy();
      }
      final byte[] key = TrieNodeKey.of(Hash.wrap(accountHash), location).toArrayUnsafe();
      nodes.add(new SortedNode(key, rlp.toArrayUnsafe()));
      sortedBytes += key.length + rlp.size();
      if (sortedBytes > maxSortedStorageBytes) {
        // the storage gets files of its own: the file of the partition ends before it
        closeFile();
        writeSorted(nodes);
        sortedBytes = 0;
        tooLarge = true;
      }
    }

    @Override
    public void commit() {
      finishAccount();
      closeFile();
      writeSorted(accountNodes);
      accountBytes = 0;
      usual.commit();
    }

    @Override
    public void abort() {
      nodes.clear();
      accountNodes.clear();
      sortedBytes = 0;
      accountBytes = 0;
      tooLarge = false;
      accountHash = null;
      if (file != null) {
        file.rollback();
        file = null;
      }
      usual.abort();
    }

    private void finishAccount() {
      if (tooLarge) {
        writeSorted(nodes);
      } else if (!nodes.isEmpty()) {
        nodes.sort((a, b) -> Arrays.compareUnsigned(a.key(), b.key()));
        if (file == null) {
          file = flat.startSortedWriter(TRIE_BRANCH_STORAGE);
        }
        nodes.forEach(node -> file.put(node.key(), node.rlp()));
      }
      nodes.clear();
      sortedBytes = 0;
      tooLarge = false;
    }

    private void closeFile() {
      if (file != null) {
        file.commit();
        file = null;
      }
    }

    // nodes in a file of their own, which RocksDB places as low as its key range allows
    private void writeSorted(final List<SortedNode> sorted) {
      if (sorted.isEmpty()) {
        return;
      }
      sorted.sort((a, b) -> Arrays.compareUnsigned(a.key(), b.key()));
      final SortedWriter own = flat.startSortedWriter(TRIE_BRANCH_STORAGE);
      try {
        sorted.forEach(node -> own.put(node.key(), node.rlp()));
        own.commit();
      } catch (final RuntimeException e) {
        own.rollback();
        throw e;
      }
      sorted.clear();
    }
  }

  @Override
  public void saveWorldState(final BlockHeader header, final Bytes rootNode) {
    final BonsaiWorldStateKeyValueStorage.Updater updater = newUpdater();
    updater.saveWorldState(
        header.getBlockHash().getBytes(), Bytes32.wrap(header.getStateRoot().getBytes()), rootNode);
    updater
        .getWorldStateTransaction()
        .put(
            TRIE_BRANCH_STORAGE,
            WORLD_BLOCK_NUMBER_KEY,
            Bytes.ofUnsignedLong(header.getNumber()).toArrayUnsafe());
    updater.commitComposedOnly();
  }

  @Override
  public void prepareForSync() {
    if (!bonsai.getFlatDbStrategy().isCodeByCodeHash()) {
      // the database decides, not the flag, once it holds code
      throw new IllegalStateException(
          "SNAP2 sync needs Bonsai to store bytecode by code hash, but this database stores it by"
              + " account: delete it, with --Xbonsai-code-using-code-hash-enabled left to true");
    }
    endSortedFlatState();
    bonsai.clear();
    if (sortedFlatState) {
      stats = new SortedIngestStats();
      ingest = new SortedStateIngest(flat, ACCOUNT_INFO_STATE, ACCOUNT_STORAGE_STORAGE, stats);
      // the compactions of the state segments wait, so that the files stay where they are put; the
      // bytecodes come in no key order, and each compaction on their way down would rewrite them
      bulkLoad =
          flat.startBulkLoad(
              trieHeldBack
                  ? List.of(ACCOUNT_INFO_STATE, ACCOUNT_STORAGE_STORAGE, TRIE_BRANCH_STORAGE)
                  : List.of(ACCOUNT_INFO_STATE, ACCOUNT_STORAGE_STORAGE),
              List.of(CODE_STORAGE));
    }
  }

  @Override
  public CompletableFuture<Void> prepareTrieBuild() {
    final SegmentBulkLoad load = bulkLoad;
    if (load == null) {
      return CompletableFuture.completedFuture(null);
    }
    // the slots written outside the sorted files are all in level 0, which every iterator of the
    // build's scan opens, one per account
    LOG.info("SNAP2 merging the slots written while downloading before the trie build");
    return load.end(ACCOUNT_STORAGE_STORAGE)
        .exceptionally(
            error -> {
              // slower, not wrong
              LOG.warn("SNAP2 trie build starts before the slots are merged", error);
              return null;
            });
  }

  @Override
  public void completeSync() {
    if (ingest != null) {
      LOG.info("SNAP2 accounts and slots written with {}", stats);
    }
    endSortedFlatState();
    if (bonsai.getFlatDbMode() == FlatDbMode.PARTIAL) {
      bonsai.upgradeToFullFlatDbMode();
    }
    bonsai.clearCrossBlockCache();
  }

  private void endSortedFlatState() {
    final SortedStateIngest current = ingest;
    if (current != null) {
      current.close();
      ingest = null;
    }
    final SegmentBulkLoad load = bulkLoad;
    if (load != null) {
      load.close();
      bulkLoad = null;
    }
  }

  // the sync is the only writer, its writes skip the conflict checks
  private BonsaiWorldStateKeyValueStorage.Updater newUpdater() {
    return new BonsaiWorldStateKeyValueStorage.Updater(
        flat.startBatchTransaction(),
        NO_TRIE_LOG,
        bonsai.getFlatDbStrategy(),
        flat,
        bonsai.getTrieNodeStrategy());
  }
}
