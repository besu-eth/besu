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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

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

  private final BonsaiWorldStateKeyValueStorage bonsai;

  public BonsaiSyncStateStorage(final BonsaiWorldStateKeyValueStorage bonsai) {
    super(bonsai.getComposedWorldStateStorage(), ACCOUNT_INFO_STATE, ACCOUNT_STORAGE_STORAGE);
    this.bonsai = bonsai;
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
    bonsai.clear();
  }

  @Override
  public void completeSync() {
    if (bonsai.getFlatDbMode() == FlatDbMode.PARTIAL) {
      bonsai.upgradeToFullFlatDbMode();
    }
    bonsai.clearCrossBlockCache();
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
