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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.SNAPSYNC_FLAT_ACCOUNTS;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.SNAPSYNC_FLAT_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Sync storage for Forest: the flat state is staged in segments of its own, cleared once the tries
 * are built, and the code and trie nodes go to the world state, keyed by hash.
 */
public final class ForestSyncStateStorage extends AbstractSyncStateStorage {

  private final ForestWorldStateKeyValueStorage forest;

  public ForestSyncStateStorage(
      final ForestWorldStateKeyValueStorage forest, final StorageProvider storageProvider) {
    super(
        storageProvider.getStorageBySegmentIdentifiers(
            List.of(SNAPSYNC_FLAT_ACCOUNTS, SNAPSYNC_FLAT_STORAGE)),
        SNAPSYNC_FLAT_ACCOUNTS,
        SNAPSYNC_FLAT_STORAGE);
    this.forest = forest;
  }

  @Override
  public boolean hasCode(final Bytes32 codeHash) {
    return forest.getCode(Hash.wrap(codeHash)).isPresent();
  }

  @Override
  public Batch batch() {
    final SegmentedKeyValueStorageTransaction staging = flat.startBatchTransaction();
    return new FlatBatch() {
      private ForestWorldStateKeyValueStorage.Updater codes;

      @Override
      public void putAccount(final Bytes32 accountHash, final Bytes account) {
        staging.put(accountSegment, accountHash.toArrayUnsafe(), account.toArrayUnsafe());
      }

      @Override
      public void removeAccount(final Bytes32 accountHash) {
        staging.remove(accountSegment, accountHash.toArrayUnsafe());
      }

      // without its leading zeros, which the trie leaves drop too
      @Override
      public void putStorage(
          final Bytes32 accountHash, final Bytes32 slotHash, final Bytes32 value) {
        staging.put(
            storageSegment,
            slotKey(accountHash, slotHash),
            value.trimLeadingZeros().toArrayUnsafe());
      }

      @Override
      public void removeStorage(final Bytes32 accountHash, final Bytes32 slotHash) {
        staging.remove(storageSegment, slotKey(accountHash, slotHash));
      }

      @Override
      public void putCode(final Bytes32 codeHash, final Bytes code) {
        if (codes == null) {
          codes = forest.batchUpdater();
        }
        codes.putCode(codeHash, code);
      }

      // the code first: no account is left pointing to a code whose commit failed
      @Override
      protected void commitWrites() {
        if (codes != null) {
          try {
            codes.commit();
          } catch (final RuntimeException e) {
            staging.rollback();
            throw e;
          }
        }
        staging.commit();
      }

      @Override
      protected void dropWrites() {
        if (codes != null) {
          codes.rollback();
        }
        staging.rollback();
      }
    };
  }

  // by hash: a trie with a root is the same whichever account wrote it
  @Override
  public Optional<Bytes> storageTrieNode(
      final Bytes32 accountHash, final Bytes location, final Bytes32 hash) {
    return forest.getAccountStorageTrieNode(hash);
  }

  @Override
  protected NodeBatch nodeBatch() {
    final ForestWorldStateKeyValueStorage.Updater updater = forest.batchUpdater();
    return new NodeBatch() {
      @Override
      public void putAccountNode(final Bytes location, final Bytes32 hash, final Bytes rlp) {
        updater.putAccountStateTrieNode(hash, rlp);
      }

      @Override
      public void putStorageNode(
          final Bytes32 accountHash, final Bytes location, final Bytes32 hash, final Bytes rlp) {
        updater.putAccountStorageTrieNode(hash, rlp);
      }

      @Override
      public void commit() {
        updater.commit();
      }

      @Override
      public void abort() {
        updater.rollback();
      }
    };
  }

  @Override
  public void saveWorldState(final BlockHeader header, final Bytes rootNode) {
    forest
        .batchUpdater()
        .saveWorldState(Bytes32.wrap(header.getStateRoot().getBytes()), rootNode)
        .commit();
  }

  @Override
  public void prepareForSync() {
    forest.clear();
    clearStaging();
  }

  @Override
  public void completeSync() {
    clearStaging();
  }

  private void clearStaging() {
    flat.clear(accountSegment);
    flat.clear(storageSegment);
  }
}
