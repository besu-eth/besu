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
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Sync storage writing straight into the Bonsai flat database and trie node store. */
public final class BonsaiSyncStateStorage extends AbstractSyncStateStorage {

  private final BonsaiWorldStateKeyValueStorage bonsai;

  public BonsaiSyncStateStorage(final BonsaiWorldStateKeyValueStorage bonsai) {
    super(bonsai.getComposedWorldStateStorage(), ACCOUNT_INFO_STATE, ACCOUNT_STORAGE_STORAGE);
    this.bonsai = bonsai;
  }

  @Override
  public boolean hasCode(final Bytes32 codeHash) {
    return flat.containsKey(CODE_STORAGE, codeHash.toArrayUnsafe());
  }

  @Override
  protected Bytes storedSlot(final Bytes32 value) {
    return value;
  }

  @Override
  public Batch batch() {
    final SegmentedKeyValueStorageTransaction tx = flat.startBatchTransaction();
    return new FlatBatch(tx) {
      // stored by code hash, which prepareForSync requires
      @Override
      public void putCode(final Bytes32 codeHash, final Bytes code) {
        bonsai.getFlatDbStrategy().putFlatCode(flat, tx, Hash.ZERO, Hash.wrap(codeHash), code);
      }

      @Override
      public void commit() {
        tx.commit();
      }
    };
  }

  @Override
  public boolean hasStorageTrie(final Bytes32 accountHash, final Bytes32 storageRoot) {
    return bonsai
        .getTrieNode(TrieNodeKey.of(Hash.wrap(accountHash), Bytes.EMPTY), storageRoot)
        .isPresent();
  }

  @Override
  public Optional<Bytes> storageTrieNode(
      final Bytes32 accountHash, final Bytes location, final Bytes32 hash) {
    return bonsai.getTrieNode(TrieNodeKey.of(Hash.wrap(accountHash), location), hash);
  }

  @Override
  public TrieWriter trieWriter() {
    return new TrieWriter() {
      private BonsaiWorldStateKeyValueStorage.Updater updater;
      private long pendingBytes;

      @Override
      public void accountNode(final Bytes location, final Bytes32 hash, final Bytes rlp) {
        put(location, hash, rlp);
      }

      @Override
      public void storageNode(
          final Bytes32 accountHash, final Bytes location, final Bytes32 hash, final Bytes rlp) {
        put(TrieNodeKey.of(Hash.wrap(accountHash), location), hash, rlp);
      }

      // a failed commit leaves the writer empty
      @Override
      public void commit() {
        final BonsaiWorldStateKeyValueStorage.Updater pending = updater;
        updater = null;
        pendingBytes = 0;
        if (pending != null) {
          pending.commitComposedOnly();
        }
      }

      @Override
      public void abort() {
        final BonsaiWorldStateKeyValueStorage.Updater pending = updater;
        updater = null;
        pendingBytes = 0;
        if (pending != null) {
          pending.rollback();
        }
      }

      private void put(final Bytes key, final Bytes32 hash, final Bytes rlp) {
        if (updater == null) {
          updater = newUpdater();
        }
        updater.putTrieNode(key, hash, rlp);
        pendingBytes += key.size() + rlp.size();
        if (pendingBytes >= BYTES_PER_COMMIT) {
          commit();
        }
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
      throw new IllegalStateException(
          "SNAP2 sync needs Bonsai to store bytecode by code hash (--Xbonsai-code-using-code-hash-enabled)");
    }
    bonsai.clear();
  }

  @Override
  public void completeSync() {
    if (bonsai.getFlatDbMode() != FlatDbMode.FULL) {
      bonsai.upgradeToFullFlatDbMode();
    }
    bonsai.clearCrossBlockCache();
  }

  // the sync is the only writer, its writes skip the conflict checks
  private BonsaiWorldStateKeyValueStorage.Updater newUpdater() {
    return new BonsaiWorldStateKeyValueStorage.Updater(
        flat.startBatchTransaction(),
        bonsai.getTrieLogStorage().startBatchTransaction(),
        bonsai.getFlatDbStrategy(),
        flat,
        bonsai.getTrieNodeStrategy());
  }
}
