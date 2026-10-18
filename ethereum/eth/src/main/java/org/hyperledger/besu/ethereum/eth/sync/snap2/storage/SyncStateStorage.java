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

import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Optional;
import java.util.function.BiConsumer;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Storage used by the sync: a flat view of the state (accounts and slots keyed by hash), the
 * bytecode store and the trie node store of the target world state format.
 */
public interface SyncStateStorage {

  /** Returns the full RLP of an account. */
  Optional<Bytes> getAccount(Bytes32 accountHash);

  /** Returns whether a bytecode is already stored. */
  boolean hasCode(Bytes32 codeHash);

  /** Starts a batch of flat writes. */
  Batch batch();

  /**
   * Visits accounts with hash in {@code [from, to]} in key order, each followed by its slots in key
   * order. Slots of a missing account are visited too, after the previous account.
   */
  void scan(Bytes32 from, Bytes32 to, ScanVisitor visitor);

  /** Visits the slots stored for an account in key order, with their stored values. */
  void forEachSlotValue(Bytes32 accountHash, BiConsumer<Bytes32, Bytes> slots);

  /** Returns whether the storage trie of an account with this root is completely written. */
  boolean hasStorageTrie(Bytes32 accountHash, Bytes32 storageRoot);

  /** Returns a node of the storage trie of an account, by its location and hash. */
  Optional<Bytes> storageTrieNode(Bytes32 accountHash, Bytes location, Bytes32 hash);

  /** Removes every slot of an account, committing in bounded batches. */
  void deleteStorage(Bytes32 accountHash);

  /** Creates a writer of trie nodes, not shared between threads. */
  TrieWriter trieWriter();

  /** Records the world state root and the block it belongs to, once every node is written. */
  void saveWorldState(BlockHeader header, Bytes rootNode);

  /** Wipes the state before a new sync. */
  void prepareForSync();

  /** Releases what the sync no longer needs once the world state is complete. */
  void completeSync();

  /** Batch of flat writes. */
  interface Batch {
    void putAccount(Bytes32 accountHash, Bytes account);

    void removeAccount(Bytes32 accountHash);

    /** Stores a non-zero slot value, given on 32 bytes. */
    void putStorage(Bytes32 accountHash, Bytes32 slotHash, Bytes32 value);

    void removeStorage(Bytes32 accountHash, Bytes32 slotHash);

    void putCode(Bytes32 codeHash, Bytes code);

    void commit();
  }

  /** Visitor of the flat state. */
  interface ScanVisitor {
    /** Visits an account and returns whether its slots are to be visited too. */
    boolean onAccount(Bytes32 accountHash, Bytes account);

    void onSlot(Bytes32 accountHash, Bytes32 slotHash, Bytes value);
  }

  /** Writer of trie nodes, committing in bounded batches. */
  interface TrieWriter {
    void accountNode(Bytes location, Bytes32 hash, Bytes rlp);

    void storageNode(Bytes32 accountHash, Bytes location, Bytes32 hash, Bytes rlp);

    void commit();

    /** Drops the nodes not committed yet. */
    void abort();
  }
}
