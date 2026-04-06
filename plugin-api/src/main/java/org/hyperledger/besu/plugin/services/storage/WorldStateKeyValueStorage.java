/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.plugin.services.storage;

import java.util.Collection;

import org.apache.tuweni.bytes.Bytes32;

/** Storage interface for Ethereum world state data backed by a key-value store. */
public interface WorldStateKeyValueStorage {

  /**
   * Returns the storage format used by this world state storage.
   *
   * @return the {@link DataStorageFormat} of the underlying storage
   */
  DataStorageFormat getDataStorageFormat();

  /**
   * Creates and returns a new {@link Updater} for batching write operations.
   *
   * @return a new {@link Updater} scoped to a single write transaction
   */
  Updater updater();

  /** Removes all entries from this world state storage. */
  void clear();

  /** Callback interface for observing trie node insertions into world state storage. */
  interface NodesAddedListener {

    /**
     * Invoked after trie nodes have been added to the world state storage.
     *
     * @param nodeHash the hashes of the nodes that were added
     */
    void onNodesAdded(Collection<Bytes32> nodeHash);
  }

  /** A write transaction for batching mutations to world state storage. */
  interface Updater {

    /** Atomically persists all staged write operations to the underlying storage. */
    void commit();
  }
}
