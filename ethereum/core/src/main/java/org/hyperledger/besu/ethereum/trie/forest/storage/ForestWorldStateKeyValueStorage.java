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
package org.hyperledger.besu.ethereum.trie.forest.storage;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.util.Subscribers;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class ForestWorldStateKeyValueStorage implements WorldStateKeyValueStorage {

  private final Subscribers<NodesAddedListener> nodeAddedListeners = Subscribers.create();
  private final KeyValueStorage keyValueStorage;
  private final ReentrantLock lock = new ReentrantLock();

  public ForestWorldStateKeyValueStorage(final KeyValueStorage keyValueStorage) {
    this.keyValueStorage = keyValueStorage;
  }

  @Override
  public DataStorageFormat getDataStorageFormat() {
    return DataStorageFormat.FOREST;
  }

  public Optional<Bytes> getCode(final Hash codeHash) {
    if (codeHash.equals(Hash.EMPTY)) {
      return Optional.of(Bytes.EMPTY);
    } else {
      return keyValueStorage.get(codeHash.getBytes().toArrayUnsafe()).map(Bytes::wrap);
    }
  }

  public Optional<Bytes> getAccountStateTrieNode(final Bytes32 nodeHash) {
    return getTrieNode(nodeHash);
  }

  public Optional<Bytes> getAccountStorageTrieNode(final Bytes32 nodeHash) {
    return getTrieNode(nodeHash);
  }

  private Optional<Bytes> getTrieNode(final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    } else {
      return keyValueStorage.get(nodeHash.toArrayUnsafe()).map(Bytes::wrap);
    }
  }

  public boolean contains(final Bytes32 hash) {
    // we don't have location info
    return getNodeData(hash).isPresent();
  }

  public Optional<Bytes> getNodeData(final Bytes32 hash) {
    if (hash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    } else if (hash.equals(Hash.EMPTY.getBytes())) {
      return Optional.of(Bytes.EMPTY);
    } else {
      return keyValueStorage.get(hash.toArrayUnsafe()).map(Bytes::wrap);
    }
  }

  public boolean isWorldStateAvailable(final Bytes32 rootHash) {
    return getAccountStateTrieNode(rootHash).isPresent();
  }

  @Override
  public void clear() {
    keyValueStorage.clear();
  }

  @Override
  public Updater updater() {
    return new Updater(lock, keyValueStorage.startTransaction(), nodeAddedListeners);
  }

  public long prune(final Predicate<byte[]> inUseCheck) {
    final AtomicInteger prunedKeys = new AtomicInteger(0);
    try (final Stream<byte[]> entry = keyValueStorage.streamKeys()) {
      entry.forEach(
          key -> {
            lock.lock();
            try {
              if (!inUseCheck.test(key) && keyValueStorage.tryDelete(key)) {
                prunedKeys.incrementAndGet();
              }
            } finally {
              lock.unlock();
            }
          });
    }

    return prunedKeys.get();
  }

  public long addNodeAddedListener(final NodesAddedListener listener) {
    return nodeAddedListeners.subscribe(listener);
  }

  public void removeNodeAddedListener(final long id) {
    nodeAddedListeners.unsubscribe(id);
  }

  public static class Updater implements WorldStateKeyValueStorage.Updater {

    private final KeyValueStorageTransaction transaction;
    private final Subscribers<NodesAddedListener> nodeAddedListeners;
    private final Set<Bytes32> addedNodes = new HashSet<>();
    private final Lock lock;

    public Updater(
        final Lock lock,
        final KeyValueStorageTransaction transaction,
        final Subscribers<NodesAddedListener> nodeAddedListeners) {
      this.lock = lock;
      this.transaction = transaction;
      this.nodeAddedListeners = nodeAddedListeners;
    }

    public Updater putCode(final Bytes code) {
      // Skip the hash calculation for empty code
      final Hash codeHash = code.size() == 0 ? Hash.EMPTY : Hash.hash(code);
      return putCode(Bytes32.wrap(codeHash.getBytes()), code);
    }

    public Updater putCode(final Bytes32 codeHash, final Bytes code) {
      if (code.size() == 0) {
        // Don't save empty values
        return this;
      }

      addedNodes.add(codeHash);
      transaction.put(codeHash.toArrayUnsafe(), code.toArrayUnsafe());
      return this;
    }

    public Updater saveWorldState(final Bytes32 nodeHash, final Bytes node) {
      return putAccountStateTrieNode(nodeHash, node);
    }

    public Updater putAccountStateTrieNode(final Bytes32 nodeHash, final Bytes node) {
      if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
        // Don't save empty nodes
        return this;
      }
      addedNodes.add(nodeHash);
      transaction.put(nodeHash.toArrayUnsafe(), node.toArrayUnsafe());
      return this;
    }

    public WorldStateKeyValueStorage.Updater removeAccountStateTrieNode(final Bytes32 nodeHash) {
      transaction.remove(nodeHash.toArrayUnsafe());
      return this;
    }

    public Updater putAccountStorageTrieNode(final Bytes32 nodeHash, final Bytes node) {
      if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
        // Don't save empty nodes
        return this;
      }
      addedNodes.add(nodeHash);
      transaction.put(nodeHash.toArrayUnsafe(), node.toArrayUnsafe());
      return this;
    }

    @Override
    public void commit() {
      lock.lock();
      try {
        nodeAddedListeners.forEach(listener -> listener.onNodesAdded(addedNodes));
        transaction.commit();
      } finally {
        lock.unlock();
      }
    }

    public void rollback() {
      addedNodes.clear();
      transaction.rollback();
    }
  }
}
