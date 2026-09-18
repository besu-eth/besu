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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.PathNodeVisitor;
import org.hyperledger.besu.ethereum.trie.TrieIterator;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.BranchNode;
import org.hyperledger.besu.ethereum.trie.patricia.ExtensionNode;
import org.hyperledger.besu.ethereum.trie.patricia.LeafNode;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.Collections;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Predicate;

import kotlin.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Snap server world-state storage backed by Forest (hash-indexed MPT). Account and storage data is
 * served via trie traversal rather than a flat database.
 */
class ForestSnapWorldStateStorage implements SnapWorldStateStorage {

  private static final int MAX_TRAVERSAL_ENTRIES = 100_000;

  private final ForestWorldStateKeyValueStorage storage;
  private final StoredMerklePatriciaTrie<Bytes32, Bytes> accountTrie;

  ForestSnapWorldStateStorage(final ForestWorldStateKeyValueStorage storage, final Hash rootHash) {
    this.storage = storage;
    this.accountTrie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) -> storage.getAccountStateTrieNode(hash),
            Bytes32.wrap(rootHash.getBytes()),
            b -> b,
            b -> b);
  }

  @Override
  public NavigableMap<Bytes32, Bytes> streamFlatAccounts(
      final Bytes startKeyHash, final Predicate<Pair<Bytes32, Bytes>> takeWhile) {
    NavigableMap<Bytes32, Bytes> result = new TreeMap<>();
    TrieIterator<Bytes> iterator =
        new TrieIterator<>(
            (keyHash, node) -> {
              Bytes value = node.getValue().orElse(null);
              if (value == null) return TrieIterator.State.CONTINUE;
              if (takeWhile.test(new Pair<>(keyHash, value))) {
                result.put(keyHash, value);
                return TrieIterator.State.CONTINUE;
              }
              return TrieIterator.State.STOP;
            },
            false);
    accountTrie.entriesFrom(
        root -> {
          root.accept(iterator, CompactEncoding.bytesToPath(Bytes32.wrap(startKeyHash)));
          return result;
        });
    return result;
  }

  @Override
  public NavigableMap<Bytes32, Bytes> streamFlatAccounts(
      final Bytes startKeyHash, final Bytes32 endKeyHash, final long max) {
    NavigableMap<Bytes32, Bytes> all =
        new TreeMap<>(
            accountTrie.entriesFrom(
                Bytes32.wrap(startKeyHash), (int) Math.min(max, MAX_TRAVERSAL_ENTRIES)));
    return all.headMap(endKeyHash, true);
  }

  @Override
  public NavigableMap<Bytes32, Bytes> streamFlatStorages(
      final Hash accountHash,
      final Bytes startKeyHash,
      final Predicate<Pair<Bytes32, Bytes>> takeWhile) {
    return getStorageTrie(accountHash)
        .map(
            storageTrie -> {
              NavigableMap<Bytes32, Bytes> result = new TreeMap<>();
              TrieIterator<Bytes> iterator =
                  new TrieIterator<>(
                      (keyHash, node) -> {
                        Bytes value = node.getValue().orElse(null);
                        if (value == null) return TrieIterator.State.CONTINUE;
                        if (takeWhile.test(new Pair<>(keyHash, value))) {
                          result.put(keyHash, value);
                          return TrieIterator.State.CONTINUE;
                        }
                        return TrieIterator.State.STOP;
                      },
                      false);
              storageTrie.entriesFrom(
                  root -> {
                    root.accept(iterator, CompactEncoding.bytesToPath(Bytes32.wrap(startKeyHash)));
                    return result;
                  });
              return result;
            })
        .orElseGet(TreeMap::new);
  }

  @Override
  public NavigableMap<Bytes32, Bytes> streamFlatStorages(
      final Hash accountHash, final Bytes startKeyHash, final Bytes32 endKeyHash, final long max) {
    return getStorageTrie(accountHash)
        .map(
            storageTrie -> {
              NavigableMap<Bytes32, Bytes> all =
                  new TreeMap<>(
                      storageTrie.entriesFrom(
                          Bytes32.wrap(startKeyHash), (int) Math.min(max, MAX_TRAVERSAL_ENTRIES)));
              return all.headMap(endKeyHash, true);
            })
        .orElseGet(TreeMap::new);
  }

  @Override
  public Optional<Bytes> getTrieNodeUnsafe(final Bytes location) {
    if (location.size() >= Bytes32.SIZE) {
      // storage trie node: first 32 bytes = account hash, remainder = nibble path
      Hash accountHash = Hash.wrap(Bytes32.wrap(location.slice(0, Bytes32.SIZE)));
      Bytes storagePath = location.slice(Bytes32.SIZE);
      return getStorageTrie(accountHash).flatMap(trie -> getNodeAtPath(trie, storagePath));
    }
    return getNodeAtPath(accountTrie, location);
  }

  @Override
  public Optional<Bytes> getAccount(final Hash accountHash) {
    return accountTrie.get(Bytes32.wrap(accountHash.getBytes()));
  }

  @Override
  public Hash getAccountStorageRoot(final Hash accountHash) {
    return accountTrie
        .get(Bytes32.wrap(accountHash.getBytes()))
        .map(rlp -> PmtStateTrieAccountValue.readFrom(RLP.input(rlp)).getStorageRoot())
        .orElse(Hash.EMPTY_TRIE_HASH);
  }

  private Optional<StoredMerklePatriciaTrie<Bytes32, Bytes>> getStorageTrie(
      final Hash accountHash) {
    return accountTrie
        .get(Bytes32.wrap(accountHash.getBytes()))
        .flatMap(
            accountRlp -> {
              Hash storageRoot =
                  PmtStateTrieAccountValue.readFrom(RLP.input(accountRlp)).getStorageRoot();
              if (storageRoot.equals(Hash.EMPTY_TRIE_HASH)) {
                return Optional.empty();
              }
              return Optional.of(
                  new StoredMerklePatriciaTrie<>(
                      (location, hash) -> storage.getAccountStorageTrieNode(hash),
                      Bytes32.wrap(storageRoot.getBytes()),
                      b -> b,
                      b -> b));
            });
  }

  @Override
  public WorldStateKeyValueStorage asWorldStateKeyValueStorage() {
    return storage;
  }

  private <V> Optional<Bytes> getNodeAtPath(
      final MerkleTrie<Bytes32, V> trie, final Bytes nibblePath) {
    NodeAtPathVisitor<V> visitor = new NodeAtPathVisitor<>();
    trie.entriesFrom(
        root -> {
          root.accept(visitor, nibblePath);
          return Collections.emptyMap();
        });
    return visitor.getResult();
  }

  /**
   * PathNodeVisitor that walks a trie following a nibble path and returns the RLP-encoded bytes of
   * the node at that path position.
   */
  private static class NodeAtPathVisitor<V> implements PathNodeVisitor<V> {

    private Optional<Bytes> result = Optional.empty();

    Optional<Bytes> getResult() {
      return result;
    }

    @Override
    public Node<V> visit(final BranchNode<V> node, final Bytes path) {
      if (path.isEmpty() || path.get(0) == CompactEncoding.LEAF_TERMINATOR) {
        result = Optional.of(node.getEncodedBytes());
      } else {
        node.child(path.get(0)).accept(this, path.slice(1));
      }
      return node;
    }

    @Override
    public Node<V> visit(final ExtensionNode<V> node, final Bytes path) {
      if (path.isEmpty()) {
        result = Optional.of(node.getEncodedBytes());
        return node;
      }
      Bytes ext = node.getPath();
      int commonLen = ext.commonPrefixLength(path);
      if (commonLen == ext.size()) {
        node.getChild().accept(this, path.slice(commonLen));
      }
      return node;
    }

    @Override
    public Node<V> visit(final LeafNode<V> node, final Bytes path) {
      if (path.equals(node.getPath())) {
        result = Optional.of(node.getEncodedBytes());
      }
      return node;
    }

    @Override
    public Node<V> visit(
        final org.hyperledger.besu.ethereum.trie.NullNode<V> node, final Bytes path) {
      return node;
    }
  }
}
