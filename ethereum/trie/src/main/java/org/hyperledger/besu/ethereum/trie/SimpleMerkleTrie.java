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
package org.hyperledger.besu.ethereum.trie;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.stream.Collectors.toUnmodifiableSet;

import org.hyperledger.besu.ethereum.trie.patricia.DefaultNodeFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * An in-memory {@link MerkleTrie}.
 *
 * @param <V> The type of values stored by this trie.
 */
public abstract class SimpleMerkleTrie<K extends Bytes, V> implements MerkleTrie<K, V> {

  protected final DefaultNodeFactory<V> nodeFactory;

  private Node<V> root;

  /**
   * Create a trie.
   *
   * @param valueSerializer A function for serializing values to bytes.
   */
  public SimpleMerkleTrie(final Function<V, Bytes> valueSerializer) {
    this.nodeFactory = new DefaultNodeFactory<>(valueSerializer);
    this.root = NullNode.instance();
  }

  @Override
  public Optional<V> get(final K key) {
    checkNotNull(key);
    return root.accept(getGetVisitor(), keyToPath(key), 0).getValue();
  }

  @Override
  public Optional<V> getPath(final K path) {
    checkNotNull(path);
    return root.accept(getGetVisitor(), path.toArrayUnsafe(), 0).getValue();
  }

  @Override
  public Proof<V> getValueWithProof(final K key) {
    checkNotNull(key);
    final ProofVisitor<V> proofVisitor = new ProofVisitor<>(root);
    final Optional<V> value = root.accept(proofVisitor, keyToPath(key), 0).getValue();
    final List<Bytes> proof =
        proofVisitor.getProof().stream()
            .map(node -> Bytes.wrap(node.encoded()))
            .collect(Collectors.toList());
    return new Proof<>(value, proof);
  }

  @Override
  public void put(final K key, final V value) {
    checkNotNull(key);
    checkNotNull(value);
    this.root = root.accept(getPutVisitor(value), keyToPath(key), 0);
  }

  @Override
  public void putPath(final K path, final V value) {
    checkNotNull(path);
    checkNotNull(value);
    this.root = root.accept(getPutVisitor(value), path.toArray(), 0);
  }

  @Override
  public void put(final K key, final PathNodeVisitor<V> putVisitor) {
    checkNotNull(key);
    this.root = root.accept(putVisitor, keyToPath(key), 0);
  }

  @Override
  public void remove(final K key) {
    checkNotNull(key);
    this.root = root.accept(getRemoveVisitor(), keyToPath(key), 0);
  }

  @Override
  public void removePath(final K path, final PathNodeVisitor<V> removeVisitor) {
    checkNotNull(path);
    this.root = root.accept(removeVisitor, path.toArrayUnsafe(), 0);
  }

  @Override
  public Bytes32 getRootHash() {
    return Bytes32.wrap(root.hash());
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[" + getRootHash() + "]";
  }

  @Override
  public void commit(final NodeUpdater nodeUpdater) {
    // Nothing to do here
  }

  @Override
  public void commit(final NodeUpdater nodeUpdater, final CommitVisitor<V> commitVisitor) {
    // Nothing to do here
  }

  @Override
  public Map<Bytes32, V> entriesFrom(final Bytes32 startKeyHash, final int limit) {
    return StorageEntriesCollector.collectEntries(root, startKeyHash, limit);
  }

  @Override
  public Map<Bytes32, V> entriesFrom(final Function<Node<V>, Map<Bytes32, V>> handler) {
    return handler.apply(root);
  }

  @Override
  public void visitAll(final Consumer<Node<V>> nodeConsumer) {
    root.accept(new AllNodesVisitor<>(nodeConsumer));
  }

  @Override
  public CompletableFuture<Void> visitAll(
      final Consumer<Node<V>> nodeConsumer, final ExecutorService executorService) {
    return CompletableFuture.allOf(
        Stream.concat(
                Stream.of(
                    CompletableFuture.runAsync(() -> nodeConsumer.accept(root), executorService)),
                root.getChildren().stream()
                    .map(
                        rootChild ->
                            CompletableFuture.runAsync(
                                () -> rootChild.accept(new AllNodesVisitor<>(nodeConsumer)),
                                executorService)))
            .collect(toUnmodifiableSet())
            .toArray(CompletableFuture[]::new));
  }

  @Override
  public void visitLeafs(final TrieIterator.LeafHandler<V> handler) {
    final TrieIterator<V> visitor = new TrieIterator<>(handler, true);
    root.accept(visitor, CompactEncoding.bytesToPath(new byte[Bytes32.SIZE]), 0);
  }

  protected static byte[] keyToPath(final Bytes key) {
    return CompactEncoding.bytesToPath(key.toArrayUnsafe());
  }

  public abstract PathNodeVisitor<V> getGetVisitor();

  public abstract PathNodeVisitor<V> getRemoveVisitor();

  public abstract PathNodeVisitor<V> getPutVisitor(final V value);
}
