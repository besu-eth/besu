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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A noop {@link MerkleTrie}.
 *
 * @param <V> The type of values of this trie.
 */
public class NoOpMerkleTrie<K extends Bytes, V> implements MerkleTrie<K, V> {

  public NoOpMerkleTrie() {}

  @Override
  public Optional<V> get(final K key) {
    return Optional.empty();
  }

  @Override
  public Optional<V> getPath(final K path) {
    return Optional.empty();
  }

  @Override
  public Proof<V> getValueWithProof(final K key) {
    return new Proof<>(Optional.empty(), new ArrayList<>());
  }

  @Override
  public void put(final K key, final V value) {
    // noop
  }

  @Override
  public void putPath(final K path, final V value) {
    // noop
  }

  @Override
  public void put(final K key, final PathNodeVisitor<V> putVisitor) {
    // noop
  }

  @Override
  public void putDeferred(final K key, final Function<Optional<V>, Optional<V>> merger) {
    // noop
  }

  @Override
  public void remove(final K key) {
    // noop
  }

  @Override
  public void removePath(final K path, final PathNodeVisitor<V> removeVisitor) {
    // noop
  }

  @Override
  public Bytes32 getRootHash() {
    return EMPTY_TRIE_NODE_HASH;
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
    return new HashMap<>();
  }

  @Override
  public Map<Bytes32, V> entriesFrom(final Function<Node<V>, Map<Bytes32, V>> handler) {
    return new HashMap<>();
  }

  @Override
  public void visitAll(final Consumer<Node<V>> nodeConsumer) {
    // noop
  }

  @Override
  public CompletableFuture<Void> visitAll(
      final Consumer<Node<V>> nodeConsumer, final ExecutorService executorService) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void visitLeafs(final TrieIterator.LeafHandler<V> handler) {
    // nopop
  }
}
