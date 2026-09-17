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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.InnerNodeDiscoveryManager;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.SnapCommitVisitor;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.immutables.value.Value;

/**
 * StackTrie represents a stack-based Merkle Patricia Trie used in the context of snapsync
 * synchronization. It allows adding elements, retrieving elements, and committing the trie changes
 * to a node updater and flat database updater. The trie operates on a stack of segments and commits
 * the changes once the number of segments reaches a threshold. It utilizes proofs and keys to build
 * and update the trie structure.
 */
public class StackTrie {

  private final Bytes32 rootHash;
  private final AtomicInteger nbSegments;
  private final int maxSegments;
  private final Bytes32 startKeyHash;
  private Map<Bytes32, TaskElement> elements;
  private AtomicLong elementsCount;

  public StackTrie(final Hash rootHash, final Bytes32 startKeyHash) {
    this(rootHash, 1, 1, startKeyHash);
  }

  public StackTrie(
      final Hash rootHash,
      final int nbSegments,
      final int maxSegments,
      final Bytes32 startKeyHash) {
    this.rootHash = Bytes32.wrap(rootHash.getBytes());
    this.nbSegments = new AtomicInteger(nbSegments);
    this.maxSegments = maxSegments;
    this.startKeyHash = startKeyHash;
    this.elements = new LinkedHashMap<>();
    this.elementsCount = new AtomicLong();
  }

  public void addElement(
      final Bytes32 taskIdentifier,
      final List<Bytes> proofs,
      final NavigableMap<Bytes32, Bytes> keys) {
    this.elementsCount.addAndGet(keys.size());
    this.elements.put(
        taskIdentifier, ImmutableTaskElement.builder().proofs(proofs).keys(keys).build());
  }

  public void removeElement(final Bytes32 taskIdentifier) {
    if (this.elements.containsKey(taskIdentifier)) {
      this.elementsCount.addAndGet(-this.elements.remove(taskIdentifier).keys().size());
    }
  }

  public TaskElement getElement(final Bytes32 taskIdentifier) {
    return this.elements.get(taskIdentifier);
  }

  public AtomicLong getElementsCount() {
    return elementsCount;
  }

  public void commit(final NodeUpdater nodeUpdater) {
    commit((key, value) -> {}, nodeUpdater, false);
  }

  public void commit(
      final FlatDatabaseUpdater flatDatabaseUpdater,
      final NodeUpdater nodeUpdater,
      final boolean includeHealNeededNodes) {

    if (nbSegments.decrementAndGet() <= 0 && !elements.isEmpty()) {

      final List<Bytes> proofs = new ArrayList<>();
      final TreeMap<Bytes32, Bytes> keys = new TreeMap<>();

      elements
          .values()
          .forEach(
              taskElement -> {
                proofs.addAll(taskElement.proofs());
                keys.putAll(taskElement.keys());
              });

      if (keys.isEmpty()) {
        return; // empty range we can ignore it
      }

      final Map<Bytes32, Bytes> proofsEntries = new HashMap<>();
      for (Bytes proof : proofs) {
        proofsEntries.put(Bytes32.wrap(Hash.hash(proof).getBytes()), proof);
      }

      if (!keys.isEmpty()) {
        final InnerNodeDiscoveryManager<Bytes> snapStoredNodeFactory =
            new InnerNodeDiscoveryManager<>(
                (location, hash) -> Optional.ofNullable(proofsEntries.get(hash)),
                Function.identity(),
                Function.identity(),
                startKeyHash,
                proofs.isEmpty() ? RangeManager.MAX_RANGE : keys.lastKey(),
                true);

        final MerkleTrie<Bytes, Bytes> trie =
            new StoredMerklePatriciaTrie<>(
                snapStoredNodeFactory,
                proofs.isEmpty() ? MerkleTrie.EMPTY_TRIE_NODE_HASH : rootHash);

        for (Map.Entry<Bytes32, Bytes> entry : keys.entrySet()) {
          trie.put(entry.getKey(), entry.getValue());
        }

        keys.forEach(flatDatabaseUpdater::update);

        trie.commit(
            nodeUpdater,
            (new SnapCommitVisitor<>(
                nodeUpdater,
                startKeyHash,
                proofs.isEmpty() ? RangeManager.MAX_RANGE : keys.lastKey()) {
              @Override
              public void maybeStoreNode(final Bytes location, final Node<Bytes> node) {
                if (!node.isHealNeeded() || includeHealNeededNodes) {
                  super.maybeStoreNode(location, node);
                }
              }
            }));
      }
    }
  }

  public void clear() {
    this.elements = new LinkedHashMap<>();
    this.elementsCount = new AtomicLong();
  }

  public boolean addSegment() {
    if (nbSegments.get() > maxSegments) {
      return false;
    } else {
      nbSegments.incrementAndGet();
      return true;
    }
  }

  public interface FlatDatabaseUpdater {

    static FlatDatabaseUpdater noop() {
      return (key, value) -> {};
    }

    void update(final Bytes32 key, final Bytes value);
  }

  @Value.Immutable
  public abstract static class TaskElement {

    @Value.Default
    public List<Bytes> proofs() {
      return new ArrayList<>();
    }

    @Value.Default
    public NavigableMap<Bytes32, Bytes> keys() {
      return new TreeMap<>();
    }
  }
}
