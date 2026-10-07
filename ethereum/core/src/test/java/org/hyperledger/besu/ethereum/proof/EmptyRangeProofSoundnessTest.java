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
package org.hyperledger.besu.ethereum.proof;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.TrieGenerator;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.RangeStorageEntriesCollector;
import org.hyperledger.besu.ethereum.trie.TrieIterator;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EmptyRangeProofSoundnessTest {

  private static final Bytes32 MAX_RANGE =
      Bytes32.wrap(
          Hash.fromHexString("0x0fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
              .getBytes());

  private NavigableMap<Bytes, Optional<byte[]>> backing;
  private WorldStateStorageCoordinator storageCoordinator;
  private WorldStateProofProvider proofProvider;

  @BeforeEach
  public void setup() {
    backing = new TreeMap<>();
    storageCoordinator =
        new WorldStateStorageCoordinator(
            new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage(backing)));
    proofProvider = new WorldStateProofProvider(storageCoordinator);
  }

  @Test
  public void emptyClaimIsAcceptedForARangeThatIsNotEmpty() {
    final MerkleTrie<Bytes, Bytes> trie = TrieGenerator.generateTrie(storageCoordinator, 15);
    final Bytes32 start = Bytes32.wrap(Hash.ZERO.getBytes());

    final RangeStorageEntriesCollector collector =
        RangeStorageEntriesCollector.createCollector(start, MAX_RANGE, 10, Integer.MAX_VALUE);
    final TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);
    @SuppressWarnings("unchecked")
    final TreeMap<Bytes32, Bytes> actual =
        (TreeMap<Bytes32, Bytes>)
            trie.entriesFrom(
                root ->
                    RangeStorageEntriesCollector.collectEntries(collector, visitor, root, start));

    assertThat(actual).as("precondition: the range really does contain accounts").isNotEmpty();

    final List<Bytes> proofs =
        proofProvider.getAccountProofRelatedNodes(Hash.wrap(trie.getRootHash()), start);
    proofs.addAll(
        proofProvider.getAccountProofRelatedNodes(Hash.wrap(trie.getRootHash()), actual.lastKey()));

    final boolean accepted =
        proofProvider.isValidRangeProof(
            start, actual.lastKey(), trie.getRootHash(), proofs, new TreeMap<>());

    assertThat(accepted)
        .as("a range containing %d accounts must not validate as empty", actual.size())
        .isFalse();
  }

  @Test
  public void emptyClaimIsCorrectlyAcceptedForAGenuinelyEmptyRange() {
    final MerkleTrie<Bytes, Bytes> trie = TrieGenerator.generateTrie(storageCoordinator, 15);
    final Bytes32 high =
        Bytes32.fromHexString("0x0ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0");

    final List<Bytes> proofs =
        proofProvider.getAccountProofRelatedNodes(Hash.wrap(trie.getRootHash()), high);

    final boolean accepted =
        proofProvider.isValidRangeProof(
            high, MAX_RANGE, trie.getRootHash(), proofs, new TreeMap<>());

    assertThat(accepted).isTrue();
  }

  @Test
  public void emptyClaimWithCompleteSubtrieSupplied() {
    final MerkleTrie<Bytes, Bytes> trie = TrieGenerator.generateTrie(storageCoordinator, 15);
    final Bytes32 start = Bytes32.wrap(Hash.ZERO.getBytes());

    final RangeStorageEntriesCollector collector =
        RangeStorageEntriesCollector.createCollector(start, MAX_RANGE, 10, Integer.MAX_VALUE);
    final TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);
    @SuppressWarnings("unchecked")
    final TreeMap<Bytes32, Bytes> actual =
        (TreeMap<Bytes32, Bytes>)
            trie.entriesFrom(
                root ->
                    RangeStorageEntriesCollector.collectEntries(collector, visitor, root, start));
    assertThat(actual).isNotEmpty();

    final List<Bytes> allNodes =
        backing.values().stream()
            .filter(Optional::isPresent)
            .map(v -> Bytes.wrap(v.get()))
            .collect(Collectors.toList());

    final boolean accepted =
        proofProvider.isValidRangeProof(
            start, actual.lastKey(), trie.getRootHash(), allNodes, new TreeMap<>());

    assertThat(accepted)
        .as(
            "range containing %d accounts must not be accepted as empty when the full subtrie is supplied",
            actual.size())
        .isFalse();
  }
}
