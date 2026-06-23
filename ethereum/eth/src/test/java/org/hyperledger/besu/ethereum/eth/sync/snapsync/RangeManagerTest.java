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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.TrieGenerator;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.RangeStorageEntriesCollector;
import org.hyperledger.besu.ethereum.trie.TrieIterator;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public final class RangeManagerTest {

  @Test
  public void testRemainingRangesEqualToOneWhenFirstRangeContainsMoreThanHalf() {
    TreeMap<Bytes32, Bytes> items = new TreeMap<>();
    items.put(Bytes32.fromHexString("bb".repeat(32)), Bytes.wrap(new byte[] {0x03}));
    int nbRanges =
        RangeManager.getRangeCount(RangeManager.MIN_RANGE, RangeManager.MAX_RANGE, items);
    assertThat(nbRanges).isEqualTo(1);
  }

  @Test
  public void testRemainingRangesEqualToOneWhenFirstRangeContainsLessThanHalf() {
    TreeMap<Bytes32, Bytes> items = new TreeMap<>();
    items.put(Bytes32.fromHexString("77".repeat(32)), Bytes.wrap(new byte[] {0x03}));
    int nbRanges =
        RangeManager.getRangeCount(RangeManager.MIN_RANGE, RangeManager.MAX_RANGE, items);
    assertThat(nbRanges).isEqualTo(2);
  }

  @Test
  public void testGenerateAllRangesWithSize1() {
    final Map<Bytes32, Bytes32> expectedResult = new HashMap<>();
    expectedResult.put(
        Bytes32.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000000"),
        Bytes32.fromHexString(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"));
    final Map<Bytes32, Bytes32> ranges = RangeManager.generateAllRanges(1);
    assertThat(ranges.size()).isEqualTo(1);
    assertThat(ranges).isEqualTo(expectedResult);
  }

  @Test
  public void testGenerateAllRangesWithSize3() {
    final Map<Bytes32, Bytes32> expectedResult = new HashMap<>();
    expectedResult.put(
        Bytes32.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000000"),
        Bytes32.fromHexString(
            "0x5555555555555555555555555555555555555555555555555555555555555555"));
    expectedResult.put(
        Bytes32.fromHexString("0x5555555555555555555555555555555555555555555555555555555555555556"),
        Bytes32.fromHexString(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"));
    expectedResult.put(
        Bytes32.fromHexString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaac"),
        Bytes32.fromHexString(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"));
    final Map<Bytes32, Bytes32> ranges = RangeManager.generateAllRanges(3);
    assertThat(ranges.size()).isEqualTo(3);
    assertThat(ranges).isEqualTo(expectedResult);
  }

  @Test
  public void testGenerateRangesWithSize3() {
    final Map<Bytes32, Bytes32> expectedResult = new HashMap<>();
    expectedResult.put(
        Bytes32.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000000"),
        Bytes32.fromHexString(
            "0x5555555555555555555555555555555555555555555555555555555555555555"));
    expectedResult.put(
        Bytes32.fromHexString("0x5555555555555555555555555555555555555555555555555555555555555556"),
        Bytes32.fromHexString(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"));
    expectedResult.put(
        Bytes32.fromHexString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaac"),
        Bytes32.fromHexString(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"));
    final Map<Bytes32, Bytes32> ranges =
        RangeManager.generateRanges(
            Bytes32.fromHexString(
                "0x0000000000000000000000000000000000000000000000000000000000000000"),
            Bytes32.fromHexString(
                "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
            3);
    assertThat(ranges.size()).isEqualTo(3);
    assertThat(ranges).isEqualTo(expectedResult);
  }

  @Test
  public void testFindNewBeginElement() {

    final ForestWorldStateKeyValueStorage worldStateKeyValueStorage =
        new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());

    final WorldStateStorageCoordinator worldStateStorageCoordinator =
        new WorldStateStorageCoordinator(worldStateKeyValueStorage);

    final MerkleTrie<Bytes, Bytes> accountStateTrie =
        TrieGenerator.generateTrie(worldStateStorageCoordinator, 15);

    final RangeStorageEntriesCollector collector =
        RangeStorageEntriesCollector.createCollector(
            Bytes32.wrap(Hash.ZERO.getBytes()), RangeManager.MAX_RANGE, 10, Integer.MAX_VALUE);
    final TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);
    final TreeMap<Bytes32, Bytes> accounts =
        (TreeMap<Bytes32, Bytes>)
            accountStateTrie.entriesFrom(
                root ->
                    RangeStorageEntriesCollector.collectEntries(
                        collector, visitor, root, Bytes32.wrap(Hash.ZERO.getBytes())));

    final WorldStateProofProvider worldStateProofProvider =
        new WorldStateProofProvider(worldStateStorageCoordinator);

    // generate the proof
    final List<Bytes> proofs =
        worldStateProofProvider.getAccountProofRelatedNodes(
            Hash.wrap(accountStateTrie.getRootHash()), Bytes32.wrap(Hash.ZERO.getBytes()));
    proofs.addAll(
        worldStateProofProvider.getAccountProofRelatedNodes(
            Hash.wrap(accountStateTrie.getRootHash()), accounts.lastKey()));

    final Optional<Bytes32> newBeginElementInRange =
        RangeManager.findNewBeginElementInRange(
            accountStateTrie.getRootHash(), proofs, accounts, RangeManager.MAX_RANGE);

    assertThat(newBeginElementInRange)
        .contains(Bytes32.leftPad(Bytes.wrap(Bytes.ofUnsignedShort(0x0b))));
  }

  @Test
  public void testFindNewBeginElementWhenNothingIsMissing() {

    final ForestWorldStateKeyValueStorage worldStateKeyValueStorage =
        new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
    final WorldStateStorageCoordinator worldStateStorageCoordinator =
        new WorldStateStorageCoordinator(worldStateKeyValueStorage);

    final MerkleTrie<Bytes, Bytes> accountStateTrie =
        TrieGenerator.generateTrie(worldStateStorageCoordinator, 15);

    final RangeStorageEntriesCollector collector =
        RangeStorageEntriesCollector.createCollector(
            Bytes32.wrap(Hash.ZERO.getBytes()), RangeManager.MAX_RANGE, 15, Integer.MAX_VALUE);
    final TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);
    final TreeMap<Bytes32, Bytes> accounts =
        (TreeMap<Bytes32, Bytes>)
            accountStateTrie.entriesFrom(
                root ->
                    RangeStorageEntriesCollector.collectEntries(
                        collector, visitor, root, Bytes32.wrap(Hash.ZERO.getBytes())));

    final WorldStateProofProvider worldStateProofProvider =
        new WorldStateProofProvider(worldStateStorageCoordinator);

    // generate the proof
    final List<Bytes> proofs =
        worldStateProofProvider.getAccountProofRelatedNodes(
            Hash.wrap(accountStateTrie.getRootHash()), Bytes32.wrap(Hash.ZERO.getBytes()));
    proofs.addAll(
        worldStateProofProvider.getAccountProofRelatedNodes(
            Hash.wrap(accountStateTrie.getRootHash()), accounts.lastKey()));

    final Optional<Bytes32> newBeginElementInRange =
        RangeManager.findNewBeginElementInRange(
            accountStateTrie.getRootHash(), proofs, accounts, RangeManager.MAX_RANGE);

    assertThat(newBeginElementInRange).isEmpty();
  }

  @Test
  public void testFindNewBeginElementWhenResponseTruncatedWithFullCoverageProofs() {
    // A responder that returns only a subset of the keys but provides proofs that cover every
    // leaf in the range must be detected as incomplete. With boundary-only proofs the existing
    // missing-node probe already handles this; the harder case is when the proofs themselves
    // make every leaf reachable.
    final ForestWorldStateKeyValueStorage worldStateKeyValueStorage =
        new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
    final WorldStateStorageCoordinator worldStateStorageCoordinator =
        new WorldStateStorageCoordinator(worldStateKeyValueStorage);

    final MerkleTrie<Bytes, Bytes> accountStateTrie =
        TrieGenerator.generateTrie(worldStateStorageCoordinator, 15);

    final RangeStorageEntriesCollector collector =
        RangeStorageEntriesCollector.createCollector(
            Bytes32.wrap(Hash.ZERO.getBytes()), RangeManager.MAX_RANGE, 15, Integer.MAX_VALUE);
    final TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);
    final TreeMap<Bytes32, Bytes> allAccounts =
        (TreeMap<Bytes32, Bytes>)
            accountStateTrie.entriesFrom(
                root ->
                    RangeStorageEntriesCollector.collectEntries(
                        collector, visitor, root, Bytes32.wrap(Hash.ZERO.getBytes())));

    final WorldStateProofProvider worldStateProofProvider =
        new WorldStateProofProvider(worldStateStorageCoordinator);

    final List<Bytes> proofs = new java.util.ArrayList<>();
    for (Bytes32 key : allAccounts.keySet()) {
      proofs.addAll(
          worldStateProofProvider.getAccountProofRelatedNodes(
              Hash.wrap(accountStateTrie.getRootHash()), key));
    }

    final TreeMap<Bytes32, Bytes> partialAccounts = new TreeMap<>();
    int taken = 0;
    for (Map.Entry<Bytes32, Bytes> e : allAccounts.entrySet()) {
      if (taken++ < 2) partialAccounts.put(e.getKey(), e.getValue());
    }

    final Optional<Bytes32> newBeginElementInRange =
        RangeManager.findNewBeginElementInRange(
            accountStateTrie.getRootHash(), proofs, partialAccounts, RangeManager.MAX_RANGE);

    assertThat(newBeginElementInRange).isPresent();
  }

  @Test
  public void testFindNewBeginElementWhenNoKeysReturnedButRangeNonEmpty() {
    // A responder that returns zero keys but proofs that resolve all leaves in the range must
    // still cause the caller to schedule follow-up fetches for the omitted entries.
    final ForestWorldStateKeyValueStorage worldStateKeyValueStorage =
        new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
    final WorldStateStorageCoordinator worldStateStorageCoordinator =
        new WorldStateStorageCoordinator(worldStateKeyValueStorage);

    final MerkleTrie<Bytes, Bytes> accountStateTrie =
        TrieGenerator.generateTrie(worldStateStorageCoordinator, 15);

    final RangeStorageEntriesCollector collector =
        RangeStorageEntriesCollector.createCollector(
            Bytes32.wrap(Hash.ZERO.getBytes()), RangeManager.MAX_RANGE, 15, Integer.MAX_VALUE);
    final TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);
    final TreeMap<Bytes32, Bytes> allAccounts =
        (TreeMap<Bytes32, Bytes>)
            accountStateTrie.entriesFrom(
                root ->
                    RangeStorageEntriesCollector.collectEntries(
                        collector, visitor, root, Bytes32.wrap(Hash.ZERO.getBytes())));

    final WorldStateProofProvider worldStateProofProvider =
        new WorldStateProofProvider(worldStateStorageCoordinator);

    final List<Bytes> proofs = new java.util.ArrayList<>();
    for (Bytes32 key : allAccounts.keySet()) {
      proofs.addAll(
          worldStateProofProvider.getAccountProofRelatedNodes(
              Hash.wrap(accountStateTrie.getRootHash()), key));
    }

    final Optional<Bytes32> newBeginElementInRange =
        RangeManager.findNewBeginElementInRange(
            accountStateTrie.getRootHash(),
            proofs,
            new TreeMap<>(),
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE);

    assertThat(newBeginElementInRange).isPresent();
  }

  @Test
  public void testGenerateRangesPreservesSortedKeyOrder() {
    final Bytes32 min =
        Bytes32.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000000");
    final Bytes32 max =
        Bytes32.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

    final Map<Bytes32, Bytes32> ranges = RangeManager.generateRanges(min, max, 5);

    assertThat(ranges).isNotEmpty();
    assertKeysAreStrictlyIncreasing(ranges);
  }

  @Test
  public void testGenerateRangesWithLargerCountPreservesSortedKeyOrder() {
    final Bytes32 min =
        Bytes32.fromHexString("0x1000000000000000000000000000000000000000000000000000000000000000");
    final Bytes32 max =
        Bytes32.fromHexString("0x2000000000000000000000000000000000000000000000000000000000000000");

    final Map<Bytes32, Bytes32> ranges =
        RangeManager.generateRanges(min.toUnsignedBigInteger(), max.toUnsignedBigInteger(), 10);

    assertThat(ranges).isNotEmpty();
    assertKeysAreStrictlyIncreasing(ranges);
    for (final Map.Entry<Bytes32, Bytes32> entry : ranges.entrySet()) {
      assertThat(entry.getKey().compareTo(entry.getValue())).isLessThanOrEqualTo(0);
    }
  }

  @Test
  public void testGenerateRangesLargeNbRangePreservesSortedKeyOrder() {
    final BigInteger min = BigInteger.valueOf(1000);
    final BigInteger max = BigInteger.valueOf(2000);

    final Map<Bytes32, Bytes32> ranges = RangeManager.generateRanges(min, max, 100);

    assertThat(ranges).isNotEmpty();
    assertKeysAreStrictlyIncreasing(ranges);
  }

  @Test
  public void testGenerateRangesWithNarrowRangePreservesSortedKeyOrder() {
    final BigInteger min = BigInteger.ZERO;
    final BigInteger max = BigInteger.valueOf(1000);

    final Map<Bytes32, Bytes32> ranges = RangeManager.generateRanges(min, max, 7);

    assertThat(ranges).isNotEmpty();
    assertThat(ranges).hasSize(7);
    assertKeysAreStrictlyIncreasing(ranges);
  }

  @Test
  public void testGenerateRangesStartsWithMin() {
    final Bytes32 bytesMin =
        Bytes32.fromHexString("0x1000000000000000000000000000000000000000000000000000000000000000");
    final Bytes32 bytesMax =
        Bytes32.fromHexString("0x2000000000000000000000000000000000000000000000000000000000000000");

    final Map<Bytes32, Bytes32> bytesRanges = RangeManager.generateRanges(bytesMin, bytesMax, 5);
    assertThat(bytesRanges.entrySet().iterator().next().getKey()).isEqualTo(bytesMin);

    final Map<Bytes32, Bytes32> bigRanges =
        RangeManager.generateRanges(BigInteger.valueOf(1000), BigInteger.valueOf(2000), 100);
    final Bytes32 expectedBigMin =
        Bytes32.leftPad(Bytes.of(BigInteger.valueOf(1000).toByteArray()).trimLeadingZeros());
    assertThat(bigRanges.entrySet().iterator().next().getKey()).isEqualTo(expectedBigMin);

    final Map<Bytes32, Bytes32> singleRange = RangeManager.generateRanges(bytesMin, bytesMax, 1);
    assertThat(singleRange.entrySet().iterator().next().getKey()).isEqualTo(bytesMin);
  }

  private static void assertKeysAreStrictlyIncreasing(final Map<Bytes32, Bytes32> ranges) {
    Bytes32 previousKey = null;
    for (final Bytes32 key : ranges.keySet()) {
      if (previousKey != null) {
        assertThat(key.toUnsignedBigInteger()).isGreaterThan(previousKey.toUnsignedBigInteger());
      }
      previousKey = key;
    }
  }
}
