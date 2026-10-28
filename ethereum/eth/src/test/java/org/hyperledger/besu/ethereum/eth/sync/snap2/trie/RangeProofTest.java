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
package org.hyperledger.besu.ethereum.eth.sync.snap2.trie;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.patricia.DefaultNodeFactory;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RangeProofTest {

  private NavigableMap<Bytes32, Bytes> entries;
  private SimpleMerklePatriciaTrie<Bytes32, Bytes> trie;
  private Bytes32 root;

  @BeforeEach
  void setUp() {
    entries = randomEntries(new Random(42), 500, 40);
    trie = new SimpleMerklePatriciaTrie<>(Function.identity());
    entries.forEach(trie::put);
    root = trie.getRootHash();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 17, 250, 498})
  void acceptsAValidSlice(final int startIndex) {
    final List<Bytes32> keys = new ArrayList<>(entries.keySet());
    final Bytes32 origin = keys.get(startIndex);
    final NavigableMap<Bytes32, Bytes> slice =
        entries.subMap(origin, true, keys.get(Math.min(startIndex + 40, keys.size() - 1)), true);

    final RangeProof.Result result = verify(origin, slice, proof(origin, slice.lastKey()));

    assertThat(result.valid()).isTrue();
    assertThat(result.hasMore()).isEqualTo(!slice.lastKey().equals(entries.lastKey()));
  }

  @Test
  void acceptsAnOriginThatIsNotAKey() {
    final Bytes32 origin = keyBefore(entries.ceilingKey(Bytes32.fromHexString("0x80")));
    final NavigableMap<Bytes32, Bytes> slice = firstEntriesFrom(origin, 30);

    assertThat(verify(origin, slice, proof(origin, slice.lastKey())).valid()).isTrue();
  }

  @Test
  void acceptsTheWholeTrieWithoutProof() {
    final Map<Bytes, Bytes> nodes = new HashMap<>();
    final RangeProof.Result result = verify(Bytes32.ZERO, entries, List.of(), storing(nodes));

    assertThat(result.valid()).isTrue();
    assertThat(result.hasMore()).isFalse();
    assertStored(nodes, root, entries);
  }

  @Test
  void acceptsAnEmptyTrieWithoutProof() {
    final List<Bytes> handed = new ArrayList<>();
    final RangeProof.Result result =
        RangeProof.verify(
            MerkleTrie.EMPTY_TRIE_NODE_HASH,
            Bytes32.ZERO,
            List.of(),
            List.of(),
            List.of(),
            (location, hash, node) -> handed.add(node));

    assertThat(result.valid()).isTrue();
    assertThat(result.hasMore()).isFalse();
    assertThat(handed).isEmpty();
    // no keys without proof is the whole trie only when it is empty
    assertThat(verify(Bytes32.ZERO, new TreeMap<>(), List.of()).valid()).isFalse();
  }

  @Test
  void rejectsAPartialTrieWithoutProof() {
    assertThat(verify(Bytes32.ZERO, firstEntriesFrom(Bytes32.ZERO, 10), List.of()).valid())
        .isFalse();
  }

  @Test
  void acceptsAnEmptyRangeAfterTheLastKey() {
    final Bytes32 origin = keyAfter(entries.lastKey());
    final List<Bytes> proof = proof(origin, origin);
    final List<Bytes> handed = new ArrayList<>();
    final RangeProof.Result result =
        verify(origin, new TreeMap<>(), proof, (location, hash, node) -> handed.add(node));

    assertThat(result.valid()).isTrue();
    assertThat(result.hasMore()).isFalse();
    // it rebuilds only nodes of the proof
    assertThat(handed).isNotEmpty();
    assertThat(proof).containsAll(handed);
  }

  @Test
  void rejectsAnEmptyRangeWhenKeysExist() {
    final Bytes32 origin = keyBefore(entries.ceilingKey(Bytes32.fromHexString("0x80")));
    assertThat(verify(origin, new TreeMap<>(), proof(origin, origin)).valid()).isFalse();
  }

  @Test
  void rejectsAGap() {
    final Bytes32 origin = entries.firstKey();
    final TreeMap<Bytes32, Bytes> slice = new TreeMap<>(firstEntriesFrom(origin, 30));
    final List<Bytes> proof = proof(origin, slice.lastKey());
    assertThat(verify(origin, slice, proof).valid()).isTrue();
    slice.remove(new ArrayList<>(slice.keySet()).get(10));

    assertThat(verify(origin, slice, proof).valid()).isFalse();
  }

  @Test
  void rejectsAMissingFirstKey() {
    final Bytes32 origin = entries.higherKey(entries.firstKey());
    final Bytes32 requested = keyBefore(origin);
    final TreeMap<Bytes32, Bytes> slice = new TreeMap<>(firstEntriesFrom(origin, 20));
    final List<Bytes> proof = proof(requested, slice.lastKey());
    assertThat(verify(requested, slice, proof).valid()).isTrue();
    slice.remove(origin);

    assertThat(verify(requested, slice, proof).valid()).isFalse();
  }

  @Test
  void rejectsAModifiedValue() {
    final Bytes32 origin = entries.firstKey();
    final TreeMap<Bytes32, Bytes> slice = new TreeMap<>(firstEntriesFrom(origin, 30));
    final List<Bytes> proof = proof(origin, slice.lastKey());
    assertThat(verify(origin, slice, proof).valid()).isTrue();
    slice.put(new ArrayList<>(slice.keySet()).get(5), Bytes.of(1, 2, 3));

    assertThat(verify(origin, slice, proof).valid()).isFalse();
  }

  // the sink only gets the nodes of a response whose root matches
  @Test
  void handsNothingForAnInvalidResponse() {
    final Bytes32 origin = entries.firstKey();
    final TreeMap<Bytes32, Bytes> slice = new TreeMap<>(firstEntriesFrom(origin, 30));
    final List<Bytes> proof = proof(origin, slice.lastKey());
    slice.put(new ArrayList<>(slice.keySet()).get(5), Bytes.of(1, 2, 3));
    final TreeMap<Bytes32, Bytes> whole = new TreeMap<>(entries);
    whole.put(whole.firstKey(), Bytes.of(1, 2, 3));
    final List<Bytes> handed = new ArrayList<>();
    final NodeUpdater sink = (location, hash, node) -> handed.add(node);

    assertThat(verify(origin, slice, proof, sink).valid()).isFalse();
    assertThat(verify(Bytes32.ZERO, whole, List.of(), sink).valid()).isFalse();
    assertThat(handed).isEmpty();
  }

  @Test
  void rejectsAnExtraKey() {
    final Bytes32 origin = entries.firstKey();
    final TreeMap<Bytes32, Bytes> slice = new TreeMap<>(firstEntriesFrom(origin, 30));
    final List<Bytes> proof = proof(origin, slice.lastKey());
    assertThat(verify(origin, slice, proof).valid()).isTrue();
    slice.put(keyAfter(new ArrayList<>(slice.keySet()).get(3)), Bytes.of(9));

    assertThat(verify(origin, slice, proof).valid()).isFalse();
  }

  @Test
  void rejectsAMissingProofNode() {
    final Bytes32 origin = entries.firstKey();
    final NavigableMap<Bytes32, Bytes> slice = firstEntriesFrom(origin, 30);
    final List<Bytes> proof = proof(origin, slice.lastKey());
    assertThat(verify(origin, slice, proof).valid()).isTrue();
    proof.removeLast();

    assertThat(verify(origin, slice, proof).valid()).isFalse();
  }

  @Test
  void rejectsKeysBeforeOrigin() {
    final Bytes32 origin = new ArrayList<>(entries.keySet()).get(10);
    final NavigableMap<Bytes32, Bytes> slice = firstEntriesFrom(entries.firstKey(), 30);
    final List<Bytes> proof = proof(origin, slice.lastKey());
    assertThat(verify(origin, slice.tailMap(origin, true), proof).valid()).isTrue();

    assertThat(verify(origin, slice, proof).valid()).isFalse();
  }

  @Test
  void acceptsASingleLeafTrie() {
    final SimpleMerklePatriciaTrie<Bytes32, Bytes> single =
        new SimpleMerklePatriciaTrie<>(Function.identity());
    final Bytes32 key = entries.firstKey();
    single.put(key, entries.firstEntry().getValue());
    final List<Bytes> proof = single.getValueWithProof(key).getProofRelatedNodes();

    final RangeProof.Result result =
        RangeProof.verify(
            single.getRootHash(),
            Bytes32.ZERO,
            List.of(key),
            List.of(entries.firstEntry().getValue()),
            proof);
    assertThat(result.valid()).isTrue();
    assertThat(result.hasMore()).isFalse();
  }

  // the ranges of one root, verified in any order, give every node of its trie, inlined or not
  @Test
  void handsEveryNodeOfATrieOverItsRanges() {
    for (final long seed : new long[] {2, 3, 5, 7, 11, 13, 17, 19}) {
      final Map<Bytes, Bytes> nodes = new HashMap<>();
      final Random random = new Random(seed);
      final boolean clustered = seed > 11;
      final NavigableMap<Bytes32, Bytes> slots =
          clustered
              ? clusteredEntries(random)
              : randomEntries(random, 300 + random.nextInt(2000), 40);
      final SimpleMerklePatriciaTrie<Bytes32, Bytes> other =
          new SimpleMerklePatriciaTrie<>(Function.identity());
      slots.forEach(other::put);
      final List<Map.Entry<Bytes32, NavigableMap<Bytes32, Bytes>>> ranges = split(random, slots);
      if (clustered) {
        // some ranges end inside a cluster, so their proofs meet inlined nodes
        assertThat(ranges)
            .anyMatch(
                range -> {
                  final Bytes32 next = slots.higherKey(range.getValue().lastKey());
                  return next != null
                      && next.slice(0, 31).equals(range.getValue().lastKey().slice(0, 31));
                });
      }
      Collections.shuffle(ranges, random);

      for (final Map.Entry<Bytes32, NavigableMap<Bytes32, Bytes>> range : ranges) {
        final List<Bytes> proof =
            RangeProofs.proof(other, range.getKey(), range.getValue().lastKey());
        assertThat(
                RangeProof.verify(
                        other.getRootHash(),
                        range.getKey(),
                        new ArrayList<>(range.getValue().keySet()),
                        new ArrayList<>(range.getValue().values()),
                        proof,
                        storing(nodes))
                    .valid())
            .isTrue();
      }
      assertStored(nodes, other.getRootHash(), slots);
    }
  }

  @Test
  void rejectsAMalformedInlinedNode() {
    // a root whose first child is an inlined list of three items, which no trie node is
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeRaw(Bytes.fromHexString("0xc3010203"));
    for (int i = 1; i < 16; i++) {
      if (i == 5) {
        out.writeBytes(Bytes32.ZERO);
      } else {
        out.writeNull();
      }
    }
    out.writeNull();
    out.endList();
    final Bytes rootNode = out.encoded();

    final RangeProof.Result result =
        RangeProof.verify(
            Bytes32.wrap(Hash.hash(rootNode).getBytes()),
            Bytes32.fromHexString("0x10" + "00".repeat(31)),
            List.of(),
            List.of(),
            List.of(rootNode));
    assertThat(result.valid()).isFalse();
  }

  @Test
  void rejectsABranchBelowTheKeyDepth() {
    // an extension of 63 nibbles over a branch puts the next branch at depth 64, below every key
    final DefaultNodeFactory<Bytes> factory = new DefaultNodeFactory<>(Function.identity());
    final Node<Bytes> leaf =
        factory.createLeaf(Bytes.of(CompactEncoding.LEAF_TERMINATOR), Bytes.wrap(new byte[40]));
    final Node<Bytes> deepest = factory.createBranch((byte) 0, leaf, (byte) 1, leaf);
    final Node<Bytes> branch = factory.createBranch((byte) 1, deepest, (byte) 2, leaf);
    final Node<Bytes> rootNode =
        factory.createExtension(Bytes.fromHexString("0x" + "01".repeat(63)), branch);

    final RangeProof.Result result =
        RangeProof.verify(
            rootNode.getHash(),
            Bytes32.fromHexString("0x" + "11".repeat(32)),
            List.of(),
            List.of(),
            List.of(
                rootNode.getEncodedBytes(), branch.getEncodedBytes(), deepest.getEncodedBytes()));
    assertThat(result.valid()).isFalse();
  }

  // consecutive ranges as peers serve them, cut at random keys, keyed by their origin
  private static List<Map.Entry<Bytes32, NavigableMap<Bytes32, Bytes>>> split(
      final Random random, final NavigableMap<Bytes32, Bytes> slots) {
    final List<Map.Entry<Bytes32, NavigableMap<Bytes32, Bytes>>> ranges = new ArrayList<>();
    final List<Bytes32> keys = List.copyOf(slots.keySet());
    Bytes32 origin = RangeManager.MIN_RANGE;
    int from = 0;
    while (from < keys.size()) {
      final int to = Math.min(keys.size(), from + 1 + random.nextInt(keys.size() / 4));
      final Bytes32 last = keys.get(to - 1);
      ranges.add(Map.entry(origin, new TreeMap<>(slots.subMap(keys.get(from), true, last, true))));
      origin = keyAfter(last);
      from = to;
    }
    return ranges;
  }

  // the handed nodes keyed by location and hash, as Bonsai stores them
  private static NodeUpdater storing(final Map<Bytes, Bytes> nodes) {
    return (location, hash, node) -> nodes.put(Bytes.concatenate(location, hash), node);
  }

  // every leaf reads back through the handed nodes
  private static void assertStored(
      final Map<Bytes, Bytes> nodes, final Bytes32 root, final Map<Bytes32, Bytes> slots) {
    final StoredMerklePatriciaTrie<Bytes32, Bytes> stored =
        new StoredMerklePatriciaTrie<>(
            (location, hash) -> Optional.ofNullable(nodes.get(Bytes.concatenate(location, hash))),
            root,
            Function.identity(),
            Function.identity());
    assertThat(stored.entriesFrom(Bytes32.ZERO, Integer.MAX_VALUE)).isEqualTo(slots);
  }

  private RangeProof.Result verify(
      final Bytes32 origin,
      final Map<Bytes32, Bytes> slice,
      final List<Bytes> proof,
      final NodeUpdater sink) {
    return RangeProof.verify(
        root,
        origin,
        new ArrayList<>(slice.keySet()),
        new ArrayList<>(slice.values()),
        proof,
        sink);
  }

  private RangeProof.Result verify(
      final Bytes32 origin, final Map<Bytes32, Bytes> slice, final List<Bytes> proof) {
    return RangeProof.verify(
        root, origin, new ArrayList<>(slice.keySet()), new ArrayList<>(slice.values()), proof);
  }

  private List<Bytes> proof(final Bytes32 origin, final Bytes32 last) {
    return RangeProofs.proof(trie, origin, last);
  }

  private NavigableMap<Bytes32, Bytes> firstEntriesFrom(final Bytes32 origin, final int count) {
    final TreeMap<Bytes32, Bytes> out = new TreeMap<>();
    for (final Map.Entry<Bytes32, Bytes> e : entries.tailMap(origin, true).entrySet()) {
      if (out.size() == count) {
        break;
      }
      out.put(e.getKey(), e.getValue());
    }
    return out;
  }

  private static Bytes32 keyBefore(final Bytes32 key) {
    return UInt256.fromBytes(key).subtract(1).toBytes();
  }

  private static Bytes32 keyAfter(final Bytes32 key) {
    return RangeManager.incrementBytes32(key).orElseThrow();
  }

  // random slots plus clusters sharing their first 31 bytes, with 1-byte values: the deepest nodes
  // of a cluster are inlined in their parent
  private static NavigableMap<Bytes32, Bytes> clusteredEntries(final Random random) {
    final NavigableMap<Bytes32, Bytes> entries = randomEntries(random, 50, 40);
    for (int cluster = 0; cluster < 40; cluster++) {
      final byte[] key = Bytes32.random(random).toArray();
      for (int i = 2 + random.nextInt(8); i > 0; i--) {
        key[31] = (byte) random.nextInt(256);
        entries.put(Bytes32.wrap(key.clone()), Bytes.of(1 + random.nextInt(0x7f)));
      }
    }
    return entries;
  }

  private static NavigableMap<Bytes32, Bytes> randomEntries(
      final Random random, final int size, final int maxValueSize) {
    final TreeMap<Bytes32, Bytes> entries = new TreeMap<>();
    while (entries.size() < size) {
      final byte[] key = new byte[32];
      random.nextBytes(key);
      final byte[] value = new byte[1 + random.nextInt(maxValueSize)];
      random.nextBytes(value);
      entries.put(Bytes32.wrap(key), Bytes.wrap(value));
    }
    return entries;
  }
}
