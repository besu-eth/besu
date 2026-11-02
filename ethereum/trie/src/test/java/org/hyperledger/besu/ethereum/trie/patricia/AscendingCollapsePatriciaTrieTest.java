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
package org.hyperledger.besu.ethereum.trie.patricia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.StoredNode;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AscendingCollapsePatriciaTrieTest {

  // trailing nibbles a cluster varies: fixing nibble 62 below a varying nibble 61 makes short
  // extensions above inlined branches
  private static final int[][] CLUSTERS = {{63}, {62, 63}, {61, 63}, {60, 61, 62, 63}};

  @Test
  void emptyTrieHasTheEmptyRoot() {
    assertThat(new AscendingCollapsePatriciaTrie().rootHash())
        .isEqualTo(new SimpleMerklePatriciaTrie<>(Function.identity()).getRootHash());
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 17, 64, 256, 5000})
  void matchesAStoredTrieAtAnySize(final int size) {
    final List<Bytes> keys = hashedKeys(size);
    final Map<Bytes, Bytes> handed = new HashMap<>();
    final AscendingCollapsePatriciaTrie trie = new AscendingCollapsePatriciaTrie(onceEach(handed));
    keys.forEach(key -> trie.insert(key, value(key)));

    assertThat(trie.rootHash()).isEqualTo(plainRoot(keys));
    assertThat(handed).isEqualTo(storedNodes(keys));
  }

  @Test
  void handsEachSubtreeOnceTheNextKeyFinishesIt() {
    final List<Bytes> keys = hashedKeys(3000);
    final Map<Bytes, Bytes> handed = new HashMap<>();
    final AscendingCollapsePatriciaTrie trie = new AscendingCollapsePatriciaTrie(onceEach(handed));
    keys.forEach(key -> trie.insert(key, value(key)));
    final Set<Bytes> beforeRoot = new HashSet<>(handed.keySet());
    trie.rootHash();

    // the root hash hands only the path of the last key: the rest went as the keys came
    final Bytes lastPath = CompactEncoding.bytesToPath(keys.getLast());
    final Set<Bytes> atRoot = new HashSet<>(handed.keySet());
    atRoot.removeAll(beforeRoot);
    assertThat(atRoot)
        .contains(Bytes.EMPTY)
        .allMatch(location -> lastPath.commonPrefixLength(location) == location.size());
    assertThat(handed).isEqualTo(storedNodes(keys));
  }

  @Test
  void prepareHandsEachSubtreeOnceTheNextKeyFinishesIt() {
    final List<Bytes> keys = hashedKeys(3000);
    final Map<Bytes, Bytes> handed = new HashMap<>();
    final AscendingCollapsePatriciaTrie trie = new AscendingCollapsePatriciaTrie(onceEach(handed));
    // how many nodes the sink holds when prepare reads each key
    final List<Integer> handedAtKey = new ArrayList<>();
    final List<Bytes> watched =
        new AbstractList<>() {
          @Override
          public Bytes get(final int index) {
            handedAtKey.add(handed.size());
            return keys.get(index);
          }

          @Override
          public int size() {
            return keys.size();
          }
        };
    trie.prepare(watched, values(keys));

    // by the last key, the subtrees the earlier keys finished are already handed
    assertThat(handedAtKey.get(keys.size() - 1)).isGreaterThan(storedNodes(keys).size() / 2);
  }

  // each subtree of the reference trie goes in by its keys, prepared, by its reference or split
  // further, so every way to build a trie meets every node shape
  @Test
  void matchesAStoredTrieBuiltFromAnyMixOfInserts() {
    final Set<String> met = new HashSet<>();
    for (int seed = 0; seed < 100; seed++) {
      final NavigableMap<Bytes, Bytes> entries = clusteredEntries(new Random(seed));
      final StoredMerklePatriciaTrie<Bytes, Bytes> reference = storedTrie();
      entries.forEach(reference::put);
      final Map<Bytes, Bytes> stored = new HashMap<>();
      reference.commit((location, hash, node) -> stored.put(location, node));

      final Map<Bytes, Bytes> handed = new HashMap<>();
      final Mix mix =
          new Mix(
              new Random(-seed - 1),
              entries,
              stored,
              new AscendingCollapsePatriciaTrie(onceEach(handed)),
              met);
      mix.split(Bytes.EMPTY, TrieNodeDecoder.decode(Bytes.EMPTY, stored.get(Bytes.EMPTY)));

      assertThat(mix.trie.rootHash()).as("seed %d", seed).isEqualTo(reference.getRootHash());
      assertThat(mix.trie.insertCount()).as("seed %d", seed).isEqualTo(mix.inserted);
      stored.keySet().removeIf(mix::isSealed);
      assertThat(handed).as("seed %d", seed).isEqualTo(stored);
    }
    assertThat(met)
        .containsExactlyInAnyOrder(
            "prepared leaf",
            "prepared extension",
            "prepared branch",
            "sealed hashed",
            "sealed inlined leaf",
            "sealed inlined extension",
            "sealed inlined branch",
            "sealed at depth 64",
            "sealed below an extension");
  }

  // prepare reads no state of the trie: the last groups are prepared before the first insert, last
  // first, and the others while the trie holds the groups before them
  @Test
  void preparedPrefixGroupsMatchPlainInserts() {
    final List<Bytes> keys = hashedKeys(3000);
    final Map<Bytes, Bytes> expected = storedNodes(keys);
    for (int nibbles = 1; nibbles <= 4; nibbles++) {
      final Map<Bytes, Bytes> handed = new HashMap<>();
      final AscendingCollapsePatriciaTrie trie =
          new AscendingCollapsePatriciaTrie(onceEach(handed));
      final List<List<Bytes>> groups = prefixGroups(keys, nibbles);
      final int half = groups.size() / 2;
      final List<AscendingCollapsePatriciaTrie.Subtree> later = new ArrayList<>();
      for (final List<Bytes> group : groups.subList(half, groups.size()).reversed()) {
        later.addFirst(trie.prepare(group, values(group)));
      }
      for (final List<Bytes> group : groups.subList(0, half)) {
        trie.insert(trie.prepare(group, values(group)));
      }
      later.forEach(trie::insert);

      assertThat(trie.insertCount()).isEqualTo(keys.size());
      assertThat(trie.rootHash()).as("groups on %d nibble(s)", nibbles).isEqualTo(plainRoot(keys));
      assertThat(handed).as("groups on %d nibble(s)", nibbles).isEqualTo(expected);
    }
  }

  // each group goes key by key to its own sink, as the partitions of a state trie build
  @Test
  void buildsPrefixGroupsKeyByKeyIntoTheirOwnSinks() {
    final List<Bytes> keys = hashedKeys(3000);
    final Map<Bytes, Bytes> handed = new HashMap<>();
    final AscendingCollapsePatriciaTrie trie = new AscendingCollapsePatriciaTrie(onceEach(handed));
    final List<Map<Bytes, Bytes>> handedByGroups = new ArrayList<>();
    for (final List<Bytes> group : prefixGroups(keys, 2)) {
      final Map<Bytes, Bytes> groupHanded = new HashMap<>();
      handedByGroups.add(groupHanded);
      final AscendingCollapsePatriciaTrie.SubtreeBuilder subtree =
          trie.subtreeBuilder(onceEach(groupHanded));
      group.forEach(key -> subtree.insert(key, value(key)));
      trie.insert(subtree.build());
    }

    assertThat(trie.insertCount()).isEqualTo(keys.size());
    assertThat(trie.rootHash()).isEqualTo(plainRoot(keys));
    // the groups hand the nodes below their top, most of them, and the trie the others, each once
    assertThat(handedByGroups.stream().mapToInt(Map::size).sum()).isGreaterThan(handed.size());
    handedByGroups.forEach(
        groupHanded ->
            groupHanded.forEach(
                (location, node) ->
                    assertThat(handed.put(location, node))
                        .as("handed twice at %s", location)
                        .isNull()));
    assertThat(handed).isEqualTo(storedNodes(keys));
  }

  @Test
  void aBuiltSubtreeNeedsAKeyAndIsBuiltOnce() {
    final Bytes key = hashedKeys(1).getFirst();
    final AscendingCollapsePatriciaTrie.SubtreeBuilder subtree =
        new AscendingCollapsePatriciaTrie().subtreeBuilder((location, hash, node) -> {});
    assertThat(subtree.isEmpty()).isTrue();
    assertThatThrownBy(subtree::build).isInstanceOf(IllegalStateException.class);

    subtree.insert(key, value(key));
    subtree.build();

    assertThatThrownBy(() -> subtree.insert(key, value(key)))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(subtree::build).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void aPreparedSubtreeAloneIsTheWholeTrie() {
    final List<Bytes> keys = hashedKeys(40);
    final AscendingCollapsePatriciaTrie trie = new AscendingCollapsePatriciaTrie();
    trie.insert(trie.prepare(keys, values(keys)));
    assertThat(trie.rootHash()).isEqualTo(plainRoot(keys));
  }

  @Test
  void aSealedRootIsTheWholeTrie() {
    final List<Bytes> keys = hashedKeys(10);
    final AscendingCollapsePatriciaTrie trie = new AscendingCollapsePatriciaTrie();
    trie.insertSealed(Bytes.EMPTY, RLP.encodeOne(plainRoot(keys)));
    assertThat(trie.rootHash()).isEqualTo(plainRoot(keys));
  }

  @Test
  void rejectsDescendingKeys() {
    final AscendingCollapsePatriciaTrie ascending = new AscendingCollapsePatriciaTrie();
    final Bytes value = RLP.encode(out -> out.writeInt(1));
    ascending.insert(Bytes.fromHexString("0x" + "ff".repeat(32)), value);
    assertThatThrownBy(() -> ascending.insert(Bytes.fromHexString("0x" + "00".repeat(32)), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ascending");
  }

  // the visitor does not check duplicates: insert and prepare reject them first
  @Test
  void rejectsADuplicateKey() {
    final Bytes key = hashedKeys(1).getFirst();
    final AscendingCollapsePatriciaTrie trie = new AscendingCollapsePatriciaTrie();
    trie.insert(key, value(key));
    assertThatThrownBy(() -> trie.insert(key, value(key)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ascending");
    assertThatThrownBy(() -> trie.prepare(List.of(key, key), values(List.of(key, key))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ascending");
  }

  @Test
  void rejectsEmptyValues() {
    final Bytes key = hashedKeys(1).getFirst();
    final AscendingCollapsePatriciaTrie trie = new AscendingCollapsePatriciaTrie();
    assertThatThrownBy(() -> trie.insert(key, Bytes.EMPTY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("empty");
    assertThatThrownBy(() -> trie.prepare(List.of(key), List.of(Bytes.EMPTY)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("empty");
  }

  @Test
  void prepareRejectsUnsortedKeys() {
    final List<Bytes> keys = hashedKeys(2).reversed();
    final AscendingCollapsePatriciaTrie trie = new AscendingCollapsePatriciaTrie();
    assertThatThrownBy(() -> trie.prepare(keys, values(keys)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ascending");
  }

  @Test
  void rejectsAPreparedGroupMissingKeysOfItsPrefix() {
    final Bytes first = Bytes.fromHexString("0x11" + "00".repeat(31));
    final List<Bytes> group =
        List.of(
            Bytes.fromHexString("0x12" + "00".repeat(31)),
            Bytes.fromHexString("0x13" + "00".repeat(31)));
    final List<Bytes> handed = new ArrayList<>();
    final AscendingCollapsePatriciaTrie trie =
        new AscendingCollapsePatriciaTrie((location, hash, node) -> handed.add(location));
    trie.insert(first, value(first));
    // The group's prefix is nibble 1, which the first key also starts with.
    assertThatThrownBy(() -> trie.insert(trie.prepare(group, values(group))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("complete subtree");
    // the split rejects the group before it hands the first key, which the group would finish
    assertThat(handed).doesNotContain(Bytes.of(1, 1));

    // Keys that differ at the first nibble can only be the whole trie.
    final List<Bytes> whole =
        List.of(
            Bytes.fromHexString("0x20" + "00".repeat(31)),
            Bytes.fromHexString("0x30" + "00".repeat(31)));
    final AscendingCollapsePatriciaTrie other = new AscendingCollapsePatriciaTrie();
    other.insert(first, value(first));
    assertThatThrownBy(() -> other.insert(other.prepare(whole, values(whole))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("complete subtree");
  }

  @Test
  void rejectsAKeyThePreparedKeysMissed() {
    final List<Bytes> group =
        List.of(
            Bytes.fromHexString("0x00" + "00".repeat(31)),
            Bytes.fromHexString("0x0550" + "00".repeat(30)));
    final AscendingCollapsePatriciaTrie trie = new AscendingCollapsePatriciaTrie();
    trie.insert(trie.prepare(group, values(group)));
    // the missed key goes under the second prepared key, already collapsed
    final Bytes missed = Bytes.fromHexString("0x0580" + "00".repeat(30));
    assertThatThrownBy(() -> trie.insert(missed, value(missed)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("complete subtree");
  }

  @Test
  void insertsAgainAKeyWhoseInsertThrew() {
    final List<Bytes> keys = hashedKeys(2);
    final boolean[] failing = {true};
    final AscendingCollapsePatriciaTrie trie =
        new AscendingCollapsePatriciaTrie(
            (location, hash, node) -> {
              if (failing[0]) {
                failing[0] = false;
                throw new IllegalStateException("sink down");
              }
            });
    trie.insert(keys.get(0), value(keys.get(0)));
    // the second key finishes the subtree of the first, which the sink fails to take
    assertThatThrownBy(() -> trie.insert(keys.get(1), value(keys.get(1)))).hasMessage("sink down");
    assertThat(trie.insertCount()).isEqualTo(1);

    trie.insert(keys.get(1), value(keys.get(1)));
    assertThat(trie.insertCount()).isEqualTo(2);
    assertThat(trie.rootHash()).isEqualTo(plainRoot(keys));
  }

  @Test
  void rejectsAPreparedGroupBeforeTheLastKey() {
    final List<Bytes> keys = hashedKeys(10);
    final AscendingCollapsePatriciaTrie trie = new AscendingCollapsePatriciaTrie();
    trie.insert(keys.getLast(), value(keys.getLast()));
    final List<Bytes> earlier = keys.subList(0, 1);
    assertThatThrownBy(() -> trie.insert(trie.prepare(earlier, values(earlier))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ascending");
  }

  @Test
  void rejectsASealedSubtreeHoldingTheLastKey() {
    final Bytes key = Bytes.fromHexString("0x11" + "00".repeat(31));
    final AscendingCollapsePatriciaTrie trie = new AscendingCollapsePatriciaTrie();
    trie.insert(key, value(key));
    assertThatThrownBy(() -> trie.insertSealed(Bytes.of(1), RLP.encodeOne(Bytes32.ZERO)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ascending");
  }

  @Test
  void rootHashSealsTheTrie() {
    final List<Bytes> keys = hashedKeys(100);
    final Map<Bytes, Bytes> handed = new HashMap<>();
    final AscendingCollapsePatriciaTrie trie = new AscendingCollapsePatriciaTrie(onceEach(handed));
    keys.subList(0, 50).forEach(key -> trie.insert(key, value(key)));
    final Bytes32 root = trie.rootHash();

    // a second call hands nothing again, which the sink would reject
    assertThat(trie.rootHash()).isEqualTo(root);
    final Bytes later = keys.get(50);
    assertThatThrownBy(() -> trie.insert(later, value(later)))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> trie.insertSealed(Bytes.of(0x0f), RLP.encodeOne(Bytes32.ZERO)))
        .isInstanceOf(IllegalStateException.class);
  }

  // builds a trie from the subtrees of a reference one, each in a way picked at random
  private static final class Mix {
    private final Random random;
    private final NavigableMap<Bytes, Bytes> entries;
    private final Map<Bytes, Bytes> stored;
    private final AscendingCollapsePatriciaTrie trie;
    private final Set<String> met;
    private final List<Bytes> sealed = new ArrayList<>();
    private long inserted;

    private Mix(
        final Random random,
        final NavigableMap<Bytes, Bytes> entries,
        final Map<Bytes, Bytes> stored,
        final AscendingCollapsePatriciaTrie trie,
        final Set<String> met) {
      this.random = random;
      this.entries = entries;
      this.stored = stored;
      this.trie = trie;
      this.met = met;
    }

    // the subtree at a location, as its parent holds it
    private void build(final Bytes location, final Node<Bytes> held, final boolean belowExtension) {
      final Node<Bytes> node =
          held instanceof StoredNode
              ? TrieNodeDecoder.decode(location, stored.get(location))
              : held;
      switch (random.nextInt(6)) {
        case 0 -> keysUnder(location).forEach(this::insert);
        case 1 -> {
          final NavigableMap<Bytes, Bytes> keys = keysUnder(location);
          trie.insert(trie.prepare(new ArrayList<>(keys.keySet()), new ArrayList<>(keys.values())));
          inserted += keys.size();
          met.add("prepared " + kind(node));
        }
        case 2 -> {
          trie.insertSealed(location, held.getEncodedBytesRef());
          sealed.add(location);
          met.add(held instanceof StoredNode ? "sealed hashed" : "sealed inlined " + kind(node));
          if (location.size() == 64) {
            met.add("sealed at depth 64");
          }
          if (belowExtension) {
            met.add("sealed below an extension");
          }
        }
        default -> split(location, node);
      }
    }

    private void split(final Bytes location, final Node<Bytes> node) {
      if (node instanceof BranchNode<Bytes> branch) {
        for (int i = 0; i < branch.maxChild(); i++) {
          final Node<Bytes> child = branch.child((byte) i);
          if (!(child instanceof NullNode)) {
            build(Bytes.concatenate(location, Bytes.of(i)), child, false);
          }
        }
      } else if (node instanceof ExtensionNode<Bytes> extension) {
        build(Bytes.concatenate(location, extension.getPath()), extension.getChild(), true);
      } else {
        keysUnder(location).forEach(this::insert);
      }
    }

    private void insert(final Bytes key, final Bytes value) {
      trie.insert(key, value);
      inserted++;
    }

    private NavigableMap<Bytes, Bytes> keysUnder(final Bytes location) {
      return entries.subMap(keyUnder(location, 0), true, keyUnder(location, 0x0f), true);
    }

    private boolean isSealed(final Bytes location) {
      return sealed.stream().anyMatch(s -> location.commonPrefixLength(s) == s.size());
    }

    private static String kind(final Node<Bytes> node) {
      return node instanceof BranchNode
          ? "branch"
          : node instanceof LeafNode ? "leaf" : "extension";
    }
  }

  // random keys, plus clusters of keys sharing all but a few trailing nibbles with 1-byte values,
  // which inline their deepest nodes
  private static NavigableMap<Bytes, Bytes> clusteredEntries(final Random random) {
    final TreeMap<Bytes, Bytes> entries = new TreeMap<>();
    for (int i = 0; i < 20; i++) {
      entries.put(Bytes32.random(random), Bytes32.random(random).slice(0, 1 + random.nextInt(32)));
    }
    for (int cluster = 0; cluster < 8; cluster++) {
      final MutableBytes path = CompactEncoding.bytesToPath(Bytes32.random(random)).mutableCopy();
      final int[] varying = CLUSTERS[random.nextInt(CLUSTERS.length)];
      for (int i = 2 + random.nextInt(8); i > 0; i--) {
        for (final int nibble : varying) {
          path.set(nibble, (byte) random.nextInt(16));
        }
        entries.put(CompactEncoding.pathToBytes(path), Bytes.of(1 + random.nextInt(0x7f)));
      }
    }
    return entries;
  }

  // the lowest or the highest key under a location
  private static Bytes keyUnder(final Bytes location, final int fill) {
    final MutableBytes path = MutableBytes.create(65);
    path.fill((byte) fill);
    path.set(0, location);
    path.set(64, CompactEncoding.LEAF_TERMINATOR);
    return CompactEncoding.pathToBytes(path);
  }

  // a sink that fails when a location is handed twice
  private static NodeUpdater onceEach(final Map<Bytes, Bytes> handed) {
    return (location, hash, node) ->
        assertThat(handed.put(location, node)).as("handed twice at %s", location).isNull();
  }

  private static List<Bytes> hashedKeys(final int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> Hash.hash(Bytes.ofUnsignedInt(i)).getBytes())
        .sorted(Comparator.naturalOrder())
        .toList();
  }

  private static Bytes value(final Bytes key) {
    return RLP.encode(out -> out.writeBytes(key.slice(0, 1 + (key.get(31) & 0x1f))));
  }

  private static List<Bytes> values(final List<Bytes> keys) {
    return keys.stream().map(AscendingCollapsePatriciaTrieTest::value).toList();
  }

  private static StoredMerklePatriciaTrie<Bytes, Bytes> storedTrie() {
    return new StoredMerklePatriciaTrie<>(
        (location, hash) -> Optional.empty(), Function.identity(), Function.identity());
  }

  private static Map<Bytes, Bytes> storedNodes(final List<Bytes> keys) {
    final StoredMerklePatriciaTrie<Bytes, Bytes> trie = storedTrie();
    keys.forEach(key -> trie.put(key, value(key)));
    final Map<Bytes, Bytes> stored = new HashMap<>();
    trie.commit((location, hash, node) -> stored.put(location, node));
    return stored;
  }

  private static Bytes32 plainRoot(final List<Bytes> keys) {
    final MerkleTrie<Bytes, Bytes> simple = new SimpleMerklePatriciaTrie<>(Function.identity());
    keys.forEach(key -> simple.put(key, value(key)));
    return simple.getRootHash();
  }

  /** Consecutive keys sharing their first {@code nibbles} nibbles. */
  private static List<List<Bytes>> prefixGroups(final List<Bytes> keys, final int nibbles) {
    final List<List<Bytes>> groups = new ArrayList<>();
    Bytes prefix = null;
    for (final Bytes key : keys) {
      final Bytes keyPrefix = CompactEncoding.bytesToPath(key).slice(0, nibbles);
      if (!keyPrefix.equals(prefix)) {
        groups.add(new ArrayList<>());
        prefix = keyPrefix;
      }
      groups.getLast().add(key);
    }
    return groups;
  }
}
