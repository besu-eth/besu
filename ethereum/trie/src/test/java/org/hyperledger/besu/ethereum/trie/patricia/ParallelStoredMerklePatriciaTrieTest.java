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

import org.hyperledger.besu.ethereum.trie.KeyValueMerkleStorage;
import org.hyperledger.besu.ethereum.trie.MerkleStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ParallelStoredMerklePatriciaTrieTest {

  private MerkleStorage parallelStorage;
  private MerkleStorage sequentialStorage;
  private ParallelStoredMerklePatriciaTrie<Bytes, Bytes> parallelTrie;
  private StoredMerklePatriciaTrie<Bytes, Bytes> sequentialTrie;

  @BeforeEach
  void setUp() {
    parallelStorage = new KeyValueMerkleStorage(new InMemoryKeyValueStorage());
    sequentialStorage = new KeyValueMerkleStorage(new InMemoryKeyValueStorage());

    parallelTrie =
        new ParallelStoredMerklePatriciaTrie<>(
            parallelStorage::get, Function.identity(), Function.identity());

    sequentialTrie =
        new StoredMerklePatriciaTrie<>(
            sequentialStorage::get, Function.identity(), Function.identity());
  }

  @Test
  void shouldCreateEmptyTrie() {
    assertThat(parallelTrie.getRootHash()).isEqualTo(StoredMerklePatriciaTrie.EMPTY_TRIE_NODE_HASH);
  }

  @Test
  void shouldPutAndGetSingleKey() {
    final Bytes key = createKey(0x01, 0x02, 0x03, 0x04);
    final Bytes value = createValue(100);

    parallelTrie.put(key, value);
    parallelTrie.commit(parallelStorage::put);

    assertThat(parallelTrie.get(key)).isEqualTo(Optional.of(value));
  }

  @Test
  void shouldHandleMultipleKeys() {
    final int numEntries = 100;

    for (int i = 0; i < numEntries; i++) {
      final Bytes key = createKey(i);
      final Bytes value = createValue(i);
      parallelTrie.put(key, value);
      sequentialTrie.put(key, value);
    }

    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);

    // Verify parallel trie
    for (int i = 0; i < numEntries; i++) {
      final Bytes key = createKey(i);
      final Bytes value = createValue(i);
      assertThat(parallelTrie.get(key)).isEqualTo(Optional.of(value));
    }

    // Compare root hashes
    assertThat(parallelTrie.getRootHash()).isEqualTo(sequentialTrie.getRootHash());
  }

  @Test
  void shouldRemoveKey() {
    final Bytes key = createKey(0x01, 0x02, 0x03, 0x04);
    final Bytes value = createValue(100);

    // Insert in both tries
    parallelTrie.put(key, value);
    sequentialTrie.put(key, value);
    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);

    assertThat(parallelTrie.get(key)).isEqualTo(Optional.of(value));

    // Remove from both
    parallelTrie.remove(key);
    sequentialTrie.remove(key);
    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);

    assertThat(parallelTrie.get(key)).isEmpty();
    assertThat(parallelTrie.getRootHash()).isEqualTo(sequentialTrie.getRootHash());
  }

  @Test
  void shouldUpdateKey() {
    final Bytes key = createKey(0x01, 0x02, 0x03, 0x04);
    final Bytes value1 = createValue(100);
    final Bytes value2 = createValue(200);

    // Insert initial value in both
    parallelTrie.put(key, value1);
    sequentialTrie.put(key, value1);
    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);
    assertThat(parallelTrie.get(key)).isEqualTo(Optional.of(value1));

    // Update in both
    parallelTrie.put(key, value2);
    sequentialTrie.put(key, value2);
    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);

    assertThat(parallelTrie.get(key)).isEqualTo(Optional.of(value2));
    assertThat(parallelTrie.getRootHash()).isEqualTo(sequentialTrie.getRootHash());
  }

  @Test
  void shouldHandleKeysWithCommonPrefix() {
    // Create keys with common prefix (first 3 bytes identical)
    // Keys: [0x01, 0x02, 0x03, 0x00] to [0x01, 0x02, 0x03, 0x13]
    for (int i = 0; i < 20; i++) {
      final Bytes key = createKey(0x01, 0x02, 0x03, i);
      final Bytes value = createValue(i);
      parallelTrie.put(key, value);
      sequentialTrie.put(key, value);
    }

    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);

    // Verify all entries
    for (int i = 0; i < 20; i++) {
      final Bytes key = createKey(0x01, 0x02, 0x03, i);
      final Bytes value = createValue(i);
      assertThat(parallelTrie.get(key)).isEqualTo(Optional.of(value));
    }

    assertThat(parallelTrie.getRootHash()).isEqualTo(sequentialTrie.getRootHash());
  }

  @Test
  void shouldHandleDivergingKeysAtDifferentDepths() {
    // Keys that diverge at different byte positions
    final Bytes key1 = createKey(0x01, 0x00, 0x00, 0x00); // Diverges at byte 0
    final Bytes key2 = createKey(0x02, 0x00, 0x00, 0x00); // Diverges at byte 0
    final Bytes key3 = createKey(0x03, 0x04, 0x05, 0x01); // Diverges at byte 3
    final Bytes key4 = createKey(0x03, 0x04, 0x05, 0x02); // Diverges at byte 3

    parallelTrie.put(key1, createValue(1));
    parallelTrie.put(key2, createValue(2));
    parallelTrie.put(key3, createValue(3));
    parallelTrie.put(key4, createValue(4));

    sequentialTrie.put(key1, createValue(1));
    sequentialTrie.put(key2, createValue(2));
    sequentialTrie.put(key3, createValue(3));
    sequentialTrie.put(key4, createValue(4));

    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);

    assertThat(parallelTrie.get(key1)).isEqualTo(Optional.of(createValue(1)));
    assertThat(parallelTrie.get(key2)).isEqualTo(Optional.of(createValue(2)));
    assertThat(parallelTrie.get(key3)).isEqualTo(Optional.of(createValue(3)));
    assertThat(parallelTrie.get(key4)).isEqualTo(Optional.of(createValue(4)));

    assertThat(parallelTrie.getRootHash()).isEqualTo(sequentialTrie.getRootHash());
  }

  @Test
  void shouldReachLeafTerminatorWithKeys() {
    // 4 bytes = 8 nibbles
    // To reach the leaf terminator, we need keys that share all 8 nibbles
    // and then one has a value at that exact location

    // Key that will have a value at the end of its path (all 8 nibbles consumed)
    final Bytes exactKey = createKey(0x12, 0x34, 0x56, 0x78);

    // Insert the exact key - this creates a leaf at the end of the path
    parallelTrie.put(exactKey, createValue(100));
    sequentialTrie.put(exactKey, createValue(100));
    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);

    // Verify we can retrieve it
    assertThat(parallelTrie.get(exactKey)).isEqualTo(Optional.of(createValue(100)));
    assertThat(parallelTrie.getRootHash()).isEqualTo(sequentialTrie.getRootHash());

    final Bytes key2 = createKey(0x12, 0x34, 0x56, 0x79); // Differs at last nibble
    final Bytes key3 = createKey(0x12, 0x34, 0x56, 0x7A); // Differs at last nibble

    parallelTrie.put(key2, createValue(200));
    parallelTrie.put(key3, createValue(300));
    sequentialTrie.put(key2, createValue(200));
    sequentialTrie.put(key3, createValue(300));
    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);
    // All keys should be retrievable
    assertThat(parallelTrie.get(exactKey)).isEqualTo(Optional.of(createValue(100)));
    assertThat(parallelTrie.get(key2)).isEqualTo(Optional.of(createValue(200)));
    assertThat(parallelTrie.get(key3)).isEqualTo(Optional.of(createValue(300)));

    assertThat(parallelTrie.getRootHash()).isEqualTo(sequentialTrie.getRootHash());
  }

  @Test
  void shouldHandleLeafTerminatorScenarioWithBranchValue() {
    // In a Patricia trie with fixed-length keys, a "leaf terminator" scenario
    // occurs when we have a branch node that also holds a value
    // This happens when one key is exactly at a branch point

    // Create keys that will form a branch structure
    // All share first 3 bytes, diverge at last byte
    final Bytes baseKey = createKey(0xAA, 0xBB, 0xCC, 0x00); // Base - will have no value initially
    final Bytes child1 = createKey(0xAA, 0xBB, 0xCC, 0x01);
    final Bytes child2 = createKey(0xAA, 0xBB, 0xCC, 0x02);
    final Bytes child3 = createKey(0xAA, 0xBB, 0xCC, 0x03);

    // Insert children first
    parallelTrie.put(child1, createValue(1));
    parallelTrie.put(child2, createValue(2));
    parallelTrie.put(child3, createValue(3));
    sequentialTrie.put(child1, createValue(1));
    sequentialTrie.put(child2, createValue(2));
    sequentialTrie.put(child3, createValue(3));
    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);

    // Now insert a value at the base location
    // This creates a branch with a value (leaf terminator scenario)
    parallelTrie.put(baseKey, createValue(99));
    sequentialTrie.put(baseKey, createValue(99));
    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);

    // All values should be retrievable
    assertThat(parallelTrie.get(baseKey)).isEqualTo(Optional.of(createValue(99)));
    assertThat(parallelTrie.get(child1)).isEqualTo(Optional.of(createValue(1)));
    assertThat(parallelTrie.get(child2)).isEqualTo(Optional.of(createValue(2)));
    assertThat(parallelTrie.get(child3)).isEqualTo(Optional.of(createValue(3)));

    assertThat(parallelTrie.getRootHash()).isEqualTo(sequentialTrie.getRootHash());

    // Remove the base value - branch should remain with only children
    parallelTrie.remove(baseKey);
    sequentialTrie.remove(baseKey);
    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);

    assertThat(parallelTrie.get(baseKey)).isEmpty();
    assertThat(parallelTrie.get(child1)).isEqualTo(Optional.of(createValue(1)));
    assertThat(parallelTrie.get(child2)).isEqualTo(Optional.of(createValue(2)));
    assertThat(parallelTrie.get(child3)).isEqualTo(Optional.of(createValue(3)));

    assertThat(parallelTrie.getRootHash()).isEqualTo(sequentialTrie.getRootHash());
  }

  @Test
  void shouldHandleMultipleBranchesWithValues() {
    // Create multiple branch nodes that each have a value
    // This tests parallel processing of leaf terminator scenarios

    for (int i = 0; i < 16; i++) {
      // Branch base (has value)
      final Bytes branchKey = createKey(i, 0x00, 0x00, 0x00);

      // Children of this branch
      final Bytes child1 = createKey(i, 0x00, 0x00, 0x01);
      final Bytes child2 = createKey(i, 0x00, 0x00, 0x02);

      parallelTrie.put(branchKey, createValue(i * 10));
      parallelTrie.put(child1, createValue(i * 10 + 1));
      parallelTrie.put(child2, createValue(i * 10 + 2));

      sequentialTrie.put(branchKey, createValue(i * 10));
      sequentialTrie.put(child1, createValue(i * 10 + 1));
      sequentialTrie.put(child2, createValue(i * 10 + 2));
    }

    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);

    // Verify all values
    for (int i = 0; i < 16; i++) {
      final Bytes branchKey = createKey(i, 0x00, 0x00, 0x00);
      final Bytes child1 = createKey(i, 0x00, 0x00, 0x01);
      final Bytes child2 = createKey(i, 0x00, 0x00, 0x02);

      assertThat(parallelTrie.get(branchKey)).isEqualTo(Optional.of(createValue(i * 10)));
      assertThat(parallelTrie.get(child1)).isEqualTo(Optional.of(createValue(i * 10 + 1)));
      assertThat(parallelTrie.get(child2)).isEqualTo(Optional.of(createValue(i * 10 + 2)));
    }

    assertThat(parallelTrie.getRootHash()).isEqualTo(sequentialTrie.getRootHash());
  }

  @Test
  void shouldHandleMixedOperationsOnKeys() {
    final int numKeys = 50;
    final Bytes[] keys = new Bytes[numKeys];

    // Insert 50 keys in both tries
    for (int i = 0; i < numKeys; i++) {
      keys[i] = createKey(i);
      parallelTrie.put(keys[i], createValue(i));
      sequentialTrie.put(keys[i], createValue(i));
    }
    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);

    // Remove first 20
    for (int i = 0; i < 20; i++) {
      parallelTrie.remove(keys[i]);
      sequentialTrie.remove(keys[i]);
    }

    // Update next 15
    for (int i = 20; i < 35; i++) {
      parallelTrie.put(keys[i], createValue(i * 10));
      sequentialTrie.put(keys[i], createValue(i * 10));
    }

    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);

    // Verify removed
    for (int i = 0; i < 20; i++) {
      assertThat(parallelTrie.get(keys[i])).isEmpty();
    }

    // Verify updated
    for (int i = 20; i < 35; i++) {
      assertThat(parallelTrie.get(keys[i])).isEqualTo(Optional.of(createValue(i * 10)));
    }

    // Verify unchanged
    for (int i = 35; i < numKeys; i++) {
      assertThat(parallelTrie.get(keys[i])).isEqualTo(Optional.of(createValue(i)));
    }

    assertThat(parallelTrie.getRootHash()).isEqualTo(sequentialTrie.getRootHash());
  }

  @Test
  void shouldHandleFullBranchWith16Children() {
    // Create 16 keys that diverge at the first nibble
    for (int i = 0; i < 16; i++) {
      final Bytes key = createKey((i << 4), 0x00, 0x00, 0x00);
      parallelTrie.put(key, createValue(i));
      sequentialTrie.put(key, createValue(i));
    }

    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);

    for (int i = 0; i < 16; i++) {
      final Bytes key = createKey((i << 4), 0x00, 0x00, 0x00);
      assertThat(parallelTrie.get(key)).isEqualTo(Optional.of(createValue(i)));
    }

    assertThat(parallelTrie.getRootHash()).isEqualTo(sequentialTrie.getRootHash());
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 10, 50, 100, 200})
  void shouldHandleVariousKeyBatchSizes(final int size) {
    for (int i = 0; i < size; i++) {
      final Bytes key = createKey(i);
      final Bytes value = createValue(i);
      parallelTrie.put(key, value);
      sequentialTrie.put(key, value);
    }

    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);

    for (int i = 0; i < size; i++) {
      final Bytes key = createKey(i);
      final Bytes value = createValue(i);
      assertThat(parallelTrie.get(key)).isEqualTo(Optional.of(value));
    }

    assertThat(parallelTrie.getRootHash()).isEqualTo(sequentialTrie.getRootHash());
  }

  @Test
  void shouldLoadStoredKeys() {
    final int numKeys = 10;
    final Bytes[] keys = new Bytes[numKeys];

    for (int i = 0; i < numKeys; i++) {
      keys[i] = createKey(i);
      parallelTrie.put(keys[i], createValue(i));
    }
    parallelTrie.commit(parallelStorage::put);

    final org.apache.tuweni.bytes.Bytes32 rootHash = parallelTrie.getRootHash();

    // Create new trie from stored root
    final ParallelStoredMerklePatriciaTrie<Bytes, Bytes> newTrie =
        new ParallelStoredMerklePatriciaTrie<>(
            parallelStorage::get, rootHash, Function.identity(), Function.identity());

    for (int i = 0; i < numKeys; i++) {
      assertThat(newTrie.get(keys[i])).isEqualTo(Optional.of(createValue(i)));
    }
  }

  @Test
  void shouldProduceConsistentRootHashAcrossMultipleBuilds() {
    final int numKeys = 50;
    final Bytes[] keys = new Bytes[numKeys];
    for (int i = 0; i < numKeys; i++) {
      keys[i] = createKey(i);
    }

    final org.apache.tuweni.bytes.Bytes32[] rootHashes = new org.apache.tuweni.bytes.Bytes32[3];

    for (int iteration = 0; iteration < 3; iteration++) {
      final MerkleStorage freshStorage = new KeyValueMerkleStorage(new InMemoryKeyValueStorage());
      final ParallelStoredMerklePatriciaTrie<Bytes, Bytes> freshTrie =
          new ParallelStoredMerklePatriciaTrie<>(
              freshStorage::get, Function.identity(), Function.identity());

      for (int i = 0; i < numKeys; i++) {
        freshTrie.put(keys[i], createValue(i));
      }

      freshTrie.commit(freshStorage::put);
      rootHashes[iteration] = freshTrie.getRootHash();
    }

    assertThat(rootHashes[0]).isEqualTo(rootHashes[1]);
    assertThat(rootHashes[1]).isEqualTo(rootHashes[2]);
  }

  @Test
  void shouldMatchSequentialTrieWithComplexScenarios() {
    // Create multiple complex scenarios
    for (int i = 0; i < 10; i++) {
      final Bytes branchKey = createKey(i, 0x00, 0x00, 0x00);
      final Bytes child1 = createKey(i, 0x00, 0x00, 0x01);
      final Bytes child2 = createKey(i, 0x00, 0x00, 0x02);
      final Bytes child3 = createKey(i, 0x00, 0x00, 0x03);

      parallelTrie.put(branchKey, createValue(i * 100));
      parallelTrie.put(child1, createValue(i * 100 + 1));
      parallelTrie.put(child2, createValue(i * 100 + 2));
      parallelTrie.put(child3, createValue(i * 100 + 3));

      sequentialTrie.put(branchKey, createValue(i * 100));
      sequentialTrie.put(child1, createValue(i * 100 + 1));
      sequentialTrie.put(child2, createValue(i * 100 + 2));
      sequentialTrie.put(child3, createValue(i * 100 + 3));
    }

    parallelTrie.commit(parallelStorage::put);
    sequentialTrie.commit(sequentialStorage::put);

    assertThat(parallelTrie.getRootHash()).isEqualTo(sequentialTrie.getRootHash());
  }

  // Helper methods to create 4-byte keys

  private Bytes createKey(final int seed) {
    final byte[] keyBytes = new byte[4];
    keyBytes[0] = (byte) ((seed >> 24) & 0xFF);
    keyBytes[1] = (byte) ((seed >> 16) & 0xFF);
    keyBytes[2] = (byte) ((seed >> 8) & 0xFF);
    keyBytes[3] = (byte) (seed & 0xFF);
    return Bytes.wrap(keyBytes);
  }

  private Bytes createKey(final int b0, final int b1, final int b2, final int b3) {
    final byte[] keyBytes = new byte[4];
    keyBytes[0] = (byte) (b0 & 0xFF);
    keyBytes[1] = (byte) (b1 & 0xFF);
    keyBytes[2] = (byte) (b2 & 0xFF);
    keyBytes[3] = (byte) (b3 & 0xFF);
    return Bytes.wrap(keyBytes);
  }

  private Bytes createValue(final int value) {
    // Create a value between 1 and 8 bytes
    final int size = (value % 8) + 1;
    final byte[] valueBytes = new byte[size];
    for (int i = 0; i < size; i++) {
      valueBytes[i] = (byte) ((value + i) & 0xFF);
    }
    return Bytes.wrap(valueBytes);
  }
}
