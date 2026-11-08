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
package org.hyperledger.besu.ethereum.trie.patricia;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.trie.MerkleTrie;

import java.nio.charset.Charset;
import java.util.Random;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class SimpleMerklePatriciaTrieTest extends AbstractMerklePatriciaTrieTest {
  @Override
  protected MerkleTrie<Bytes, String> createTrie() {
    return new SimpleMerklePatriciaTrie<>(
        value -> (value != null) ? Bytes.wrap(value.getBytes(Charset.forName("UTF-8"))) : null);
  }

  @Test
  void rootHashInParallelIsTheRootHash() {
    for (final int size : new int[] {0, 1, 2, 17, 300, 5_000}) {
      final Bytes32 expected = randomTrie(size).getRootHash();
      for (final int depth : new int[] {0, 1, 4, 64}) {
        assertThat(randomTrie(size).getRootHashInParallel(depth))
            .as("size %d, depth %d", size, depth)
            .isEqualTo(expected);
      }
    }
  }

  private static SimpleMerklePatriciaTrie<Bytes, Bytes> randomTrie(final int size) {
    final Random random = new Random(size);
    final SimpleMerklePatriciaTrie<Bytes, Bytes> trie = new SimpleMerklePatriciaTrie<>(b -> b);
    for (int i = 0; i < size; i++) {
      trie.put(
          Bytes.random(1 + random.nextInt(32), random),
          Bytes.random(1 + random.nextInt(100), random));
    }
    return trie;
  }
}
