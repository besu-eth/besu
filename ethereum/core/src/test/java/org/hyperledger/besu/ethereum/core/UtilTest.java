/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class UtilTest {
  @Test
  public void testFastDivCeil() {
    assertThat(Util.fastDivCeiling(0, 3)).isEqualTo(1);
    assertThat(Util.fastDivCeiling(1, 3)).isEqualTo(1);
    assertThat(Util.fastDivCeiling(2, 3)).isEqualTo(1);
    assertThat(Util.fastDivCeiling(3, 3)).isEqualTo(1);

    assertThat(Util.fastDivCeiling(4, 3)).isEqualTo(2);
    assertThat(Util.fastDivCeiling(5, 3)).isEqualTo(2);
    assertThat(Util.fastDivCeiling(6, 3)).isEqualTo(2);

    assertThat(Util.fastDivCeiling(7, 3)).isEqualTo(3);
  }

  @Test
  public void rootFromListOfBytesIsTheRootOfTheIndexedTrie() {
    // around the length from which the list is hashed in parallel, and a large block's length
    for (final int size : new int[] {1, 511, 512, 513, 25_000}) {
      final Random random = new Random(size);
      final List<Bytes> entries = new ArrayList<>(size);
      final SimpleMerklePatriciaTrie<Bytes, Bytes> trie = new SimpleMerklePatriciaTrie<>(b -> b);
      for (int i = 0; i < size; i++) {
        final Bytes entry = Bytes.random(1 + random.nextInt(200), random);
        entries.add(entry);
        trie.put(Util.indexKey(i), entry);
      }

      assertThat(Util.getRootFromListOfBytes(entries).getBytes())
          .as("size %d", size)
          .isEqualTo(trie.getRootHash());
    }
  }
}
