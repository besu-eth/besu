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
package org.hyperledger.besu.ethereum.trie;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;
import org.junit.jupiter.api.Test;

class TrieBytesTest {

  private static final List<Bytes> VALUES =
      List.of(
          Bytes.EMPTY,
          Bytes.of(7),
          Bytes.fromHexString("0x0102030405"),
          Bytes32.fromHexStringLenient("0xabcdef"),
          // a slice, whose backing array is larger than the value
          Bytes.fromHexString("0x00112233445566778899").slice(3, 4),
          // a view over two values
          Bytes.wrap(Bytes.of(1, 2), Bytes.of(3)));

  @Test
  void concatMatchesTuweni() {
    for (final Bytes first : VALUES) {
      for (final Bytes second : VALUES) {
        assertThat(TrieBytes.concat(first, second)).isEqualTo(Bytes.concatenate(first, second));
      }
    }
  }

  @Test
  void appendMatchesTuweni() {
    for (final Bytes location : VALUES) {
      for (int nibble = 0; nibble < 16; nibble++) {
        assertThat(TrieBytes.append(location, (byte) nibble))
            .isEqualTo(Bytes.concatenate(location, Bytes.of(nibble)));
      }
    }
  }

  @Test
  void resultDoesNotShareTheInputs() {
    final MutableBytes first = MutableBytes.wrap(new byte[] {1, 2});
    final Bytes concatenated = TrieBytes.concat(first, Bytes.of(3));
    final Bytes appended = TrieBytes.append(first, (byte) 4);
    first.set(0, (byte) 9);

    assertThat(concatenated).isEqualTo(Bytes.of(1, 2, 3));
    assertThat(appended).isEqualTo(Bytes.of(1, 2, 4));
  }
}
