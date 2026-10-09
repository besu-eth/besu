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
import org.junit.jupiter.api.Test;

class BytesConcatenationTest {

  private static final List<Bytes> VALUES =
      List.of(
          Bytes.EMPTY,
          Bytes.of(7),
          Bytes.fromHexString("0x0102030405"),
          Bytes32.fromHexString(
              "0xa1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"),
          // a slice reads from an offset into its backing array
          Bytes.fromHexString("0xffeeddccbbaa99887766").slice(3, 4));

  @Test
  void concatenatesLikeTuweni() {
    for (final Bytes first : VALUES) {
      for (final Bytes second : VALUES) {
        assertThat(BytesConcatenation.concatenate(first, second))
            .isEqualTo(Bytes.concatenate(first, second));
      }
    }
  }

  @Test
  void appendsOneByteLikeTuweni() {
    for (final Bytes prefix : VALUES) {
      for (int nibble = 0; nibble < 16; nibble++) {
        assertThat(BytesConcatenation.append(prefix, (byte) nibble))
            .isEqualTo(Bytes.concatenate(prefix, Bytes.of((byte) nibble)));
      }
    }
  }

  @Test
  void leavesItsInputsUntouched() {
    final Bytes first = Bytes.fromHexString("0x0102");
    final Bytes second = Bytes.fromHexString("0x0304");
    final Bytes joined = BytesConcatenation.concatenate(first, second);
    assertThat(joined).isEqualTo(Bytes.fromHexString("0x01020304"));
    assertThat(first).isEqualTo(Bytes.fromHexString("0x0102"));
    assertThat(second).isEqualTo(Bytes.fromHexString("0x0304"));
  }
}
