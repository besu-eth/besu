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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.code;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.Code;

import java.util.Random;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class CodeStorageFormatTest {

  // PUSH1 0x5b; JUMPDEST; PUSH2 0x5b5b; JUMPDEST: only the two instructions are destinations
  private static final Bytes CODE = Bytes.fromHexString("0x60 5b 5b 61 5b5b 5b".replace(" ", ""));

  @Test
  void roundTripsCodeAndAnalysis() {
    final Code stored = roundTrip(CODE);

    assertThat(stored.getBytes()).isEqualTo(CODE);
    assertThat(stored.getJumpDestBitMask()).isEqualTo(Code.jumpDestBitMaskOf(CODE));
    assertThat(stored.getJumpDestBitMask()).containsExactly(0b1000100L);
  }

  @Test
  void roundTripsCodeSpanningManyEntries() {
    final byte[] raw = new byte[24576];
    new Random(42).nextBytes(raw);
    final Bytes code = Bytes.wrap(raw);

    final Code stored = roundTrip(code);

    assertThat(stored.getBytes()).isEqualTo(code);
    assertThat(stored.getJumpDestBitMask()).isEqualTo(Code.jumpDestBitMaskOf(code));
  }

  @Test
  void storedAnalysisIsUsedAsIs() {
    final Code code = roundTrip(CODE);

    assertThat(code.getJumpDestBitMask()).isNotNull();
    assertThat(code.isJumpDestInvalid(2)).isFalse();
    assertThat(code.isJumpDestInvalid(1)).isTrue();
    assertThat(code.isJumpDestInvalid(4)).isTrue();
    assertThat(code.isJumpDestInvalid(6)).isFalse();
  }

  @Test
  void rejectsValuesOfNoKnownVersion() {
    assertThatThrownBy(() -> CodeStorageFormat.of(CODE.toArrayUnsafe()))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> CodeStorageFormat.of(new byte[] {2}))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> CodeStorageFormat.of(new byte[0]))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void currentVersionIsFoundByItsFirstByte() {
    assertThat(CodeStorageFormat.of(CodeStorageFormat.CURRENT.encode(CODE)))
        .isEqualTo(CodeStorageFormat.CURRENT);
  }

  private static Code roundTrip(final Bytes code) {
    final byte[] value = CodeStorageFormat.CURRENT.encode(code);
    return CodeStorageFormat.of(value).decode(value, Hash.hash(code));
  }
}
