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
package org.hyperledger.besu.evm.frame;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;

import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class KeyedNoncesTest {

  private static final UInt256 ONE = UInt256.ONE;
  private static final UInt256 TWO = UInt256.valueOf(2);

  @Test
  void acceptsTheLegacySetAndStrictlyIncreasingKeys() {
    assertThat(KeyedNonces.validate(KeyedNonces.LEGACY_NONCE_KEYS)).isEmpty();
    assertThat(KeyedNonces.validate(List.of(ONE, TWO))).isEmpty();
    assertThat(KeyedNonces.isLegacy(KeyedNonces.LEGACY_NONCE_KEYS)).isTrue();
    assertThat(KeyedNonces.isLegacy(List.of(ONE))).isFalse();
  }

  @Test
  void rejectsMalformedKeySets() {
    assertThat(KeyedNonces.validate(Collections.emptyList())).contains("invalid nonce key count");
    final List<UInt256> seventeen =
        IntStream.rangeClosed(1, 17).mapToObj(UInt256::valueOf).toList();
    assertThat(KeyedNonces.validate(seventeen)).contains("invalid nonce key count");
    assertThat(KeyedNonces.validate(List.of(TWO, ONE)))
        .contains("nonce keys not strictly increasing");
    assertThat(KeyedNonces.validate(List.of(ONE, ONE)))
        .contains("nonce keys not strictly increasing");
    assertThat(KeyedNonces.validate(List.of(UInt256.ZERO, ONE)))
        .contains("zero nonce key alongside other keys");
  }

  @Test
  void slotsDependOnSenderAndKey() {
    final Address a = Address.fromHexString("0x01");
    final Address b = Address.fromHexString("0x02");
    assertThat(KeyedNonces.nonceSlot(a, ONE)).isEqualTo(KeyedNonces.nonceSlot(a, ONE));
    assertThat(KeyedNonces.nonceSlot(a, ONE)).isNotEqualTo(KeyedNonces.nonceSlot(a, TWO));
    assertThat(KeyedNonces.nonceSlot(a, ONE)).isNotEqualTo(KeyedNonces.nonceSlot(b, ONE));
  }

  @Test
  void sequencesAreUnsigned() {
    assertThat(KeyedNonces.unsigned(-1L)).isEqualTo(UInt256.fromHexString("0xffffffffffffffff"));
  }
}
