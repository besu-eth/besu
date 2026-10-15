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
package org.hyperledger.besu.evm.gascalculator;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OsakaGasCalculatorTest {

  @Test
  void testPrecompileSize() {
    OsakaGasCalculator subject = new OsakaGasCalculator();
    // past last l1 precompile address
    assertThat(subject.isPrecompile(Address.precompiled(0x12))).isFalse();
    // past last l2 precompile address
    assertThat(subject.isPrecompile(Address.precompiled(0x0101))).isFalse();
    assertThat(subject.isPrecompile(Address.P256_VERIFY)).isTrue();
    assertThat(subject.isPrecompile(Address.BLS12_MAP_FP2_TO_G2)).isTrue();
    assertThat(subject.isPrecompile(Address.precompiled(0x00))).isFalse();
  }

  @Test
  void testZeroLengthBaseModCost() {
    OsakaGasCalculator subject = new OsakaGasCalculator();
    Bytes input =
        Bytes.fromHexString(
            "0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000000660bfd6246b7f481d4a9955bb2bd9ee970e21063193ae484c413aea066fc949d");
    assertThat(subject.modExpGasCost(input)).isEqualTo(4064L);
  }

  @ParameterizedTest
  @ValueSource(
      longs = {
        17_179_869_177L, // 2^34 - 7: first length where 2 * words^2 no longer fits in a long
        1L << 34,
        1L << 35, // words^2 itself saturates here
        1L << 40
      })
  void modExpGasCostSaturatesInsteadOfWrappingForHugeLengths(final long modulusLength) {
    OsakaGasCalculator subject = new OsakaGasCalculator();

    assertThat(subject.modExpGasCost(modExpHeader(0, 0, modulusLength))).isEqualTo(Long.MAX_VALUE);
    assertThat(subject.modExpGasCost(modExpHeader(modulusLength, 0, 0))).isEqualTo(Long.MAX_VALUE);
    assertThat(subject.modExpGasCost(modExpHeader(modulusLength, 1, modulusLength)))
        .isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void modExpGasCostJustBelowOverflowBoundaryIsExact() {
    OsakaGasCalculator subject = new OsakaGasCalculator();
    // 2^34 - 8 bytes -> words = 2^31 - 1, 2 * words^2 = 9_223_372_028_264_841_218 still fits.
    long modulusLength = 17_179_869_176L;
    long words = (modulusLength + 7) / 8;

    assertThat(subject.modExpGasCost(modExpHeader(0, 0, modulusLength)))
        .isEqualTo(2 * words * words)
        .isEqualTo(9_223_372_028_264_841_218L);
  }

  private static Bytes modExpHeader(
      final long baseLength, final long exponentLength, final long modulusLength) {
    return Bytes.concatenate(
        Bytes32.leftPad(Bytes.ofUnsignedLong(baseLength)),
        Bytes32.leftPad(Bytes.ofUnsignedLong(exponentLength)),
        Bytes32.leftPad(Bytes.ofUnsignedLong(modulusLength)));
  }
}
