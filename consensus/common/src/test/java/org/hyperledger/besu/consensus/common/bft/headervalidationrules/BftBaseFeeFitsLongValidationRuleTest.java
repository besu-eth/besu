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
package org.hyperledger.besu.consensus.common.bft.headervalidationrules;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.math.BigInteger;

import org.junit.jupiter.api.Test;

class BftBaseFeeFitsLongValidationRuleTest {

  private final BftBaseFeeFitsLongValidationRule rule = new BftBaseFeeFitsLongValidationRule();

  private BlockHeader headerWithBaseFee(final Wei baseFee) {
    return new BlockHeaderTestFixture().baseFeePerGas(baseFee).buildHeader();
  }

  @Test
  void headerWithoutBaseFeeIsValid() {
    // pre-London headers carry no base fee
    final BlockHeader header = new BlockHeaderTestFixture().buildHeader();
    assertThat(rule.validate(header, null)).isTrue();
  }

  @Test
  void baseFeeThatFitsInALongIsValid() {
    assertThat(rule.validate(headerWithBaseFee(Wei.of(1_000_000_000L)), null)).isTrue();
    // Long.MAX_VALUE == 2^63 - 1 is the largest value that still fits
    assertThat(rule.validate(headerWithBaseFee(Wei.of(Long.MAX_VALUE)), null)).isTrue();
  }

  @Test
  void baseFeeThatOverflowsALongIsRejected() {
    // 2^63, one past Long.MAX_VALUE
    final Wei justOverLong = Wei.of(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));
    assertThat(rule.validate(headerWithBaseFee(justOverLong), null)).isFalse();
  }

  @Test
  void hugeBaseFeeIsRejected() {
    assertThat(rule.validate(headerWithBaseFee(Wei.of(BigInteger.TWO.pow(200))), null)).isFalse();
  }
}
