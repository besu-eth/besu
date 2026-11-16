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
package org.hyperledger.besu.consensus.qbft.headervalidationrules;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class GoQuorumGasLimitRangeValidationRuleTest {

  public static Stream<Arguments> data() {
    return Stream.of(
        // In range, no change = valid
        Arguments.of(
            4096,
            4096,
            new GoQuorumGasLimitRangeValidationRule(4000, 4200),
            true,
            Optional.empty()),
        // In range, delta >= 1/1024 (would fail the delta rule) = valid here, delta is not enforced
        Arguments.of(
            4100,
            4096,
            new GoQuorumGasLimitRangeValidationRule(4000, 4200),
            true,
            Optional.empty()),
        // In range, arbitrarily large delta = valid, delta is not enforced
        Arguments.of(
            9000,
            4096,
            new GoQuorumGasLimitRangeValidationRule(4000, 9001),
            true,
            Optional.empty()),
        // Below minimum = invalid, range is still enforced
        Arguments.of(
            3999,
            4096,
            new GoQuorumGasLimitRangeValidationRule(4000, 4200),
            false,
            Optional.empty()),
        // Above maximum = invalid, range is still enforced
        Arguments.of(
            4201,
            4096,
            new GoQuorumGasLimitRangeValidationRule(4000, 4200),
            false,
            Optional.empty()),
        // In range, basefee present but no base fee market = invalid
        Arguments.of(
            4100,
            4096,
            new GoQuorumGasLimitRangeValidationRule(4000, 4200),
            false,
            Optional.of(Wei.of(10L))));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void test(
      final long headerGasLimit,
      final long parentGasLimit,
      final GoQuorumGasLimitRangeValidationRule uut,
      final boolean expectedResult,
      final Optional<Wei> optionalBaseFee) {
    final BlockHeaderTestFixture blockHeaderBuilder = new BlockHeaderTestFixture();

    blockHeaderBuilder.gasLimit(headerGasLimit);
    optionalBaseFee.ifPresent(baseFee -> blockHeaderBuilder.baseFeePerGas(baseFee));
    final BlockHeader header = blockHeaderBuilder.buildHeader();

    blockHeaderBuilder.gasLimit(parentGasLimit);
    final BlockHeader parent = blockHeaderBuilder.buildHeader();

    assertThat(uut.validate(header, parent)).isEqualTo(expectedResult);
  }
}
