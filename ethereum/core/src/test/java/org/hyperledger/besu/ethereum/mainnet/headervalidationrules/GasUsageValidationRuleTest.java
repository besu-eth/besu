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
package org.hyperledger.besu.ethereum.mainnet.headervalidationrules;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class GasUsageValidationRuleTest {

  public static Stream<Arguments> data() {
    return Stream.of(
        Arguments.of(5, 6, true), // gasUsed is less than gasLimit is valid
        Arguments.of(5, 5, true), // gasUsed is the same as gaslimit is valid
        Arguments.of(5, 4, false) // gasUsed is less than gasLimit
        );
  }

  @ParameterizedTest
  @MethodSource("data")
  public void test(final long gasUsed, final long gasLimit, final boolean expectedResult) {
    final GasUsageValidationRule uut = new GasUsageValidationRule();
    final BlockHeaderTestFixture blockBuilder = new BlockHeaderTestFixture();

    blockBuilder.gasLimit(gasLimit);
    blockBuilder.gasUsed(gasUsed);

    final BlockHeader header = blockBuilder.buildHeader();

    assertThat(uut.validate(header, null)).isEqualTo(expectedResult);
  }
}
