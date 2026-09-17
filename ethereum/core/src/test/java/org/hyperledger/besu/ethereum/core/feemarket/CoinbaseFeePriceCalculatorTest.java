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
package org.hyperledger.besu.ethereum.core.feemarket;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Wei;

import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CoinbaseFeePriceCalculatorTest {

  private static final CoinbaseFeePriceCalculator FRONTIER_CALCULATOR =
      CoinbaseFeePriceCalculator.frontier();
  private static final CoinbaseFeePriceCalculator EIP_1559_CALCULATOR =
      CoinbaseFeePriceCalculator.eip1559();

  public static Stream<Arguments> data() {
    return Stream.of(
        // legacy transaction must return gas price * gas
        Arguments.of(FRONTIER_CALCULATOR, 100L, Wei.of(10L), Optional.empty(), Wei.of(1000L)),
        // EIP-1559 must return gas * (gas price - base fee)
        Arguments.of(EIP_1559_CALCULATOR, 100L, Wei.of(10L), Optional.of(Wei.of(4L)), Wei.of(600L))
        // Negative transaction gas price case
        // {EIP_1559_CALCULATOR, Gas.of(100), Wei.of(95L), Optional.of(100L), Wei.of(-500L)}
        );
  }

  @ParameterizedTest
  @MethodSource("data")
  public void assertThatCalculatorWorks(
      final CoinbaseFeePriceCalculator coinbaseFeePriceCalculator,
      final long coinbaseFee,
      final Wei transactionGasPrice,
      final Optional<Wei> baseFee,
      final Wei expectedPrice) {
    assertThat(coinbaseFeePriceCalculator.price(coinbaseFee, transactionGasPrice, baseFee))
        .isEqualByComparingTo(expectedPrice);
  }
}
