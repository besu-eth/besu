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
import static org.hyperledger.besu.datatypes.TransactionType.ACCESS_LIST;
import static org.hyperledger.besu.datatypes.TransactionType.EIP1559;
import static org.hyperledger.besu.datatypes.TransactionType.FRAME;
import static org.hyperledger.besu.datatypes.TransactionType.FRONTIER;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

public class TransactionPriceCalculatorTest {

  private static final TransactionPriceCalculator FRONTIER_CALCULATOR =
      TransactionPriceCalculator.frontier();
  private static final TransactionPriceCalculator EIP_1559_CALCULATOR =
      TransactionPriceCalculator.eip1559();

  public static Stream<Arguments> data() {
    return Arrays.stream(
            new Object[][] {
              // legacy transaction must return gas price
              {
                FRONTIER_CALCULATOR,
                FRONTIER,
                Wei.of(578L),
                null,
                null,
                Optional.empty(),
                Wei.of(578L)
              },
              // legacy transaction zero price
              {FRONTIER_CALCULATOR, FRONTIER, Wei.ZERO, null, null, Optional.empty(), Wei.ZERO},
              // ACCESSLIST transaction must return gas price
              {
                FRONTIER_CALCULATOR,
                ACCESS_LIST,
                Wei.of(578L),
                null,
                null,
                Optional.empty(),
                Wei.of(578L)
              },
              // legacy transaction must return gas price
              {
                EIP_1559_CALCULATOR,
                FRONTIER,
                Wei.of(578L),
                null,
                null,
                Optional.of(Wei.of(150L)),
                Wei.of(578L)
              },
              // london legacy transaction zero price
              {
                EIP_1559_CALCULATOR, FRONTIER, Wei.ZERO, null, null, Optional.of(Wei.ZERO), Wei.ZERO
              },
              // ACCESSLIST transaction must return gas price
              {
                EIP_1559_CALCULATOR,
                ACCESS_LIST,
                Wei.of(578L),
                null,
                null,
                Optional.of(Wei.of(150L)),
                Wei.of(578L)
              },
              // EIP-1559 must return maxPriorityFeePerGas + base fee
              {
                EIP_1559_CALCULATOR,
                EIP1559,
                null,
                Wei.of(100L),
                Wei.of(300L),
                Optional.of(Wei.of(150L)),
                Wei.of(250L)
              },
              // EIP-1559 must return fee cap
              {
                EIP_1559_CALCULATOR,
                EIP1559,
                null,
                Wei.of(100L),
                Wei.of(300L),
                Optional.of(Wei.of(250L)),
                Wei.of(300L)
              },
              // EIP-1559 transaction zero price
              {
                EIP_1559_CALCULATOR,
                EIP1559,
                null,
                Wei.ZERO,
                Wei.ZERO,
                Optional.of(Wei.ZERO),
                Wei.ZERO
              }
            })
        .map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void assertThatCalculatorWorks(
      final TransactionPriceCalculator transactionPriceCalculator,
      final TransactionType transactionType,
      final Wei gasPrice,
      final Wei maxPriorityFeePerGas,
      final Wei maxFeePerGas,
      final Optional<Wei> baseFee,
      final Wei expectedPrice) {
    assertThat(
            transactionPriceCalculator.price(
                Transaction.builder()
                    .type(transactionType)
                    .accessList(transactionType == ACCESS_LIST ? Collections.emptyList() : null)
                    .gasPrice(gasPrice)
                    .maxPriorityFeePerGas(maxPriorityFeePerGas)
                    .maxFeePerGas(maxFeePerGas)
                    .chainId(BigInteger.ONE)
                    .build(),
                baseFee))
        .isEqualByComparingTo(expectedPrice);
  }

  @ParameterizedTest(name = "maxFee={0}, maxPriorityFeePerGas={1}, baseFee={2}")
  @CsvSource({
    // the tip is capped by max_priority_fee_per_gas: 150 + 10
    "100000, 10, 150, 160",
    // the tip is capped by the budget left after the base fee: 150 + floor(1050 / 100)
    "16050, 100, 150, 160",
    // a budget below the base fee leaves no tip; the transaction processor rejects the budget
    "10000, 10, 150, 150",
    "1000, 5, 0, 5"
  })
  public void frameTransactionPaysBaseFeePlusTipItsBudgetCovers(
      final long maxFee,
      final long maxPriorityFeePerGas,
      final long baseFee,
      final long expectedPrice) {
    final Transaction frameTransaction =
        Transaction.builder()
            .type(FRAME)
            .chainId(BigInteger.ONE)
            .sender(Address.fromHexString("0x1000"))
            .gasLimit(100)
            .maxFee(Wei.of(maxFee))
            .maxPriorityFeePerGas(Wei.of(maxPriorityFeePerGas))
            .frames(
                List.of(
                    new Frame(
                        Frame.MODE_VERIFY,
                        Frame.APPROVE_SCOPE_MASK,
                        Optional.empty(),
                        100L,
                        0L,
                        Wei.ZERO,
                        Bytes.EMPTY)))
            .build();

    assertThat(EIP_1559_CALCULATOR.price(frameTransaction, Optional.of(Wei.of(baseFee))))
        .isEqualByComparingTo(Wei.of(expectedPrice));
  }
}
