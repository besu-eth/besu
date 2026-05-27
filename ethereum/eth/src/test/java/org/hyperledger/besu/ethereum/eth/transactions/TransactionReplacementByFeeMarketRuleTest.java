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
package org.hyperledger.besu.ethereum.eth.transactions;

import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.util.number.Percentage;

import java.util.Collection;
import java.util.Optional;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TransactionReplacementByFeeMarketRuleTest extends AbstractTransactionReplacementTest {

  public static Collection<Object[]> data() {
    return asList(
        new Object[][] {

          //   basefee absent
          {frontierTx(5L), frontierTx(6L), empty(), 0, 100, false},
          {frontierTx(5L), frontierTx(5L), empty(), 0, 100, false},
          {frontierTx(5L), frontierTx(4L), empty(), 0, 100, false},
          {frontierTx(100L), frontierTx(105L), empty(), 10, 100, false},
          {frontierTx(100L), frontierTx(110L), empty(), 10, 100, false},
          {frontierTx(100L), frontierTx(111L), empty(), 10, 100, false},
          //   basefee present
          {frontierTx(5L), frontierTx(6L), Optional.of(Wei.of(3L)), 0, 100, false},
          {frontierTx(5L), frontierTx(5L), Optional.of(Wei.of(3L)), 0, 100, false},
          {frontierTx(5L), frontierTx(4L), Optional.of(Wei.of(3L)), 0, 100, false},
          {frontierTx(100L), frontierTx(105L), Optional.of(Wei.of(3L)), 10, 100, false},
          {frontierTx(100L), frontierTx(110L), Optional.of(Wei.of(3L)), 10, 100, false},
          {frontierTx(100L), frontierTx(111L), Optional.of(Wei.of(3L)), 10, 100, false},
          // eip1559 replacing frontier
          {frontierTx(5L), eip1559Tx(3L, 6L), Optional.of(Wei.of(1L)), 0, 100, false},
          {frontierTx(5L), eip1559Tx(3L, 5L), Optional.of(Wei.of(3L)), 0, 100, true},
          {frontierTx(5L), eip1559Tx(3L, 6L), Optional.of(Wei.of(3L)), 0, 100, true},
          //  frontier replacing 1559
          {eip1559Tx(3L, 8L), frontierTx(7L), Optional.of(Wei.of(4L)), 0, 100, true},
          {eip1559Tx(3L, 8L), frontierTx(7L), Optional.of(Wei.of(5L)), 0, 100, false},
          {eip1559Tx(3L, 8L), frontierTx(8L), Optional.of(Wei.of(4L)), 0, 100, true},
          //  eip1559 replacing eip1559
          {eip1559Tx(3L, 6L), eip1559Tx(3L, 6L), Optional.of(Wei.of(3L)), 0, 100, true},
          {eip1559Tx(3L, 6L), eip1559Tx(3L, 7L), Optional.of(Wei.of(3L)), 0, 100, true},
          {eip1559Tx(3L, 6L), eip1559Tx(3L, 7L), Optional.of(Wei.of(4L)), 0, 100, true},
          {eip1559Tx(3L, 6L), eip1559Tx(3L, 7L), Optional.of(Wei.of(5L)), 0, 100, true},
          {eip1559Tx(3L, 6L), eip1559Tx(3L, 7L), Optional.of(Wei.of(6L)), 0, 100, true},
          {eip1559Tx(3L, 6L), eip1559Tx(3L, 7L), Optional.of(Wei.of(7L)), 0, 100, true},
          {eip1559Tx(3L, 6L), eip1559Tx(3L, 7L), Optional.of(Wei.of(8L)), 0, 100, true},
          {eip1559Tx(10L, 200L), eip1559Tx(10L, 200L), Optional.of(Wei.of(90L)), 10, 100, false},
          {eip1559Tx(10L, 200L), eip1559Tx(15L, 200L), Optional.of(Wei.of(90L)), 10, 100, false},
          {eip1559Tx(10L, 200L), eip1559Tx(20L, 200L), Optional.of(Wei.of(90L)), 10, 100, true},
          {eip1559Tx(10L, 200L), eip1559Tx(20L, 200L), Optional.of(Wei.of(200L)), 10, 100, true},
          {eip1559Tx(10L, 200L), eip1559Tx(10L, 220L), Optional.of(Wei.of(220L)), 10, 100, true},
          //  pathological, priority fee > max fee
          {eip1559Tx(8L, 6L), eip1559Tx(3L, 6L), Optional.of(Wei.of(2L)), 0, 100, false},
          {eip1559Tx(8L, 6L), eip1559Tx(3L, 7L), Optional.of(Wei.of(3L)), 0, 100, true},
          {eip1559Tx(8L, 6L), eip1559Tx(3L, 7L), Optional.of(Wei.of(4L)), 0, 100, true},
          //  pathological, eip1559 without basefee
          {eip1559Tx(8L, 6L), eip1559Tx(3L, 7L), Optional.empty(), 0, 100, false},
          {eip1559Tx(8L, 6L), eip1559Tx(3L, 7L), Optional.empty(), 0, 100, false},
          // zero base fee market
          {frontierTx(0L), frontierTx(0L), Optional.of(Wei.ZERO), 0, 100, false},
          {eip1559Tx(0L, 0L), frontierTx(0L), Optional.of(Wei.ZERO), 0, 100, true},
          {frontierTx(0L), eip1559Tx(0L, 0L), Optional.of(Wei.ZERO), 0, 100, true},
          {eip1559Tx(0L, 0L), eip1559Tx(0L, 0L), Optional.of(Wei.ZERO), 0, 100, true},
          // blob tx
          {blobTx(10L, 200L, 1L), blobTx(20L, 220L, 1L), Optional.of(Wei.of(90L)), 10, 0, true},
          {blobTx(10L, 200L, 1L), blobTx(20L, 220L, 2L), Optional.of(Wei.of(90L)), 10, 100, true},
          {blobTx(10L, 200L, 1L), blobTx(20L, 220L, 1L), Optional.of(Wei.of(90L)), 10, 100, false},
          {blobTx(10L, 200L, 2L), blobTx(20L, 220L, 3L), Optional.of(Wei.of(90L)), 10, 100, false},
          //            // blob tx must be replaced by blob tx only
          {blobTx(3L, 6L, 1L), eip1559Tx(3L, 6L), Optional.of(Wei.of(3L)), 0, 0, false},
          {eip1559Tx(3L, 6L), blobTx(3L, 6L, 1L), Optional.of(Wei.of(3L)), 0, 0, false},
        });
  }

  @ParameterizedTest
  @MethodSource("data")
  public void shouldReplace(
      final PendingTransaction oldTx,
      final PendingTransaction newTx,
      final Optional<Wei> baseFee,
      final int priceBump,
      final int blobPriceBump,
      final boolean expected) {

    assertThat(
            new TransactionReplacementByFeeMarketRule(
                    Percentage.fromInt(priceBump), Percentage.fromInt(blobPriceBump))
                .shouldReplace(oldTx, newTx, baseFee))
        .isEqualTo(expected);
  }
}
