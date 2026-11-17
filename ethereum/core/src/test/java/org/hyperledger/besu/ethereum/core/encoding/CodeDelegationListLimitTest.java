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
package org.hyperledger.besu.ethereum.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.mainnet.AmsterdamTargetingGasLimitCalculator.EIP_8037_TRANSACTION_TOTAL_GAS_LIMIT_CAP;
import static org.hyperledger.besu.ethereum.mainnet.OsakaTargetingGasLimitCalculator.EIP_7825_TRANSACTION_GAS_LIMIT_CAP;

import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.gascalculator.OsakaGasCalculator;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;

import org.junit.jupiter.api.Test;

class CodeDelegationListLimitTest {
  private static final long BLOCK_GAS_LIMIT = 60_000_000L;
  private static final long MAX_UNSIGNED = -1L;

  // Osaka: EIP-7825 caps tx.gas at 2^24.
  private static final GasLimitCalculator OSAKA_CAPS =
      caps(EIP_7825_TRANSACTION_GAS_LIMIT_CAP, Long.MAX_VALUE);
  // Amsterdam: EIP-8037 lifts the tx.gas cap to 2^32-1 and caps intrinsic gas at 2^24 instead.
  private static final GasLimitCalculator AMSTERDAM_CAPS =
      caps(EIP_8037_TRANSACTION_TOTAL_GAS_LIMIT_CAP, EIP_7825_TRANSACTION_GAS_LIMIT_CAP);

  @Test
  void forkWithoutEip7702AcceptsNoAuthorization() {
    final CodeDelegationListLimit limit =
        CodeDelegationListLimit.forFork(
            new CancunGasCalculator(), GasLimitCalculator.constant(), BLOCK_GAS_LIMIT);

    assertThat(limit.maxCodeDelegations(BLOCK_GAS_LIMIT)).isZero();
  }

  @Test
  void pragueChargesTwentyFiveThousandGasPerAuthorization() {
    final CodeDelegationListLimit limit =
        CodeDelegationListLimit.forFork(
            new PragueGasCalculator(), GasLimitCalculator.constant(), BLOCK_GAS_LIMIT);

    assertThat(limit.maxCodeDelegations(0)).isZero();
    assertThat(limit.maxCodeDelegations(24_999)).isZero();
    assertThat(limit.maxCodeDelegations(25_000)).isEqualTo(1);
    assertThat(limit.maxCodeDelegations(1_000_000)).isEqualTo(40);
  }

  @Test
  void blockGasLimitBoundsForksWithoutTransactionGasCap() {
    final CodeDelegationListLimit limit =
        CodeDelegationListLimit.forFork(
            new PragueGasCalculator(), GasLimitCalculator.constant(), 30_000_000L);

    // A gas limit above 2^63 is read as unsigned and still bounded by the block.
    assertThat(limit.maxCodeDelegations(Long.MIN_VALUE)).isEqualTo(1_200);
    assertThat(limit.maxCodeDelegations(MAX_UNSIGNED)).isEqualTo(1_200);
  }

  @Test
  void osakaTransactionGasCapBoundsTheList() {
    final CodeDelegationListLimit limit =
        CodeDelegationListLimit.forFork(new OsakaGasCalculator(), OSAKA_CAPS, BLOCK_GAS_LIMIT);

    assertThat(limit.maxCodeDelegations(BLOCK_GAS_LIMIT)).isEqualTo(671);
    assertThat(limit.maxCodeDelegations(MAX_UNSIGNED)).isEqualTo(671);
  }

  @Test
  void amsterdamRepricingMovesTheLimit() {
    final CodeDelegationListLimit limit =
        CodeDelegationListLimit.forFork(
            new AmsterdamGasCalculator(), AMSTERDAM_CAPS, BLOCK_GAS_LIMIT);

    // 7,816 gas per authorization, under the 2^24 intrinsic gas cap rather than tx.gas.
    assertThat(limit.maxCodeDelegations(100_000)).isEqualTo(12);
    assertThat(limit.maxCodeDelegations(EIP_8037_TRANSACTION_TOTAL_GAS_LIMIT_CAP)).isEqualTo(2_146);
  }

  @Test
  void orAllowsTheLongerListOfTwoForks() {
    final CodeDelegationListLimit osaka =
        CodeDelegationListLimit.forFork(new OsakaGasCalculator(), OSAKA_CAPS, BLOCK_GAS_LIMIT);
    final CodeDelegationListLimit amsterdam =
        CodeDelegationListLimit.forFork(
            new AmsterdamGasCalculator(), AMSTERDAM_CAPS, BLOCK_GAS_LIMIT);
    final CodeDelegationListLimit cancun =
        CodeDelegationListLimit.forFork(
            new CancunGasCalculator(), GasLimitCalculator.constant(), BLOCK_GAS_LIMIT);

    assertThat(osaka.or(amsterdam).maxCodeDelegations(BLOCK_GAS_LIMIT)).isEqualTo(2_146);
    assertThat(cancun.or(osaka).maxCodeDelegations(BLOCK_GAS_LIMIT)).isEqualTo(671);
  }

  private static GasLimitCalculator caps(final long transactionCap, final long intrinsicCap) {
    return new GasLimitCalculator() {
      @Override
      public long nextGasLimit(
          final long currentGasLimit, final long targetGasLimit, final long newBlockNumber) {
        return currentGasLimit;
      }

      @Override
      public long transactionGasLimitCap() {
        return transactionCap;
      }

      @Override
      public long transactionIntrinsicGasLimitCap() {
        return intrinsicCap;
      }
    };
  }
}
