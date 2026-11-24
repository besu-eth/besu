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
package org.hyperledger.besu.consensus.common.bft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.mainnet.AmsterdamTargetingGasLimitCalculator;
import org.hyperledger.besu.ethereum.mainnet.OsakaTargetingGasLimitCalculator;

import org.junit.jupiter.api.Test;

public class TransactionGasLimitOverrideGasLimitCalculatorTest {

  private final GasLimitCalculator delegate = mock(GasLimitCalculator.class);

  @Test
  public void positiveOverrideSetsTxGasCapEvenAboveOsakaDelegate() {
    when(delegate.transactionGasLimitCap())
        .thenReturn(OsakaTargetingGasLimitCalculator.EIP_7825_TRANSACTION_GAS_LIMIT_CAP);
    final long override = 40_000_000L;
    final TransactionGasLimitOverrideGasLimitCalculator calculator =
        new TransactionGasLimitOverrideGasLimitCalculator(delegate, override);

    assertThat(calculator.transactionGasLimitCap()).isEqualTo(override);
  }

  @Test
  public void positiveOverrideSetsIntrinsicCapToSameValue() {
    when(delegate.transactionIntrinsicGasLimitCap())
        .thenReturn(OsakaTargetingGasLimitCalculator.EIP_7825_TRANSACTION_GAS_LIMIT_CAP);
    final long override = 40_000_000L;
    final TransactionGasLimitOverrideGasLimitCalculator calculator =
        new TransactionGasLimitOverrideGasLimitCalculator(delegate, override);

    assertThat(calculator.transactionIntrinsicGasLimitCap()).isEqualTo(override);
  }

  @Test
  public void positiveOverrideCanLowerBelowAmsterdamDelegateCap() {
    when(delegate.transactionGasLimitCap())
        .thenReturn(AmsterdamTargetingGasLimitCalculator.EIP_8037_TRANSACTION_TOTAL_GAS_LIMIT_CAP);
    final TransactionGasLimitOverrideGasLimitCalculator calculator =
        new TransactionGasLimitOverrideGasLimitCalculator(delegate, 1_000_000L);

    assertThat(calculator.transactionGasLimitCap()).isEqualTo(1_000_000L);
  }

  @Test
  public void zeroFallsBackToDelegateTxGasCap() {
    when(delegate.transactionGasLimitCap())
        .thenReturn(AmsterdamTargetingGasLimitCalculator.EIP_8037_TRANSACTION_TOTAL_GAS_LIMIT_CAP);
    final TransactionGasLimitOverrideGasLimitCalculator calculator =
        new TransactionGasLimitOverrideGasLimitCalculator(delegate, 0L);

    assertThat(calculator.transactionGasLimitCap())
        .isEqualTo(AmsterdamTargetingGasLimitCalculator.EIP_8037_TRANSACTION_TOTAL_GAS_LIMIT_CAP);
  }

  @Test
  public void zeroFallsBackToDelegateIntrinsicCap() {
    when(delegate.transactionIntrinsicGasLimitCap())
        .thenReturn(OsakaTargetingGasLimitCalculator.EIP_7825_TRANSACTION_GAS_LIMIT_CAP);
    final TransactionGasLimitOverrideGasLimitCalculator calculator =
        new TransactionGasLimitOverrideGasLimitCalculator(delegate, 0L);

    assertThat(calculator.transactionIntrinsicGasLimitCap())
        .isEqualTo(OsakaTargetingGasLimitCalculator.EIP_7825_TRANSACTION_GAS_LIMIT_CAP);
  }

  @Test
  public void nextGasLimitDelegates() {
    final TransactionGasLimitOverrideGasLimitCalculator calculator =
        new TransactionGasLimitOverrideGasLimitCalculator(delegate, 0L);
    when(delegate.nextGasLimit(1L, 2L, 3L)).thenReturn(4L);

    assertThat(calculator.nextGasLimit(1L, 2L, 3L)).isEqualTo(4L);
    verify(delegate).nextGasLimit(1L, 2L, 3L);
  }

  @Test
  public void currentBlobGasLimitDelegates() {
    final TransactionGasLimitOverrideGasLimitCalculator calculator =
        new TransactionGasLimitOverrideGasLimitCalculator(delegate, 0L);
    when(delegate.currentBlobGasLimit()).thenReturn(42L);

    assertThat(calculator.currentBlobGasLimit()).isEqualTo(42L);
    verify(delegate).currentBlobGasLimit();
  }

  @Test
  public void getTargetBlobGasPerBlockDelegates() {
    final TransactionGasLimitOverrideGasLimitCalculator calculator =
        new TransactionGasLimitOverrideGasLimitCalculator(delegate, 0L);
    when(delegate.getTargetBlobGasPerBlock()).thenReturn(10L);

    assertThat(calculator.getTargetBlobGasPerBlock()).isEqualTo(10L);
    verify(delegate).getTargetBlobGasPerBlock();
  }

  @Test
  public void computeExcessBlobGasDelegates() {
    final TransactionGasLimitOverrideGasLimitCalculator calculator =
        new TransactionGasLimitOverrideGasLimitCalculator(delegate, 0L);
    when(delegate.computeExcessBlobGas(1L, 2L, 3L)).thenReturn(4L);

    assertThat(calculator.computeExcessBlobGas(1L, 2L, 3L)).isEqualTo(4L);
    verify(delegate).computeExcessBlobGas(1L, 2L, 3L);
  }

  @Test
  public void transactionBlobGasLimitCapDelegates() {
    final TransactionGasLimitOverrideGasLimitCalculator calculator =
        new TransactionGasLimitOverrideGasLimitCalculator(delegate, 0L);
    when(delegate.transactionBlobGasLimitCap()).thenReturn(99L);

    assertThat(calculator.transactionBlobGasLimitCap()).isEqualTo(99L);
    verify(delegate).transactionBlobGasLimitCap();
  }

  @Test
  public void blockBuilderBlobGasLimitDelegates() {
    final TransactionGasLimitOverrideGasLimitCalculator calculator =
        new TransactionGasLimitOverrideGasLimitCalculator(delegate, 0L);
    when(delegate.blockBuilderBlobGasLimit()).thenReturn(88L);

    assertThat(calculator.blockBuilderBlobGasLimit()).isEqualTo(88L);
    verify(delegate).blockBuilderBlobGasLimit();
  }
}
