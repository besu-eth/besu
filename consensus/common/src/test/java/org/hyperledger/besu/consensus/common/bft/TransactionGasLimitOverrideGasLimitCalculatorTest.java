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

import org.junit.jupiter.api.Test;

public class TransactionGasLimitOverrideGasLimitCalculatorTest {

  private final GasLimitCalculator delegate = mock(GasLimitCalculator.class);

  @Test
  public void transactionGasLimitCapReturnsOverride() {
    final TransactionGasLimitOverrideGasLimitCalculator calculator =
        new TransactionGasLimitOverrideGasLimitCalculator(delegate, 12345L);

    assertThat(calculator.transactionGasLimitCap()).isEqualTo(12345L);
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
