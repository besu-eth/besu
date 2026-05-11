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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.evm.frame.MessageFrame;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FrontierGasCalculatorTest {
  private final GasCalculator gasCalculator = new FrontierGasCalculator();

  @Mock private Transaction transaction;
  @Mock private MessageFrame messageFrame;

  @Test
  void shouldCalculateRefundWithNoSelfDestructs() {
    // Arrange
    when(messageFrame.getSelfDestructs()).thenReturn(Collections.emptySet());
    when(messageFrame.getGasRefund()).thenReturn(1000L);
    when(messageFrame.getRemainingGas()).thenReturn(5000L);
    when(transaction.getGasLimit()).thenReturn(100000L);

    // Act
    long refund = gasCalculator.calculateGasRefund(transaction, messageFrame, 500L);

    // Assert
    assertThat(refund)
        .isEqualTo(6000L); // 5000 (remaining) + min(1000 (execution refund), 47500 (max allowance))
  }

  @Test
  void shouldCalculateRefundWithMultipleSelfDestructsAndIgnoreCodeDelegation() {
    // Arrange
    Set<Address> selfDestructs = new HashSet<>();
    selfDestructs.add(Address.wrap(Bytes.random(20)));
    selfDestructs.add(Address.wrap(Bytes.random(20)));

    when(messageFrame.getSelfDestructs()).thenReturn(selfDestructs);
    when(messageFrame.getGasRefund()).thenReturn(1000L);
    when(messageFrame.getRemainingGas()).thenReturn(5000L);
    when(transaction.getGasLimit()).thenReturn(100000L);

    // Act
    long refund = gasCalculator.calculateGasRefund(transaction, messageFrame, 500L);

    // Assert
    assertThat(refund)
        .isEqualTo(
            52500L); // 5000 (remaining) + min(49500 (execution refund), 47500 (max allowance))
  }

  @Test
  void shouldRespectMaxRefundAllowance() {
    // Arrange
    when(messageFrame.getSelfDestructs()).thenReturn(Collections.emptySet());
    when(messageFrame.getGasRefund()).thenReturn(100000L);
    when(messageFrame.getRemainingGas()).thenReturn(20000L);
    when(transaction.getGasLimit()).thenReturn(100000L);

    // Act
    long refund = gasCalculator.calculateGasRefund(transaction, messageFrame, 1000L);

    // Assert
    assertThat(refund)
        .isEqualTo(
            60000L); // 20000 (remaining) + min(101000 (execution refund), 40000 (max allowance))
  }

  @Test
  void shouldHandleZeroValuesCorrectly() {
    // Arrange
    when(messageFrame.getSelfDestructs()).thenReturn(Collections.emptySet());
    when(messageFrame.getGasRefund()).thenReturn(0L);
    when(messageFrame.getRemainingGas()).thenReturn(0L);
    when(transaction.getGasLimit()).thenReturn(100000L);

    // Act
    long refund =
        gasCalculator.calculateGasRefund(
            transaction,
            messageFrame,
            0L); // 0 (remaining) + min(0 (execution refund), 50000 (max allowance))

    // Assert
    assertThat(refund).isEqualTo(0L);
  }

  @Test
  void transactionFloorCostShouldAlwaysBeZero() {
    assertThat(gasCalculator.transactionFloorCost(transaction)).isEqualTo(0L);
  }
}
