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
package org.hyperledger.besu.evm.v2.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2.getV2StackItem;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.BerlinGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.junit.jupiter.api.Test;

class GasPriceOperationV2Test {

  private final GasCalculator gasCalculator = new BerlinGasCalculator();
  private final GasPriceOperationV2 operation = new GasPriceOperationV2(gasCalculator);

  @Test
  void shouldPushGasPriceToStack() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().gasPrice(Wei.of(20_000_000_000L)).build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0)).isEqualTo(new UInt256(0, 0, 0, 20_000_000_000L));
  }

  @Test
  void shouldPushZeroGasPrice() {
    final MessageFrame frame = new TestMessageFrameBuilderV2().gasPrice(Wei.ZERO).build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void shouldReturnCorrectGasCost() {
    final MessageFrame frame = new TestMessageFrameBuilderV2().gasPrice(Wei.of(1L)).build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getGasCost()).isEqualTo(gasCalculator.getBaseTierGasCost());
  }

  @Test
  void shouldHaltOnInsufficientGas() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().initialGas(1L).gasPrice(Wei.of(10L)).build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  @Test
  void shouldHaltOnStackOverflow() {
    final MessageFrame frame = new TestMessageFrameBuilderV2().gasPrice(Wei.of(1L)).build();
    frame.setTopV2(MessageFrame.DEFAULT_MAX_STACK_SIZE);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.TOO_MANY_STACK_ITEMS);
  }
}
