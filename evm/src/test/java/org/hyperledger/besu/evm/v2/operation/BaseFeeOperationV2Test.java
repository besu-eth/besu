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
import org.hyperledger.besu.evm.testutils.FakeBlockValues;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.util.Optional;

import org.junit.jupiter.api.Test;

class BaseFeeOperationV2Test {

  private final GasCalculator gasCalculator = new BerlinGasCalculator();
  private final BaseFeeOperationV2 operation = new BaseFeeOperationV2(gasCalculator);

  @Test
  void shouldReturnGasCost() {
    final MessageFrame frame = createFrame(100, Optional.of(Wei.of(5L)));
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getGasCost()).isEqualTo(gasCalculator.getBaseTierGasCost());
    assertThat(result.getHaltReason()).isNull();
  }

  @Test
  void shouldWriteBaseFeeToStack() {
    final MessageFrame frame = createFrame(100, Optional.of(Wei.of(5L)));
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0)).isEqualTo(new UInt256(0, 0, 0, 5L));
  }

  @Test
  void shouldWriteLargeBaseFeeToStack() {
    final Wei largeFee =
        Wei.fromHexString("0x00000000000000010000000000000002000000000000000300000000000000FF");
    final MessageFrame frame = createFrame(100, Optional.of(largeFee));
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    final UInt256 stackVal = getV2StackItem(frame, 0);
    assertThat(stackVal)
        .isEqualTo(
            new UInt256(
                0x0000000000000001L,
                0x0000000000000002L,
                0x0000000000000003L,
                0x00000000000000FFL));
  }

  @Test
  void shouldHaltIfNoBaseFeeInBlockHeader() {
    final MessageFrame frame = createFrame(100, Optional.empty());
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INVALID_OPERATION);
  }

  @Test
  void shouldTriggerIvalidOperationEvenStackOverflow() {
    final MessageFrame frame = createFrame(100, Optional.empty());
    frame.setTopV2(MessageFrame.DEFAULT_MAX_STACK_SIZE);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INVALID_OPERATION);
  }

  @Test
  void shouldHaltOnInsufficientGas() {
    final MessageFrame frame = createFrame(1, Optional.of(Wei.of(5L)));
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  @Test
  void shouldHaltOnInsufficientGasEvenStackOverflow() {
    final MessageFrame frame = createFrame(1, Optional.of(Wei.of(5L)));
    frame.setTopV2(MessageFrame.DEFAULT_MAX_STACK_SIZE);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  @Test
  void shouldHaltOnStackOverflow() {
    final MessageFrame frame = createFrame(100, Optional.of(Wei.of(5L)));
    frame.setTopV2(MessageFrame.DEFAULT_MAX_STACK_SIZE);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.TOO_MANY_STACK_ITEMS);
  }

  private MessageFrame createFrame(final long initialGas, final Optional<Wei> baseFee) {
    return new TestMessageFrameBuilderV2()
        .initialGas(initialGas)
        .blockValues(new FakeBlockValues(baseFee))
        .build();
  }
}
