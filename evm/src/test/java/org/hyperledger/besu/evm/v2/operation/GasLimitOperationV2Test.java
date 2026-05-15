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

class GasLimitOperationV2Test {

  private final GasCalculator gasCalculator = new BerlinGasCalculator();
  private final GasLimitOperationV2 operation = new GasLimitOperationV2(gasCalculator);

  @Test
  void shouldPushGasLimitToStack() {
    final MessageFrame frame = createFrame(Long.MAX_VALUE, 60_000_000L);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0)).isEqualTo(new UInt256(0, 0, 0, 60_000_000L));
  }

  @Test
  void shouldPushZeroGasLimit() {
    final MessageFrame frame = createFrame(Long.MAX_VALUE, 0L);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void shouldPushMaxLongGasLimit() {
    final MessageFrame frame = createFrame(Long.MAX_VALUE, Long.MAX_VALUE);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0)).isEqualTo(new UInt256(0, 0, 0, Long.MAX_VALUE));
  }

  @Test
  void shouldReturnCorrectGasCost() {
    final MessageFrame frame = createFrame(Long.MAX_VALUE, 60_000_000L);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getGasCost()).isEqualTo(gasCalculator.getBaseTierGasCost());
  }

  @Test
  void shouldHaltOnInsufficientGas() {
    final MessageFrame frame = createFrame(1L, 60_000_000L);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  @Test
  void shouldHaltOnStackOverflow() {
    final MessageFrame frame = createFrame(Long.MAX_VALUE, 60_000_000L);
    frame.setTopV2(MessageFrame.DEFAULT_MAX_STACK_SIZE);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.TOO_MANY_STACK_ITEMS);
  }

  @Test
  void shouldHaltOnInsufficientGasEvenStackOverflow() {
    final MessageFrame frame = createFrame(1, 60_000_000L);
    frame.setTopV2(MessageFrame.DEFAULT_MAX_STACK_SIZE);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  private MessageFrame createFrame(final long initialGas, final long gasLimit) {
    return new TestMessageFrameBuilderV2()
        .initialGas(initialGas)
        .blockValues(new FakeBlockValues(1337L, Optional.empty(), gasLimit, null))
        .build();
  }
}
