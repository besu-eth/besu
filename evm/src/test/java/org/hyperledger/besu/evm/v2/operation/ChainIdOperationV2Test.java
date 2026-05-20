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
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class ChainIdOperationV2Test {

  private final GasCalculator gasCalculator = new BerlinGasCalculator();

  @Test
  void shouldPushSmallChainIdToStack() {
    final Bytes32 chainId = Bytes32.leftPad(Bytes.of(0x01));
    final ChainIdOperationV2 operation = new ChainIdOperationV2(gasCalculator, chainId);
    final MessageFrame frame = new TestMessageFrameBuilderV2().build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0)).isEqualTo(new UInt256(0, 0, 0, 1L));
  }

  @Test
  void shouldPushZeroChainId() {
    final ChainIdOperationV2 operation = new ChainIdOperationV2(gasCalculator, Bytes32.ZERO);
    final MessageFrame frame = new TestMessageFrameBuilderV2().build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void shouldPushLargeChainIdToStack() {
    final Bytes32 chainId =
        Bytes32.fromHexString("0x00000000000000010000000000000002000000000000000300000000000000FF");
    final ChainIdOperationV2 operation = new ChainIdOperationV2(gasCalculator, chainId);
    final MessageFrame frame = new TestMessageFrameBuilderV2().build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0))
        .isEqualTo(
            new UInt256(
                0x0000000000000001L,
                0x0000000000000002L,
                0x0000000000000003L,
                0x00000000000000FFL));
  }

  @Test
  void shouldReturnCorrectGasCost() {
    final ChainIdOperationV2 operation =
        new ChainIdOperationV2(gasCalculator, Bytes32.leftPad(Bytes.of(1)));
    final MessageFrame frame = new TestMessageFrameBuilderV2().build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getGasCost()).isEqualTo(gasCalculator.getBaseTierGasCost());
  }

  @Test
  void shouldExposeConfiguredChainId() {
    final Bytes32 chainId = Bytes32.leftPad(Bytes.of(0x2A));
    final ChainIdOperationV2 operation = new ChainIdOperationV2(gasCalculator, chainId);
    assertThat(operation.getChainId()).isEqualTo(chainId);
  }

  @Test
  void shouldHaltOnInsufficientGas() {
    final ChainIdOperationV2 operation =
        new ChainIdOperationV2(gasCalculator, Bytes32.leftPad(Bytes.of(1)));
    final MessageFrame frame = new TestMessageFrameBuilderV2().initialGas(1L).build();
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  @Test
  void shouldHaltOnStackOverflow() {
    final ChainIdOperationV2 operation =
        new ChainIdOperationV2(gasCalculator, Bytes32.leftPad(Bytes.of(1)));
    final MessageFrame frame = new TestMessageFrameBuilderV2().build();
    frame.setTopV2(MessageFrame.DEFAULT_MAX_STACK_SIZE);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.TOO_MANY_STACK_ITEMS);
  }

  @Test
  void shouldHaltOnInsufficientGasEvenStackOverflow() {
    final ChainIdOperationV2 operation =
        new ChainIdOperationV2(gasCalculator, Bytes32.leftPad(Bytes.of(1)));
    final MessageFrame frame = new TestMessageFrameBuilderV2().initialGas(1L).build();
    frame.setTopV2(MessageFrame.DEFAULT_MAX_STACK_SIZE);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }
}
