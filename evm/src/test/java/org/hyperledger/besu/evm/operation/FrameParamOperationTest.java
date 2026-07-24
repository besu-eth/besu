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
package org.hyperledger.besu.evm.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class FrameParamOperationTest {

  private final FrameParamOperation operation = new FrameParamOperation(new PragueGasCalculator());
  private final EVM fakeEVM = mock(EVM.class);

  @Test
  void validFrameIndexReturnsZeroForNow() {
    MessageFrame frame = mock(MessageFrame.class);
    when(frame.popStackItem())
        .thenReturn(Bytes.of(0)) // frameIndex
        .thenReturn(Bytes.of(FrameParamOperation.PARAM_TARGET)); // param
    when(frame.getRemainingGas()).thenReturn(100L);

    Operation.OperationResult result = operation.execute(frame, fakeEVM);

    assertThat(result.getGasCost()).isEqualTo(2L);
    assertThat(result.getHaltReason()).isNull();
    verify(frame).pushStackItem(Bytes32.ZERO);
  }

  @Test
  void outOfBoundsFrameIndexHalts() {
    MessageFrame frame = mock(MessageFrame.class);
    when(frame.popStackItem())
        .thenReturn(Bytes32.repeat((byte) 0xFF)) // huge index
        .thenReturn(Bytes.of(0));
    when(frame.getRemainingGas()).thenReturn(100L);

    Operation.OperationResult result = operation.execute(frame, fakeEVM);

    assertThat(result.getGasCost()).isEqualTo(2L);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.OUT_OF_BOUNDS);
  }

  @Test
  void insufficientGasReturnsHalt() {
    MessageFrame frame = mock(MessageFrame.class);
    when(frame.popStackItem()).thenReturn(Bytes.of(0)).thenReturn(Bytes.of(0));
    when(frame.getRemainingGas()).thenReturn(1L);

    Operation.OperationResult result = operation.execute(frame, fakeEVM);

    assertThat(result.getGasCost()).isEqualTo(2L);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }
}
