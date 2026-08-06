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
import org.junit.jupiter.api.Test;

class ApproveOperationTest {

  private final ApproveOperation operation = new ApproveOperation(new PragueGasCalculator());
  private final EVM fakeEVM = mock(EVM.class);

  @Test
  void approvePaymentSetsState() {
    MessageFrame frame = mock(MessageFrame.class);
    when(frame.popStackItem())
        .thenReturn(Bytes.of(ApproveOperation.APPROVE_PAYMENT))
        .thenReturn(Bytes.EMPTY) // length
        .thenReturn(Bytes.EMPTY); // offset
    when(frame.getRemainingGas()).thenReturn(100L);

    Operation.OperationResult result = operation.execute(frame, fakeEVM);

    assertThat(result.getGasCost()).isEqualTo(0L);
    assertThat(result.getHaltReason()).isNull();
    verify(frame).setState(MessageFrame.State.COMPLETED_SUCCESS);
    verify(frame).setOutputData(Bytes.of(ApproveOperation.APPROVE_PAYMENT));
  }

  @Test
  void approveExecutionSetsState() {
    MessageFrame frame = mock(MessageFrame.class);
    when(frame.popStackItem())
        .thenReturn(Bytes.of(ApproveOperation.APPROVE_EXECUTION))
        .thenReturn(Bytes.EMPTY)
        .thenReturn(Bytes.EMPTY);
    when(frame.getRemainingGas()).thenReturn(100L);

    Operation.OperationResult result = operation.execute(frame, fakeEVM);

    assertThat(result.getGasCost()).isEqualTo(0L);
    assertThat(result.getHaltReason()).isNull();
    verify(frame).setState(MessageFrame.State.COMPLETED_SUCCESS);
    verify(frame).setOutputData(Bytes.of(ApproveOperation.APPROVE_EXECUTION));
  }

  @Test
  void approveBothSetsState() {
    MessageFrame frame = mock(MessageFrame.class);
    when(frame.popStackItem())
        .thenReturn(Bytes.of(ApproveOperation.APPROVE_EXECUTION_AND_PAYMENT))
        .thenReturn(Bytes.EMPTY)
        .thenReturn(Bytes.EMPTY);
    when(frame.getRemainingGas()).thenReturn(100L);

    Operation.OperationResult result = operation.execute(frame, fakeEVM);

    assertThat(result.getGasCost()).isEqualTo(0L);
    assertThat(result.getHaltReason()).isNull();
    verify(frame).setState(MessageFrame.State.COMPLETED_SUCCESS);
    verify(frame).setOutputData(Bytes.of(ApproveOperation.APPROVE_EXECUTION_AND_PAYMENT));
  }

  @Test
  void approveNoneSetsState() {
    MessageFrame frame = mock(MessageFrame.class);
    when(frame.popStackItem())
        .thenReturn(Bytes.of(ApproveOperation.APPROVE_NONE))
        .thenReturn(Bytes.EMPTY)
        .thenReturn(Bytes.EMPTY);
    when(frame.getRemainingGas()).thenReturn(100L);

    Operation.OperationResult result = operation.execute(frame, fakeEVM);

    assertThat(result.getGasCost()).isEqualTo(0L);
    assertThat(result.getHaltReason()).isNull();
  }

  @Test
  void invalidScopeReturnsError() {
    MessageFrame frame = mock(MessageFrame.class);
    when(frame.popStackItem())
        .thenReturn(Bytes.of(0x04)) // invalid scope > 3
        .thenReturn(Bytes.EMPTY)
        .thenReturn(Bytes.EMPTY);
    when(frame.getRemainingGas()).thenReturn(100L);

    Operation.OperationResult result = operation.execute(frame, fakeEVM);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INVALID_OPERATION);
  }
}
