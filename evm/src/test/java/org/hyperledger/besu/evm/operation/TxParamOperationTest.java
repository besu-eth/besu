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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class TxParamOperationTest {

  private final TxParamOperation operation = new TxParamOperation(new PragueGasCalculator());
  private final EVM fakeEVM = mock(EVM.class);

  @Test
  void txTypeReturns0x06() {
    MessageFrame frame = mock(MessageFrame.class);
    when(frame.popStackItem()).thenReturn(Bytes.of(TxParamOperation.PARAM_TX_TYPE));
    when(frame.getRemainingGas()).thenReturn(100L);

    Operation.OperationResult result = operation.execute(frame, fakeEVM);

    assertThat(result.getGasCost()).isEqualTo(2L);
    assertThat(result.getHaltReason()).isNull();
    verify(frame).pushStackItem(Bytes32.leftPad(Bytes.of(0x06)));
  }

  @Test
  void senderReturnsOriginatorAddress() {
    MessageFrame frame = mock(MessageFrame.class);
    Address sender = Address.fromHexString("0x1234567890abcdef1234567890abcdef12345678");
    when(frame.popStackItem()).thenReturn(Bytes.of(TxParamOperation.PARAM_SENDER));
    when(frame.getRemainingGas()).thenReturn(100L);
    when(frame.getOriginatorAddress()).thenReturn(sender);

    Operation.OperationResult result = operation.execute(frame, fakeEVM);

    assertThat(result.getGasCost()).isEqualTo(2L);
    assertThat(result.getHaltReason()).isNull();
    verify(frame).pushStackItem(Bytes32.leftPad(sender.getBytes()));
  }

  @Test
  void nonceReturnsZeroForNow() {
    MessageFrame frame = mock(MessageFrame.class);
    when(frame.popStackItem()).thenReturn(Bytes.of(TxParamOperation.PARAM_NONCE));
    when(frame.getRemainingGas()).thenReturn(100L);

    Operation.OperationResult result = operation.execute(frame, fakeEVM);

    assertThat(result.getGasCost()).isEqualTo(2L);
    assertThat(result.getHaltReason()).isNull();
    verify(frame).pushStackItem(Bytes32.ZERO);
  }

  @Test
  void unknownParamReturnsZero() {
    MessageFrame frame = mock(MessageFrame.class);
    when(frame.popStackItem()).thenReturn(Bytes.of(0xFF));
    when(frame.getRemainingGas()).thenReturn(100L);

    Operation.OperationResult result = operation.execute(frame, fakeEVM);

    assertThat(result.getGasCost()).isEqualTo(2L);
    assertThat(result.getHaltReason()).isNull();
    verify(frame).pushStackItem(Bytes32.ZERO);
  }

  @Test
  void insufficientGasReturnsHalt() {
    MessageFrame frame = mock(MessageFrame.class);
    when(frame.popStackItem()).thenReturn(Bytes.of(0));
    when(frame.getRemainingGas()).thenReturn(1L);

    Operation.OperationResult result = operation.execute(frame, fakeEVM);

    assertThat(result.getGasCost()).isEqualTo(2L);
    assertThat(result.getHaltReason())
        .isEqualTo(org.hyperledger.besu.evm.frame.ExceptionalHaltReason.INSUFFICIENT_GAS);
  }
}
