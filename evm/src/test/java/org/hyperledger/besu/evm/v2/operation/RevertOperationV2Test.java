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

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.ByzantiumGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class RevertOperationV2Test {

  private static final Bytes MEMORY = Bytes.fromHexString("0x0102030405060708090a");

  private final RevertOperationV2 operation = new RevertOperationV2(new ByzantiumGasCalculator());

  @Test
  void revertsWithTheMemoryRange() {
    final MessageFrame frame = frame("0x02", "0x04");

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.getState()).isEqualTo(MessageFrame.State.REVERT);
    assertThat(frame.getOutputData()).isEqualTo(Bytes.fromHexString("0x03040506"));
    assertThat(frame.getRevertReason()).contains(Bytes.fromHexString("0x03040506"));
    assertThat(frame.stackTopV2()).isZero();
  }

  @Test
  void chargesForExpandingTheMemory() {
    final Operation.OperationResult result = operation.execute(frame("0x20", "0x02"), null);

    assertThat(result.getGasCost()).isEqualTo(3);
  }

  @Test
  void haltsOnALengthBeyondAnyGas() {
    final Operation.OperationResult result =
        operation.execute(frame("0x00", "0x0100000000000000"), null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  @Test
  void haltsOnStackUnderflow() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().pushStackItem(Bytes32.fromHexString("0x00")).build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  private static MessageFrame frame(final String offset, final String length) {
    return new TestMessageFrameBuilderV2()
        .initialGas(1_000_000)
        .memory(MEMORY)
        .pushStackItem(Bytes32.fromHexString(length))
        .pushStackItem(Bytes32.fromHexString(offset))
        .build();
  }
}
