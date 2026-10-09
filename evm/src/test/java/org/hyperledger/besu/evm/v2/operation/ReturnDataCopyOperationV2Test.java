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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ReturnDataCopyOperationV2Test {

  private static final Bytes RETURN_DATA = Bytes.fromHexString("0x0102030405060708090a");

  private final ReturnDataCopyOperationV2 operation =
      new ReturnDataCopyOperationV2(new ByzantiumGasCalculator());

  @Test
  void copiesTheReturnData() {
    final MessageFrame frame = frame("0x01", "0x06", "0x04");

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isZero();
    assertThat(frame.readMemory(0, 6)).isEqualTo(Bytes.fromHexString("0x000708090a00"));
    // 3 for the copy, 3 for the word copied and 3 for the word of memory
    assertThat(result.getGasCost()).isEqualTo(9);
  }

  @ParameterizedTest(name = "offset {0}, length {1}")
  @CsvSource({"0x07, 0x04", "0x0b, 0x00", "0x00, 0x0b"})
  void haltsOnReadingPastTheReturnData(final String sourceOffset, final String length) {
    final MessageFrame frame = frame("0x00", sourceOffset, length);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason())
        .isEqualTo(ExceptionalHaltReason.INVALID_RETURN_DATA_BUFFER_ACCESS);
  }

  @Test
  void haltsWhenTheEndOverflows() {
    final MessageFrame frame = frame("0x00", "0x7fffffffffffffff", "0x01");

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.OUT_OF_BOUNDS);
  }

  @Test
  void copiesNothingAtTheEndOfTheReturnData() {
    final MessageFrame frame = frame("0x00", "0x0a", "0x00");

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getGasCost()).isEqualTo(3);
  }

  @Test
  void haltsOnStackUnderflow() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString("0x00"))
            .pushStackItem(Bytes32.fromHexString("0x00"))
            .build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  private static MessageFrame frame(
      final String memOffset, final String sourceOffset, final String length) {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .initialGas(1_000_000)
            .pushStackItem(Bytes32.fromHexString(length))
            .pushStackItem(Bytes32.fromHexString(sourceOffset))
            .pushStackItem(Bytes32.fromHexString(memOffset))
            .build();
    frame.setReturnData(RETURN_DATA);
    return frame;
  }
}
