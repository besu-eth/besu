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

import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class JumpiOperationV2Test {

  // JUMPDEST at 3
  private static final Code CODE = new Code(Bytes.fromHexString("0x6003575b00"));

  @ParameterizedTest(name = "condition {0}")
  @ValueSource(
      strings = {
        "0x01",
        "0x8000000000000000000000000000000000000000000000000000000000000000",
        "0x0000000000000000000000000000000100000000000000000000000000000000"
      })
  void jumpsOnANonZeroCondition(final String condition) {
    final MessageFrame frame = frame("0x03", condition);

    final Operation.OperationResult result = JumpiOperationV2.staticOperation(frame);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getPcIncrement()).isZero();
    assertThat(result.getGasCost()).isEqualTo(10);
    assertThat(frame.getPC()).isEqualTo(3);
    assertThat(frame.stackTopV2()).isZero();
  }

  @Test
  void continuesOnAZeroConditionWithoutCheckingTheDestination() {
    final MessageFrame frame = frame("0x02", "0x00");

    final Operation.OperationResult result = JumpiOperationV2.staticOperation(frame);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getPcIncrement()).isEqualTo(1);
    assertThat(result.getGasCost()).isEqualTo(10);
    assertThat(frame.getPC()).isZero();
    assertThat(frame.stackTopV2()).isZero();
  }

  @Test
  void haltsOnAnInvalidDestination() {
    final MessageFrame frame = frame("0x02", "0x01");

    final Operation.OperationResult result = JumpiOperationV2.staticOperation(frame);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INVALID_JUMP_DESTINATION);
  }

  @Test
  void haltsOnStackUnderflow() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .code(CODE)
            .pushStackItem(Bytes32.fromHexString("0x03"))
            .build();

    final Operation.OperationResult result = JumpiOperationV2.staticOperation(frame);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  private static MessageFrame frame(final String destination, final String condition) {
    return new TestMessageFrameBuilderV2()
        .code(CODE)
        .pushStackItem(Bytes32.fromHexString(condition))
        .pushStackItem(Bytes32.fromHexString(destination))
        .build();
  }
}
