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

class JumpOperationV2Test {

  // JUMPDESTs at 3 and 7, and a 0x5b at 6 that is PUSH1 data
  private static final Code CODE = new Code(Bytes.fromHexString("0x6003565b00605b5b"));

  @Test
  void jumpsToAJumpDest() {
    final MessageFrame frame = frame("0x03");

    final Operation.OperationResult result = JumpOperationV2.staticOperation(frame);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getPcIncrement()).isZero();
    assertThat(result.getGasCost()).isEqualTo(8);
    assertThat(frame.getPC()).isEqualTo(3);
    assertThat(frame.stackTopV2()).isZero();
  }

  @ParameterizedTest(name = "destination {0}")
  @ValueSource(
      strings = {
        // not a JUMPDEST
        "0x02",
        // PUSH1 data
        "0x06",
        // past the end of the code
        "0x08",
        "0x7fffffff",
        "0x80000000",
        // the least significant limb alone would be a JUMPDEST
        "0x0000000000000000000000000000000100000000000000000000000000000003",
        "0x0000000000000001000000000000000000000000000000000000000000000003",
        "0x0100000000000000000000000000000000000000000000000000000000000003"
      })
  void haltsOnAnInvalidDestination(final String destination) {
    final MessageFrame frame = frame(destination);

    final Operation.OperationResult result = JumpOperationV2.staticOperation(frame);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INVALID_JUMP_DESTINATION);
  }

  @Test
  void jumpsToTheLastByte() {
    final MessageFrame frame = frame("0x07");

    final Operation.OperationResult result = JumpOperationV2.staticOperation(frame);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.getPC()).isEqualTo(7);
  }

  @Test
  void haltsOnStackUnderflow() {
    final MessageFrame frame = new TestMessageFrameBuilderV2().code(CODE).build();

    final Operation.OperationResult result = JumpOperationV2.staticOperation(frame);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  private static MessageFrame frame(final String destination) {
    return new TestMessageFrameBuilderV2()
        .code(CODE)
        .pushStackItem(Bytes32.fromHexString(destination))
        .build();
  }
}
