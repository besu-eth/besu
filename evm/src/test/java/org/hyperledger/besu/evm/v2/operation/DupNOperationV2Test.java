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

import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class DupNOperationV2Test {

  @Test
  void duplicatesTheItemTheImmediateNames() {
    // 0x80 decodes to n = 17
    final MessageFrame frame = frame(Bytes.of(0xe6, 0x80), 20);

    final Operation.OperationResult result = execute(frame);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getPcIncrement()).isEqualTo(2);
    assertThat(result.getGasCost()).isEqualTo(3);
    assertThat(frame.stackTopV2()).isEqualTo(21);
    // item i from the top holds the value 1000 + i, so the 17th item holds 1016
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.fromLong(1016));
    assertThat(getV2StackItem(frame, 17)).isEqualTo(UInt256.fromLong(1016));
  }

  @Test
  void readsAMissingImmediateAsZero() {
    // 0 decodes to n = 145
    final MessageFrame frame = frame(Bytes.of(0xe6), 145);

    final Operation.OperationResult result = execute(frame);

    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.fromLong(1144));
  }

  @Test
  void haltsOnAnInvalidImmediate() {
    final MessageFrame frame = frame(Bytes.of(0xe6, 0x5b), 20);

    final Operation.OperationResult result = execute(frame);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INVALID_OPERATION);
    assertThat(frame.stackTopV2()).isEqualTo(20);
  }

  @Test
  void haltsOnStackUnderflow() {
    final Operation.OperationResult result = execute(frame(Bytes.of(0xe6, 0x80), 16));

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  @Test
  void haltsOnStackOverflow() {
    final MessageFrame frame = frame(Bytes.of(0xe6, 0x80), 20);
    frame.setTopV2(MessageFrame.DEFAULT_MAX_STACK_SIZE);

    final Operation.OperationResult result = execute(frame);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.TOO_MANY_STACK_ITEMS);
  }

  private static Operation.OperationResult execute(final MessageFrame frame) {
    return DupNOperationV2.staticOperation(
        frame, frame.getCode().getBytes().toArrayUnsafe(), frame.getPC());
  }

  /** A frame whose item i from the top holds 1000 + i. */
  private static MessageFrame frame(final Bytes code, final int items) {
    final TestMessageFrameBuilderV2 builder = new TestMessageFrameBuilderV2().code(new Code(code));
    for (int i = items - 1; i >= 0; i--) {
      builder.pushStackItem(Bytes32.leftPad(Bytes.ofUnsignedInt(1000 + i)));
    }
    return builder.build();
  }
}
