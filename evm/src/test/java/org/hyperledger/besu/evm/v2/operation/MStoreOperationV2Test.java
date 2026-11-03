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
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class MStoreOperationV2Test {

  private static final Bytes32 VALUE =
      Bytes32.fromHexString("0x0123456789abcdeffedcba98765432100f1e2d3c4b5a69788796a5b4c3d2e1f0");

  private final MStoreOperationV2 operation = new MStoreOperationV2(new FrontierGasCalculator());

  @Test
  void storesAllFourLimbsAtTheOffset() {
    final MessageFrame frame = frame(VALUE, Bytes32.fromHexString("0x03"));

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isZero();
    assertThat(frame.readMemory(0, 3)).isEqualTo(Bytes.wrap(new byte[3]));
    assertThat(frame.readMemory(3, 32)).isEqualTo(VALUE);
    // 3 for the store and 3 for each of the two words it touches
    assertThat(result.getGasCost()).isEqualTo(9);
    assertThat(frame.memoryByteSize()).isEqualTo(64);
  }

  @Test
  void storesASmallValueRightAligned() {
    final MessageFrame frame = frame(Bytes32.fromHexString("0x2a"), Bytes32.fromHexString("0x00"));

    operation.execute(frame, null);

    assertThat(frame.readMemory(0, 32)).isEqualTo(Bytes32.fromHexString("0x2a"));
  }

  @Test
  void haltsOnAnOffsetBeyondAnyGas() {
    final MessageFrame frame = frame(VALUE, Bytes32.ZERO.not());

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  @Test
  void haltsOnStackUnderflow() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().pushStackItem(Bytes32.fromHexString("0x00")).build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  private static MessageFrame frame(final Bytes32 value, final Bytes32 offset) {
    return new TestMessageFrameBuilderV2()
        .initialGas(1_000_000)
        .pushStackItem(value)
        .pushStackItem(offset)
        .build();
  }
}
