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
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class MCopyOperationV2Test {

  private static final Bytes MEMORY =
      Bytes.fromHexString("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20");

  private final MCopyOperationV2 operation = new MCopyOperationV2(new CancunGasCalculator());

  @Test
  void copiesOverlappingRegions() {
    final MessageFrame frame = frame("0x02", "0x00", "0x08");

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isZero();
    assertThat(frame.readMemory(0, 12))
        .isEqualTo(Bytes.fromHexString("0x010201020304050607080b0c"));
    // 3 for the copy and 3 for the word copied, all within the memory in use
    assertThat(result.getGasCost()).isEqualTo(6);
  }

  @Test
  void chargesTheExpansionOfTheFurtherRegion() {
    final MessageFrame frame = frame("0x00", "0x30", "0x10");

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    // 3 for the copy, 3 for the word copied and 3 for the word the source adds to the memory
    assertThat(result.getGasCost()).isEqualTo(9);
    assertThat(frame.memoryByteSize()).isEqualTo(64);
    assertThat(frame.readMemory(0, 16)).isEqualTo(Bytes.wrap(new byte[16]));
  }

  @Test
  void copiesNothingForAZeroLength() {
    final MessageFrame frame = frame("0x00", "0xffffffffffffffff", "0x00");

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getGasCost()).isEqualTo(3);
    assertThat(frame.readMemory(0, 32)).isEqualTo(MEMORY);
  }

  @Test
  void haltsOnALengthBeyondAnyGas() {
    final MessageFrame frame = frame("0x00", "0x00", "0x0100000000000000");

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
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

  private static MessageFrame frame(final String dst, final String src, final String length) {
    return new TestMessageFrameBuilderV2()
        .initialGas(1_000_000)
        .memory(MEMORY)
        .pushStackItem(Bytes32.fromHexString(length))
        .pushStackItem(Bytes32.fromHexString(src))
        .pushStackItem(Bytes32.fromHexString(dst))
        .build();
  }
}
