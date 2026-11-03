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

import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class MLoadOperationV2Test {

  private static final Bytes MEMORY =
      Bytes.fromHexString(
          "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728");

  private final MLoadOperationV2 operation = new MLoadOperationV2(new FrontierGasCalculator());

  @Test
  void loadsTheWordAtTheOffset() {
    final MessageFrame frame = frame(Bytes32.fromHexString("0x04"));

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isEqualTo(1);
    assertThat(getV2StackItem(frame, 0))
        .isEqualTo(
            UInt256.fromBytesBE(
                Bytes.fromHexString(
                        "0x05060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f2021222324")
                    .toArrayUnsafe()));
  }

  @Test
  void loadsZerosPastTheWrittenBytes() {
    final MessageFrame frame = frame(Bytes32.fromHexString("0x20"));

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0)).isEqualTo(new UInt256(0x2122232425262728L, 0, 0, 0));
  }

  @Test
  void expandsTheMemory() {
    final MessageFrame frame = frame(Bytes32.fromHexString("0x30"));

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    // the two words in use grow to three: 3 for the load and 3 for the third word
    assertThat(result.getGasCost()).isEqualTo(6);
    assertThat(frame.memoryByteSize()).isEqualTo(96);
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void costsTheVeryLowTierWithinTheMemoryInUse() {
    final Operation.OperationResult result =
        operation.execute(frame(Bytes32.fromHexString("0x00")), null);

    assertThat(result.getGasCost()).isEqualTo(3);
  }

  @Test
  void haltsOnAnOffsetBeyondAnyGas() {
    final MessageFrame frame =
        frame(
            Bytes32.fromHexString(
                "0x0000000000000000000000000000000000000000000000010000000000000000"));

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  @Test
  void haltsOnStackUnderflow() {
    final MessageFrame frame = new TestMessageFrameBuilderV2().build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  private static MessageFrame frame(final Bytes32 offset) {
    return new TestMessageFrameBuilderV2()
        .initialGas(1_000_000)
        .memory(MEMORY)
        .pushStackItem(offset)
        .build();
  }
}
