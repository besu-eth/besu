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

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class Keccak256OperationV2Test {

  private static final Bytes MEMORY =
      Bytes.fromHexString("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20");

  private final Keccak256OperationV2 operation =
      new Keccak256OperationV2(new FrontierGasCalculator());

  @Test
  void hashesTheMemoryRange() {
    final MessageFrame frame = frame("0x04", "0x0a");

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isEqualTo(1);
    assertThat(getV2StackItem(frame, 0))
        .isEqualTo(UInt256.fromBytesBE(Hash.keccak256(MEMORY.slice(4, 10)).toArrayUnsafe()));
    // 30 for the hash and 6 for the word it covers
    assertThat(result.getGasCost()).isEqualTo(36);
  }

  @Test
  void hashesTheEmptyRange() {
    final MessageFrame frame = frame("0xffffffffffff", "0x00");

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0))
        .isEqualTo(UInt256.fromBytesBE(Hash.keccak256(Bytes.EMPTY).toArrayUnsafe()));
    assertThat(result.getGasCost()).isEqualTo(30);
  }

  @Test
  void haltsOnALengthBeyondAnyGas() {
    final MessageFrame frame = frame("0x00", "0x0100000000000000000000");

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

  private static MessageFrame frame(final String offset, final String length) {
    return new TestMessageFrameBuilderV2()
        .initialGas(1_000_000)
        .memory(MEMORY)
        .pushStackItem(Bytes32.fromHexString(length))
        .pushStackItem(Bytes32.fromHexString(offset))
        .build();
  }
}
