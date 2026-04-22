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
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.util.List;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class AddOperationV2Test {
  private final GasCalculator gasCalculator = new FrontierGasCalculator();
  private final AddOperationV2 operation = new AddOperationV2(gasCalculator);

  /**
   * Structural test data for add(a, b) = expected. Arithmetic correctness is covered by
   * UInt256PropertyBasedTest; these cases verify stack arity, limb-level read/write wiring, and
   * 256-bit wrap handling.
   *
   * <p>Push order when building the frame: b first (deepest), then a (top).
   */
  static Iterable<Arguments> data() {
    return List.of(
        // (a, b, expected)
        // Happy path: low-limb only.
        Arguments.of("0x02", "0x03", "0x05"),
        // Zero identity with operand ordering: confirms b is read from the deeper slot.
        Arguments.of("0x00", "0x03", "0x03"),
        // 256-bit wrap: all 4 result limbs written as zero.
        Arguments.of(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "0x01", "0x00"),
        // All 4 limbs populated on both inputs and the result (no carries): verifies every limb is
        // read and written through stackDataV2(). Four distinct limb values catch any
        // limb-index/offset mistakes.
        Arguments.of(
            "0x1000000000000001200000000000000230000000000000034000000000000004",
            "0x1000000000000001100000000000000110000000000000011000000000000001",
            "0x2000000000000002300000000000000340000000000000045000000000000005"));
  }

  @ParameterizedTest(name = "{index}: add({0}, {1}) = {2}")
  @MethodSource("data")
  void addOperation(final String a, final String b, final String expectedResult) {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString(b)) // pushed first → deepest (top-2)
            .pushStackItem(Bytes32.fromHexString(a)) // pushed last → top (top-1)
            .build();
    assertThat(frame.stackTopV2()).isEqualTo(2);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    // ADD consumes 2 items and produces 1: net stack change is -1.
    assertThat(frame.stackTopV2()).isEqualTo(1);

    final UInt256 expected =
        UInt256.fromBytesBE(Bytes32.fromHexString(expectedResult).toArrayUnsafe());
    assertThat(getV2StackItem(frame, 0)).isEqualTo(expected);
  }

  @Test
  void addOperationUnderflowNoItems() {
    final MessageFrame frame = new TestMessageFrameBuilderV2().build();
    assertThat(frame.stackTopV2()).isEqualTo(0);

    final Operation.OperationResult result = AddOperationV2.staticOperation(frame);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    assertThat(frame.stackTopV2()).isEqualTo(0);
  }

  @Test
  void addOperationUnderflowOnlyOneItem() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString("0x01")) // top-2, missing top-1
            .build();
    assertThat(frame.stackTopV2()).isEqualTo(1);

    final Operation.OperationResult result = AddOperationV2.staticOperation(frame);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    assertThat(frame.stackTopV2()).isEqualTo(1);
  }

  @Test
  void addOperationPreservesDeeperStackItems() {
    // Use a distinctive 4-limb value for the untouched deep slot so any accidental write is
    // detectable in any limb.
    final Bytes32 untouched =
        Bytes32.fromHexString("0xdeadbeefdeadbeefcafebabecafebabe0123456789abcdeff1e2d3c4b5a69788");
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(untouched) // top-3 (untouched by ADD)
            .pushStackItem(Bytes32.fromHexString("0x07")) // top-2 (b)
            .pushStackItem(Bytes32.fromHexString("0x05")) // top-1 (a)
            .build();
    assertThat(frame.stackTopV2()).isEqualTo(3);

    operation.execute(frame, null);

    assertThat(frame.stackTopV2()).isEqualTo(2);
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.fromInt(12)); // 5 + 7
    assertThat(getV2StackItem(frame, 1)).isEqualTo(UInt256.fromBytesBE(untouched.toArrayUnsafe()));
  }

  @Test
  void addOperationGasCostIsVeryLowTier() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString("0x01"))
            .pushStackItem(Bytes32.fromHexString("0x02"))
            .build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(gasCalculator.getVeryLowTierGasCost());
  }
}
