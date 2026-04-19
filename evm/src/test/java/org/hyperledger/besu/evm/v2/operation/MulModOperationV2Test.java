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

class MulModOperationV2Test {

  private final GasCalculator gasCalculator = new FrontierGasCalculator();
  private final MulModOperationV2 operation = new MulModOperationV2(gasCalculator);

  /**
   * Test data for mulmod(a, b, m) = expected.
   *
   * <p>Push order when building the frame: m first (deepest), then b, then a (top).
   */
  static Iterable<Arguments> data() {
    return List.of(
        // (a, b, m, expected)
        Arguments.of("0x02", "0x03", "0x05", "0x01"), // 2*3=6, 6 mod 5 = 1
        Arguments.of("0x02", "0x03", "0x06", "0x"), // 2*3=6, 6 mod 6 = 0 (exact multiple)
        Arguments.of("0x00", "0xff", "0x07", "0x"), // 0 * anything = 0
        Arguments.of("0xff", "0xff", "0x01", "0x"), // anything mod 1 = 0
        Arguments.of("0x02", "0x03", "0x00", "0x"), // zero modulus → 0 (special EVM rule)
        // Large value spanning all four 64-bit limbs: MAX * 1 mod 7.
        // 2^256 ≡ 2 (mod 7), so MAX = 2^256-1 ≡ 1 (mod 7).
        Arguments.of(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0x01",
            "0x07",
            "0x01"),
        // Cross-limb: (2^128) * (2^128) mod (2^128 + 1) = 1.
        // 2^128 ≡ -1 (mod 2^128+1), so 2^256 = (2^128)^2 ≡ 1 (mod 2^128+1).
        Arguments.of(
            "0x00000000000000000000000000000001" + "00000000000000000000000000000000", // 2^128
            "0x00000000000000000000000000000001" + "00000000000000000000000000000000", // 2^128
            "0x00000000000000000000000000000001" + "00000000000000000000000000000001", // 2^128+1
            "0x01"));
  }

  @ParameterizedTest(name = "{index}: mulmod({0}, {1}, {2}) = {3}")
  @MethodSource("data")
  void mulModOperation(
      final String a, final String b, final String m, final String expectedResult) {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexStringLenient(m)) // pushed first → deepest (top-3)
            .pushStackItem(Bytes32.fromHexStringLenient(b)) // top-2
            .pushStackItem(Bytes32.fromHexStringLenient(a)) // pushed last → top (top-1)
            .build();
    assertThat(frame.stackTopV2()).isEqualTo(3);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    // MULMOD consumes 3 items and produces 1: net stack change is -2.
    assertThat(frame.stackTopV2()).isEqualTo(1);

    final UInt256 expected =
        expectedResult.equals("0x") || expectedResult.equals("0x0")
            ? UInt256.ZERO
            : UInt256.fromBytesBE(Bytes32.fromHexStringLenient(expectedResult).toArrayUnsafe());
    assertThat(getV2StackItem(frame, 0)).isEqualTo(expected);
  }

  @Test
  void mulModOperationUnderflowNoItems() {
    final MessageFrame frame = new TestMessageFrameBuilderV2().build();
    assertThat(frame.stackTopV2()).isEqualTo(0);

    final Operation.OperationResult result = MulModOperationV2.staticOperation(frame);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    assertThat(frame.stackTopV2()).isEqualTo(0);
  }

  @Test
  void mulModOperationUnderflowOnlyTwoItems() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexStringLenient("0x1")) // deepest (top-3)
            .pushStackItem(Bytes32.fromHexStringLenient("0x2")) // top-2, missing top-1
            .build();
    assertThat(frame.stackTopV2()).isEqualTo(2);

    final Operation.OperationResult result = MulModOperationV2.staticOperation(frame);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    assertThat(frame.stackTopV2()).isEqualTo(2);
  }
}
