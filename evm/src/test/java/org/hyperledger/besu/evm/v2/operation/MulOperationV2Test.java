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

class MulOperationV2Test extends BinaryOperationV2Test {
  private final GasCalculator gasCalculator = new FrontierGasCalculator();

  public MulOperationV2Test() {
    super(new MulOperationV2(new FrontierGasCalculator()));
  }

  /**
   * Structural test data for MUL(a, b) = expected. Arithmetic correctness is covered by
   * UInt256PropertyBasedTest; these cases verify stack arity, limb-level read/write wiring, and
   * 256-bit wrap handling.
   *
   * <p>Push order when building the frame: b first (deepest), then a (top).
   */
  static Iterable<Arguments> data() {
    return List.of(
        // (a, b, expected)
        // Happy path: low-limb only.
        Arguments.of("0x02", "0x03", "0x06"),
        // Zero absorbing element: 0 × b = 0, verifies MUL yields zero when a multiplicand is zero.
        Arguments.of("0x00", "0x03", "0x00"),
        // 256-bit wrap: 2^255 × 2 = 2^256 ≡ 0 (mod 2^256). All 4 result limbs must be written as
        // zero — catches bugs where high limbs are left stale after an overflowing product.
        Arguments.of(
            "0x8000000000000000000000000000000000000000000000000000000000000000", "0x02", "0x00"),
        // All 4 limbs populated on both inputs and the result: verifies every limb is read and
        // written through stackDataV2(). Four distinct limb values on `a` catch any
        // limb-index/offset mistakes in either input or the output.
        Arguments.of(
            "0x1000000000000001200000000000000230000000000000034000000000000004",
            "0x1000000000000001100000000000000110000000000000011000000000000001",
            "0x490000000000000b2700000000000009e4000000000000078000000000000004"));
  }

  @ParameterizedTest(name = "{index}: MUL({0}, {1}) = {2}")
  @MethodSource("data")
  void mulOperation(final String a, final String b, final String expectedResult) {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString(b)) // pushed first → deepest (top-2)
            .pushStackItem(Bytes32.fromHexString(a)) // pushed last → top (top-1)
            .build();
    assertThat(frame.stackTopV2()).isEqualTo(2);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    // MUL consumes 2 items and produces 1: net stack change is -1.
    assertThat(frame.stackTopV2()).isEqualTo(1);

    final UInt256 expected =
        UInt256.fromBytesBE(Bytes32.fromHexString(expectedResult).toArrayUnsafe());
    assertThat(getV2StackItem(frame, 0)).isEqualTo(expected);
  }

  @Test
  void mulOperationGasCostIsLowTier() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString("0x01"))
            .pushStackItem(Bytes32.fromHexString("0x02"))
            .build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(gasCalculator.getLowTierGasCost());
  }
}
