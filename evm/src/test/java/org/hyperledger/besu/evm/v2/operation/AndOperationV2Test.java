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

class AndOperationV2Test extends BinaryOperationV2Test {
  private final GasCalculator gasCalculator = new FrontierGasCalculator();

  public AndOperationV2Test() {
    super(new AndOperationV2(new FrontierGasCalculator()));
  }

  /**
   * Structural test data for AND(a, b) = expected. Bitwise correctness is covered by
   * UInt256PropertyBasedTest; these cases verify stack arity and limb-level read/write wiring.
   *
   * <p>Push order when building the frame: b first (deepest), then a (top).
   */
  static Iterable<Arguments> data() {
    return List.of(
        // (a, b, expected)
        // Happy path: low-limb only.
        Arguments.of("0x0f", "0xfc", "0x0c"),
        // Zero absorbing element: every result limb must be written as zero.
        Arguments.of(
            "0x00", "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "0x00"),
        // All-ones identity: confirms b is read from the deeper slot.
        Arguments.of(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "0x03", "0x03"),
        // All 4 limbs populated on both inputs and the result: four distinct limb values catch any
        // limb-index/offset mistakes.
        Arguments.of(
            "0x0123456789abcdeffedcba98765432100f0f0f0f0f0f0f0ff0f0f0f0f0f0f0f0",
            "0xffffffff0000000000000000ffffffffff00ff00ff00ff0000ff00ff00ff00ff",
            "0x012345670000000000000000765432100f000f000f000f0000f000f000f000f0"));
  }

  @ParameterizedTest(name = "{index}: and({0}, {1}) = {2}")
  @MethodSource("data")
  void andOperation(final String a, final String b, final String expectedResult) {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString(b)) // pushed first → deepest (top-2)
            .pushStackItem(Bytes32.fromHexString(a)) // pushed last → top (top-1)
            .build();
    assertThat(frame.stackTopV2()).isEqualTo(2);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    // AND consumes 2 items and produces 1: net stack change is -1.
    assertThat(frame.stackTopV2()).isEqualTo(1);

    final UInt256 expected =
        UInt256.fromBytesBE(Bytes32.fromHexString(expectedResult).toArrayUnsafe());
    assertThat(getV2StackItem(frame, 0)).isEqualTo(expected);
  }

  @Test
  void gasCostIsVeryLowTier() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString("0x01"))
            .pushStackItem(Bytes32.fromHexString("0x02"))
            .build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(gasCalculator.getVeryLowTierGasCost());
  }
}
