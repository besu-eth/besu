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

class NotOperationV2Test extends UnaryOperationV2Test {
  private final GasCalculator gasCalculator = new FrontierGasCalculator();

  public NotOperationV2Test() {
    super(new NotOperationV2(new FrontierGasCalculator()));
  }

  /**
   * Structural test data for NOT(a) = expected. Bitwise correctness is covered by
   * UInt256PropertyBasedTest; these cases verify stack arity and limb-level read/write wiring.
   */
  static Iterable<Arguments> data() {
    return List.of(
        // (a, expected)
        // Zero becomes all ones: every result limb must be written.
        Arguments.of("0x00", "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        // All ones becomes zero: every result limb must be written as zero.
        Arguments.of("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "0x00"),
        // All 4 limbs populated: four distinct limb values catch any limb-index/offset mistakes.
        Arguments.of(
            "0x0123456789abcdeffedcba98765432100f0f0f0f0f0f0f0ff0f0f0f0f0f0f0f0",
            "0xfedcba98765432100123456789abcdeff0f0f0f0f0f0f0f00f0f0f0f0f0f0f0f"));
  }

  @ParameterizedTest(name = "{index}: not({0}) = {1}")
  @MethodSource("data")
  void notOperation(final String a, final String expectedResult) {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().pushStackItem(Bytes32.fromHexString(a)).build();
    assertThat(frame.stackTopV2()).isEqualTo(1);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    // NOT consumes 1 item and produces 1: the stack size is unchanged.
    assertThat(frame.stackTopV2()).isEqualTo(1);

    final UInt256 expected =
        UInt256.fromBytesBE(Bytes32.fromHexString(expectedResult).toArrayUnsafe());
    assertThat(getV2StackItem(frame, 0)).isEqualTo(expected);
  }

  @Test
  void gasCostIsVeryLowTier() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().pushStackItem(Bytes32.fromHexString("0x01")).build();

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(gasCalculator.getVeryLowTierGasCost());
  }
}
