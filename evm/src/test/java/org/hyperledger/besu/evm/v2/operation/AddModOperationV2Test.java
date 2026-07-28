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
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.util.List;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class AddModOperationV2Test extends TernaryOperationV2Test {

  public AddModOperationV2Test() {
    super(new AddModOperationV2(new FrontierGasCalculator()));
  }

  /**
   * Test data for addmod(a, b, m) = expected.
   *
   * <p>Push order when building the frame: m first (deepest), then b, then a (top).
   */
  static Iterable<Arguments> data() {
    return List.of(
        // one limb
        Arguments.of("0x03", "0x03", "0x05", "0x01"),
        // two limbs
        Arguments.of(
            "0x030000000000000000",
            "0x030000000000000000",
            "0x050000000000000000",
            "0x010000000000000000"),
        // three limbs
        Arguments.of(
            "0x0300000000000000000000000000000000",
            "0x0300000000000000000000000000000000",
            "0x0500000000000000000000000000000000",
            "0x0100000000000000000000000000000000"),
        // four limbs
        Arguments.of(
            "0x03000000000000000000000000000000000000000000000000",
            "0x03000000000000000000000000000000000000000000000000",
            "0x05000000000000000000000000000000000000000000000000",
            "0x01000000000000000000000000000000000000000000000000"));
  }

  @ParameterizedTest(name = "{index}: addmod({0}, {1}, {2}) = {3}")
  @MethodSource("data")
  void addModOperation(
      final String a, final String b, final String m, final String expectedResult) {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString(m))
            .pushStackItem(Bytes32.fromHexString(b))
            .pushStackItem(Bytes32.fromHexString(a))
            .build();
    assertThat(frame.stackTopV2()).isEqualTo(3);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isEqualTo(1);

    final UInt256 expected =
        UInt256.fromBytesBE(Bytes32.fromHexString(expectedResult).toArrayUnsafe());
    assertThat(getV2StackItem(frame, 0)).isEqualTo(expected);
  }
}
