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

class ByteOperationV2Test extends BinaryOperationV2Test {
  private final GasCalculator gasCalculator = new FrontierGasCalculator();

  /** Byte i of this value is 0xa0 + i, so a result can't be mistaken for its index. */
  private static final String SEQUENCE =
      "0xa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf";

  public ByteOperationV2Test() {
    super(new ByteOperationV2(new FrontierGasCalculator()));
  }

  /**
   * Test data for BYTE(index, value) = expected.
   *
   * <p>Push order when building the frame: value first (deepest), then index (top).
   */
  static Iterable<Arguments> data() {
    return List.of(
        // (index, value, expected)
        // First and last byte of each limb: catches limb selection and in-limb shift mistakes.
        Arguments.of("0x00", SEQUENCE, "0xa0"),
        Arguments.of("0x07", SEQUENCE, "0xa7"),
        Arguments.of("0x08", SEQUENCE, "0xa8"),
        Arguments.of("0x0f", SEQUENCE, "0xaf"),
        Arguments.of("0x10", SEQUENCE, "0xb0"),
        Arguments.of("0x17", SEQUENCE, "0xb7"),
        Arguments.of("0x18", SEQUENCE, "0xb8"),
        Arguments.of("0x1f", SEQUENCE, "0xbf"),
        // The result is the byte alone, not sign-extended.
        Arguments.of(
            "0x00", "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "0xff"),
        // Index 32 and above is out of range.
        Arguments.of("0x20", SEQUENCE, "0x00"),
        Arguments.of("0xffffffffffffffff", SEQUENCE, "0x00"),
        // An index whose low 32 bits alone would be in range.
        Arguments.of("0x0100000003", SEQUENCE, "0x00"),
        // A large index whose low limb alone would be in range.
        Arguments.of("0x010000000000000003", SEQUENCE, "0x00"),
        Arguments.of(
            "0x0100000000000000000000000000000000000000000000000000000000000003", SEQUENCE, "0x00"),
        Arguments.of(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            SEQUENCE,
            "0x00"));
  }

  @ParameterizedTest(name = "{index}: byte({0}, {1}) = {2}")
  @MethodSource("data")
  void byteOperation(final String index, final String value, final String expectedResult) {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString(value)) // pushed first → deepest (top-2)
            .pushStackItem(Bytes32.fromHexString(index)) // pushed last → top (top-1)
            .build();
    assertThat(frame.stackTopV2()).isEqualTo(2);

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    // BYTE consumes 2 items and produces 1: net stack change is -1.
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
