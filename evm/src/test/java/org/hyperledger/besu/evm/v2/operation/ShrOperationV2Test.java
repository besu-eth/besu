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

import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.SpuriousDragonGasCalculator;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.util.Arrays;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ShrOperationV2Test {

  private final GasCalculator gasCalculator = new SpuriousDragonGasCalculator();
  private final ShrOperationV2 operation = new ShrOperationV2(gasCalculator);

  static Iterable<Arguments> data() {
    return Arrays.asList(
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000001",
            "0x00",
            "0x0000000000000000000000000000000000000000000000000000000000000001"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000001",
            "0x01",
            "0x0000000000000000000000000000000000000000000000000000000000000000"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000002",
            "0x01",
            "0x0000000000000000000000000000000000000000000000000000000000000001"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000004",
            "0x01",
            "0x0000000000000000000000000000000000000000000000000000000000000002"),
        Arguments.of(
            "0x000000000000000000000000000000000000000000000000000000000000000f",
            "0x01",
            "0x0000000000000000000000000000000000000000000000000000000000000007"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000008",
            "0x01",
            "0x0000000000000000000000000000000000000000000000000000000000000004"),
        Arguments.of(
            "0x8000000000000000000000000000000000000000000000000000000000000000",
            "0xff",
            "0x0000000000000000000000000000000000000000000000000000000000000001"),
        Arguments.of(
            "0x8000000000000000000000000000000000000000000000000000000000000000", "0x100", "0x"),
        Arguments.of(
            "0x8000000000000000000000000000000000000000000000000000000000000000", "0x101", "0x"),
        Arguments.of(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0x0",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Arguments.of(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0x01",
            "0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Arguments.of(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0xff",
            "0x0000000000000000000000000000000000000000000000000000000000000001"),
        Arguments.of(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "0x100", "0x"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            "0x01",
            "0x0000000000000000000000000000000000000000000000000000000000000000"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000400",
            "0x80",
            "0x0000000000000000000000000000000000000000000000000000000000000000"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000400", "0x8000", "0x"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000400",
            "0x80000000",
            "0x"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000400",
            "0x8000000000000000",
            "0x"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000400",
            "0x80000000000000000000000000000000",
            "0x"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000400",
            "0x8000000000000000000000000000000000000000000000000000000000000000",
            "0x"));
  }

  @ParameterizedTest
  @MethodSource("data")
  void shiftOperation(final String number, final String shift, final String expectedResult) {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexStringLenient(number))
            .pushStackItem(Bytes32.fromHexStringLenient(shift))
            .build();
    operation.execute(frame, null);
    UInt256 expected;
    if (expectedResult.equals("0x") || expectedResult.equals("0x0")) {
      expected = UInt256.ZERO;
    } else {
      expected = UInt256.fromBytesBE(Bytes32.fromHexStringLenient(expectedResult).toArrayUnsafe());
    }
    assertThat(getV2StackItem(frame, 0)).isEqualTo(expected);
  }

  private static UInt256 getV2StackItem(final MessageFrame frame, final int offset) {
    final long[] s = frame.stackDataV2();
    final int idx = (frame.stackTopV2() - 1 - offset) << 2;
    return new UInt256(s[idx], s[idx + 1], s[idx + 2], s[idx + 3]);
  }
}
