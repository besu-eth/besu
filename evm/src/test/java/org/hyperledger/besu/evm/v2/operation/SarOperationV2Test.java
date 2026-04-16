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

import java.util.List;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SarOperationV2Test {

  private final GasCalculator gasCalculator = new SpuriousDragonGasCalculator();
  private final SarOperationV2 operation = new SarOperationV2(gasCalculator);

  static Iterable<Arguments> data() {
    return List.of(
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
            "0x01",
            "0xc000000000000000000000000000000000000000000000000000000000000000"),
        Arguments.of(
            "0x8000000000000000000000000000000000000000000000000000000000000000",
            "0xff",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Arguments.of(
            "0x8000000000000000000000000000000000000000000000000000000000000000",
            "0x0100",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Arguments.of(
            "0x8000000000000000000000000000000000000000000000000000000000000000",
            "0x0101",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Arguments.of(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0x00",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Arguments.of(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0x01",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Arguments.of(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0xff",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Arguments.of(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0x0100",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            "0x01",
            "0x0000000000000000000000000000000000000000000000000000000000000000"),
        Arguments.of(
            "0x4000000000000000000000000000000000000000000000000000000000000000",
            "0xfe",
            "0x0000000000000000000000000000000000000000000000000000000000000001"),
        Arguments.of(
            "0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0xf8",
            "0x000000000000000000000000000000000000000000000000000000000000007f"),
        Arguments.of(
            "0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0xfe",
            "0x0000000000000000000000000000000000000000000000000000000000000001"),
        Arguments.of(
            "0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0xff",
            "0x0000000000000000000000000000000000000000000000000000000000000000"),
        Arguments.of(
            "0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "0x0100", "0x00"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000400",
            "0x80",
            "0x0000000000000000000000000000000000000000000000000000000000000000"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000400", "0x8000", "0x"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000400",
            "0x80000000",
            "0x00"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000400",
            "0x8000000000000000",
            "0x00"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000400",
            "0x80000000000000000000000000000000",
            "0x00"),
        Arguments.of(
            "0x0000000000000000000000000000000000000000000000000000000000000400",
            "0x8000000000000000000000000000000000000000000000000000000000000000",
            "0x00"),
        Arguments.of(
            "0x8000000000000000000000000000000000000000000000000000000000000400",
            "0x80",
            "0xffffffffffffffffffffffffffffffff80000000000000000000000000000000"),
        Arguments.of(
            "0x8000000000000000000000000000000000000000000000000000000000000400",
            "0x8000",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Arguments.of(
            "0x8000000000000000000000000000000000000000000000000000000000000400",
            "0x80000000",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Arguments.of(
            "0x8000000000000000000000000000000000000000000000000000000000000400",
            "0x8000000000000000",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Arguments.of(
            "0x8000000000000000000000000000000000000000000000000000000000000400",
            "0x80000000000000000000000000000000",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Arguments.of(
            "0x8000000000000000000000000000000000000000000000000000000000000400",
            "0x8000000000000000000000000000000000000000000000000000000000000000",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"));
  }

  @ParameterizedTest(name = "{index}: {0}, {1}, {2}")
  @MethodSource("data")
  void shiftOperation(final String number, final String shift, final String expectedResult) {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString(number))
            .pushStackItem(Bytes32.fromHexString(shift))
            .build();
    operation.execute(frame, null);
    UInt256 expected = UInt256.fromBytesBE(Bytes32.fromHexString(expectedResult).toArrayUnsafe());
    assertThat(getStackItem(frame, 0)).isEqualTo(expected);
    assertThat(frame.stackTopV2()).isEqualTo(1);
  }

  private static UInt256 getStackItem(final MessageFrame frame, final int offset) {
    final long[] s = frame.stackDataV2();
    final int idx = (frame.stackTopV2() - 1 - offset) << 2;
    return new UInt256(s[idx], s[idx + 1], s[idx + 2], s[idx + 3]);
  }

  @Test
  void stackTopUpdated() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString("0x01"))
            .pushStackItem(Bytes32.fromHexString("0x02"))
            .pushStackItem(Bytes32.fromHexString("0x03"))
            .pushStackItem(
                Bytes32.fromHexString(
                    "0x8000000000000000000000000000000000000000000000000000000000000400"))
            .pushStackItem(
                Bytes32.fromHexString(
                    "0x8000000000000000000000000000000000000000000000000000000000000000"))
            .build();
    operation.execute(frame, null);
    assertThat(frame.stackTopV2()).isEqualTo(4);
    assertThat(getStackItem(frame, 0))
        .isEqualTo(
            UInt256.fromBytesBE(
                Bytes32.fromHexString(
                        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                    .toArrayUnsafe()));
  }
}
