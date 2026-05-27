/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.evm.operation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.SpuriousDragonGasCalculator;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SarOperationTest {

  private final GasCalculator gasCalculator = new SpuriousDragonGasCalculator();
  private final SarOperation operation = new SarOperation(gasCalculator);

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
            "0x100",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Arguments.of(
            "0x8000000000000000000000000000000000000000000000000000000000000000",
            "0x101",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Arguments.of(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0x0",
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
            "0x100",
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
            "0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "0x100", "0x"),
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
            "0x"),
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
    final MessageFrame frame = mock(MessageFrame.class);
    when(frame.stackSize()).thenReturn(2);
    when(frame.getRemainingGas()).thenReturn(100L);
    when(frame.popStackItem())
        .thenReturn(Bytes32.fromHexStringLenient(shift))
        .thenReturn(Bytes.fromHexString(number));
    operation.execute(frame, null);
    verify(frame).pushStackItem(Bytes.fromHexString(expectedResult));
  }
}
