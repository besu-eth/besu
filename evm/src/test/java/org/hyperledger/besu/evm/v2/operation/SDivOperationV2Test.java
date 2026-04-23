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
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.util.List;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SDivOperationV2Test {
  private final SDivOperationV2 operation = new SDivOperationV2(mock(GasCalculator.class));

  static Iterable<Arguments> data() {
    return List.of(
        Arguments.of("0x00", "0x00", "0x00"),
        Arguments.of("0x01", "0x00", "0x00"),
        Arguments.of("0x00", "0x01", "0x00"),
        Arguments.of("0x02", "0x0a", "0x00"),
        Arguments.of(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "0x00", "0x00"),
        Arguments.of("0x0a", "0x02", "0x05"),
        Arguments.of(
            "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff6",
            "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe",
            "0x05"));
  }

  @ParameterizedTest(name = "sdiv({0}, {1}) = {2}")
  @MethodSource("data")
  void stackUpdatedCorrectly(final String a, final String b, final String expectedResult) {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString(b))
            .pushStackItem(Bytes32.fromHexString(a))
            .build();
    assertThat(frame.stackTopV2()).isEqualTo(2);

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isEqualTo(1);

    final UInt256 expected =
        UInt256.fromBytesBE(Bytes32.fromHexString(expectedResult).toArrayUnsafe());
    assertThat(getV2StackItem(frame, 0)).isEqualTo(expected);
  }

  private static Stream<List<String>> operandStacks() {
    return Stream.of(List.of(), List.of("0x01"));
  }

  @ParameterizedTest(name = "stack {0}")
  @MethodSource("operandStacks")
  void stackUnderflowNoItems(final List<String> stackItems) {
    final TestMessageFrameBuilderV2 frameBuilder = new TestMessageFrameBuilderV2();
    for (String stackItem : stackItems) {
      frameBuilder.pushStackItem(Bytes32.fromHexString(stackItem));
    }
    final MessageFrame frame = frameBuilder.build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  @Test
  void preservesDeeperStackItems() {
    final Bytes32 untouched =
        Bytes32.fromHexString("0xdeadbeefdeadbeefcafebabecafebabe0123456789abcdeff1e2d3c4b5a69788");
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(untouched)
            .pushStackItem(Bytes32.fromHexString("0x02"))
            .pushStackItem(Bytes32.fromHexString("0x0a"))
            .build();
    assertThat(frame.stackTopV2()).isEqualTo(3);

    operation.execute(frame, null);

    assertThat(frame.stackTopV2()).isEqualTo(2);
    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.fromInt(5));
    assertThat(getV2StackItem(frame, 1)).isEqualTo(UInt256.fromBytesBE(untouched.toArrayUnsafe()));
  }

  @Test
  void gasCostIsVeryLowTier() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(Bytes32.fromHexString("0x01"))
            .pushStackItem(Bytes32.fromHexString("0x02"))
            .build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getGasCost()).isEqualTo(5);
  }
}
