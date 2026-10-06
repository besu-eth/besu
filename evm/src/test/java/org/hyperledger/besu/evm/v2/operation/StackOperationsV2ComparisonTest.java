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

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.OverflowException;
import org.hyperledger.besu.evm.internal.UnderflowException;
import org.hyperledger.besu.evm.operation.DupNOperation;
import org.hyperledger.besu.evm.operation.DupOperation;
import org.hyperledger.besu.evm.operation.Eip8024Decoder;
import org.hyperledger.besu.evm.operation.ExchangeOperation;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.SwapNOperation;
import org.hyperledger.besu.evm.operation.SwapOperation;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Compares the v2 stack manipulation operations with the original Bytes-based implementations on
 * the same stacks: every DUP and SWAP index, and every immediate of DUPN, SWAPN and EXCHANGE, at
 * the stack depths around underflow and overflow.
 */
class StackOperationsV2ComparisonTest {

  private static final int MAX = MessageFrame.DEFAULT_MAX_STACK_SIZE;

  // region Cases

  static Stream<Arguments> dupCases() {
    return IntStream.rangeClosed(1, 16)
        .boxed()
        .flatMap(n -> depths(0, n - 1, n, n + 1, MAX - 1, MAX).map(d -> Arguments.of(n, d)));
  }

  static Stream<Arguments> swapCases() {
    return IntStream.rangeClosed(1, 16)
        .boxed()
        .flatMap(n -> depths(0, n, n + 1, n + 2, MAX).map(d -> Arguments.of(n, d)));
  }

  static Stream<Arguments> dupNCases() {
    return IntStream.range(0, 256)
        .boxed()
        .flatMap(
            imm -> {
              if (!Eip8024Decoder.VALID_SINGLE[imm]) return depths(0, MAX).map(d -> code(imm, d));
              final int n = Eip8024Decoder.DECODE_SINGLE[imm];
              return depths(n - 1, n, MAX - 1, MAX).map(d -> code(imm, d));
            });
  }

  static Stream<Arguments> swapNCases() {
    return IntStream.range(0, 256)
        .boxed()
        .flatMap(
            imm -> {
              if (!Eip8024Decoder.VALID_SINGLE[imm]) return depths(0, MAX).map(d -> code(imm, d));
              final int n = Eip8024Decoder.DECODE_SINGLE[imm];
              return depths(n, n + 1, MAX).map(d -> code(imm, d));
            });
  }

  static Stream<Arguments> exchangeCases() {
    return IntStream.range(0, 256)
        .boxed()
        .flatMap(
            imm -> {
              final int packed = Eip8024Decoder.DECODE_PAIR_PACKED[imm];
              if (packed == Eip8024Decoder.INVALID_PAIR) {
                return depths(0, MAX).map(d -> code(imm, d));
              }
              final int deepest = Math.max(packed & 0xFF, (packed >>> 8) & 0xFF);
              return depths(deepest, deepest + 1, MAX).map(d -> code(imm, d));
            });
  }

  /**
   * The immediate past the end of the code reads as 0, which is a valid immediate for all three.
   */
  static Stream<Arguments> endOfCodeCases() {
    return depths(0, 16, 17, 18, 30, MAX).map(d -> Arguments.of(d));
  }

  // endregion

  // region Tests

  @ParameterizedTest(name = "DUP{0} at depth {1}")
  @MethodSource("dupCases")
  void dupMatchesOriginal(final int n, final int depth) {
    assertMatches(
        depth,
        frame -> DupOperation.staticOperation(frame, n),
        frame -> DupOperationV2.staticOperation(frame, n));
  }

  @ParameterizedTest(name = "SWAP{0} at depth {1}")
  @MethodSource("swapCases")
  void swapMatchesOriginal(final int n, final int depth) {
    assertMatches(
        depth,
        frame -> SwapOperation.staticOperation(frame, n),
        frame -> SwapOperationV2.staticOperation(frame, n));
  }

  @ParameterizedTest(name = "DUPN {0} at depth {1}")
  @MethodSource("dupNCases")
  void dupNMatchesOriginal(final int imm, final int depth) {
    final byte[] code = {(byte) DupNOperationV2.OPCODE, (byte) imm};
    assertMatches(
        depth,
        code,
        frame -> DupNOperation.staticOperation(frame, code, 0),
        frame -> DupNOperationV2.staticOperation(frame, code, 0));
  }

  @ParameterizedTest(name = "SWAPN {0} at depth {1}")
  @MethodSource("swapNCases")
  void swapNMatchesOriginal(final int imm, final int depth) {
    final byte[] code = {(byte) SwapNOperationV2.OPCODE, (byte) imm};
    assertMatches(
        depth,
        code,
        frame -> SwapNOperation.staticOperation(frame, code, 0),
        frame -> SwapNOperationV2.staticOperation(frame, code, 0));
  }

  @ParameterizedTest(name = "EXCHANGE {0} at depth {1}")
  @MethodSource("exchangeCases")
  void exchangeMatchesOriginal(final int imm, final int depth) {
    final byte[] code = {(byte) ExchangeOperationV2.OPCODE, (byte) imm};
    assertMatches(
        depth,
        code,
        frame -> ExchangeOperation.staticOperation(frame, code, 0),
        frame -> ExchangeOperationV2.staticOperation(frame, code, 0));
  }

  @ParameterizedTest(name = "at depth {0}")
  @MethodSource("endOfCodeCases")
  void immediateOperationsMatchOriginalAtEndOfCode(final int depth) {
    final byte[] dupN = {(byte) DupNOperationV2.OPCODE};
    assertMatches(
        depth,
        dupN,
        frame -> DupNOperation.staticOperation(frame, dupN, 0),
        frame -> DupNOperationV2.staticOperation(frame, dupN, 0));
    final byte[] swapN = {(byte) SwapNOperationV2.OPCODE};
    assertMatches(
        depth,
        swapN,
        frame -> SwapNOperation.staticOperation(frame, swapN, 0),
        frame -> SwapNOperationV2.staticOperation(frame, swapN, 0));
    final byte[] exchange = {(byte) ExchangeOperationV2.OPCODE};
    assertMatches(
        depth,
        exchange,
        frame -> ExchangeOperation.staticOperation(frame, exchange, 0),
        frame -> ExchangeOperationV2.staticOperation(frame, exchange, 0));
  }

  // endregion

  // region Helpers

  private static Stream<Integer> depths(final int... depths) {
    final TreeSet<Integer> valid = new TreeSet<>();
    for (final int depth : depths) {
      if (depth >= 0 && depth <= MAX) valid.add(depth);
    }
    return valid.stream();
  }

  private static Arguments code(final int imm, final int depth) {
    return Arguments.of(imm, depth);
  }

  /**
   * Runs both implementations on a stack of {@code depth} distinct items and compares the halt
   * reason, the gas and PC increment of a returned result, and the resulting stack.
   */
  private static void assertMatches(
      final int depth,
      final Function<MessageFrame, Operation.OperationResult> original,
      final Function<MessageFrame, Operation.OperationResult> v2) {
    final List<Bytes32> items = items(depth);

    final TestMessageFrameBuilder v1Builder = new TestMessageFrameBuilder();
    final TestMessageFrameBuilderV2 v2Builder = new TestMessageFrameBuilderV2();
    items.forEach(
        item -> {
          v1Builder.pushStackItem(item);
          v2Builder.pushStackItem(item);
        });
    final MessageFrame v1Frame = v1Builder.build();
    final MessageFrame v2Frame = v2Builder.build();

    Operation.OperationResult v1Result = null;
    ExceptionalHaltReason v1Halt;
    try {
      v1Result = original.apply(v1Frame);
      v1Halt = v1Result.getHaltReason();
    } catch (final UnderflowException e) {
      v1Halt = ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS;
    } catch (final OverflowException e) {
      v1Halt = ExceptionalHaltReason.TOO_MANY_STACK_ITEMS;
    }
    final Operation.OperationResult v2Result = v2.apply(v2Frame);

    assertThat(v2Result.getHaltReason()).isEqualTo(v1Halt);
    if (v1Result != null) {
      assertThat(v2Result.getGasCost()).isEqualTo(v1Result.getGasCost());
      assertThat(v2Result.getPcIncrement()).isEqualTo(v1Result.getPcIncrement());
    } else {
      // The original signals stack errors by exception, which the v1 loop answers with 0 gas.
      assertThat(v2Result.getGasCost()).isZero();
    }
    assertThat(v2Stack(v2Frame)).isEqualTo(v1Stack(v1Frame));
  }

  /** Random items, so any misplaced item or limb is detected. */
  private static List<Bytes32> items(final int depth) {
    final Random random = new Random(depth);
    final List<Bytes32> items = new ArrayList<>(depth);
    for (int i = 0; i < depth; i++) {
      final byte[] bytes = new byte[32];
      random.nextBytes(bytes);
      items.add(Bytes32.wrap(bytes));
    }
    return items;
  }

  /** The stack from top to bottom. */
  private static List<Bytes32> v1Stack(final MessageFrame frame) {
    final List<Bytes32> stack = new ArrayList<>(frame.stackSize());
    for (int i = 0; i < frame.stackSize(); i++) {
      stack.add(Bytes32.leftPad(frame.getStackItem(i)));
    }
    return stack;
  }

  /** The stack from top to bottom. */
  private static List<Bytes32> v2Stack(final MessageFrame frame) {
    final List<Bytes32> stack = new ArrayList<>(frame.stackTopV2());
    for (int i = 0; i < frame.stackTopV2(); i++) {
      stack.add(Bytes32.wrap(getV2StackItem(frame, i).toBytesBE()));
    }
    return stack;
  }

  // endregion
}
