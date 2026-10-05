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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.AndOperation;
import org.hyperledger.besu.evm.operation.ByteOperation;
import org.hyperledger.besu.evm.operation.CountLeadingZerosOperation;
import org.hyperledger.besu.evm.operation.NotOperation;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.OrOperation;
import org.hyperledger.besu.evm.operation.XorOperation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Function;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Property-based tests comparing the v2 bitwise operations against the original Bytes-based
 * implementations.
 */
public class BitwiseOperationsV2PropertyBasedTest {

  // region Arbitrary Providers

  @Provide
  Arbitrary<byte[]> values1to32() {
    return Arbitraries.bytes().array(byte[].class).ofMinSize(1).ofMaxSize(32);
  }

  @Provide
  Arbitrary<byte[]> byteIndices() {
    return Arbitraries.oneOf(
        Arbitraries.integers().between(0, 40).map(i -> new byte[] {i.byteValue()}),
        Arbitraries.bytes().array(byte[].class).ofMinSize(1).ofMaxSize(32));
  }

  /** Values whose most significant one bit is at any of the 256 positions, or zero. */
  @Provide
  Arbitrary<byte[]> leadingZeroValues() {
    return Arbitraries.integers()
        .between(0, 256)
        .flatMap(
            zeros ->
                Arbitraries.bytes()
                    .array(byte[].class)
                    .ofSize(32)
                    .map(bytes -> withLeadingZeros(bytes, zeros)));
  }

  // endregion

  // region Property Tests

  @Property(tries = 10000)
  void property_andV2_matchesOriginal(
      @ForAll("values1to32") final byte[] a, @ForAll("values1to32") final byte[] b) {
    assertBinaryMatches(AndOperation::staticOperation, AndOperationV2::staticOperation, a, b);
  }

  @Property(tries = 10000)
  void property_orV2_matchesOriginal(
      @ForAll("values1to32") final byte[] a, @ForAll("values1to32") final byte[] b) {
    assertBinaryMatches(OrOperation::staticOperation, OrOperationV2::staticOperation, a, b);
  }

  @Property(tries = 10000)
  void property_xorV2_matchesOriginal(
      @ForAll("values1to32") final byte[] a, @ForAll("values1to32") final byte[] b) {
    assertBinaryMatches(XorOperation::staticOperation, XorOperationV2::staticOperation, a, b);
  }

  @Property(tries = 10000)
  void property_notV2_matchesOriginal(@ForAll("values1to32") final byte[] a) {
    assertUnaryMatches(NotOperation::staticOperation, NotOperationV2::staticOperation, a);
  }

  @Property(tries = 10000)
  void property_byteV2_matchesOriginal(
      @ForAll("byteIndices") final byte[] index, @ForAll("values1to32") final byte[] value) {
    assertBinaryMatches(
        ByteOperation::staticOperation, ByteOperationV2::staticOperation, index, value);
  }

  @Property(tries = 10000)
  void property_clzV2_matchesOriginal(@ForAll("leadingZeroValues") final byte[] a) {
    assertUnaryMatches(
        CountLeadingZerosOperation::staticOperation,
        CountLeadingZerosOperationV2::staticOperation,
        a);
  }

  // endregion

  // region Helpers

  private static void assertBinaryMatches(
      final Function<MessageFrame, Operation.OperationResult> original,
      final Function<MessageFrame, Operation.OperationResult> v2,
      final byte[] top,
      final byte[] second) {
    final Bytes a = Bytes.wrap(top);
    final Bytes b = Bytes.wrap(second);

    final Bytes32 originalResult = Bytes32.leftPad(runOriginal(original, a, b));
    final Bytes32 v2Result = runV2(v2, a, b);

    assertThat(v2Result)
        .as("mismatch for a=%s, b=%s", a.toHexString(), b.toHexString())
        .isEqualTo(originalResult);
  }

  private static void assertUnaryMatches(
      final Function<MessageFrame, Operation.OperationResult> original,
      final Function<MessageFrame, Operation.OperationResult> v2,
      final byte[] top) {
    final Bytes a = Bytes.wrap(top);

    final Bytes32 originalResult = Bytes32.leftPad(runOriginal(original, a));
    final Bytes32 v2Result = runV2(v2, a);

    assertThat(v2Result).as("mismatch for a=%s", a.toHexString()).isEqualTo(originalResult);
  }

  /** Runs a v1 operation on a mocked Bytes stack; the first item is the top of the stack. */
  private static Bytes runOriginal(
      final Function<MessageFrame, Operation.OperationResult> operation, final Bytes... items) {
    final MessageFrame frame = mock(MessageFrame.class);
    final Deque<Bytes> stack = new ArrayDeque<>();
    for (int i = items.length - 1; i >= 0; i--) {
      stack.push(items[i]);
    }

    when(frame.popStackItem()).thenAnswer(invocation -> stack.pop());

    final Bytes[] result = new Bytes[1];
    doAnswer(
            invocation -> {
              result[0] = invocation.getArgument(0);
              return null;
            })
        .when(frame)
        .pushStackItem(any(Bytes.class));

    assertThat(operation.apply(frame).getHaltReason()).isNull();
    return result[0];
  }

  /** Runs a v2 operation on a long[] stack; the first item is the top of the stack. */
  private static Bytes32 runV2(
      final Function<MessageFrame, Operation.OperationResult> operation, final Bytes... items) {
    final TestMessageFrameBuilderV2 builder = new TestMessageFrameBuilderV2();
    for (int i = items.length - 1; i >= 0; i--) {
      builder.pushStackItem(Bytes32.leftPad(items[i]));
    }
    final MessageFrame frame = builder.build();

    assertThat(operation.apply(frame).getHaltReason()).isNull();

    assertThat(frame.stackTopV2()).isEqualTo(1);
    return Bytes32.wrap(getV2StackItem(frame, 0).toBytesBE());
  }

  private static byte[] withLeadingZeros(final byte[] bytes, final int zeros) {
    for (int bit = 0; bit < zeros; bit++) {
      bytes[bit >>> 3] &= (byte) ~(0x80 >>> (bit & 7));
    }
    if (zeros < 256) {
      bytes[zeros >>> 3] |= (byte) (0x80 >>> (zeros & 7));
    }
    return bytes;
  }

  // endregion
}
