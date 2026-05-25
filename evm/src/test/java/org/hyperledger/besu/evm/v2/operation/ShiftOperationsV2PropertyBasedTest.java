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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.SarOperation;
import org.hyperledger.besu.evm.operation.ShlOperation;
import org.hyperledger.besu.evm.operation.ShrOperation;

import java.util.ArrayDeque;
import java.util.Deque;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Property-based tests comparing v2 StackArithmetic shift operations against the original
 * non-optimized Bytes-based implementations.
 */
public class ShiftOperationsV2PropertyBasedTest {

  // region Arbitrary Providers

  @Provide
  Arbitrary<byte[]> values1to32() {
    return Arbitraries.bytes().array(byte[].class).ofMinSize(1).ofMaxSize(32);
  }

  @Provide
  Arbitrary<byte[]> shiftAmounts() {
    return Arbitraries.bytes().array(byte[].class).ofMinSize(0).ofMaxSize(32);
  }

  @Provide
  Arbitrary<Integer> smallShifts() {
    return Arbitraries.integers().between(0, 255);
  }

  @Provide
  Arbitrary<Integer> overflowShifts() {
    return Arbitraries.integers().between(256, 1024);
  }

  @Provide
  Arbitrary<byte[]> negativeValues() {
    return Arbitraries.bytes()
        .array(byte[].class)
        .ofSize(32)
        .map(
            bytes -> {
              bytes[0] = (byte) (bytes[0] | 0x80);
              return bytes;
            });
  }

  @Provide
  Arbitrary<byte[]> positiveValues() {
    return Arbitraries.bytes()
        .array(byte[].class)
        .ofMinSize(1)
        .ofMaxSize(32)
        .map(
            bytes -> {
              bytes[0] = (byte) (bytes[0] & 0x7F);
              return bytes;
            });
  }

  // endregion

  // region SHL Property Tests

  @Property(tries = 10000)
  void property_shlV2_matchesOriginal_randomInputs(
      @ForAll("values1to32") final byte[] valueBytes,
      @ForAll("shiftAmounts") final byte[] shiftBytes) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shift = Bytes.wrap(shiftBytes);

    final Bytes32 originalResult = Bytes32.leftPad(runOriginalShl(shift, value));
    final Bytes32 v2Result = runV2Shl(shift, value);

    assertThat(v2Result)
        .as(
            "SHL v2 mismatch for shift=%s, value=%s",
            shift.toHexString(), Bytes32.leftPad(value).toHexString())
        .isEqualTo(originalResult);
  }

  @Property(tries = 5000)
  void property_shlV2_matchesOriginal_smallShifts(
      @ForAll("values1to32") final byte[] valueBytes, @ForAll("smallShifts") final int shift) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shiftBytes = Bytes.of(shift);

    final Bytes32 originalResult = Bytes32.leftPad(runOriginalShl(shiftBytes, value));
    final Bytes32 v2Result = runV2Shl(shiftBytes, value);

    assertThat(v2Result).as("SHL v2 mismatch for shift=%d", shift).isEqualTo(originalResult);
  }

  @Property(tries = 1000)
  void property_shlV2_matchesOriginal_overflowShifts(
      @ForAll("values1to32") final byte[] valueBytes, @ForAll("overflowShifts") final int shift) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shiftBytes = intToMinimalBytes(shift);

    final Bytes32 originalResult = Bytes32.leftPad(runOriginalShl(shiftBytes, value));
    final Bytes32 v2Result = runV2Shl(shiftBytes, value);

    assertThat(v2Result)
        .as("SHL v2 overflow mismatch for shift=%d", shift)
        .isEqualTo(originalResult);
  }

  // endregion

  // region SHR Property Tests

  @Property(tries = 10000)
  void property_shrV2_matchesOriginal_randomInputs(
      @ForAll("values1to32") final byte[] valueBytes,
      @ForAll("shiftAmounts") final byte[] shiftBytes) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shift = Bytes.wrap(shiftBytes);

    final Bytes32 originalResult = Bytes32.leftPad(runOriginalShr(shift, value));
    final Bytes32 v2Result = runV2Shr(shift, value);

    assertThat(v2Result)
        .as(
            "SHR v2 mismatch for shift=%s, value=%s",
            shift.toHexString(), Bytes32.leftPad(value).toHexString())
        .isEqualTo(originalResult);
  }

  @Property(tries = 5000)
  void property_shrV2_matchesOriginal_smallShifts(
      @ForAll("values1to32") final byte[] valueBytes, @ForAll("smallShifts") final int shift) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shiftBytes = Bytes.of(shift);

    final Bytes32 originalResult = Bytes32.leftPad(runOriginalShr(shiftBytes, value));
    final Bytes32 v2Result = runV2Shr(shiftBytes, value);

    assertThat(v2Result).as("SHR v2 mismatch for shift=%d", shift).isEqualTo(originalResult);
  }

  @Property(tries = 1000)
  void property_shrV2_matchesOriginal_overflowShifts(
      @ForAll("values1to32") final byte[] valueBytes, @ForAll("overflowShifts") final int shift) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shiftBytes = intToMinimalBytes(shift);

    final Bytes32 originalResult = Bytes32.leftPad(runOriginalShr(shiftBytes, value));
    final Bytes32 v2Result = runV2Shr(shiftBytes, value);

    assertThat(v2Result)
        .as("SHR v2 overflow mismatch for shift=%d", shift)
        .isEqualTo(originalResult);
  }

  // endregion

  // region SAR Property Tests - Random Inputs

  @Property(tries = 10000)
  void property_sarV2_matchesOriginal_randomInputs(
      @ForAll("values1to32") final byte[] valueBytes,
      @ForAll("shiftAmounts") final byte[] shiftBytes) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shift = Bytes.wrap(shiftBytes);

    final Bytes32 originalResult = Bytes32.leftPad(runOriginalSar(shift, value));
    final Bytes32 v2Result = runV2Sar(shift, value);

    assertThat(v2Result)
        .as(
            "SAR v2 mismatch for shift=%s, value=%s",
            shift.toHexString(), Bytes32.leftPad(value).toHexString())
        .isEqualTo(originalResult);
  }

  @Property(tries = 5000)
  void property_sarV2_matchesOriginal_smallShifts(
      @ForAll("values1to32") final byte[] valueBytes, @ForAll("smallShifts") final int shift) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shiftBytes = Bytes.of(shift);

    final Bytes32 originalResult = Bytes32.leftPad(runOriginalSar(shiftBytes, value));
    final Bytes32 v2Result = runV2Sar(shiftBytes, value);

    assertThat(v2Result).as("SAR v2 mismatch for shift=%d", shift).isEqualTo(originalResult);
  }

  @Property(tries = 1000)
  void property_sarV2_matchesOriginal_overflowShifts(
      @ForAll("values1to32") final byte[] valueBytes, @ForAll("overflowShifts") final int shift) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shiftBytes = intToMinimalBytes(shift);

    final Bytes32 originalResult = Bytes32.leftPad(runOriginalSar(shiftBytes, value));
    final Bytes32 v2Result = runV2Sar(shiftBytes, value);

    assertThat(v2Result)
        .as("SAR v2 overflow mismatch for shift=%d", shift)
        .isEqualTo(originalResult);
  }

  // endregion

  // region SAR Property Tests - Negative Values (Sign Extension)

  @Property(tries = 5000)
  void property_sarV2_matchesOriginal_negativeValues_smallShifts(
      @ForAll("negativeValues") final byte[] valueBytes, @ForAll("smallShifts") final int shift) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shiftBytes = Bytes.of(shift);

    final Bytes32 originalResult = Bytes32.leftPad(runOriginalSar(shiftBytes, value));
    final Bytes32 v2Result = runV2Sar(shiftBytes, value);

    assertThat(v2Result)
        .as("SAR v2 negative mismatch for shift=%d, value=%s", shift, value.toHexString())
        .isEqualTo(originalResult);
  }

  @Property(tries = 1000)
  void property_sarV2_matchesOriginal_negativeValues_overflowShifts(
      @ForAll("negativeValues") final byte[] valueBytes,
      @ForAll("overflowShifts") final int shift) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shiftBytes = intToMinimalBytes(shift);

    final Bytes32 originalResult = Bytes32.leftPad(runOriginalSar(shiftBytes, value));
    final Bytes32 v2Result = runV2Sar(shiftBytes, value);

    assertThat(v2Result)
        .as("SAR v2 negative overflow mismatch for shift=%d", shift)
        .isEqualTo(originalResult);
  }

  // endregion

  // region SAR Property Tests - Negative/Positive Values at Shift 255

  @Property(tries = 3000)
  void property_sarV2_matchesOriginal_negativeValues_shift255(
      @ForAll("negativeValues") final byte[] valueBytes) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shift = Bytes.of(255);

    final Bytes32 originalResult = Bytes32.leftPad(runOriginalSar(shift, value));
    final Bytes32 v2Result = runV2Sar(shift, value);

    assertThat(v2Result)
        .as("SAR v2 negative shift=255 mismatch for value=%s", value.toHexString())
        .isEqualTo(originalResult);
  }

  @Property(tries = 3000)
  void property_sarV2_matchesOriginal_positiveValues_shift255(
      @ForAll("positiveValues") final byte[] valueBytes) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shift = Bytes.of(255);

    final Bytes32 originalResult = Bytes32.leftPad(runOriginalSar(shift, value));
    final Bytes32 v2Result = runV2Sar(shift, value);

    assertThat(v2Result)
        .as("SAR v2 positive shift=255 mismatch for value=%s", value.toHexString())
        .isEqualTo(originalResult);
  }

  // endregion

  // region SAR Property Tests - Positive Values

  @Property(tries = 5000)
  void property_sarV2_matchesOriginal_positiveValues_smallShifts(
      @ForAll("positiveValues") final byte[] valueBytes, @ForAll("smallShifts") final int shift) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shiftBytes = Bytes.of(shift);

    final Bytes32 originalResult = Bytes32.leftPad(runOriginalSar(shiftBytes, value));
    final Bytes32 v2Result = runV2Sar(shiftBytes, value);

    assertThat(v2Result)
        .as("SAR v2 positive mismatch for shift=%d, value=%s", shift, value.toHexString())
        .isEqualTo(originalResult);
  }

  @Property(tries = 1000)
  void property_sarV2_matchesOriginal_positiveValues_overflowShifts(
      @ForAll("positiveValues") final byte[] valueBytes,
      @ForAll("overflowShifts") final int shift) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shiftBytes = intToMinimalBytes(shift);

    final Bytes32 originalResult = Bytes32.leftPad(runOriginalSar(shiftBytes, value));
    final Bytes32 v2Result = runV2Sar(shiftBytes, value);

    assertThat(v2Result)
        .as("SAR v2 positive overflow mismatch for shift=%d", shift)
        .isEqualTo(originalResult);
  }

  // endregion

  // region Edge Case Tests - SHL / SHR

  @Property(tries = 1000)
  void property_shlV2_shiftByZero_returnsOriginalValue(
      @ForAll("values1to32") final byte[] valueBytes) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shift = Bytes.of(0);

    final Bytes32 v2Result = runV2Shl(shift, value);

    assertThat(v2Result).isEqualTo(Bytes32.leftPad(value));
  }

  @Property(tries = 1000)
  void property_shrV2_shiftByZero_returnsOriginalValue(
      @ForAll("values1to32") final byte[] valueBytes) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shift = Bytes.of(0);

    final Bytes32 v2Result = runV2Shr(shift, value);

    assertThat(v2Result).isEqualTo(Bytes32.leftPad(value));
  }

  @Property(tries = 500)
  void property_shlV2_largeShift_returnsZero(@ForAll("values1to32") final byte[] valueBytes) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes largeShift = Bytes.fromHexString("0x010000000000");

    final Bytes32 v2Result = runV2Shl(largeShift, value);

    assertThat(v2Result).isEqualTo(Bytes32.ZERO);
  }

  @Property(tries = 500)
  void property_shrV2_largeShift_returnsZero(@ForAll("values1to32") final byte[] valueBytes) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes largeShift = Bytes.fromHexString("0x010000000000");

    final Bytes32 v2Result = runV2Shr(largeShift, value);

    assertThat(v2Result).isEqualTo(Bytes32.ZERO);
  }

  // endregion

  // region Edge Case Tests - SAR

  @Property(tries = 1000)
  void property_sarV2_shiftByZero_returnsOriginalValue(
      @ForAll("values1to32") final byte[] valueBytes) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes shift = Bytes.of(0);

    final Bytes32 v2Result = runV2Sar(shift, value);

    assertThat(v2Result).isEqualTo(Bytes32.leftPad(value));
  }

  @Property(tries = 500)
  void property_sarV2_negativeValue_largeShift_returnsAllOnes(
      @ForAll("negativeValues") final byte[] valueBytes) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes largeShift = Bytes.fromHexString("0x010000000000");

    final Bytes32 v2Result = runV2Sar(largeShift, value);

    final Bytes32 allOnes =
        Bytes32.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
    assertThat(v2Result).isEqualTo(allOnes);
  }

  @Property(tries = 500)
  void property_sarV2_positiveValue_largeShift_returnsZero(
      @ForAll("positiveValues") final byte[] valueBytes) {

    final Bytes value = Bytes.wrap(valueBytes);
    final Bytes largeShift = Bytes.fromHexString("0x010000000000");

    final Bytes32 v2Result = runV2Sar(largeShift, value);

    assertThat(v2Result).isEqualTo(Bytes32.ZERO);
  }

  @Property(tries = 500)
  void property_sarV2_allOnes_anyShift_returnsAllOnes(@ForAll("smallShifts") final int shift) {

    final Bytes value =
        Bytes.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
    final Bytes shiftBytes = Bytes.of(shift);

    final Bytes32 v2Result = runV2Sar(shiftBytes, value);

    assertThat(v2Result).isEqualTo(Bytes32.leftPad(value));
  }

  @Property(tries = 500)
  void property_sarV2_minValue_shift255_returnsAllOnes() {

    final Bytes value =
        Bytes.fromHexString("0x8000000000000000000000000000000000000000000000000000000000000000");
    final Bytes shift = Bytes.of(255);

    final Bytes32 v2Result = runV2Sar(shift, value);

    final Bytes32 allOnes =
        Bytes32.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
    assertThat(v2Result).isEqualTo(allOnes);
  }

  @Property(tries = 500)
  void property_sarV2_maxPositive_shift255_returnsZero() {

    final Bytes value =
        Bytes.fromHexString("0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
    final Bytes shift = Bytes.of(255);

    final Bytes32 v2Result = runV2Sar(shift, value);

    assertThat(v2Result).isEqualTo(Bytes32.ZERO);
  }

  // endregion

  // region V1 Oracle Helpers (mock-based, Bytes stack)

  private Bytes runOriginalShl(final Bytes shift, final Bytes value) {
    return runOriginalOperation(shift, value, ShlOperation::staticOperation);
  }

  private Bytes runOriginalShr(final Bytes shift, final Bytes value) {
    return runOriginalOperation(shift, value, ShrOperation::staticOperation);
  }

  private Bytes runOriginalSar(final Bytes shift, final Bytes value) {
    return runOriginalOperation(shift, value, SarOperation::staticOperation);
  }

  @FunctionalInterface
  interface OriginalOperationExecutor {
    Operation.OperationResult execute(MessageFrame frame);
  }

  private Bytes runOriginalOperation(
      final Bytes shift, final Bytes value, final OriginalOperationExecutor executor) {
    final MessageFrame frame = mock(MessageFrame.class);
    final Deque<Bytes> stack = new ArrayDeque<>();
    stack.push(value);
    stack.push(shift);

    when(frame.popStackItem()).thenAnswer(invocation -> stack.pop());

    final Bytes[] result = new Bytes[1];
    doAnswer(
            invocation -> {
              result[0] = invocation.getArgument(0);
              return null;
            })
        .when(frame)
        .pushStackItem(any(Bytes.class));

    executor.execute(frame);
    return result[0];
  }

  // endregion

  // region V2 Helpers (long[] stack)

  private Bytes32 runV2Shl(final Bytes shift, final Bytes value) {
    return runV2Operation(shift, value, UInt256::shl);
  }

  private Bytes32 runV2Shr(final Bytes shift, final Bytes value) {
    return runV2Operation(shift, value, UInt256::shr);
  }

  private Bytes32 runV2Sar(final Bytes shift, final Bytes value) {
    return runV2Operation(shift, value, UInt256::sar);
  }

  @FunctionalInterface
  interface V2OperationExecutor {
    UInt256 execute(UInt256 value, UInt256 shift);
  }

  private Bytes32 runV2Operation(
      final Bytes shift, final Bytes value, final V2OperationExecutor executor) {
    final UInt256 shiftVal = UInt256.fromBytesBE(Bytes32.leftPad(shift).toArrayUnsafe());
    final UInt256 valueVal = UInt256.fromBytesBE(Bytes32.leftPad(value).toArrayUnsafe());

    final UInt256 result = executor.execute(valueVal, shiftVal);

    return Bytes32.wrap(result.toBytesBE());
  }

  // endregion

  // region Utility

  private Bytes intToMinimalBytes(final int value) {
    if (value == 0) {
      return Bytes.EMPTY;
    }
    if (value <= 0xFF) {
      return Bytes.of(value);
    }
    if (value <= 0xFFFF) {
      return Bytes.of(value >> 8, value & 0xFF);
    }
    if (value <= 0xFFFFFF) {
      return Bytes.of(value >> 16, (value >> 8) & 0xFF, value & 0xFF);
    }
    return Bytes.of(value >> 24, (value >> 16) & 0xFF, (value >> 8) & 0xFF, value & 0xFF);
  }

  // endregion
}
