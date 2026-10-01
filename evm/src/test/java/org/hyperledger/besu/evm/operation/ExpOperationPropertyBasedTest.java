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
package org.hyperledger.besu.evm.operation;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/** Compares {@link ExpOperationOptimized} with the BigInteger based {@link ExpOperation}. */
public class ExpOperationPropertyBasedTest {

  private static final GasCalculator PRAGUE = new PragueGasCalculator();
  private static final GasCalculator FRONTIER = new FrontierGasCalculator();

  @Provide
  Arbitrary<byte[]> values0to32() {
    return Arbitraries.bytes().array(byte[].class).ofMinSize(0).ofMaxSize(32);
  }

  @Provide
  Arbitrary<byte[]> fullWidthOddValues() {
    return Arbitraries.bytes()
        .array(byte[].class)
        .ofSize(32)
        .map(
            bytes -> {
              bytes[31] |= 1;
              return bytes;
            });
  }

  @Property(tries = 10000)
  void property_expOptimized_matchesOriginal_randomInputs(
      @ForAll("values0to32") final byte[] base, @ForAll("values0to32") final byte[] exponent) {
    assertSameResult(Bytes.wrap(base), Bytes.wrap(exponent), PRAGUE);
  }

  @Property(tries = 5000)
  void property_expOptimized_matchesOriginal_oddBaseFullExponent(
      @ForAll("fullWidthOddValues") final byte[] base,
      @ForAll("fullWidthOddValues") final byte[] exponent) {
    assertSameResult(Bytes.wrap(base), Bytes.wrap(exponent), PRAGUE);
  }

  @Property(tries = 1000)
  void property_expOptimized_matchesOriginal_frontierGas(
      @ForAll("values0to32") final byte[] base, @ForAll("values0to32") final byte[] exponent) {
    assertSameResult(Bytes.wrap(base), Bytes.wrap(exponent), FRONTIER);
  }

  @Test
  void executionSpecsBenchmarkInputs() {
    final long[] values = {3, 5, 7, 11, 13, 136279841};
    for (final long base : values) {
      Bytes exponent = Bytes.ofUnsignedLong(base);
      for (int step = 0; step < 16; step++) {
        exponent = assertSameResult(Bytes.ofUnsignedLong(base), exponent, PRAGUE);
      }
    }
    final Bytes max = Bytes32.ZERO.not();
    assertSameResult(max, max, PRAGUE);
  }

  @Test
  void insufficientGasHaltsLikeOriginal() {
    final Bytes base = Bytes.of(3);
    final Bytes exponent = Bytes32.ZERO.not();
    final long cost = PRAGUE.expOperationGasCost(32);

    final MessageFrame frame = frame(base, exponent, cost - 1);
    final Operation.OperationResult result = ExpOperationOptimized.staticOperation(frame, PRAGUE);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
    assertThat(result.getGasCost()).isEqualTo(cost);
    assertThat(frame.stackSize()).isZero();
  }

  /** Runs both implementations and returns their common result. */
  private static Bytes assertSameResult(
      final Bytes base, final Bytes exponent, final GasCalculator gasCalculator) {
    final MessageFrame originalFrame = frame(base, exponent, Long.MAX_VALUE);
    final MessageFrame optimizedFrame = frame(base, exponent, Long.MAX_VALUE);

    final Operation.OperationResult original =
        ExpOperation.staticOperation(originalFrame, gasCalculator);
    final Operation.OperationResult optimized =
        ExpOperationOptimized.staticOperation(optimizedFrame, gasCalculator);

    assertThat(optimized.getHaltReason()).isEqualTo(original.getHaltReason()).isNull();
    assertThat(optimized.getGasCost()).isEqualTo(original.getGasCost());
    final Bytes result = Bytes32.leftPad(optimizedFrame.popStackItem());
    assertThat(result)
        .as("EXP mismatch for base=%s, exponent=%s", base.toHexString(), exponent.toHexString())
        .isEqualTo(Bytes32.leftPad(originalFrame.popStackItem()));
    return result;
  }

  private static MessageFrame frame(final Bytes base, final Bytes exponent, final long gas) {
    return new TestMessageFrameBuilder()
        .initialGas(gas)
        .pushStackItem(exponent)
        .pushStackItem(base)
        .build();
  }
}
