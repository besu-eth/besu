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
package org.hyperledger.besu.ethereum.vm.operations;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;
import org.hyperledger.besu.evm.operation.ExpOperation;
import org.hyperledger.besu.evm.operation.ExpOperationOptimized;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * EXP on the inputs of execution-specs {@code
 * tests/benchmark/compute/instruction/test_arithmetic.py}.
 *
 * <p>{@code test_exp_bench_arithmetic} runs {@code DUP2 EXP} in a loop, so every EXP raises the
 * same base to the previous result. The pools hold 64 consecutive steps of that chain, taken once
 * the exponent has its full 256-bit width. {@code MAX} is the EXP case of {@code test_arithmetic}:
 * {@code (2^256 - 1)^(2^256 - 1)}.
 */
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class ExpOperationExecutionSpecsBenchmark {

  private static final GasCalculator GAS_CALCULATOR = new PragueGasCalculator();
  private static final BigInteger MODULUS = BigInteger.ONE.shiftLeft(256);
  private static final int POOL_SIZE = 64;

  /** Chain steps skipped before the pool starts: the exponent is short in the first ones. */
  private static final int WARM_UP_STEPS = 3;

  @Param({"3", "5", "7", "11", "13", "136279841", "MAX"})
  public String base;

  private Bytes[] bases;
  private Bytes[] exponents;
  private int index;
  private MessageFrame frame;

  @Setup
  public void setUp() {
    frame = BenchmarkHelper.createMessageCallFrame();
    bases = new Bytes[POOL_SIZE];
    exponents = new Bytes[POOL_SIZE];
    if ("MAX".equals(base)) {
      Arrays.fill(bases, Bytes32.ZERO.not());
      Arrays.fill(exponents, Bytes32.ZERO.not());
    } else {
      final BigInteger b = new BigInteger(base);
      BigInteger e = BigInteger.valueOf(3);
      for (int i = 0; i < WARM_UP_STEPS; i++) {
        e = b.modPow(e, MODULUS);
      }
      for (int i = 0; i < POOL_SIZE; i++) {
        bases[i] = toBytes(b);
        exponents[i] = toBytes(e);
        e = b.modPow(e, MODULUS);
      }
    }
    index = 0;
  }

  @Benchmark
  public void bigInteger(final Blackhole blackhole) {
    frame.pushStackItem(exponents[index]);
    frame.pushStackItem(bases[index]);
    blackhole.consume(ExpOperation.staticOperation(frame, GAS_CALCULATOR));
    blackhole.consume(frame.popStackItem());
    index = (index + 1) % POOL_SIZE;
  }

  @Benchmark
  public void optimized(final Blackhole blackhole) {
    frame.pushStackItem(exponents[index]);
    frame.pushStackItem(bases[index]);
    blackhole.consume(ExpOperationOptimized.staticOperation(frame, GAS_CALCULATOR));
    blackhole.consume(frame.popStackItem());
    index = (index + 1) % POOL_SIZE;
  }

  private static Bytes toBytes(final BigInteger value) {
    return Bytes.wrap(value.toByteArray()).trimLeadingZeros();
  }
}
