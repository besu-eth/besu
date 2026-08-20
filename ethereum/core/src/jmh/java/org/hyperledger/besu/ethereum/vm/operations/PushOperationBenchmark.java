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

import static org.hyperledger.besu.evm.operation.PushOperation.staticOperation;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.PushOperation;
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
 * Benchmarks {@link PushOperation#staticOperation}.
 *
 * <p>Dimensions:
 *
 * <ul>
 *   <li>{@code pushSize}: 1 (single byte), 8 (one limb), 20 (address-sized), 32 (full word), or
 *       RANDOM = uniform in 1..32 per sample
 *   <li>{@code position}: MID = full push data available with trailing code; END = push data ends
 *       exactly at the last code byte; TRUNCATED = code runs out mid-push so the value is
 *       left-shifted to zero-pad; OOB = pc past the data, nothing to read; RANDOM = uniform over
 *       the previous four per sample, to keep the JIT from specializing on one branch shape
 *   <li>{@code codeSize}: SNIPPET = 64-byte code; CONTRACT = 24k-byte code
 * </ul>
 */
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class PushOperationBenchmark {

  private static final int SAMPLE_SIZE = 30_000;
  private static final int SMALL_CODE_SIZE = 64;
  private static final int LARGE_CODE_SIZE = 24_576;

  public enum Position {
    // PC is within the first 16 bytes of the code (measure slow path to parse unaligned long values
    // out of the code): pc + 1 <= 16
    FIRST_16BYTES,
    // full data, code continues past it: pc + 1 <= code.length - pushSize
    MID,
    // push data ends exactly at the last byte of code: pc + 1 == code.length - pushSize
    END,
    // only 1...size-1 data bytes available: value must be shifted left to pad
    TRUNCATED,
    // pc at end of code: nothing to read, pushes zero
    OOB,
    // one of the other modes is picked at random
    RANDOM;
  }

  @Param({"1", "8", "20", "32", "RANDOM"})
  private String pushSize;

  @Param protected Position pc;

  @Param({"SMALL", "BIG"})
  private String codeSize;

  private MessageFrame frame;
  private byte[] code;
  private int[] pcPool;
  private int[] pushSizePool;
  private int index;

  @Setup
  public void setUp() {
    frame = BenchmarkHelper.createMessageCallFrame();
    final Random random = new Random();
    code = new byte[
      switch(codeSize) {
        case "SMALL" -> SMALL_CODE_SIZE;
        case "BIG" -> LARGE_CODE_SIZE;
        default -> throw new IllegalArgumentException("unknown code size " + codeSize);
      }];
    random.nextBytes(code);

    final boolean randomSize = "RANDOM".equals(pushSize);
    final int fixedSize = randomSize ? -1 : Integer.parseInt(pushSize);

    pcPool = new int[SAMPLE_SIZE];
    pushSizePool = new int[SAMPLE_SIZE];
    for (int i = 0; i < SAMPLE_SIZE; i++) {
      final int size = randomSize ? random.nextInt(1, 33) : fixedSize;
      if (pc == Position.RANDOM) {
        pc =
            Arrays.stream(Position.values())
                .filter(x -> x != Position.RANDOM)
                .toList()
                .get(random.nextInt(Position.values().length - 1));
      }
      pushSizePool[i] = size;
      pcPool[i] =
          switch (pc) {
            case FIRST_16BYTES -> random.nextInt(16);
            case MID -> random.nextInt(code.length - size);
            case END -> code.length - 1 - size;
            case TRUNCATED -> code.length - 1 - (size > 1 ? random.nextInt(1, size) : 0);
            case OOB -> code.length - 1;
            default -> throw new IllegalArgumentException("unknown position " + pc);
          };
    }
    index = 0;
  }

  @Benchmark
  public void executeOperation(final Blackhole blackhole) {
    blackhole.consume(staticOperation(frame, code, pcPool[index], pushSizePool[index]));

    frame.popStackItem();

    index = (index + 1) % SAMPLE_SIZE;
  }
}
