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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks {@link HexWriter#encodeTo}. Each invocation encodes {@link #N} values to simulate a
 * realistic per-opcode workload (stack entries).
 *
 * <p>Run with: {@code ./gradlew :ethereum:api:jmh -Pincludes=CompactHexBenchmark}
 */
@State(Scope.Thread)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(3)
public class CompactHexBenchmark {

  private static final int N = 16;
  private static final int BUF_SIZE = 32 * 1024;

  public enum ValueSize {
    BYTES_1(1),
    BYTES_8(8),
    BYTES_20(20),
    BYTES_32(32);

    final int size;

    ValueSize(final int size) {
      this.size = size;
    }
  }

  @Param({"BYTES_1", "BYTES_8", "BYTES_20", "BYTES_32"})
  private ValueSize valueSize;

  private byte[][] values;
  private byte[] writeBuf;

  @Setup
  public void setup() {
    final Random rng = new Random(42);
    values = new byte[N][];
    for (int i = 0; i < N; i++) {
      values[i] = new byte[valueSize.size];
      rng.nextBytes(values[i]);
      // ~25% of values have leading zero bytes (realistic for stack values < 256 bits)
      if (i % 4 == 0 && values[i].length > 1) {
        values[i][0] = 0;
      }
    }
    writeBuf = new byte[BUF_SIZE];
  }

  @Benchmark
  @OperationsPerInvocation(N)
  public void encodeTo(final Blackhole bh) {
    int pos = 0;
    for (final byte[] bytes : values) {
      pos = HexWriter.encodeTo(bytes, bytes.length, writeBuf, pos, true);
    }
    bh.consume(pos);
  }
}
