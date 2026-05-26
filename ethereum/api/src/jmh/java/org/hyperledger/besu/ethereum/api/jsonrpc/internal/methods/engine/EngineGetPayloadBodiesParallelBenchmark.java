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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

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
 * Benchmarks sequential vs parallel block body lookups for engine_getPayloadBodies methods.
 *
 * <p>The real RocksDB lookup latency is the key variable. In production each call to {@code
 * blockchain.getBlockBody(hash)} involves a RocksDB point-read, typically costing 100µs–1ms
 * depending on cache state. With 1024 blocks sequential that is 100ms–1s total; parallelising
 * across {@code ForkJoinPool.commonPool()} threads reduces this proportionally.
 *
 * <p>To model real I/O without a running Besu node, the benchmark simulates per-lookup latency
 * using {@link LockSupport#parkNanos(long)}. Run with a range of {@code lookupLatencyNanos} values
 * to cover warm-cache (≈0) and cold-cache (≈500_000 = 500µs) scenarios.
 *
 * <p>Run with:
 *
 * <pre>
 *   ./gradlew :ethereum:api:jmh -Pincludes=EngineGetPayloadBodiesParallel
 * </pre>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
public class EngineGetPayloadBodiesParallelBenchmark {

  /** Number of blocks in the request — mirrors the range callers actually use. */
  @Param({"1", "10", "100", "1024"})
  public int blockCount;

  /**
   * Simulated per-lookup latency in nanoseconds.
   *
   * <ul>
   *   <li>0 — no I/O simulation (measures pure stream overhead)
   *   <li>100_000 — 100µs, typical warm RocksDB read
   *   <li>500_000 — 500µs, moderate cache miss
   *   <li>1_000_000 — 1ms, cold read / heavy write pressure
   * </ul>
   */
  @Param({"0", "100000", "500000"})
  public long lookupLatencyNanos;

  private long[] blockNumbers;
  private byte[][] hashBytes; // pre-generated hash data, prevents allocation in the hot path

  @Setup
  public void setUp() {
    final Random rng = new Random(0xBE5);
    blockNumbers = LongStream.range(0, blockCount).toArray();
    hashBytes = new byte[blockCount][32];
    for (final byte[] hash : hashBytes) {
      rng.nextBytes(hash);
    }
  }

  /**
   * Simulates a single RocksDB getBlockBody call. Returns a non-empty Optional so downstream
   * collectors don't short-circuit, and consumes the simulated latency.
   */
  private Optional<byte[]> simulateLookup(final int index) {
    if (lookupLatencyNanos > 0) {
      try {
        TimeUnit.NANOSECONDS.sleep(lookupLatencyNanos);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    return Optional.of(hashBytes[index]);
  }

  // -------------------------------------------------------------------------
  // ByHash benchmarks  (EngineGetPayloadBodiesByHashV1 / V2 pattern)
  // -------------------------------------------------------------------------

  @Benchmark
  public void byHashSequential(final Blackhole bh) {
    final List<Optional<byte[]>> result =
        Arrays.stream(blockNumbers)
            .mapToObj(i -> simulateLookup((int) i))
            .collect(Collectors.toList());
    bh.consume(result);
  }

  @Benchmark
  public void byHashParallel(final Blackhole bh) {
    final List<Optional<byte[]>> result =
        Arrays.stream(blockNumbers)
            .parallel()
            .mapToObj(i -> simulateLookup((int) i))
            .collect(Collectors.toList());
    bh.consume(result);
  }

  // -------------------------------------------------------------------------
  // ByRange benchmarks  (EngineGetPayloadBodiesByRangeV1 / V2 pattern)
  // Two lookups per block: getBlockBody + getBlockAccessList (V2)
  // -------------------------------------------------------------------------

  @Benchmark
  public void byRangeV2Sequential(final Blackhole bh) {
    final List<long[]> result =
        LongStream.range(0, blockCount)
            .mapToObj(
                i -> {
                  final Optional<byte[]> body = simulateLookup((int) i);
                  final Optional<byte[]> accessList = simulateLookup((int) i);
                  return new long[] {body.hashCode(), accessList.hashCode()};
                })
            .collect(Collectors.toList());
    bh.consume(result);
  }

  @Benchmark
  public void byRangeV2Parallel(final Blackhole bh) {
    final List<long[]> result =
        LongStream.range(0, blockCount)
            .parallel()
            .mapToObj(
                i -> {
                  final Optional<byte[]> body = simulateLookup((int) i);
                  final Optional<byte[]> accessList = simulateLookup((int) i);
                  return new long[] {body.hashCode(), accessList.hashCode()};
                })
            .collect(Collectors.toList());
    bh.consume(result);
  }
}
