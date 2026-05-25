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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks for {@link Address#addressHash()}, comparing the per-instance volatile-field cache
 * (current implementation) against the previous Caffeine global-cache approach.
 *
 * <p>Three scenarios cover the relevant production cases:
 *
 * <ol>
 *   <li>{@code cachedSameInstance} — repeated calls on a single instance; both implementations
 *       should be equally fast (one computation, then a field read or a cache hit).
 *   <li>{@code differentInstancesSameBytes} — the cross-instance deduplication case: 1,000
 *       distinct {@code Address} objects wrapping the same bytes (simulating a popular address such
 *       as USDC appearing across many transactions). The global cache would hit on calls 2-1000;
 *       the per-instance approach pays keccak256 once per object.
 *   <li>{@code differentAddresses} — 1,000 objects each with unique bytes; both approaches pay
 *       keccak256 once per object (global cache misses every time), so results should be equal.
 * </ol>
 *
 * <p>Run with:
 *
 * <pre>
 *   ./gradlew :ethereum:core:jmh -Pincludes=AddressHashBenchmark
 * </pre>
 */
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class AddressHashBenchmark {

  private static final int POOL_SIZE = 1_000;

  // Scenario 1: same instance, repeated calls
  private Address singleInstance;

  // Scenario 2: many instances, same bytes (cross-instance deduplication)
  private Address[] sameBytesDifferentInstances;

  // Scenario 3: many instances, all unique bytes
  private Address[] uniqueAddresses;

  @Setup
  public void setUp() {
    singleInstance = Address.fromHexString("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");

    sameBytesDifferentInstances = new Address[POOL_SIZE];
    for (int i = 0; i < POOL_SIZE; i++) {
      sameBytesDifferentInstances[i] =
          Address.fromHexString("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");
    }

    uniqueAddresses = new Address[POOL_SIZE];
    for (int i = 0; i < POOL_SIZE; i++) {
      // Unique address per slot: pad index into 20 bytes
      uniqueAddresses[i] = Address.fromHexString(String.format("0x%040x", i + 1));
    }
  }

  /** Warm per-instance cache — identical for both implementations. */
  @Benchmark
  public Hash cachedSameInstance() {
    return singleInstance.addressHash();
  }

  /**
   * Cross-instance deduplication: 1,000 objects wrapping the same bytes. The global Caffeine cache
   * would hit on calls 2-1000; the per-instance approach pays keccak256 once per object. This is
   * the key benchmark for assessing the cost of removing cross-instance deduplication.
   */
  @Benchmark
  public void differentInstancesSameBytes(final Blackhole bh) {
    for (final Address a : sameBytesDifferentInstances) {
      bh.consume(a.addressHash());
    }
  }

  /**
   * All unique addresses — both approaches pay keccak256 once per object (no deduplication
   * possible). Results should be equal; confirms the benchmark infrastructure is sound.
   */
  @Benchmark
  public void differentAddresses(final Blackhole bh) {
    for (final Address a : uniqueAddresses) {
      bh.consume(a.addressHash());
    }
  }
}
