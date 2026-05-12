/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmarks ordering strategies for the {@code TreeMap} in {@code FlatDbStrategy.toNavigableMap},
 * across three comparators and three key-construction shapes.
 *
 * <p>Comparators:
 *
 * <ul>
 *   <li><b>hex</b>: {@code Comparator.comparing(Bytes::toHexString)} — prior baseline.
 *   <li><b>byteUnsigned</b>: {@code Comparator.comparing(Bytes::toArrayUnsafe,
 *       Arrays::compareUnsigned)} — current production. Uses the {@code Arrays.compareUnsigned}
 *       HotSpot intrinsic.
 *   <li><b>getLoop</b>: per-byte unsigned compare via {@code Bytes.get(int)}. Allocation-free but
 *       loses the vectorized intrinsic.
 *   <li><b>compareTo</b>: Tuweni's {@code Bytes.compareTo(Bytes)} — itself a per-byte {@code
 *       get(i)} loop, but prefaced with {@code bitLength()} scans on both operands.
 * </ul>
 *
 * <p>Key shapes (mirror real production hot paths from {@code BonsaiFlatDbStrategy}):
 *
 * <ul>
 *   <li><b>account</b>: {@code Bytes32.wrap(byte[])} — array-backed, offset=0. {@code
 *       toArrayUnsafe()} is zero-allocation.
 *   <li><b>storageSliced</b>: {@code Bytes32.wrap(Bytes.wrap(byte[]).slice(32))} — current
 *       production. Produces a {@code DelegatingBytes32} around an offset=32 slice. {@code
 *       toArrayUnsafe()} allocates a fresh 32-byte copy.
 *   <li><b>storageMaterialized</b>: {@code Bytes32.wrap(Arrays.copyOfRange(byte[], 32, 64))} —
 *       producer-side fix. One extra 32-byte allocation at construction time in exchange for a
 *       zero-allocation {@code toArrayUnsafe()} on every subsequent compare.
 * </ul>
 *
 * <p>End-to-end benchmarks measure the realistic flat-DB heal workload (construct keys from raw
 * 64-byte storage records + populate TreeMap + iterate) so the producer-side fix is paying its own
 * construction cost.
 */
@State(Scope.Benchmark)
@Fork(value = 2)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class FlatDbStrategyComparatorBenchmark {

  private static final Comparator<Bytes> HEX = Comparator.comparing(Bytes::toHexString);

  private static final Comparator<Bytes> BYTE_UNSIGNED =
      Comparator.comparing(Bytes::toArrayUnsafe, Arrays::compareUnsigned);

  private static final Comparator<Bytes> GET_LOOP =
      (a, b) -> {
        final int size = a.size();
        for (int i = 0; i < size; i++) {
          final int diff = Byte.toUnsignedInt(a.get(i)) - Byte.toUnsignedInt(b.get(i));
          if (diff != 0) {
            return diff;
          }
        }
        return 0;
      };

  private static final Comparator<Bytes> COMPARE_TO = Bytes::compareTo;

  @Param({"4096"})
  public int size;

  private Bytes32[] accountKeys;
  private Bytes32[] storageKeysSliced;
  private Bytes32[] storageKeysMaterialized;
  private byte[][] rawStorageBytes;
  private int[] pairsA;
  private int[] pairsB;

  @Setup(Level.Trial)
  public void setUp() {
    final Random rng = new Random(0xC0FFEEL);
    accountKeys = new Bytes32[size];
    storageKeysSliced = new Bytes32[size];
    storageKeysMaterialized = new Bytes32[size];
    rawStorageBytes = new byte[size][];
    pairsA = new int[size];
    pairsB = new int[size];
    for (int i = 0; i < size; i++) {
      // Account stream: Bytes32.wrap(byte[]) -> ArrayWrappingBytes32 (offset=0).
      final byte[] accountBytes = new byte[Bytes32.SIZE];
      rng.nextBytes(accountBytes);
      accountKeys[i] = Bytes32.wrap(accountBytes);

      // Storage stream: 64-byte raw key (accountHash || slotHash).
      final byte[] storageBytes = new byte[Bytes32.SIZE * 2];
      rng.nextBytes(storageBytes);
      rawStorageBytes[i] = storageBytes;

      // Current production shape: DelegatingBytes32 around an offset=32 slice.
      storageKeysSliced[i] = Bytes32.wrap(Bytes.wrap(storageBytes).slice(Bytes32.SIZE));

      // Producer-side fix: ArrayWrappingBytes32 over a fresh 32-byte copy.
      storageKeysMaterialized[i] =
          Bytes32.wrap(Arrays.copyOfRange(storageBytes, Bytes32.SIZE, Bytes32.SIZE * 2));

      pairsA[i] = rng.nextInt(size);
      pairsB[i] = rng.nextInt(size);
    }
  }

  // --- Pairwise compare (isolated comparator+key-shape cost) ---

  @Benchmark
  public int pairwiseAccount_hex() {
    return pairwise(accountKeys, HEX);
  }

  @Benchmark
  public int pairwiseAccount_byteUnsigned() {
    return pairwise(accountKeys, BYTE_UNSIGNED);
  }

  @Benchmark
  public int pairwiseAccount_getLoop() {
    return pairwise(accountKeys, GET_LOOP);
  }

  @Benchmark
  public int pairwiseAccount_compareTo() {
    return pairwise(accountKeys, COMPARE_TO);
  }

  @Benchmark
  public int pairwiseStorageSliced_hex() {
    return pairwise(storageKeysSliced, HEX);
  }

  @Benchmark
  public int pairwiseStorageSliced_byteUnsigned() {
    return pairwise(storageKeysSliced, BYTE_UNSIGNED);
  }

  @Benchmark
  public int pairwiseStorageSliced_getLoop() {
    return pairwise(storageKeysSliced, GET_LOOP);
  }

  @Benchmark
  public int pairwiseStorageSliced_compareTo() {
    return pairwise(storageKeysSliced, COMPARE_TO);
  }

  @Benchmark
  public int pairwiseStorageMaterialized_byteUnsigned() {
    return pairwise(storageKeysMaterialized, BYTE_UNSIGNED);
  }

  @Benchmark
  public int pairwiseStorageMaterialized_getLoop() {
    return pairwise(storageKeysMaterialized, GET_LOOP);
  }

  private int pairwise(final Bytes32[] keys, final Comparator<Bytes> cmp) {
    int sum = 0;
    for (int i = 0; i < size; i++) {
      sum += cmp.compare(keys[pairsA[i]], keys[pairsB[i]]);
    }
    return sum;
  }

  // --- TreeMap workload with pre-built keys ---

  @Benchmark
  public int treeMapAccount_hex() {
    return populateAndDrain(accountKeys, HEX);
  }

  @Benchmark
  public int treeMapAccount_byteUnsigned() {
    return populateAndDrain(accountKeys, BYTE_UNSIGNED);
  }

  @Benchmark
  public int treeMapAccount_getLoop() {
    return populateAndDrain(accountKeys, GET_LOOP);
  }

  @Benchmark
  public int treeMapAccount_compareTo() {
    return populateAndDrain(accountKeys, COMPARE_TO);
  }

  @Benchmark
  public int treeMapStorageSliced_hex() {
    return populateAndDrain(storageKeysSliced, HEX);
  }

  @Benchmark
  public int treeMapStorageSliced_byteUnsigned() {
    return populateAndDrain(storageKeysSliced, BYTE_UNSIGNED);
  }

  @Benchmark
  public int treeMapStorageSliced_getLoop() {
    return populateAndDrain(storageKeysSliced, GET_LOOP);
  }

  @Benchmark
  public int treeMapStorageSliced_compareTo() {
    return populateAndDrain(storageKeysSliced, COMPARE_TO);
  }

  @Benchmark
  public int treeMapStorageMaterialized_byteUnsigned() {
    return populateAndDrain(storageKeysMaterialized, BYTE_UNSIGNED);
  }

  @Benchmark
  public int treeMapStorageMaterialized_getLoop() {
    return populateAndDrain(storageKeysMaterialized, GET_LOOP);
  }

  private static int populateAndDrain(final Bytes32[] keys, final Comparator<Bytes> cmp) {
    final TreeMap<Bytes32, Bytes> map = new TreeMap<>(cmp);
    for (Bytes32 k : keys) {
      map.put(k, Bytes.EMPTY);
    }
    int sum = 0;
    for (Bytes32 k : map.keySet()) {
      sum += k.get(0);
    }
    return sum;
  }

  // --- End-to-end: build Bytes32 from raw 64-byte records + populate + iterate ---

  /** Status quo: sliced Bytes32 + current byteUnsigned comparator. */
  @Benchmark
  public int endToEndStorage_sliced_byteUnsigned() {
    final TreeMap<Bytes32, Bytes> map = new TreeMap<>(BYTE_UNSIGNED);
    for (byte[] raw : rawStorageBytes) {
      map.put(Bytes32.wrap(Bytes.wrap(raw).slice(Bytes32.SIZE)), Bytes.EMPTY);
    }
    int sum = 0;
    for (Bytes32 k : map.keySet()) {
      sum += k.get(0);
    }
    return sum;
  }

  /** Producer-side fix: copyOfRange Bytes32 + current byteUnsigned comparator. */
  @Benchmark
  public int endToEndStorage_materialized_byteUnsigned() {
    final TreeMap<Bytes32, Bytes> map = new TreeMap<>(BYTE_UNSIGNED);
    for (byte[] raw : rawStorageBytes) {
      map.put(Bytes32.wrap(Arrays.copyOfRange(raw, Bytes32.SIZE, Bytes32.SIZE * 2)), Bytes.EMPTY);
    }
    int sum = 0;
    for (Bytes32 k : map.keySet()) {
      sum += k.get(0);
    }
    return sum;
  }

  /** Copilot's proposal: sliced Bytes32 + per-byte getLoop comparator. */
  @Benchmark
  public int endToEndStorage_sliced_getLoop() {
    final TreeMap<Bytes32, Bytes> map = new TreeMap<>(GET_LOOP);
    for (byte[] raw : rawStorageBytes) {
      map.put(Bytes32.wrap(Bytes.wrap(raw).slice(Bytes32.SIZE)), Bytes.EMPTY);
    }
    int sum = 0;
    for (Bytes32 k : map.keySet()) {
      sum += k.get(0);
    }
    return sum;
  }

  /** Tuweni-idiomatic: sliced Bytes32 + Bytes.compareTo comparator. */
  @Benchmark
  public int endToEndStorage_sliced_compareTo() {
    final TreeMap<Bytes32, Bytes> map = new TreeMap<>(COMPARE_TO);
    for (byte[] raw : rawStorageBytes) {
      map.put(Bytes32.wrap(Bytes.wrap(raw).slice(Bytes32.SIZE)), Bytes.EMPTY);
    }
    int sum = 0;
    for (Bytes32 k : map.keySet()) {
      sum += k.get(0);
    }
    return sum;
  }
}
