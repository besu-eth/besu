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

import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

public class BenchmarkHelper {
  /**
   * Creates a minimal {@link MessageFrame} suitable for opcode benchmarks.
   *
   * <p>The frame is configured with mocked dependencies and deterministic zero/default values.
   *
   * @return a message-call frame ready to use in benchmark setup
   */
  public static MessageFrame createMessageCallFrame() {
    return MessageFrame.builder()
        .worldUpdater(mock(WorldUpdater.class))
        .originator(Address.ZERO)
        .gasPrice(Wei.ONE)
        .blobGasPrice(Wei.ONE)
        .blockValues(mock(BlockValues.class))
        .miningBeneficiary(Address.ZERO)
        .blockHashLookup((__, ___) -> Hash.ZERO)
        .type(MessageFrame.Type.MESSAGE_CALL)
        .initialGas(Long.MAX_VALUE)
        .address(Address.ZERO)
        .contract(Address.ZERO)
        .inputData(Bytes32.ZERO)
        .sender(Address.ZERO)
        .value(Wei.ZERO)
        .apparentValue(Wei.ZERO)
        .code(Code.EMPTY_CODE)
        .completer(__ -> {})
        .build();
  }

  /**
   * Creates a minimal {@link MessageFrame} with custom call data for benchmarks.
   *
   * @param callData the input data to attach to the frame
   * @return a message-call frame initialized with {@code callData}
   */
  public static MessageFrame createMessageCallFrameWithCallData(final Bytes callData) {
    return MessageFrame.builder()
        .worldUpdater(mock(WorldUpdater.class))
        .originator(Address.ZERO)
        .gasPrice(Wei.ONE)
        .blobGasPrice(Wei.ONE)
        .blockValues(mock(BlockValues.class))
        .miningBeneficiary(Address.ZERO)
        .blockHashLookup((__, ___) -> Hash.ZERO)
        .type(MessageFrame.Type.MESSAGE_CALL)
        .initialGas(Long.MAX_VALUE)
        .address(Address.ZERO)
        .contract(Address.ZERO)
        .inputData(callData)
        .sender(Address.ZERO)
        .value(Wei.ZERO)
        .apparentValue(Wei.ZERO)
        .code(Code.EMPTY_CODE)
        .completer(__ -> {})
        .build();
  }

  /**
   * Fills an array with random 32-byte values.
   *
   * @param pool the destination array
   */
  public static void fillPool(final Bytes[] pool) {
    final Random random = new Random();
    fillPool(pool, () -> 1 + random.nextInt(32)); // [1, 32]
  }

  /**
   * Fills an array with random byte values of the specified size.
   *
   * @param pool the destination array
   * @param sizeSupplier size of the Bytes
   */
  public static void fillPool(final Bytes[] pool, final Supplier<Integer> sizeSupplier) {
    final Random random = new Random();
    for (int i = 0; i < pool.length; i++) {
      final byte[] a = new byte[sizeSupplier.get()];
      random.nextBytes(a);
      pool[i] = Bytes.wrap(a);
    }
  }

  /**
   * FIlls an array with random byte values of the specified size that fulfil the specified
   * condition.
   *
   * @param pool the destination array
   * @param sizeSupplier size of the Bytes
   * @param condition predicate for whether to add value to the pool
   */
  public static void fillPool(
      final Bytes[] pool, final Supplier<Integer> sizeSupplier, final Predicate<Bytes> condition) {
    final Random random = new Random();
    for (int i = 0; i < pool.length; i++) {
      final byte[] arrayA = new byte[sizeSupplier.get()];
      Bytes a;
      do {
        random.nextBytes(arrayA);
        a = Bytes.wrap(arrayA);
      } while (!condition.test(a));
      pool[i] = a;
    }
  }

  /**
   * Fill 2 arrays with random byte values that have a relationship between elements of a specified
   * size.
   *
   * @param aPool destination array for pool a
   * @param bPool destination array for pool b
   * @param aSizeSupplier size of the Bytes for pool a
   * @param bSizeSupplier size of the Bytes for pool b
   * @param condition whether to include generated bytes in pools
   */
  public static void fillPools(
      final Bytes[] aPool,
      final Bytes[] bPool,
      final Supplier<Integer> aSizeSupplier,
      final Supplier<Integer> bSizeSupplier,
      final BiPredicate<Bytes, Bytes> condition) {

    if (aPool.length != bPool.length) {
      throw new IllegalArgumentException("pools should have same length");
    }

    final Random random = new Random();
    for (int i = 0; i < aPool.length; i++) {
      final int aSize = aSizeSupplier.get();
      final int bSize = bSizeSupplier.get();
      Bytes a, b;
      do {
        final byte[] arrayA = new byte[aSize];
        final byte[] arrayB = new byte[bSize];
        random.nextBytes(arrayA);
        random.nextBytes(arrayB);
        a = Bytes.wrap(arrayA);
        b = Bytes.wrap(arrayB);
      } while (!condition.test(a, b));
      aPool[i] = a;
      bPool[i] = b;
    }
  }

  /**
   * Creates call data payload for benchmarks.
   *
   * @param size size of the payload in bytes
   * @param nonZero whether to fill payload with deterministic non-zero bytes
   * @return call data payload
   */
  static Bytes createCallData(final int size, final boolean nonZero) {
    byte[] data = new byte[size];
    if (nonZero) {
      for (int i = 0; i < size; i++) {
        data[i] = (byte) (i % 256);
      }
    }
    return Bytes.wrap(data);
  }

  /**
   * Fills COPY-like benchmark pools for call-data operations.
   *
   * @param sizePool destination pool for copy sizes
   * @param destOffsetPool destination pool for destination offsets
   * @param srcOffsetPool destination pool for source offsets
   * @param dataSize call-data size used to populate the size/source ranges
   * @param fixedSrcDst whether to use fixed zero source/destination offsets
   */
  static void fillPoolsForCallData(
      final Bytes[] sizePool,
      final Bytes[] destOffsetPool,
      final Bytes[] srcOffsetPool,
      final int dataSize,
      final boolean fixedSrcDst) {
    for (int i = 0; i < sizePool.length; i++) {
      sizePool[i] = Bytes.wrap(UInt256.valueOf(dataSize));

      if (fixedSrcDst) {
        destOffsetPool[i] = Bytes.wrap(UInt256.valueOf(0));
        srcOffsetPool[i] = Bytes.wrap(UInt256.valueOf(0));
      } else {
        destOffsetPool[i] = Bytes.wrap(UInt256.valueOf((i * 32) % 1024));
        srcOffsetPool[i] = Bytes.wrap(UInt256.valueOf(i % Math.max(1, dataSize)));
      }
    }
  }

  /**
   * Generates a random 32-byte value.
   *
   * @param random thread-local random source
   * @return random 32-byte value
   */
  static Bytes randomValue(final ThreadLocalRandom random) {
    final byte[] value = new byte[32];
    random.nextBytes(value);
    return Bytes.wrap(value);
  }

  /**
   * Generates a random positive signed 256-bit value (sign bit cleared).
   *
   * @param random thread-local random source
   * @return random positive 32-byte value
   */
  static Bytes randomPositiveValue(final ThreadLocalRandom random) {
    final byte[] value = new byte[32];
    random.nextBytes(value);
    value[0] = (byte) (value[0] & 0x7F);
    return Bytes.wrap(value);
  }

  /**
   * Generates a random negative signed 256-bit value (sign bit set).
   *
   * @param random thread-local random source
   * @return random negative 32-byte value
   */
  static Bytes randomNegativeValue(final ThreadLocalRandom random) {
    final byte[] value = new byte[32];
    random.nextBytes(value);
    value[0] = (byte) (value[0] | 0x80);
    return Bytes.wrap(value);
  }

  static Bytes pow2(final int n) {
    final byte[] bytes = new byte[32];
    int nBytes = Integer.divideUnsigned(n, 8);
    int nBits = Integer.remainderUnsigned(n, 8);
    bytes[31 - nBytes] = (byte) (1 << nBits);
    return Bytes.wrap(bytes);
  }
}
