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
package org.hyperledger.besu.evm.frame;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.internal.AddressStorageSlotKey;
import org.hyperledger.besu.evm.toy.ToyBlockValues;
import org.hyperledger.besu.evm.toy.ToyWorld;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/**
 * Regression test asserting that an attacker who grinds many {@link Address}/{@link
 * AddressStorageSlotKey} keys sharing the same non-treeified {@code hashCode()} cannot force O(n)
 * bucket walks per warm-up insert. {@link MessageFrame}'s warm-address and warm-storage collections
 * are keyed on exactly such colliding values below, and the whole batch of inserts is required to
 * complete well inside a budget that a quadratic blow-up would blow through by orders of magnitude.
 */
class WarmStorageHashDosTest {

  private static final int COLLIDING_HASHES = 10_000;

  private static MessageFrame newFrame(final Address address) {
    return MessageFrame.builder()
        .worldUpdater(new ToyWorld())
        .originator(Address.ZERO)
        .gasPrice(Wei.ONE)
        .blobGasPrice(Wei.ONE)
        .blockValues(new ToyBlockValues())
        .miningBeneficiary(Address.ZERO)
        .blockHashLookup((__, ___) -> Hash.ZERO)
        .type(MessageFrame.Type.MESSAGE_CALL)
        .initialGas(1)
        .address(address)
        .contract(Address.ZERO)
        .inputData(Bytes32.ZERO)
        .sender(Address.ZERO)
        .value(Wei.ZERO)
        .apparentValue(Wei.ZERO)
        .code(Code.EMPTY_CODE)
        .completer(messageFrame -> {})
        .build();
  }

  static class Eip2929Storage {
    private static final int SLOT_LANES = 8;
    private static final int ADDRESS_LANES = 5;
    private static final int BASE_LANE = SLOT_LANES + ADDRESS_LANES;
    private static final int SOLVER_LANE = 0;
    private static final int NONCE_LOW_LANE = 1;
    private static final int NONCE_HIGH_LANE = 2;
    private static final int COLLIDING_HASH_CODE = 0x2BADF00D;
    private static final int MAX_NONCE_RETRIES = 64;
    private static final BigInteger TWO_64 = BigInteger.ONE.shiftLeft(64);
    private static final BigInteger TWO_32 = BigInteger.ONE.shiftLeft(32);
    private static final BigInteger MASK_64 = TWO_64.subtract(BigInteger.ONE);
    private static long[] seeds;
    private static final Map<Address, Crafter> CRAFTERS = new HashMap<>();

    private static final class Crafter {
      private final long addressPart;
      private final BigInteger multiplier;
      private final BigInteger[][] basis;
      private final List<Bytes32> collidingPool = new ArrayList<>();
      private long collidingNonce;

      Crafter(final long addressPart, final BigInteger multiplier) {
        this.addressPart = addressPart;
        this.multiplier = multiplier;
        this.basis =
            reduce(new BigInteger[][] {{BigInteger.ONE, multiplier}, {BigInteger.ZERO, TWO_64}});
      }
    }

    private static synchronized long[] seeds() throws Exception {
      if (seeds == null) {
        final Field field = AddressStorageSlotKey.class.getDeclaredField("SEEDS");
        field.setAccessible(true);
        seeds = ((long[]) field.get(null)).clone();
      }
      return seeds;
    }

    private static synchronized Crafter crafter(final Address address) throws Exception {
      Crafter crafter = CRAFTERS.get(address);
      if (crafter == null) {
        final long[] s = seeds();
        final ByteBuffer bytes =
            ByteBuffer.wrap(address.getBytes().toArrayUnsafe()).order(ByteOrder.LITTLE_ENDIAN);
        long addressPart = s[BASE_LANE];
        for (int lane = 0; lane < ADDRESS_LANES; lane++) {
          addressPart +=
              Integer.toUnsignedLong(bytes.getInt(lane * Integer.BYTES)) * s[SLOT_LANES + lane];
        }
        crafter = new Crafter(addressPart, unsigned(s[SOLVER_LANE]));
        CRAFTERS.put(address, crafter);
      }
      return crafter;
    }

    // all of the code to generate collisions is copied from AddressStorageSlotKeyHashing as there's
    // no access
    // to this class from here. These tests should be removed anyway once benchmarks for SLOAD/STORE
    // are done
    private static synchronized Bytes32 collidingHash(final Address address, final int index)
        throws Exception {
      final Crafter crafter = crafter(address);
      int misses = 0;
      while (crafter.collidingPool.size() <= index) {
        final long nonce = crafter.collidingNonce++;
        final Bytes32 slot = craft(crafter, COLLIDING_HASH_CODE, nonce);
        if (slot != null) {
          crafter.collidingPool.add(slot);
          misses = 0;
        } else if (++misses >= MAX_NONCE_RETRIES) {
          throw new IllegalStateException(
              "no colliding slot found for hash code " + COLLIDING_HASH_CODE);
        }
      }
      return crafter.collidingPool.get(index);
    }

    private static Bytes32 craft(final Crafter crafter, final int hashCode, final long nonce)
        throws Exception {
      final long[] s = seeds();
      final int[] lanes = new int[SLOT_LANES];
      lanes[NONCE_LOW_LANE] = (int) nonce;
      lanes[NONCE_HIGH_LANE] = (int) (nonce >>> 32);

      long fixed = crafter.addressPart;
      for (int lane = 0; lane < SLOT_LANES; lane++) {
        if (lane != SOLVER_LANE) {
          fixed += Integer.toUnsignedLong(lanes[lane]) * s[lane];
        }
      }

      final BigInteger window = BigInteger.valueOf(Integer.toUnsignedLong(hashCode)).shiftLeft(32);
      final long solved = solve(unsigned(fixed), crafter.multiplier, window, crafter.basis);
      if (solved < 0) {
        return null;
      }
      lanes[SOLVER_LANE] = (int) solved;

      final ByteBuffer slot =
          ByteBuffer.wrap(new byte[Bytes32.SIZE]).order(ByteOrder.LITTLE_ENDIAN);
      for (int lane = 0; lane < SLOT_LANES; lane++) {
        slot.putInt(lane * Integer.BYTES, lanes[lane]);
      }
      return Bytes32.wrap(slot.array());
    }

    private static long solve(
        final BigInteger fixed,
        final BigInteger multiplier,
        final BigInteger window,
        final BigInteger[][] basis) {
      final BigInteger yCentre = window.subtract(fixed).add(TWO_32.shiftRight(1));
      final BigInteger xCentre = TWO_32.shiftRight(1);

      BigInteger det =
          basis[0][0].multiply(basis[1][1]).subtract(basis[0][1].multiply(basis[1][0]));
      if (det.signum() == 0) {
        return -1;
      }
      BigInteger n0 = xCentre.multiply(basis[1][1]).subtract(yCentre.multiply(basis[1][0]));
      BigInteger n1 = yCentre.multiply(basis[0][0]).subtract(xCentre.multiply(basis[0][1]));
      if (det.signum() < 0) {
        det = det.negate();
        n0 = n0.negate();
        n1 = n1.negate();
      }
      final BigInteger c0 = roundDiv(n0, det);
      final BigInteger c1 = roundDiv(n1, det);

      for (int d0 = -2; d0 <= 2; d0++) {
        for (int d1 = -2; d1 <= 2; d1++) {
          final BigInteger x =
              c0.add(BigInteger.valueOf(d0))
                  .multiply(basis[0][0])
                  .add(c1.add(BigInteger.valueOf(d1)).multiply(basis[1][0]));
          if (x.signum() < 0 || x.compareTo(TWO_32) >= 0) {
            continue;
          }
          final BigInteger h = fixed.add(x.multiply(multiplier)).and(MASK_64);
          if (h.subtract(window).and(MASK_64).compareTo(TWO_32) < 0) {
            return x.longValueExact();
          }
        }
      }
      return -1;
    }

    private static BigInteger[][] reduce(final BigInteger[][] basis) {
      BigInteger[][] b = basis;
      while (true) {
        if (norm(b[1]).compareTo(norm(b[0])) < 0) {
          final BigInteger[] swap = b[0];
          b[0] = b[1];
          b[1] = swap;
        }
        final BigInteger n0 = norm(b[0]);
        if (n0.signum() == 0) {
          return b;
        }
        final BigInteger dot = b[1][0].multiply(b[0][0]).add(b[1][1].multiply(b[0][1]));
        final BigInteger mu = roundDiv(dot, n0);
        if (mu.signum() == 0) {
          return b;
        }
        b[1] =
            new BigInteger[] {
              b[1][0].subtract(mu.multiply(b[0][0])), b[1][1].subtract(mu.multiply(b[0][1]))
            };
      }
    }

    private static BigInteger norm(final BigInteger[] vector) {
      return vector[0].multiply(vector[0]).add(vector[1].multiply(vector[1]));
    }

    private static BigInteger roundDiv(final BigInteger a, final BigInteger b) {
      final BigInteger[] quotientRemainder = a.divideAndRemainder(b);
      BigInteger quotient = quotientRemainder[0];
      final BigInteger remainder = quotientRemainder[1];
      if (remainder.abs().shiftLeft(1).compareTo(b) >= 0) {
        quotient =
            remainder.signum() >= 0
                ? quotient.add(BigInteger.ONE)
                : quotient.subtract(BigInteger.ONE);
      }
      return quotient;
    }

    private static BigInteger unsigned(final long value) {
      return BigInteger.valueOf(value).and(MASK_64);
    }

    // TODO: Remove once benchmarks for SLOAD/SSTORE are done
    @Test
    void hashCodeKeysCollide() throws Exception {
      for (int i = 0; i < COLLIDING_HASHES; i++) {
        for (int j = 0; j < i; j++) {
          final AddressStorageSlotKey key_i =
              new AddressStorageSlotKey(Address.ZERO, collidingHash(Address.ZERO, i));
          final AddressStorageSlotKey key_j =
              new AddressStorageSlotKey(Address.ZERO, collidingHash(Address.ZERO, j));

          assertThat(key_i.hashCode()).isEqualTo(key_j.hashCode());
          assertThat(key_i).isNotEqualTo(key_j);
        }
      }
    }

    // TODO: Remove once benchmarks for SLOAD/SSTORE are done
    @Test
    void warmedUpStorageResistsHashCollisionFlood() throws Exception {
      final MessageFrame frame = newFrame(Address.ZERO);
      final List<Bytes32> slots = new ArrayList<>(COLLIDING_HASHES);
      for (int i = 0; i < COLLIDING_HASHES; i++) {
        slots.add(collidingHash(Address.ZERO, i));
      }

      AtomicInteger integer = new AtomicInteger();
      assertTimeoutPreemptively(
          ofSeconds(2),
          () -> {
            for (final Bytes32 slot : slots) {
              frame.warmUpStorage(Address.ZERO, slot);
              integer.incrementAndGet();
            }
          },
          () -> "stopped at iteration=" + integer);

      for (final Bytes32 slot : slots) {
        assertThat(
                frame.getWarmedUpStorage().contains(new AddressStorageSlotKey(Address.ZERO, slot)))
            .isTrue();
      }
    }
  }

  static class Addresses {
    /**
     * Two bytes (a, b) contribute {@code 31*a + b} to Tuweni's base-31 polynomial {@code
     * hashCode()} at the position of {@code a}. Each of these three pairs contributes exactly zero,
     * so tiling any combination of them across a key leaves its hashCode unchanged (all keys
     * collide into a single hash bucket) while the underlying bytes - and thus the keys themselves
     * - remain distinct.
     */
    private static void writeZeroSumPair(final byte[] bytes, final int offset, final int digit) {
      switch (digit) {
        case 0 -> {
          bytes[offset] = 0;
          bytes[offset + 1] = 0;
        }
        case 1 -> {
          bytes[offset] = 1;
          bytes[offset + 1] = (byte) -31;
        }
        default -> {
          bytes[offset] = (byte) -1;
          bytes[offset + 1] = 31;
        }
      }
    }

    private static Address collidingAddress(final long index) {
      final byte[] bytes = new byte[Address.SIZE];
      long remaining = index;
      for (int pair = 0; pair < Address.SIZE / 2; pair++) {
        writeZeroSumPair(bytes, pair * 2, (int) (remaining % 3));
        remaining /= 3;
      }
      return Address.wrap(Bytes.wrap(bytes));
    }

    @Test
    void generatedAddressesActuallyCollide() {
      List<Address> addresses =
          IntStream.range(0, COLLIDING_HASHES).mapToObj(Addresses::collidingAddress).toList();

      HashSet<Integer> hashes =
          new HashSet<>(addresses.stream().map(addr -> addr.getBytes().hashCode()).toList());

      assertThat(hashes.size()).isEqualTo(1);

      for (int i = 0; i < COLLIDING_HASHES; i++) {
        for (int j = 0; j < i; j++) {
          assertThat(addresses.get(i).getBytes()).isNotEqualTo(addresses.get(j).getBytes());
        }
      }
    }

    @Test
    void warmedUpAddressesResistHashCollisionFlood() {
      final MessageFrame frame = newFrame(Address.ZERO);
      final List<Address> addresses = new ArrayList<>(COLLIDING_HASHES);
      for (long i = 0; i < COLLIDING_HASHES; i++) {
        addresses.add(collidingAddress(i));
      }

      AtomicInteger integer = new AtomicInteger();
      assertTimeoutPreemptively(
          ofSeconds(2),
          () -> {
            for (final Address address : addresses) {
              frame.warmUpAddress(address);
              integer.incrementAndGet();
            }
          },
          () -> "stopped at iteration=" + integer);

      for (final Address address : addresses) {
        assertThat(frame.isAddressWarm(address)).isTrue();
      }
    }
  }
}
