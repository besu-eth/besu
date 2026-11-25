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
package org.hyperledger.besu.ethereum.util;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.internal.AddressStorageSlotKey;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes32;

/**
 * Crafts storage slots whose {@link AddressStorageSlotKey} hash codes either all coincide or are
 * all distinct, so the TLOAD/TSTORE benchmarks can measure the degenerate and the ordinary case.
 *
 * <p>The hash is {@code h = additive + SUM(lane_i * seed_i) (mod 2^64)} over the key's 32-bit
 * lanes, with {@code hashCode == (int)(h >>> 32)}. Fix every lane but one and the hash code becomes
 * {@code (K + x * S) >>> 32}, where {@code K} folds in the address and the fixed lanes, {@code S}
 * is the free lane's seed and {@code x} that lane's value. Pinning the hash code to a chosen {@code
 * u} is therefore exactly
 *
 * <pre>  find x in [0, 2^32) with (K + x*S) mod 2^64 in [u * 2^32, (u+1) * 2^32)</pre>
 *
 * <p>a lattice point inside a 2^32 x 2^32 box. The box holds about one point, so a Gauss-reduced
 * basis plus Babai rounding lands on it in O(log) time - deterministic, no scanning. Varying the
 * other lanes gives a fresh {@code K} and therefore a fresh key on the same hash code; roughly 90%
 * of them yield a solution, so the pools below just skip the misses.
 *
 * <p><strong>This class reads the seeds by reflection, and that is the whole point.</strong> The
 * hash it attacks used to sum 64-bit words, which let anyone solve for a colliding slot in closed
 * form with no seed knowledge at all: a change confined to the top bits of a word could only reach
 * the top bits of the accumulator, so flipping the top bit of two words canceled for every possible
 * seed. Lanes narrower than the seeds remove that shortcut, and the strongest adversary left is the
 * one modeled here - one who already knows the seeds and still has to solve a lattice problem per
 * key.
 */
public final class AddressStorageSlotKeyHashing {

  private AddressStorageSlotKeyHashing() {
    throw new UnsupportedOperationException("utility class");
  }

  /** A 32-byte slot is 8 lanes, a 20-byte address is 5. */
  private static final int SLOT_LANES = 8;

  private static final int ADDRESS_LANES = 5;

  /** Index of the additive seed in the production seed array. */
  private static final int BASE_LANE = SLOT_LANES + ADDRESS_LANES;

  /** Lane solved for. Any lane works; the others carry the nonce that varies the key. */
  private static final int SOLVER_LANE = 0;

  /** Lanes carrying the nonce that makes each key on a given hash code a different key. */
  private static final int NONCE_LOW_LANE = 1;

  private static final int NONCE_HIGH_LANE = 2;

  /** Hash code every colliding key is forced onto. The value itself is arbitrary. */
  private static final int COLLIDING_HASH_CODE = 0x2BADF00D;

  /** Misses are independent at roughly one in ten, so this bound is never reached in practice. */
  private static final int MAX_NONCE_RETRIES = 64;

  private static final BigInteger TWO_64 = BigInteger.ONE.shiftLeft(64);
  private static final BigInteger TWO_32 = BigInteger.ONE.shiftLeft(32);
  private static final BigInteger MASK_64 = TWO_64.subtract(BigInteger.ONE);

  private static long[] seeds;

  private static final Map<Address, Crafter> CRAFTERS = new HashMap<>();

  /** Everything that depends only on the address, so it is computed once per address. */
  private static final class Crafter {
    private final long addressPart;
    private final BigInteger multiplier;
    private final BigInteger[][] basis;
    private final List<Bytes32> collidingPool = new ArrayList<>();
    private final Map<Integer, Bytes32> distinctPool = new HashMap<>();
    private long collidingNonce;

    Crafter(final long addressPart, final BigInteger multiplier) {
      this.addressPart = addressPart;
      this.multiplier = multiplier;
      this.basis =
          reduce(new BigInteger[][] {{BigInteger.ONE, multiplier}, {BigInteger.ZERO, TWO_64}});
    }
  }

  /** Lane multipliers occupy 0..12, the additive seed sits at {@link #BASE_LANE}. */
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

  private static int scatter(final int index) {
    int h = index;
    h ^= h >>> 16;
    h *= 0x85EBCA6B;
    h ^= h >>> 13;
    h *= 0xC2B2AE35;
    h ^= h >>> 16;
    return h;
  }

  /**
   * A slot whose hash code under this address is unique to {@code index}, so distinct indices never
   * share a bucket. Hash codes generated by this method are scattered instead of sequential to work
   * around certain optimizations that rarely happen in a real life workload.
   *
   * @param address address the slot will be looked up under
   * @param index free variable selecting which slot to return
   * @return the slot
   * @throws Exception if the seeds cannot be read or no distinct slot has been found
   */
  public static synchronized Bytes32 distinctHash(final Address address, final int index)
      throws Exception {
    final Crafter crafter = crafter(address);
    final Bytes32 cached = crafter.distinctPool.get(index);
    if (cached != null) {
      return cached;
    }
    final int hashCode = scatter(index);
    // About one nonce in ten leaves the box empty; a handful of retries is always enough.
    for (long nonce = 0; nonce < MAX_NONCE_RETRIES; nonce++) {
      final Bytes32 slot = craft(crafter, hashCode, nonce);
      if (slot != null) {
        crafter.distinctPool.put(index, slot);
        return slot;
      }
    }
    throw new IllegalStateException("no distinct slot found for hash code " + hashCode);
  }

  /**
   * A slot that shares its hash code - and therefore its bucket, at every table size - with every
   * other index under this address, while remaining a different key.
   *
   * @param address address the slot will be looked up under
   * @param index free variable selecting which slot to return
   * @return the slot
   * @throws Exception if the seeds cannot be read or no colliding slot has been found
   */
  public static synchronized Bytes32 collidingHash(final Address address, final int index)
      throws Exception {
    final Crafter crafter = crafter(address);
    // Counted consecutively rather than in total: filling a large pool legitimately needs about
    // one nonce per key plus the misses, so a total bound would trip on a healthy run.
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

  /**
   * Builds the slot with the given hash code, carrying {@code nonce} in the lanes that are not
   * being solved for. Returns null when the box holds no lattice point, which happens for roughly
   * one nonce in ten; callers simply try the next one.
   */
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

    final ByteBuffer slot = ByteBuffer.wrap(new byte[Bytes32.SIZE]).order(ByteOrder.LITTLE_ENDIAN);
    for (int lane = 0; lane < SLOT_LANES; lane++) {
      slot.putInt(lane * Integer.BYTES, lanes[lane]);
    }
    return Bytes32.wrap(slot.array());
  }

  /**
   * Finds {@code x} in {@code [0, 2^32)} with {@code (fixed + x * multiplier) mod 2^64} inside the
   * 2^32-wide window starting at {@code window}, or -1 if the box is empty.
   */
  private static long solve(
      final BigInteger fixed,
      final BigInteger multiplier,
      final BigInteger window,
      final BigInteger[][] basis) {
    final BigInteger yCentre = window.subtract(fixed).add(TWO_32.shiftRight(1));
    final BigInteger xCentre = TWO_32.shiftRight(1);

    BigInteger det = basis[0][0].multiply(basis[1][1]).subtract(basis[0][1].multiply(basis[1][0]));
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

    // The reduced basis puts the right point within a step or two of the rounded coefficients.
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

  /** Lagrange-Gauss reduction of a two dimensional basis. */
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

  /** Round-half-away division, for a strictly positive divisor. */
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
}
