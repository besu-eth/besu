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
package org.hyperledger.besu.ethereum.trie;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;

/**
 * This class helps to generate ranges according to several parameters (the start and the end of the
 * range, its size)
 */
public class RangeManager {

  public static final Bytes32 MIN_RANGE = Bytes32.wrap(Hash.wrap(Bytes32.ZERO).getBytes());
  public static final Bytes32 MAX_RANGE =
      Bytes32.wrap(
          Hash.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
              .getBytes());

  private RangeManager() {}

  public static int getRangeCount(
      final Bytes32 min, final Bytes32 max, final NavigableMap<Bytes32, Bytes> items) {
    if (min.equals(MIN_RANGE) && max.equals(MAX_RANGE)) {
      return MAX_RANGE
          .toUnsignedBigInteger()
          .divide(items.lastKey().toUnsignedBigInteger().subtract(min.toUnsignedBigInteger()))
          .intValue();
    }
    return 1;
  }

  public static Map<Bytes32, Bytes32> generateAllRanges(final int sizeRange) {
    if (sizeRange == 1) {
      return Map.ofEntries(Map.entry(MIN_RANGE, MAX_RANGE));
    }
    return generateRanges(
        MIN_RANGE.toUnsignedBigInteger(), MAX_RANGE.toUnsignedBigInteger(), sizeRange);
  }

  /**
   * Generate a range
   *
   * @param min start of the range in bytes
   * @param max the max end of the range in bytes
   * @param nbRange number of ranges
   * @return the start and end of the generated range
   */
  public static Map<Bytes32, Bytes32> generateRanges(
      final Bytes32 min, final Bytes32 max, final int nbRange) {
    return generateRanges(min.toUnsignedBigInteger(), max.toUnsignedBigInteger(), nbRange);
  }

  /**
   * Generate a range
   *
   * @param min start of the range
   * @param max the max end of the range
   * @param nbRange number of ranges
   * @return the start and end of the generated range
   */
  public static Map<Bytes32, Bytes32> generateRanges(
      final BigInteger min, final BigInteger max, final int nbRange) {
    final BigInteger rangeSize = max.subtract(min).divide(BigInteger.valueOf(nbRange));
    final TreeMap<Bytes32, Bytes32> ranges = new TreeMap<>();
    if (min.compareTo(max) > 0) {
      return ranges;
    }
    if (min.equals(max) || nbRange == 1) {
      ranges.put(format(min), format(max));
      return ranges;
    }
    BigInteger currentStart = min;
    BigInteger currentEnd = min;
    while (max.subtract(currentEnd).compareTo(rangeSize) > 0) {
      currentEnd = currentStart.add(rangeSize);
      ranges.put(format(currentStart), format(currentEnd));
      currentStart = currentStart.add(rangeSize).add(BigInteger.ONE);
    }
    if (max.subtract(currentEnd).compareTo(BigInteger.ZERO) > 0) {
      currentEnd = currentStart.add(max.subtract(currentEnd)).subtract(BigInteger.ONE);
      ranges.put(format(currentStart), format(currentEnd));
    }
    return ranges;
  }

  /**
   * Helps to create a new range according to the last data obtained. This happens when a peer
   * doesn't return all of the data in a range.
   *
   * @param worldstateRootHash the root hash
   * @param proofs proof received
   * @param endKeyHash the end of the range initially wanted
   * @param receivedKeys the keys already returned by the responder
   * @return begin of the new range
   */
  public static Optional<Bytes32> findNewBeginElementInRange(
      final Bytes32 worldstateRootHash,
      final List<Bytes> proofs,
      final NavigableMap<Bytes32, Bytes> receivedKeys,
      final Bytes32 endKeyHash) {
    return findNewBeginElementInRange(
        worldstateRootHash, proofs, receivedKeys, MIN_RANGE, endKeyHash);
  }

  /**
   * Helps to create a new range according to the last data obtained. This happens when a peer
   * doesn't return all of the data in a range.
   *
   * @param worldstateRootHash the root hash
   * @param proofs proof received
   * @param receivedKeys the keys already returned by the responder
   * @param startKeyHash the start of the range initially wanted (used as the probe origin when no
   *     keys were returned)
   * @param endKeyHash the end of the range initially wanted
   * @return begin of the new range
   */
  public static Optional<Bytes32> findNewBeginElementInRange(
      final Bytes32 worldstateRootHash,
      final List<Bytes> proofs,
      final NavigableMap<Bytes32, Bytes> receivedKeys,
      final Bytes32 startKeyHash,
      final Bytes32 endKeyHash) {
    if (!receivedKeys.isEmpty() && receivedKeys.lastKey().compareTo(endKeyHash) >= 0) {
      return Optional.empty();
    }
    final Bytes32 probeStart = receivedKeys.isEmpty() ? startKeyHash : receivedKeys.lastKey();
    final Map<Bytes32, Bytes> proofsEntries = new HashMap<>();
    for (Bytes proof : proofs) {
      proofsEntries.put(Bytes32.wrap(Hash.hash(proof).getBytes()), proof);
    }
    final InnerNodeDiscoveryManager<Bytes> manager =
        new InnerNodeDiscoveryManager<>(
            (location, key) -> Optional.ofNullable(proofsEntries.get(key)),
            Function.identity(),
            Function.identity(),
            probeStart,
            endKeyHash,
            false);
    final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
        new StoredMerklePatriciaTrie<>(manager, worldstateRootHash);

    try {
      storageTrie.visitAll(bytesNode -> {});
    } catch (MerkleTrieException e) {
      return Optional.of(InnerNodeDiscoveryManager.decodePath(e.getLocation()));
    }
    return Optional.empty();
  }

  private static Bytes32 format(final BigInteger data) {
    return Bytes32.leftPad(Bytes.of(data.toByteArray()).trimLeadingZeros());
  }

  /**
   * Checks if a given location is within a specified range. This method determines whether a given
   * location (represented as {@link Bytes}) falls within the range defined by a start key path and
   * an end key path.
   *
   * @param location The location to check, represented as {@link Bytes}.
   * @param startKeyPath The start of the range as path, represented as {@link Bytes}.
   * @param endKeyPath The end of the range as path, represented as {@link Bytes}.
   * @return {@code true} if the location is within the range (inclusive); {@code false} otherwise.
   */
  public static boolean isInRange(
      final Bytes location, final Bytes startKeyPath, final Bytes endKeyPath) {
    final MutableBytes path = MutableBytes.create(Bytes32.SIZE * 2);
    path.set(0, location);
    return !location.isEmpty()
        && Arrays.compare(path.toArrayUnsafe(), startKeyPath.toArrayUnsafe()) >= 0
        && Arrays.compare(path.toArrayUnsafe(), endKeyPath.toArrayUnsafe()) <= 0;
  }

  /**
   * Transforms a list of bytes into a path. This method processes a sequence of bytes, expanding
   * each byte into two separate bytes to form a new path. The resulting path will have twice the
   * length of the input byte sequence.
   *
   * @param bytes The byte sequence to be transformed into a path.
   * @return A {@link Bytes} object representing the newly formed path.
   */
  public static Bytes createPath(final Bytes bytes) {
    final MutableBytes path = MutableBytes.create(bytes.size() * 2);
    int j = 0;
    for (int i = 0; i < bytes.size(); i += 1, j += 2) {
      final byte b = bytes.get(i);
      path.set(j, (byte) ((b >>> 4) & 0x0f));
      path.set(j + 1, (byte) (b & 0x0f));
    }
    return path;
  }
}
