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
package org.hyperledger.besu.datatypes;

import static java.util.Objects.requireNonNull;

import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.math.BigInteger;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.jspecify.annotations.Nullable;

/**
 * The chain id, as introduced by EIP-155, or its absence for transactions that are not replay
 * protected.
 *
 * <p>A node runs on a single network for its entire life, so almost every chain id it sees is the
 * chain id of that network. Once the network chain id is registered at startup with {@link
 * #initNetworkChainId(ChainId)}, every factory method returns that same instance for that value
 * instead of allocating a new one, and the conversions to {@link BigInteger} and {@link Optional}
 * are served from a cache, so the chain id of transactions decoded from the network costs no
 * allocation. Zero, used by EIP-7702 authorizations valid on any chain, and the absent chain id are
 * shared too.
 *
 * <p>Values that fit in a {@code long}, which are the chain ids of all the known networks, are
 * stored as primitive, only larger values are backed by a {@link BigInteger}.
 *
 * <p>This class is a candidate to become a {@code value class} once Project Valhalla (JEP 401) is
 * available: it is final with only final fields, its equality is based only on its value, and its
 * instances are created only through factory methods, that are free to return a shared instance.
 * For this reason, instances must be compared with {@link #equals(Object)}, never by identity, and
 * must not be used for synchronization.
 */
public final class ChainId {
  private static final long ABSENT_MARKER = -1L;
  private static final long BIG_MARKER = -2L;

  private static final ChainId ABSENT = new ChainId(ABSENT_MARKER, null);
  private static final ChainId ZERO = new ChainId(0L, null);

  /** The chain id of the running network, with its conversions computed once. */
  private record NetworkChainId(
      ChainId chainId, BigInteger asBigInteger, Optional<BigInteger> asOptional) {}

  private static volatile @Nullable NetworkChainId network;

  /** The value when it fits in a non-negative long, otherwise one of the markers. */
  private final long longValue;

  /** Non-null only when the value does not fit in a long. */
  private final @Nullable BigInteger bigValue;

  private ChainId(final long longValue, final @Nullable BigInteger bigValue) {
    this.longValue = longValue;
    this.bigValue = bigValue;
  }

  /**
   * Registers the chain id of the running network, so that the factory methods return the given
   * instance every time they are asked for its value. It is meant to be called once at startup; a
   * later call replaces the registered chain id, which only affects which instance gets shared, not
   * the outcome of any comparison.
   *
   * @param chainId the chain id of the running network, absent clears the registration
   * @return the given chain id
   */
  public static ChainId initNetworkChainId(final ChainId chainId) {
    requireNonNull(chainId);
    if (chainId.isAbsent()) {
      network = null;
    } else {
      final BigInteger asBigInteger =
          chainId.bigValue != null ? chainId.bigValue : BigInteger.valueOf(chainId.longValue);
      network = new NetworkChainId(chainId, asBigInteger, Optional.of(asBigInteger));
    }
    return chainId;
  }

  /**
   * Returns the absent chain id, for transactions that are not replay protected.
   *
   * @return the absent chain id
   */
  public static ChainId absent() {
    return ABSENT;
  }

  /**
   * Returns the chain id with the given value.
   *
   * @param value the chain id value, must not be negative
   * @return the chain id
   */
  public static ChainId of(final long value) {
    if (value < 0) {
      throw new IllegalArgumentException("Negative chain id: " + value);
    }
    if (value == 0) {
      return ZERO;
    }
    final NetworkChainId n = network;
    if (n != null && n.chainId.longValue == value) {
      return n.chainId;
    }
    return new ChainId(value, null);
  }

  /**
   * Returns the chain id with the given value.
   *
   * @param value the chain id value, must not be negative
   * @return the chain id
   */
  public static ChainId of(final BigInteger value) {
    requireNonNull(value);
    if (value.signum() < 0) {
      throw new IllegalArgumentException("Negative chain id: " + value);
    }
    if (value.bitLength() < Long.SIZE) {
      return of(value.longValue());
    }
    final NetworkChainId n = network;
    if (n != null && value.equals(n.chainId.bigValue)) {
      return n.chainId;
    }
    return new ChainId(BIG_MARKER, value);
  }

  /**
   * Returns the chain id with the given value, or the absent chain id if the value is empty.
   *
   * @param maybeValue the optional chain id value
   * @return the chain id
   */
  public static ChainId fromOptional(final Optional<BigInteger> maybeValue) {
    return maybeValue.isPresent() ? of(maybeValue.get()) : ABSENT;
  }

  /**
   * Returns the chain id whose value is the given bytes, read as an unsigned big-endian integer.
   *
   * @param bytes the value bytes
   * @return the chain id
   */
  public static ChainId fromBytes(final Bytes bytes) {
    final Bytes trimmed = bytes.trimLeadingZeros();
    if (trimmed.size() < Long.BYTES || (trimmed.size() == Long.BYTES && trimmed.get(0) >= 0)) {
      return of(trimmed.toLong());
    }
    return of(trimmed.toUnsignedBigInteger());
  }

  /**
   * Returns the chain id represented by the given string, either a decimal or a {@code 0x} prefixed
   * hexadecimal number.
   *
   * @param value the string representation of the chain id
   * @return the chain id
   */
  public static ChainId fromString(final String value) {
    requireNonNull(value);
    if (value.startsWith("0x") || value.startsWith("0X")) {
      return fromBytes(Bytes.fromHexStringLenient(value));
    }
    return of(new BigInteger(value));
  }

  /**
   * Reads a chain id encoded as an RLP scalar, without allocating when the value is the network
   * chain id.
   *
   * @param input the RLP input
   * @return the chain id
   */
  public static ChainId readFrom(final RLPInput input) {
    if (input.nextSize() < Long.BYTES) {
      return of(input.readLongScalar());
    }
    return of(input.readBigIntegerScalar());
  }

  /**
   * Reads a chain id encoded as an RLP scalar of at most the given size, without allocating when
   * the value is the network chain id.
   *
   * @param input the RLP input
   * @param maxSizeBytes the maximum accepted size of the encoded value
   * @return the chain id
   */
  public static ChainId readFrom(final RLPInput input, final int maxSizeBytes) {
    if (input.nextSize() < Long.BYTES) {
      return of(input.readLongScalar());
    }
    return of(input.readBigIntegerScalar(maxSizeBytes));
  }

  /**
   * Writes this chain id as an RLP scalar.
   *
   * @param output the RLP output
   * @throws NoSuchElementException if this chain id is absent
   */
  public void writeTo(final RLPOutput output) {
    if (bigValue != null) {
      output.writeBigIntegerScalar(bigValue);
    } else {
      output.writeLongScalar(checkPresent());
    }
  }

  /**
   * Whether the chain id is present.
   *
   * @return true if the chain id is present
   */
  public boolean isPresent() {
    return longValue != ABSENT_MARKER;
  }

  /**
   * Whether the chain id is absent.
   *
   * @return true if the chain id is absent
   */
  public boolean isAbsent() {
    return longValue == ABSENT_MARKER;
  }

  /**
   * Whether the chain id is zero, that EIP-7702 uses for authorizations valid on any chain.
   *
   * @return true if the chain id is zero
   */
  public boolean isZero() {
    return longValue == 0L;
  }

  /**
   * Whether the chain id is present and its value fits in a long.
   *
   * @return true if the value can be read with {@link #toLong()}
   */
  public boolean fitsLong() {
    return longValue >= 0L;
  }

  /**
   * Returns the value as a long.
   *
   * @return the value
   * @throws NoSuchElementException if this chain id is absent
   * @throws ArithmeticException if the value does not fit in a long
   */
  public long toLong() {
    if (bigValue != null) {
      throw new ArithmeticException("Chain id does not fit in a long: " + bigValue);
    }
    return checkPresent();
  }

  /**
   * Returns the value as a {@link BigInteger}.
   *
   * @return the value
   * @throws NoSuchElementException if this chain id is absent
   */
  public BigInteger toBigInteger() {
    if (bigValue != null) {
      return bigValue;
    }
    final long value = checkPresent();
    final NetworkChainId n = network;
    if (n != null && n.chainId.longValue == value) {
      return n.asBigInteger;
    }
    return BigInteger.valueOf(value);
  }

  /**
   * Returns the value as an {@link Optional} of {@link BigInteger}, that is empty if the chain id
   * is absent.
   *
   * @return the optional value
   */
  public Optional<BigInteger> toOptionalBigInteger() {
    if (isAbsent()) {
      return Optional.empty();
    }
    final NetworkChainId n = network;
    if (n != null && equals(n.chainId)) {
      return n.asOptional;
    }
    return Optional.of(toBigInteger());
  }

  /**
   * Returns the value as a hexadecimal string with no leading zeros, prefixed by {@code 0x}.
   *
   * @return the hexadecimal representation of the value
   * @throws NoSuchElementException if this chain id is absent
   */
  public String toHexString() {
    if (bigValue != null) {
      return "0x" + bigValue.toString(16);
    }
    return "0x" + Long.toHexString(checkPresent());
  }

  private long checkPresent() {
    if (longValue == ABSENT_MARKER) {
      throw new NoSuchElementException("Chain id is absent");
    }
    return longValue;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ChainId other)) {
      return false;
    }
    // values that fit in a long are never backed by a BigInteger, so the representation is unique
    return longValue == other.longValue
        && (bigValue == null ? other.bigValue == null : bigValue.equals(other.bigValue));
  }

  @Override
  public int hashCode() {
    return bigValue != null ? bigValue.hashCode() : Long.hashCode(longValue);
  }

  /**
   * Returns the decimal representation of the value, or {@code absent}.
   *
   * @return the string representation
   */
  @Override
  public String toString() {
    if (bigValue != null) {
      return bigValue.toString();
    }
    return isAbsent() ? "absent" : Long.toString(longValue);
  }
}
