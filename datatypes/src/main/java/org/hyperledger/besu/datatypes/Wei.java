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
package org.hyperledger.besu.datatypes;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Locale;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.BaseUInt256Value;
import org.apache.tuweni.units.bigints.UInt256;

/** A particular quantity of Wei, the Ethereum currency. */
public final class Wei extends BaseUInt256Value<Wei> implements Quantity {

  /** The constant ZERO. */
  public static final Wei ZERO = of(0);

  /** The constant ONE. */
  public static final Wei ONE = of(1);

  /** The constant MAX_WEI. */
  public static final Wei MAX_WEI = of(UInt256.MAX_VALUE);

  /**
   * Instantiates a new Wei.
   *
   * @param value the value
   */
  Wei(final UInt256 value) {
    super(value, Wei::new);
  }

  private Wei(final long v) {
    this(UInt256.valueOf(v));
  }

  private Wei(final BigInteger v) {
    this(UInt256.valueOf(v));
  }

  private Wei(final String hexString) {
    this(UInt256.fromHexString(hexString));
  }

  /**
   * Wei of value.
   *
   * @param value the value
   * @return the wei
   */
  public static Wei of(final long value) {
    return new Wei(value);
  }

  /**
   * Wei of value.
   *
   * @param value the value
   * @return the wei
   */
  public static Wei of(final BigInteger value) {
    return new Wei(value);
  }

  /**
   * Wei of value.
   *
   * @param value the value
   * @return the wei
   */
  public static Wei of(final UInt256 value) {
    return new Wei(value);
  }

  /**
   * Wei of value.
   *
   * @param value the value
   * @return the wei
   */
  public static Wei ofNumber(final Number value) {
    return new Wei((BigInteger) value);
  }

  /**
   * Wrap wei.
   *
   * @param value the value
   * @return the wei
   */
  public static Wei wrap(final Bytes value) {
    return new Wei(UInt256.fromBytes(value));
  }

  /**
   * From hex string to wei.
   *
   * @param str the str
   * @return the wei
   */
  public static Wei fromHexString(final String str) {
    return new Wei(str);
  }

  /**
   * From eth to wei.
   *
   * @param eth the eth
   * @return the wei
   */
  public static Wei fromEth(final long eth) {
    return Wei.of(BigInteger.valueOf(eth).multiply(BigInteger.TEN.pow(18)));
  }

  @Override
  public BigInteger getAsBigInteger() {
    return toBigInteger();
  }

  @Override
  public String toHexString() {
    return super.toHexString();
  }

  @Override
  public String toShortHexString() {
    return super.isZero() ? "0x0" : super.toShortHexString();
  }

  /**
   * From quantity to wei.
   *
   * @param quantity the quantity
   * @return the wei
   */
  public static Wei fromQuantity(final Quantity quantity) {
    return Wei.wrap((Bytes) quantity);
  }

  /**
   * Wei to human-readable string.
   *
   * @return the string
   */
  public String toHumanReadableString() {
    return toHumanReadableStringWithPadding(1);
  }

  /**
   * Wei to human-readable string, with padding
   *
   * @return the string
   */
  public String toHumanReadablePaddedString() {
    return toHumanReadableStringWithPadding(6);
  }

  /**
   * Returns a human-readable String, the number of returned characters depends on the width
   * parameter
   *
   * @param width the number of digits to use
   * @return a human-readable String
   */
  private String toHumanReadableStringWithPadding(final int width) {
    final BigInteger amount = toBigInteger();
    final int numOfDigits = amount.toString().length();
    final Unit preferredUnit = Unit.getPreferred(numOfDigits);
    final double res = amount.doubleValue() / preferredUnit.divisor;
    return String.format("%" + width + "." + preferredUnit.decimals + "f %s", res, preferredUnit);
  }

  /** The enum Unit. */
  enum Unit {
    /** Wei unit. */
    Wei(0, 0),
    /** K wei unit. */
    KWei(3),
    /** M wei unit. */
    MWei(6),
    /** G wei unit. */
    GWei(9),
    /** Szabo unit. */
    Szabo(12),
    /** Finney unit. */
    Finney(15),
    /** Ether unit. */
    Ether(18),
    /** K ether unit. */
    KEther(21),
    /** M ether unit. */
    MEther(24),
    /** G ether unit. */
    GEther(27),
    /** T ether unit. */
    TEther(30);

    /** The Pow. */
    final int pow;

    /** The Divisor. */
    final double divisor;

    /** The Decimals. */
    final int decimals;

    Unit(final int pow) {
      this(pow, 2);
    }

    Unit(final int pow, final int decimals) {
      this.pow = pow;
      this.decimals = decimals;
      this.divisor = Math.pow(10, pow);
    }

    /**
     * Gets preferred.
     *
     * @param numOfDigits the num of digits
     * @return the preferred
     */
    static Unit getPreferred(final int numOfDigits) {
      return Arrays.stream(values())
          .filter(u -> numOfDigits <= u.pow + 3)
          .findFirst()
          .orElse(TEther);
    }

    @Override
    public String toString() {
      return name().toLowerCase(Locale.ROOT);
    }
  }
}
