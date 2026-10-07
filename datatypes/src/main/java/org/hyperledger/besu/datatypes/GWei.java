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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.BaseUInt64Value;
import org.apache.tuweni.units.bigints.UInt64;

/** A particular quantity of GWei, the Ethereum currency. */
public final class GWei extends BaseUInt64Value<GWei> implements Quantity {

  /** The constant ZERO. */
  public static final GWei ZERO = of(0);

  /** The constant ONE. */
  public static final GWei ONE = of(1);

  /** The constant MAX_GWEI. */
  public static final GWei MAX_GWEI = of(UInt64.MAX_VALUE);

  /** The constant GWEI_TO_WEI_MULTIPLIER. */
  private static final BigInteger GWEI_TO_WEI_MULTIPLIER = BigInteger.valueOf(1_000_000_000L);

  /**
   * Instantiates a new GWei.
   *
   * @param value the value
   */
  GWei(final UInt64 value) {
    super(value, GWei::new);
  }

  private GWei(final long v) {
    this(UInt64.valueOf(v));
  }

  private GWei(final BigInteger v) {
    this(UInt64.valueOf(v));
  }

  private GWei(final String hexString) {
    this(UInt64.fromHexString(hexString));
  }

  /**
   * Returns GWei of value.
   *
   * @param value the value
   * @return the GWei
   */
  public static GWei of(final long value) {
    return new GWei(value);
  }

  /**
   * Returns GWei of BigInteger value.
   *
   * @param value the value
   * @return the GWei
   */
  public static GWei of(final BigInteger value) {
    return new GWei(value);
  }

  /**
   * Returns GWei of UInt64 value.
   *
   * @param value the value
   * @return the GWei
   */
  @JsonCreator
  public static GWei of(final UInt64 value) {
    return new GWei(value);
  }

  /**
   * Wrap Bytes into GWei.
   *
   * @param value the value
   * @return the g wei
   */
  public static GWei wrap(final Bytes value) {
    return new GWei(UInt64.fromBytes(value));
  }

  /**
   * From hex string to GWei.
   *
   * @param str the hex string
   * @return the GWei
   */
  public static GWei fromHexString(final String str) {
    return new GWei(str);
  }

  /**
   * Convert GWei to Wei
   *
   * @return Wei
   */
  public Wei getAsWei() {
    return Wei.of(getAsBigInteger().multiply(GWEI_TO_WEI_MULTIPLIER));
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
  @JsonValue
  public String toShortHexString() {
    return super.isZero() ? "0x0" : super.toShortHexString();
  }
}
