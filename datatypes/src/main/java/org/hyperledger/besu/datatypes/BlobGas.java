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

/** A particular quantity of BlobGas */
public final class BlobGas extends BaseUInt64Value<BlobGas> implements Quantity {

  /** The constant ZERO. */
  public static final BlobGas ZERO = of(0);

  /** The constant ONE. */
  public static final BlobGas ONE = of(1);

  /** The constant MAX_BLOB_GAS. */
  public static final BlobGas MAX_BLOB_GAS = of(UInt64.MAX_VALUE);

  /**
   * Instantiates a new BlobGas.
   *
   * @param value the value
   */
  BlobGas(final UInt64 value) {
    super(value, BlobGas::new);
  }

  private BlobGas(final long v) {
    this(UInt64.valueOf(v));
  }

  private BlobGas(final BigInteger v) {
    this(UInt64.valueOf(v));
  }

  private BlobGas(final String hexString) {
    this(UInt64.fromHexString(hexString));
  }

  /**
   * blob gas of value.
   *
   * @param value the value
   * @return the blob gas
   */
  public static BlobGas of(final long value) {
    return new BlobGas(value);
  }

  /**
   * blob gas of value.
   *
   * @param value the value
   * @return the blob gas
   */
  public static BlobGas of(final BigInteger value) {
    return new BlobGas(value);
  }

  /**
   * blob gas of value.
   *
   * @param value the value
   * @return the blob gas
   */
  public static BlobGas of(final UInt64 value) {
    return new BlobGas(value);
  }

  /**
   * blob gas of value.
   *
   * @param value the value
   * @return the blob gas
   */
  public static BlobGas ofNumber(final Number value) {
    return new BlobGas((BigInteger) value);
  }

  /**
   * Wrap blob gas.
   *
   * @param value the value
   * @return the blob gas
   */
  public static BlobGas wrap(final Bytes value) {
    return new BlobGas(UInt64.fromBytes(value));
  }

  /**
   * From hex string to blob gas.
   *
   * @param str the str
   * @return the blob gas
   */
  @JsonCreator
  public static BlobGas fromHexString(final String str) {
    return new BlobGas(str);
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

  /**
   * From quantity to blob gas.
   *
   * @param quantity the quantity
   * @return the blob gas
   */
  public static BlobGas fromQuantity(final Quantity quantity) {
    return BlobGas.of(quantity.getAsBigInteger());
  }
}
