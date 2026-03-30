/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import java.math.BigInteger;
import java.util.Objects;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.apache.tuweni.units.bigints.UInt256Value;
import org.apache.tuweni.units.bigints.UInt64Value;

/**
 * Utility for formatting "quantity" fields and results to be returned. Quantity fields are
 * represented as minimal length hex strings with no zero-padding. There is one exception to this
 * rule: quantities equal to zero are represented by the hex string "0x0".
 */
public class Quantity {

  private static final String HEX_PREFIX = "0x";
  public static final String HEX_ZERO = "0x0";

  private Quantity() {}

  public static String create(final UInt256Value<?> value) {
    return uint256ToHex(value);
  }

  public static String create(final UInt64Value<?> value) {
    return (value == null || value.isZero()) ? HEX_ZERO : value.toMinimalBytes().toShortHexString();
  }

  public static String create(final int value) {
    return HEX_PREFIX + Integer.toHexString(value);
  }

  public static String create(final long value) {
    return HEX_PREFIX + Long.toHexString(value);
  }

  public static String create(final byte value) {
    return HEX_PREFIX + Integer.toHexString(value);
  }

  public static String create(final Bytes value) {
    return uint256ToHex(UInt256.fromBytes(Bytes32.leftPad(value)));
  }

  public static String create(final byte[] value) {
    return create(Bytes.wrap(value));
  }

  public static String create(final BigInteger value) {
    return uint256ToHex(UInt256.valueOf(value));
  }

  /**
   * Fixed-length bytes sequences and should be returned as hex strings zero-padded to the expected
   * length.
   *
   * @param val the value to encode in the string
   * @param byteLength the number of bytes to be represented in the output string.
   * @return A zero-padded string containing byteLength * 2 characters, not including the 0x prefix
   */
  public static String longToPaddedHex(final long val, final int byteLength) {
    final String formatted = Long.toHexString(val);
    final String zeroPadding = "0".repeat(byteLength * 2 - formatted.length());
    return String.format("%s%s%s", HEX_PREFIX, zeroPadding, formatted);
  }

  /**
   * Checks if value is prefixed with '0x'
   *
   * @param value that is checked
   * @return true if value is prefixed with '0x', false otherwise
   */
  public static boolean isValid(final String value) {
    return value.startsWith(HEX_PREFIX);
  }

  private static String uint256ToHex(final UInt256Value<?> value) {
    return value == null ? null : formatMinimalValue(value.toMinimalBytes().toShortHexString());
  }

  private static String formatMinimalValue(final String hexValue) {
    final String prefixedHexString = prefixHexNotation(hexValue);
    return Objects.equals(prefixedHexString, HEX_PREFIX) ? HEX_ZERO : prefixedHexString;
  }

  private static String prefixHexNotation(final String hexValue) {
    return hexValue.startsWith(HEX_PREFIX) ? hexValue : HEX_PREFIX + hexValue;
  }
}
