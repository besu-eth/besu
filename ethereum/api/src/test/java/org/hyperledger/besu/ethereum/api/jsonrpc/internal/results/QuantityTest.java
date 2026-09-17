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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.Test;

public class QuantityTest {

  // --- create(int) ---

  @Test
  public void createInt_zero() {
    assertThat(Quantity.create(0)).isEqualTo("0x0");
  }

  @Test
  public void createInt_one() {
    assertThat(Quantity.create(1)).isEqualTo("0x1");
  }

  @Test
  public void createInt_maxValue() {
    assertThat(Quantity.create(Integer.MAX_VALUE))
        .isEqualTo("0x" + Integer.toHexString(Integer.MAX_VALUE));
  }

  // --- create(long) ---

  @Test
  public void createLong_zero() {
    assertThat(Quantity.create(0L)).isEqualTo("0x0");
  }

  @Test
  public void createLong_one() {
    assertThat(Quantity.create(1L)).isEqualTo("0x1");
  }

  @Test
  public void createLong_maxValue() {
    assertThat(Quantity.create(Long.MAX_VALUE)).isEqualTo("0x" + Long.toHexString(Long.MAX_VALUE));
  }

  // --- create(byte) ---

  @Test
  public void createByte_zero() {
    assertThat(Quantity.create((byte) 0)).isEqualTo("0x0");
  }

  @Test
  public void createByte_one() {
    assertThat(Quantity.create((byte) 1)).isEqualTo("0x1");
  }

  @Test
  public void createByte_maxPositive() {
    assertThat(Quantity.create(Byte.MAX_VALUE)).isEqualTo("0x7f");
  }

  // --- create(Bytes) ---

  @Test
  public void createBytes_empty() {
    assertThat(Quantity.create(Bytes.EMPTY)).isEqualTo("0x0");
  }

  @Test
  public void createBytes_zero() {
    assertThat(Quantity.create(Bytes.of(0x00))).isEqualTo("0x0");
  }

  @Test
  public void createBytes_oneByte() {
    assertThat(Quantity.create(Bytes.of(0x01))).isEqualTo("0x1");
  }

  @Test
  public void createBytes_leadingZerosStripped() {
    assertThat(Quantity.create(Bytes.fromHexString("0x000001"))).isEqualTo("0x1");
  }

  @Test
  public void createBytes_multipleBytes() {
    assertThat(Quantity.create(Bytes.fromHexString("0x0100"))).isEqualTo("0x100");
  }

  // --- create(byte[]) ---

  @Test
  public void createByteArray_zero() {
    assertThat(Quantity.create(new byte[] {0x00})).isEqualTo("0x0");
  }

  @Test
  public void createByteArray_leadingZerosStripped() {
    assertThat(Quantity.create(new byte[] {0x00, 0x00, 0x01})).isEqualTo("0x1");
  }

  @Test
  public void createByteArray_multipleBytes() {
    assertThat(Quantity.create(new byte[] {0x01, 0x00})).isEqualTo("0x100");
  }

  // --- create(BigInteger) ---

  @Test
  public void createBigInteger_zero() {
    assertThat(Quantity.create(BigInteger.ZERO)).isEqualTo("0x0");
  }

  @Test
  public void createBigInteger_one() {
    assertThat(Quantity.create(BigInteger.ONE)).isEqualTo("0x1");
  }

  @Test
  public void createBigInteger_large() {
    final BigInteger value = BigInteger.TWO.pow(128);
    assertThat(Quantity.create(value)).isEqualTo("0x" + value.toString(16));
  }

  // --- create(UInt256Value) ---

  @Test
  public void createUInt256_null() {
    assertThat(Quantity.create((UInt256) null)).isNull();
  }

  @Test
  public void createUInt256_zero() {
    assertThat(Quantity.create(UInt256.ZERO)).isEqualTo("0x0");
  }

  @Test
  public void createUInt256_one() {
    assertThat(Quantity.create(UInt256.ONE)).isEqualTo("0x1");
  }

  @Test
  public void createUInt256_maxValue() {
    assertThat(Quantity.create(UInt256.MAX_VALUE))
        .startsWith("0x")
        .hasSize(66); // 0x + 64 hex chars
  }

  // --- create(UInt64Value) ---

  @Test
  public void createUInt64_null() {
    assertThat(Quantity.create((UInt64) null)).isEqualTo("0x0");
  }

  @Test
  public void createUInt64_zero() {
    assertThat(Quantity.create(UInt64.ZERO)).isEqualTo("0x0");
  }

  @Test
  public void createUInt64_one() {
    assertThat(Quantity.create(UInt64.ONE)).startsWith("0x");
  }

  // --- longToPaddedHex ---

  @Test
  public void longToPaddedHex_zero() {
    assertThat(Quantity.longToPaddedHex(0L, 4)).isEqualTo("0x00000000");
  }

  @Test
  public void longToPaddedHex_padded() {
    assertThat(Quantity.longToPaddedHex(1L, 4)).isEqualTo("0x00000001");
  }

  @Test
  public void longToPaddedHex_full() {
    assertThat(Quantity.longToPaddedHex(0xdeadbeefL, 4)).isEqualTo("0xdeadbeef");
  }

  // --- isValid ---

  @Test
  public void isValid_withPrefix() {
    assertThat(Quantity.isValid("0x1")).isTrue();
  }

  @Test
  public void isValid_withoutPrefix() {
    assertThat(Quantity.isValid("1")).isFalse();
  }

  @Test
  public void isValid_zeroHex() {
    assertThat(Quantity.isValid("0x0")).isTrue();
  }
}
