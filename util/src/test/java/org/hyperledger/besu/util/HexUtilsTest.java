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
package org.hyperledger.besu.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class HexUtilsTest {

  @Test
  public void fromHexStringDecodesLikeTuweni() {
    for (final String hex :
        new String[] {"0x", "0x00", "0x0aFf", "0x" + Bytes.random(1000).toUnprefixedHexString()}) {
      assertThat(HexUtils.fromHexString(hex)).isEqualTo(Bytes.fromHexString(hex));
    }
    // other forms are left to tuweni
    for (final String hex : new String[] {"0a0b", ""}) {
      assertThat(HexUtils.fromHexString(hex)).isEqualTo(Bytes.fromHexString(hex));
    }
  }

  @Test
  public void fromHexStringRejectsWhatTuweniRejects() {
    for (final String hex : new String[] {"0x0g", "0x1", "0X0a"}) {
      assertThatThrownBy(() -> HexUtils.fromHexString(hex))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(catchMessage(() -> Bytes.fromHexString(hex)));
    }
  }

  private static String catchMessage(final Runnable decode) {
    try {
      decode.run();
      return null;
    } catch (final IllegalArgumentException e) {
      return e.getMessage();
    }
  }

  @Test
  public void testToFastHexEmptyWithPrefix() {
    Bytes emptyBytes = Bytes.EMPTY;
    String result = HexUtils.toFastHex(emptyBytes, true);
    assertEquals("0x", result, "Expected '0x' for an empty byte array with prefix");
  }

  @Test
  public void testToFastHexEmptyWithoutPrefix() {
    Bytes emptyBytes = Bytes.EMPTY;
    String result = HexUtils.toFastHex(emptyBytes, false);
    assertEquals("", result, "Expected '' for an empty byte array without prefix");
  }

  @Test
  public void testToFastHexSingleByteWithPrefix() {
    Bytes bytes = Bytes.fromHexString("0x01");
    String result = HexUtils.toFastHex(bytes, true);
    assertEquals("0x01", result, "Expected '0x01' for the byte 0x01 with prefix");
  }

  @Test
  public void testToFastHexSingleByteWithoutPrefix() {
    Bytes bytes = Bytes.fromHexString("0x01");
    String result = HexUtils.toFastHex(bytes, false);
    assertEquals("01", result, "Expected '01' for the byte 0x01 without prefix");
  }

  @Test
  public void testToFastHexMultipleBytesWithPrefix() {
    Bytes bytes = Bytes.fromHexString("0x010203");
    String result = HexUtils.toFastHex(bytes, true);
    assertEquals("0x010203", result, "Expected '0x010203' for the byte array 0x010203 with prefix");
  }

  @Test
  public void testToFastHexMultipleBytesWithoutPrefix() {
    Bytes bytes = Bytes.fromHexString("0x010203");
    String result = HexUtils.toFastHex(bytes, false);
    assertEquals("010203", result, "Expected '010203' for the byte array 0x010203 without prefix");
  }

  @Test
  public void testToFastHexWithLeadingZeros() {
    Bytes bytes = Bytes.fromHexString("0x0001");
    String result = HexUtils.toFastHex(bytes, true);
    assertEquals(
        "0x0001",
        result,
        "Expected '0x0001' for the byte array 0x0001 with prefix (leading zeros retained)");
  }

  @Test
  public void testToFastHexWithLargeData() {
    Bytes bytes = Bytes.fromHexString("0x0102030405060708090a");
    String result = HexUtils.toFastHex(bytes, true);
    assertEquals("0x0102030405060708090a", result, "Expected correct hex output for large data");
  }
}
