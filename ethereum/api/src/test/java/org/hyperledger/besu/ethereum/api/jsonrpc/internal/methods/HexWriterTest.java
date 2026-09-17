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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class HexWriterTest {

  private final byte[] dest = new byte[256];

  private String encode(final byte[] input, final boolean stripLeading) {
    final int end = HexWriter.encodeTo(input, input.length, dest, 0, stripLeading);
    return new String(dest, 0, end, StandardCharsets.US_ASCII);
  }

  // ── stripLeading = true ────────────────────────────────────────────

  static Stream<Arguments> stripLeadingCases() {
    return Stream.of(
        // empty input
        Arguments.of(new byte[] {}, "0x0"),
        // single zero byte
        Arguments.of(new byte[] {0x00}, "0x0"),
        // single byte, value 1
        Arguments.of(new byte[] {0x01}, "0x1"),
        // single byte, high nibble only (0xF0)
        Arguments.of(new byte[] {(byte) 0xF0}, "0xf0"),
        // single byte, low nibble only (0x0F) — must not lose the 'f'
        Arguments.of(new byte[] {0x0F}, "0xf"),
        // single byte, max (0xFF)
        Arguments.of(new byte[] {(byte) 0xFF}, "0xff"),
        // two bytes, leading zero byte
        Arguments.of(new byte[] {0x00, 0x01}, "0x1"),
        // two bytes, leading zero nibble on first byte
        Arguments.of(new byte[] {0x0A, (byte) 0xBC}, "0xabc"),
        // two bytes, no leading zeros
        Arguments.of(new byte[] {(byte) 0xAB, (byte) 0xCD}, "0xabcd"),
        // all zeros, multiple bytes
        Arguments.of(new byte[] {0x00, 0x00, 0x00}, "0x0"),
        // leading zeros then value in last byte
        Arguments.of(new byte[] {0x00, 0x00, 0x01}, "0x1"),
        // 32-byte uint256 value = 1 (stack entry)
        Arguments.of(uint256(1), "0x1"),
        // 32-byte uint256 value = 0xFF
        Arguments.of(uint256(0xFF), "0xff"),
        // 32-byte uint256 value = 0x100
        Arguments.of(uint256(0x100), "0x100"),
        // 32-byte all zeros
        Arguments.of(new byte[32], "0x0"),
        // 32-byte all 0xFF
        Arguments.of(filledBytes(32, 0xFF), "0x" + "ff".repeat(32)));
  }

  @ParameterizedTest
  @MethodSource("stripLeadingCases")
  void shortPackStripLeading(final byte[] input, final String expected) {
    assertThat(encode(input, true)).isEqualTo(expected);
  }

  // ── stripLeading = false (full-width) ──────────────────────────────

  static Stream<Arguments> fullWidthCases() {
    return Stream.of(
        // empty input
        Arguments.of(new byte[] {}, "0x0"),
        // single zero byte → "0x00" not "0x0"
        Arguments.of(new byte[] {0x00}, "0x00"),
        // single byte, value 1
        Arguments.of(new byte[] {0x01}, "0x01"),
        // single byte, max
        Arguments.of(new byte[] {(byte) 0xFF}, "0xff"),
        // two bytes with leading zero
        Arguments.of(new byte[] {0x00, 0x01}, "0x0001"),
        // 32-byte uint256 value = 1 (storage key format)
        Arguments.of(
            uint256(1), "0x0000000000000000000000000000000000000000000000000000000000000001"),
        // 32-byte all zeros
        Arguments.of(
            new byte[32], "0x0000000000000000000000000000000000000000000000000000000000000000"),
        // 32-byte all 0xFF
        Arguments.of(filledBytes(32, 0xFF), "0x" + "ff".repeat(32)));
  }

  @ParameterizedTest
  @MethodSource("fullWidthCases")
  void shortPackFullWidth(final byte[] input, final String expected) {
    assertThat(encode(input, false)).isEqualTo(expected);
  }

  // ── destPos offset ─────────────────────────────────────────────────

  @Test
  void writesAtNonZeroOffset() {
    dest[0] = 'Z';
    final int end = HexWriter.encodeTo(new byte[] {(byte) 0xAB}, 1, dest, 1, true);
    assertThat(dest[0]).isEqualTo((byte) 'Z'); // untouched
    assertThat(new String(dest, 1, end - 1, StandardCharsets.US_ASCII)).isEqualTo("0xab");
  }

  // ── tight buffer (no room to spare) ────────────────────────────────

  @Test
  void tightBufferStripLeading() {
    // 1 byte → worst case "0xff" = 4 chars
    final byte[] tight = new byte[4];
    final int end = HexWriter.encodeTo(new byte[] {(byte) 0xFF}, 1, tight, 0, true);
    assertThat(end).isEqualTo(4);
    assertThat(new String(tight, StandardCharsets.US_ASCII)).isEqualTo("0xff");
  }

  @Test
  void tightBufferStripLeadingSingleNibble() {
    // 0x01 → "0x1" = 3 chars
    final byte[] tight = new byte[3];
    final int end = HexWriter.encodeTo(new byte[] {0x01}, 1, tight, 0, true);
    assertThat(end).isEqualTo(3);
    assertThat(new String(tight, 0, end, StandardCharsets.US_ASCII)).isEqualTo("0x1");
  }

  @Test
  void tightBufferFullWidth() {
    // 1 byte → "0x01" = 4 chars
    final byte[] tight = new byte[4];
    final int end = HexWriter.encodeTo(new byte[] {0x01}, 1, tight, 0, false);
    assertThat(end).isEqualTo(4);
    assertThat(new String(tight, StandardCharsets.US_ASCII)).isEqualTo("0x01");
  }

  @Test
  void tightBufferEmpty() {
    // empty → "0x0" = 3 chars
    final byte[] tight = new byte[3];
    final int end = HexWriter.encodeTo(new byte[] {}, 0, tight, 0, true);
    assertThat(end).isEqualTo(3);
    assertThat(new String(tight, 0, end, StandardCharsets.US_ASCII)).isEqualTo("0x0");
  }

  // ── partial length (len < bytes.length) ────────────────────────────

  @Test
  void partialLength() {
    // only encode the first 2 bytes of a 4-byte array
    final byte[] input = new byte[] {(byte) 0xAB, (byte) 0xCD, (byte) 0xEF, 0x01};
    final int end = HexWriter.encodeTo(input, 2, dest, 0, true);
    assertThat(new String(dest, 0, end, StandardCharsets.US_ASCII)).isEqualTo("0xabcd");
  }

  // ── odd byte counts ────────────────────────────────────────────────

  @Test
  void oddByteCountStripLeading() {
    // 3 bytes with leading zero → "0xbcde"
    final byte[] input = new byte[] {0x00, (byte) 0xBC, (byte) 0xDE};
    assertThat(encode(input, true)).isEqualTo("0xbcde");
  }

  @Test
  void oddByteCountFullWidth() {
    // 3 bytes → "0x00bcde"
    final byte[] input = new byte[] {0x00, (byte) 0xBC, (byte) 0xDE};
    assertThat(encode(input, false)).isEqualTo("0x00bcde");
  }

  // ── large values (> 32 bytes) ───────────────────────────────────────

  @Test
  void largeValueStripLeading() {
    // 64 bytes, first 60 are zero, last 4 are 0xDEADBEEF
    final byte[] input = new byte[64];
    input[60] = (byte) 0xDE;
    input[61] = (byte) 0xAD;
    input[62] = (byte) 0xBE;
    input[63] = (byte) 0xEF;
    assertThat(encode(input, true)).isEqualTo("0xdeadbeef");
  }

  @Test
  void largeValueFullWidth() {
    // 64 bytes, first 60 are zero, last 4 are 0xDEADBEEF
    final byte[] input = new byte[64];
    input[60] = (byte) 0xDE;
    input[61] = (byte) 0xAD;
    input[62] = (byte) 0xBE;
    input[63] = (byte) 0xEF;
    assertThat(encode(input, false)).isEqualTo("0x" + "00".repeat(60) + "deadbeef");
  }

  @Test
  void revertReasonSized() {
    // Typical Solidity revert: 4-byte selector + 32-byte offset + 32-byte length + message.
    // Total could easily be 100+ bytes.
    final byte[] input = new byte[100];
    input[0] = 0x08;
    input[1] = (byte) 0xC3;
    input[2] = 0x79;
    input[3] = (byte) 0xA0;
    input[99] = 0x21; // '!' character in the message area
    final byte[] largeDest = new byte[2 + 100 * 2];
    final int end = HexWriter.encodeTo(input, input.length, largeDest, 0, false);
    final String result = new String(largeDest, 0, end, StandardCharsets.US_ASCII);
    assertThat(result).startsWith("0x08c379a0");
    assertThat(result).hasSize(2 + 200); // "0x" + 200 hex chars
    assertThat(result).endsWith("21");
  }

  @Test
  void largeValueTightBuffer() {
    // 128 bytes → 258 chars ("0x" + 256 hex chars)
    final byte[] input = filledBytes(128, 0xAB);
    final byte[] tight = new byte[258];
    final int end = HexWriter.encodeTo(input, input.length, tight, 0, false);
    assertThat(end).isEqualTo(258);
    assertThat(new String(tight, 0, end, StandardCharsets.US_ASCII))
        .isEqualTo("0x" + "ab".repeat(128));
  }

  @Test
  void largeValueAllZerosStripLeading() {
    final byte[] input = new byte[128];
    assertThat(encode(input, true)).isEqualTo("0x0");
  }

  // ── regression: buffer overflow with values exceeding dest capacity ──
  //
  // The original compactHexBytes wrote into a fixed hexBuf without checking
  // whether the hex output (2 + len*2 bytes) would fit, causing AIOBE for
  // large revert reasons or any value whose hex output exceeded the buffer.

  @Test
  void regression_veryLargeValueDoesNotOverflow() {
    // 1024 bytes → "0x" + 2048 hex chars = 2050 bytes total
    final byte[] input = filledBytes(1024, 0xCD);
    final byte[] largeDest = new byte[2 + 1024 * 2];
    final int end = HexWriter.encodeTo(input, input.length, largeDest, 0, false);
    assertThat(end).isEqualTo(2050);
    assertThat(new String(largeDest, 0, end, StandardCharsets.US_ASCII))
        .isEqualTo("0x" + "cd".repeat(1024));
  }

  @Test
  void regression_largeStripLeadingWithOneNonZeroByte() {
    // 512 bytes, all zeros except the last byte — strips to "0xff"
    final byte[] input = new byte[512];
    input[511] = (byte) 0xFF;
    final byte[] largeDest = new byte[2 + 512 * 2];
    final int end = HexWriter.encodeTo(input, input.length, largeDest, 0, true);
    assertThat(new String(largeDest, 0, end, StandardCharsets.US_ASCII)).isEqualTo("0xff");
  }

  @Test
  void regression_largeStripLeadingWithLeadingZeroNibble() {
    // 256 bytes, first 254 are zero, then 0x0A, 0xBC
    // Should produce "0xabc" (strip leading zero nibble of 0x0A)
    final byte[] input = new byte[256];
    input[254] = 0x0A;
    input[255] = (byte) 0xBC;
    final byte[] largeDest = new byte[2 + 256 * 2];
    final int end = HexWriter.encodeTo(input, input.length, largeDest, 0, true);
    assertThat(new String(largeDest, 0, end, StandardCharsets.US_ASCII)).isEqualTo("0xabc");
  }

  @Test
  void regression_destExactlyFitsOutput() {
    // Ensure no off-by-one when dest is exactly the right size
    final byte[] input = new byte[] {(byte) 0xAB, (byte) 0xCD};
    // "0xabcd" = 6 bytes
    final byte[] tight = new byte[6];
    final int end = HexWriter.encodeTo(input, input.length, tight, 0, true);
    assertThat(end).isEqualTo(6);
    assertThat(new String(tight, StandardCharsets.US_ASCII)).isEqualTo("0xabcd");
  }

  @Test
  void regression_destExactlyFitsFullWidth() {
    final byte[] input = new byte[] {0x00, 0x01};
    // "0x0001" = 6 bytes
    final byte[] tight = new byte[6];
    final int end = HexWriter.encodeTo(input, input.length, tight, 0, false);
    assertThat(end).isEqualTo(6);
    assertThat(new String(tight, StandardCharsets.US_ASCII)).isEqualTo("0x0001");
  }

  // ── regression: empty input writes 3 bytes, not 2 ──────────────────
  //
  // writeHex computed maxLen = 2 + bytes.length * 2.  For an empty byte[]
  // (Bytes.EMPTY.toArrayUnsafe()) that evaluates to 2, but encodeTo always
  // emits "0x0" — three bytes.  When the write buffer had exactly two bytes
  // of headroom the third byte wrote past the array end, producing an
  // ArrayIndexOutOfBoundsException that corrupted the streaming JSON output.
  // Fix: maxLen = Math.max(3, 2 + bytes.length * 2).

  @Test
  void regression_emptyInputNeedsThreeNotTwoBytes() {
    // Simulate a buffer with only 2 bytes of space remaining.
    // Before the fix, the caller would think 2 bytes were enough.
    final byte[] buf = new byte[10];
    final int destPos = buf.length - 2; // only 2 bytes left

    // encodeTo writes 3 bytes ("0x0") — one past the end → AIOOBE
    assertThatThrownBy(() -> HexWriter.encodeTo(new byte[] {}, 0, buf, destPos, true))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class);
  }

  @Test
  void regression_emptyInputSucceedsWithThreeBytesOfRoom() {
    // With 3 bytes of space, the same call succeeds.
    final byte[] buf = new byte[10];
    final int destPos = buf.length - 3;
    final int end = HexWriter.encodeTo(new byte[] {}, 0, buf, destPos, true);
    assertThat(end).isEqualTo(buf.length);
    assertThat(new String(buf, destPos, end - destPos, StandardCharsets.US_ASCII)).isEqualTo("0x0");
  }

  // ── helpers ────────────────────────────────────────────────────────

  private static byte[] uint256(final long value) {
    final byte[] bytes = new byte[32];
    long v = value;
    for (int i = 31; i >= 0 && v != 0; i--) {
      bytes[i] = (byte) (v & 0xFF);
      v >>>= 8;
    }
    return bytes;
  }

  private static byte[] filledBytes(final int size, final int value) {
    final byte[] bytes = new byte[size];
    java.util.Arrays.fill(bytes, (byte) value);
    return bytes;
  }
}
