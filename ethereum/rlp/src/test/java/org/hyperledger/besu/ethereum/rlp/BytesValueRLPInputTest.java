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
package org.hyperledger.besu.ethereum.rlp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.Test;

public class BytesValueRLPInputTest {

  private static Bytes h(final String hex) {
    return Bytes.fromHexString(hex);
  }

  private static String times(final String base, final int times) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < times; i++) sb.append(base);
    return sb.toString();
  }

  @Test
  public void empty() {
    final RLPInput in = RLP.input(Bytes.EMPTY);
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void singleByte() {
    final RLPInput in = RLP.input(h("0x01"));
    assertThat(in.isDone()).isFalse();
    assertThat(in.readByte()).isEqualTo((byte) 1);
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void singleByteLowerBoundary() {
    final RLPInput in = RLP.input(h("0x00"));
    assertThat(in.isDone()).isFalse();
    assertThat(in.readByte()).isEqualTo((byte) 0);
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void singleByteUpperBoundary() {
    final RLPInput in = RLP.input(h("0x7f"));
    assertThat(in.isDone()).isFalse();
    assertThat(in.readByte()).isEqualTo((byte) 0x7f);
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void singleShortElement() {
    final RLPInput in = RLP.input(h("0x81FF"));
    assertThat(in.isDone()).isFalse();
    assertThat(in.readByte()).isEqualTo((byte) 0xFF);
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void singleBarelyShortElement() {
    final RLPInput in = RLP.input(h("0xb7" + times("2b", 55)));
    assertThat(in.isDone()).isFalse();
    assertThat(in.readBytes()).isEqualTo(h(times("2b", 55)));
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void singleBarelyLongElement() {
    final RLPInput in = RLP.input(h("0xb838" + times("2b", 56)));
    assertThat(in.isDone()).isFalse();
    assertThat(in.readBytes()).isEqualTo(h(times("2b", 56)));
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void singleLongElement() {
    final RLPInput in = RLP.input(h("0xb908c1" + times("3c", 2241)));

    assertThat(in.isDone()).isFalse();
    assertThat(in.readBytes()).isEqualTo(h(times("3c", 2241)));
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void singleLongElementBoundaryCase_1() {
    final RLPInput in = RLP.input(h("0xb8ff" + times("3c", 255)));
    assertThat(in.isDone()).isFalse();
    assertThat(in.readBytes()).isEqualTo(h(times("3c", 255)));
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void singleLongElementBoundaryCase_2() {
    final RLPInput in = RLP.input(h("0xb90100" + times("3c", 256)));
    assertThat(in.isDone()).isFalse();
    assertThat(in.readBytes()).isEqualTo(h(times("3c", 256)));
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void singleLongElementBoundaryCase_3() {
    final RLPInput in = RLP.input(h("0xb9ffff" + times("3c", 65535)));
    assertThat(in.isDone()).isFalse();
    assertThat(in.readBytes()).isEqualTo(h(times("3c", 65535)));
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void singleLongElementBoundaryCase_4() {
    final RLPInput in = RLP.input(h("0xba010000" + times("3c", 65536)));
    assertThat(in.isDone()).isFalse();
    assertThat(in.readBytes()).isEqualTo(h(times("3c", 65536)));
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void singleLongElementBoundaryCase_5() {
    final RLPInput in = RLP.input(h("0xbaffffff" + times("3c", 16777215)));
    assertThat(in.isDone()).isFalse();
    assertThat(in.readBytes()).isEqualTo(h(times("3c", 16777215)));
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void singleLongElementBoundaryCase_6() {
    // A RLPx Frame can have a maximum length of 0xffffff, so boundary above this
    // will be not be real world scenarios.
    final RLPInput in = RLP.input(h("0xbb01000000" + times("3c", 16777216)));
    assertThat(in.isDone()).isFalse();
    assertThat(in.readBytes()).isEqualTo(h(times("3c", 16777216)));
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void assertUnsignedIntScalar() {
    // Scalar should be encoded as the minimal byte array representing the number. For 0, that means
    // the empty byte array, which is a short element of zero-length, so 0x80.
    assertUnsignedIntScalar(0L, h("0x80"));

    assertUnsignedIntScalar(1L, h("0x01"));
    assertUnsignedIntScalar(15L, h("0x0F"));
    assertUnsignedIntScalar(1024L, h("0x820400"));
    assertUnsignedIntScalar((1L << 32) - 1, h("0x84ffffffff"));
  }

  private void assertUnsignedIntScalar(final long expected, final Bytes toTest) {
    final RLPInput in = RLP.input(toTest);
    assertThat(in.isDone()).isFalse();
    assertThat(in.readUnsignedIntScalar()).isEqualTo(expected);
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void assertLongScalar() {
    // Scalar should be encoded as the minimal byte array representing the number. For 0, that means
    // the empty byte array, which is a short element of zero-length, so 0x80.
    assertLongScalar(0L, h("0x80"));

    assertLongScalar(1L, h("0x01"));
    assertLongScalar(15L, h("0x0F"));
    assertLongScalar(1024L, h("0x820400"));
  }

  private void assertLongScalar(final long expected, final Bytes toTest) {
    final RLPInput in = RLP.input(toTest);
    assertThat(in.isDone()).isFalse();
    assertThat(in.readLongScalar()).isEqualTo(expected);
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void intScalar() {
    // Scalar should be encoded as the minimal byte array representing the number. For 0, that means
    // the empty byte array, which is a short element of zero-length, so 0x80.
    assertIntScalar(0, h("0x80"));

    assertIntScalar(1, h("0x01"));
    assertIntScalar(15, h("0x0F"));
    assertIntScalar(1024, h("0x820400"));
  }

  private void assertIntScalar(final int expected, final Bytes toTest) {
    final RLPInput in = RLP.input(toTest);
    assertThat(in.isDone()).isFalse();
    assertThat(in.readIntScalar()).isEqualTo(expected);
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void emptyList() {
    final RLPInput in = RLP.input(h("0xc0"));
    assertThat(in.isDone()).isFalse();
    assertThat(in.enterList()).isZero();
    assertThat(in.isDone()).isFalse();
    in.leaveList();
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void emptyByteString() {
    final RLPInput in = RLP.input(h("0x80"));
    assertThat(in.isDone()).isFalse();
    assertThat(in.readBytes()).isEqualTo(Bytes.EMPTY);
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void simpleShortList() {
    final RLPInput in = RLP.input(h("0xc22c3b"));

    assertThat(in.isDone()).isFalse();
    assertThat(in.enterList()).isEqualTo(2);
    assertThat(in.readByte()).isEqualTo((byte) 0x2c);
    assertThat(in.readByte()).isEqualTo((byte) 0x3b);
    in.leaveList();
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void simpleIntBeforeShortList() {
    final RLPInput in = RLP.input(h("0x02c22c3b"));

    assertThat(in.isDone()).isFalse();
    assertThat(in.readIntScalar()).isEqualTo(2);
    assertThat(in.enterList()).isEqualTo(2);
    assertThat(in.readByte()).isEqualTo((byte) 0x2c);
    assertThat(in.readByte()).isEqualTo((byte) 0x3b);
    in.leaveList();
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void simpleShortListUpperBoundary() {
    final RLPInput in = RLP.input(h("0xf7" + times("3c", 55)));
    assertThat(in.isDone()).isFalse();
    assertThat(in.enterList()).isEqualTo(55);
    for (int i = 0; i < 55; i++) {
      assertThat(in.readByte()).isEqualTo((byte) 0x3c);
    }
    in.leaveList();
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void simpleLongListLowerBoundary() {
    final RLPInput in = RLP.input(h("0xf838" + times("3c", 56)));
    assertThat(in.isDone()).isFalse();
    assertThat(in.enterList()).isEqualTo(56);
    for (int i = 0; i < 56; i++) {
      assertThat(in.readByte()).isEqualTo((byte) 0x3c);
    }
    in.leaveList();
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void simpleLongListBoundaryCase_1() {
    final RLPInput in = RLP.input(h("0xf8ff" + times("3c", 255)));
    assertThat(in.isDone()).isFalse();
    assertThat(in.enterList()).isEqualTo(255);
    for (int i = 0; i < 255; i++) {
      assertThat(in.readByte()).isEqualTo((byte) 0x3c);
    }
    in.leaveList();
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void simpleLongListBoundaryCase_2() {
    final RLPInput in = RLP.input(h("0xf90100" + times("3c", 256)));
    assertThat(in.isDone()).isFalse();
    assertThat(in.enterList()).isEqualTo(256);
    for (int i = 0; i < 256; i++) {
      assertThat(in.readByte()).isEqualTo((byte) 0x3c);
    }
    in.leaveList();
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void simpleLongListBoundaryCase_3() {
    final RLPInput in = RLP.input(h("0xf9ffff" + times("3c", 65535)));
    assertThat(in.isDone()).isFalse();
    assertThat(in.enterList()).isEqualTo(65535);
    for (int i = 0; i < 65535; i++) {
      assertThat(in.readByte()).isEqualTo((byte) 0x3c);
    }
    in.leaveList();
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void simpleLongListBoundaryCase_4() {
    final RLPInput in = RLP.input(h("0xfa010000" + times("3c", 65536)));
    assertThat(in.isDone()).isFalse();
    assertThat(in.enterList()).isEqualTo(65536);
    for (int i = 0; i < 65536; i++) {
      assertThat(in.readByte()).isEqualTo((byte) 0x3c);
    }
    in.leaveList();
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void simpleLongListBoundaryCase_5() {
    final RLPInput in = RLP.input(h("0xfaffffff" + times("3c", 16777215)));
    assertThat(in.isDone()).isFalse();
    assertThat(in.enterList()).isEqualTo(16777215);
    for (int i = 0; i < 16777215; i++) {
      assertThat(in.readByte()).isEqualTo((byte) 0x3c);
    }
    in.leaveList();
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void simpleLongListBoundaryCase_6() {
    // A RLPx Frame can have a maximum length of 0xffffff, so boundary above this
    // will be not be real world scenarios.
    final RLPInput in = RLP.input(h("0xfb01000000" + times("3c", 16777216)));
    assertThat(in.isDone()).isFalse();
    assertThat(in.enterList()).isEqualTo(16777216);
    for (int i = 0; i < 16777216; i++) {
      assertThat(in.readByte()).isEqualTo((byte) 0x3c);
    }
    in.leaveList();
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void simpleListwithBytes() {
    final RLPInput in = RLP.input(h("0xc28180"));
    assertThat(in.isDone()).isFalse();
    assertThat(in.enterList()).isEqualTo(1);
    assertThat(in.readBytes()).isEqualTo(h("0x80"));
    in.leaveList();
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void simpleNestedList() {
    final RLPInput in = RLP.input(h("0xc52cc203123b"));

    assertThat(in.isDone()).isFalse();
    assertThat(in.enterList()).isEqualTo(3);
    assertThat(in.readByte()).isEqualTo((byte) 0x2c);
    assertThat(in.enterList()).isEqualTo(2);
    assertThat(in.readByte()).isEqualTo((byte) 0x03);
    assertThat(in.readByte()).isEqualTo((byte) 0x12);
    in.leaveList();
    assertThat(in.readByte()).isEqualTo((byte) 0x3b);
    in.leaveList();
    assertThat(in.isDone()).isTrue();
  }

  @Test
  public void readAsRlp() {
    // Test null value
    final Bytes nullValue = h("0x80");
    final RLPInput nv = RLP.input(nullValue);
    assertThat(nv.readAsRlp().raw()).isEqualTo(nv.raw());
    nv.reset();
    assertThat(nv.nextIsNull()).isTrue();
    assertThat(nv.readAsRlp().nextIsNull()).isTrue();

    // Test empty list
    final Bytes emptyList = h("0xc0");
    final RLPInput el = RLP.input(emptyList);
    assertThat(el.readAsRlp().raw()).isEqualTo(emptyList);
    el.reset();
    assertThat(el.readAsRlp().enterList()).isZero();
    el.reset();
    assertThat(el.enterList()).isZero();

    final Bytes nestedList =
        RLP.encode(
            out -> {
              out.startList();
              out.writeByte((byte) 0x01);
              out.writeByte((byte) 0x02);
              out.startList();
              out.writeByte((byte) 0x11);
              out.writeByte((byte) 0x12);
              out.startList();
              out.writeByte((byte) 0x21);
              out.writeByte((byte) 0x22);
              out.endList();
              out.endList();
              out.endList();
            });

    final RLPInput nl = RLP.input(nestedList);
    final RLPInput compare = nl.readAsRlp();
    assertThat(compare.raw()).isEqualTo(nl.raw());
    nl.reset();
    nl.enterList();
    nl.skipNext(); // 0x01

    // Read the next byte that's inside the list, extract it as raw RLP and assert it's its own
    // representation.
    assertThat(nl.readAsRlp().raw()).isEqualTo(h("0x02"));
    // Extract the inner list.
    assertThat(nl.readAsRlp().raw()).isEqualTo(h("0xc51112c22122"));
    // Reset
    nl.reset();
    nl.enterList();
    nl.skipNext();
    nl.skipNext();
    nl.enterList();
    nl.skipNext();
    nl.skipNext();

    // Assert on the inner list of depth 3.
    assertThat(nl.readAsRlp().raw()).isEqualTo(h("0xc22122"));
  }

  @Test
  public void raw() {
    final Bytes initial = h("0xc80102c51112c22122");
    final RLPInput in = RLP.input(initial);
    assertThat(in.raw()).isEqualTo(initial);
  }

  @Test
  public void reset() {
    final RLPInput in = RLP.input(h("0xc80102c51112c22122"));
    for (int i = 0; i < 100; i++) {
      assertThat(in.enterList()).isEqualTo(3);
      assertThat(in.readByte()).isEqualTo((byte) 0x01);
      assertThat(in.readByte()).isEqualTo((byte) 0x02);
      assertThat(in.enterList()).isEqualTo(3);
      assertThat(in.readByte()).isEqualTo((byte) 0x11);
      assertThat(in.readByte()).isEqualTo((byte) 0x12);
      assertThat(in.enterList()).isEqualTo(2);
      assertThat(in.readByte()).isEqualTo((byte) 0x21);
      assertThat(in.readByte()).isEqualTo((byte) 0x22);
      in.reset();
    }
  }

  @Test
  public void sizeAndPosition() {
    final RLPInput in = RLP.input(h("0xc80102c51112c22122"));

    assertOffsetAndSize(in, 1, 8);
    assertThat(in.enterList()).isEqualTo(3);

    assertOffsetAndSize(in, 1, 1);
    assertThat(in.readByte()).isEqualTo((byte) 0x01);

    assertOffsetAndSize(in, 2, 1);
    assertThat(in.readByte()).isEqualTo((byte) 0x02);

    assertOffsetAndSize(in, 4, 5);
    assertThat(in.enterList()).isEqualTo(3);

    assertOffsetAndSize(in, 4, 1);
    assertThat(in.readByte()).isEqualTo((byte) 0x11);

    assertOffsetAndSize(in, 5, 1);
    assertThat(in.readByte()).isEqualTo((byte) 0x12);

    assertOffsetAndSize(in, 7, 2);
    assertThat(in.enterList()).isEqualTo(2);

    assertOffsetAndSize(in, 7, 1);
    assertThat(in.readByte()).isEqualTo((byte) 0x21);

    assertOffsetAndSize(in, 8, 1);
    assertThat(in.readByte()).isEqualTo((byte) 0x22);

    assertOffsetAndSize(in, 9, 0);
    in.leaveList();
    assertThat(in.isDone()).isFalse();

    assertOffsetAndSize(in, 9, 0);
    in.leaveList();
    assertThat(in.isDone()).isFalse();

    assertOffsetAndSize(in, 9, 0);
    in.leaveList();
    assertThat(in.isDone()).isTrue();
  }

  private void assertOffsetAndSize(final RLPInput in, final int offset, final int size) {
    assertThat(in.nextOffset()).isEqualTo(offset);
    assertThat(in.nextSize()).isEqualTo(size);
  }

  @Test
  public void ignoreListTail() {
    final RLPInput in = RLP.input(h("0xc80102c51112c22122"));
    assertThat(in.enterList()).isEqualTo(3);
    assertThat(in.readByte()).isEqualTo((byte) 0x01);
    in.leaveListLenient();
  }

  @Test
  public void leaveListEarly() {
    final RLPInput in = RLP.input(h("0xc80102c51112c22122"));
    assertThat(in.enterList()).isEqualTo(3);
    assertThat(in.readByte()).isEqualTo((byte) 0x01);
    assertThatThrownBy(() -> in.leaveList())
        .isInstanceOf(RLPException.class)
        .hasMessageStartingWith("Not at the end of the current list");
  }

  @Test
  public void failsWhenPayloadSizeIsTruncated() {
    // The prefix B9 indicates this is a long value that requires 2 bytes to encode the payload size
    // Only 1 byte follows the prefix
    Bytes bytes = h("0xB901");
    assertThatThrownBy(() -> RLP.input(bytes))
        .isInstanceOf(RLPException.class)
        .hasRootCauseInstanceOf(CorruptedRLPInputException.class)
        .hasMessageContaining(
            "value of size 2 has not enough bytes to read the 2 bytes payload size ");
  }

  @Test
  public void failsWhenPayloadSizeHasLeadingZeroes() {
    // Sanity check correctly encoded value: a byte string of 56 bytes, requiring 1 byte to encode
    // size 56
    final Bytes correctBytes =
        h(
            "0xB8380102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738");
    assertThat(
            h(
                "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738"))
        .isEqualTo(RLP.input(correctBytes).readBytes());

    // Encode same value, but use 2 bytes to represent the size, and pad size value with leading
    // zeroes
    final Bytes incorrectBytes =
        h(
            "0xB900380102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738");
    assertThatThrownBy(() -> RLP.input(incorrectBytes))
        .isInstanceOf(RLPException.class)
        .hasRootCauseInstanceOf(MalformedRLPInputException.class)
        .hasMessageContaining("size of payload has leading zeros");
  }

  @Test
  public void failsWhenShortByteStringEncodedAsLongByteString() {
    // Sanity check correctly encoded value: a byte string of 55 bytes encoded as short byte string
    final Bytes correctBytes =
        h(
            "0xB70102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f3031323334353637");
    assertThat(
            h(
                "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f3031323334353637"))
        .isEqualTo(RLP.input(correctBytes).readBytes());

    // Encode same value using long format
    final Bytes incorrectBytes =
        h(
            "0xB8370102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f3031323334353637");
    assertThatThrownBy(() -> RLP.input(incorrectBytes))
        .isInstanceOf(RLPException.class)
        .hasRootCauseInstanceOf(MalformedRLPInputException.class)
        .hasMessageContaining("written as a long item, but size 55 < 56 bytes");
  }

  @Test
  public void rlpItemSizeHoldsMaxValue() {
    // Size value encode max positive int.  So, size is decoded, but
    // RLP is malformed because the actual payload is not present
    AssertionsForClassTypes.assertThatThrownBy(() -> RLP.input(h("0xBB7FFFFFFF")).readBytes())
        .isInstanceOf(CorruptedRLPInputException.class)
        .hasMessageContaining("payload should start at offset 5 but input has only 5 bytes");
  }

  @Test
  public void rlpItemSizeOverflowsSignedInt() {
    // Size value encoded in 4 bytes but exceeds max positive int value
    AssertionsForClassTypes.assertThatThrownBy(() -> RLP.input(h("0xBB80000000")))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining(
            "RLP item at offset 1 with size value consuming 4 bytes exceeds max supported size of 2147483647");
  }

  @Test
  public void rlpItemSizeOverflowsInt() {
    // Size value is encoded with 5 bytes - overflowing int
    AssertionsForClassTypes.assertThatThrownBy(() -> RLP.input(h("0xBC0100000000")))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining(
            "RLP item at offset 1 with size value consuming 5 bytes exceeds max supported size of 2147483647");
  }

  @Test
  public void rlpListSizeHoldsMaxValue() {
    // Size value encode max positive int.  So, size is decoded, but
    // RLP is malformed because the actual payload is not present
    AssertionsForClassTypes.assertThatThrownBy(() -> RLP.input(h("0xFB7FFFFFFF")).readBytes())
        .isInstanceOf(CorruptedRLPInputException.class)
        .hasMessageContaining(
            "Input doesn't have enough data for RLP encoding: encoding advertise a payload ending at byte 2147483652 but input has size 5");
  }

  @Test
  public void rlpListSizeOverflowsSignedInt() {
    // Size value encoded in 4 bytes but exceeds max positive int value
    AssertionsForClassTypes.assertThatThrownBy(() -> RLP.input(h("0xFB80000000")))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining(
            "RLP item at offset 1 with size value consuming 4 bytes exceeds max supported size of 2147483647");
  }

  @Test
  public void rlpListSizeOverflowsInt() {
    // Size value is encoded with 5 bytes - overflowing int
    AssertionsForClassTypes.assertThatThrownBy(() -> RLP.input(h("0xFC0100000000")))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining(
            "RLP item at offset 1 with size value consuming 5 bytes exceeds max supported size of 2147483647");
  }

  @SuppressWarnings("ReturnValueIgnored")
  @Test
  public void decodeValueWithLeadingZerosAsScalar() {
    String value = "0x8200D0";

    List<Function<RLPInput, Object>> invalidDecoders =
        Arrays.asList(
            RLPInput::readBigIntegerScalar,
            RLPInput::readIntScalar,
            RLPInput::readLongScalar,
            RLPInput::readUInt256Scalar);

    for (Function<RLPInput, Object> decoder : invalidDecoders) {
      RLPInput in = RLP.input(h(value));
      AssertionsForClassTypes.assertThatThrownBy(() -> decoder.apply(in))
          .isInstanceOf(MalformedRLPInputException.class)
          .hasMessageContaining("Invalid scalar");
    }
  }

  @Test
  public void decodeValueWithLeadingZerosAsUnsignedInt() {
    RLPInput in = RLP.input(h("0x84000000D0"));
    assertThat(in.readUnsignedInt()).isEqualTo(208);
  }

  @Test
  public void decodeValueWithLeadingZerosAsUnsignedShort() {
    RLPInput in = RLP.input(h("0x8200D0"));
    assertThat(in.readUnsignedShort()).isEqualTo(208);
  }

  @Test
  public void decodeValueWithLeadingZerosAsSignedInt() {
    RLPInput in = RLP.input(h("0x84000000D0"));
    assertThat(in.readInt()).isEqualTo(208);
  }

  @Test
  public void readListRejectsWhenExceedingMaxElements() {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.writeList(List.of(1, 2, 3), (v, o) -> o.writeInt(v));
    assertThatThrownBy(() -> RLP.input(out.encoded()).readList(RLPInput::readInt, 2))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining("exceeds the maximum permitted size");
  }

  @Test
  public void readListAcceptsWhenExactlyAtMaxElements() {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.writeList(List.of(1, 2, 3), (v, o) -> o.writeInt(v));
    final List<Integer> result = RLP.input(out.encoded()).readList(RLPInput::readInt, 3);
    assertThat(result).containsExactly(1, 2, 3);
  }

  @Test
  public void decodeValueWithLeadingZerosAsSignedLong() {
    RLPInput in = RLP.input(h("0x8800000000000000D0"));
    assertThat(in.readLong()).isEqualTo(208);
  }

  @Test
  public void decodeValueWithLeadingZerosAsSignedShort() {
    RLPInput in = RLP.input(h("0x8200D0"));
    assertThat(in.readShort()).isEqualTo((short) 208);
  }

  @Test
  public void decodeValueWithLeadingZerosAsBytes() {
    RLPInput in = RLP.input(h("0x8800000000000000D0"));
    assertThat(in.readBytes().getLong(0)).isEqualTo(208);
  }
}
