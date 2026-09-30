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
package org.hyperledger.besu.evm;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CodeJumpDestBitMaskTest {

  private static final int JUMPDEST = 0x5b;
  private static final int PUSH1 = 0x60;
  private static final int PUSH32 = 0x7f;

  @Test
  void jumpDestInPushDataIsNotADestination() {
    final Code code = new Code(Bytes.fromHexString("0x605b5b"));

    assertThat(code.isJumpDestInvalid(1)).isTrue();
    assertThat(code.isJumpDestInvalid(2)).isFalse();
  }

  @Test
  void jumpDestAfterPushDataCrossingAnEntryIsADestination() {
    final byte[] bytes = new byte[100];
    bytes[40] = (byte) PUSH32;
    Arrays.fill(bytes, 41, 74, (byte) JUMPDEST);
    final Code code = new Code(Bytes.wrap(bytes));

    assertThat(code.isJumpDestInvalid(72)).isTrue();
    assertThat(code.isJumpDestInvalid(73)).isFalse();
  }

  @Test
  void pushTruncatedByTheEndOfCodeMarksNothingAfterIt() {
    final Code code = new Code(Bytes.fromHexString("0x5b7f5b5b"));

    assertThat(code.isJumpDestInvalid(0)).isFalse();
    assertThat(code.isJumpDestInvalid(2)).isTrue();
    assertThat(code.isJumpDestInvalid(3)).isTrue();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("shapes")
  void matchesReferenceWalk(final String name, final byte[] bytes) {
    assertMatchesReference(bytes);
  }

  @ParameterizedTest(name = "seed {0}")
  @MethodSource("seeds")
  void matchesReferenceWalkOnRandomCode(final int seed) {
    final Random random = new Random(seed);
    final int[] likely = {JUMPDEST, PUSH1, PUSH1 + 1, PUSH1 + 15, PUSH32 - 1, PUSH32, 0x00, 0x5f};
    for (int i = 0; i < 200; i++) {
      final byte[] bytes = new byte[random.nextInt(i % 10 == 0 ? 2000 : 200)];
      for (int j = 0; j < bytes.length; j++) {
        bytes[j] =
            random.nextBoolean()
                ? (byte) likely[random.nextInt(likely.length)]
                : (byte) random.nextInt(256);
      }
      assertMatchesReference(bytes);
    }
  }

  @ParameterizedTest(name = "seed {0}")
  @MethodSource("seeds")
  void matchesReferenceWalkOnChunksWithFewAndManyPushes(final int seed) {
    final Random random = new Random(seed);
    final int[] withoutPush = {0x00, 0x01, JUMPDEST, 0x5f, 0x80, 0xff};
    for (int i = 0; i < 50; i++) {
      final byte[] bytes = new byte[64 * (1 + random.nextInt(8)) + random.nextInt(64)];
      for (int chunkStart = 0; chunkStart < bytes.length; chunkStart += 64) {
        final int chunkEnd = Math.min(chunkStart + 64, bytes.length);
        for (int j = chunkStart; j < chunkEnd; j++) {
          bytes[j] = (byte) withoutPush[random.nextInt(withoutPush.length)];
        }
        // Up to 20 PUSHes, around the number that decides how a chunk is analysed, whose data may
        // run into the next chunk
        for (int pushes = random.nextInt(21); pushes > 0; pushes--) {
          bytes[chunkStart + random.nextInt(chunkEnd - chunkStart)] =
              (byte) (PUSH1 + random.nextInt(32));
        }
      }
      assertMatchesReference(bytes);
    }
  }

  static Stream<Arguments> shapes() {
    final Random random = new Random(42);
    final Stream.Builder<Arguments> shapes = Stream.builder();
    shapes.add(Arguments.of("empty", new byte[0]));
    shapes.add(Arguments.of("all JUMPDEST", filled(65536, JUMPDEST)));
    shapes.add(Arguments.of("PUSH1 JUMPDEST periodic", periodic(1000, PUSH1, JUMPDEST)));
    shapes.add(Arguments.of("all PUSH1 after one ADD", prefixed(filled(1000, PUSH1), 0x01)));
    shapes.add(Arguments.of("all PUSH32", filled(1000, PUSH32)));
    shapes.add(Arguments.of("PUSH32 JUMPDEST periodic", periodic(1000, PUSH32, JUMPDEST)));
    shapes.add(
        Arguments.of("random STOP JUMPDEST PUSH1", randomOf(random, 5000, 0x00, 0x5b, 0x60)));
    shapes.add(Arguments.of("random bytes", randomBytes(random, 5000)));
    // Each width of PUSH at the offsets from which its data reaches into or past the next entry
    for (int push = PUSH1; push <= PUSH32; push++) {
      for (final int offset : new int[] {0, 31, 62, 63}) {
        final byte[] bytes = filled(130, JUMPDEST);
        bytes[offset] = (byte) push;
        shapes.add(Arguments.of(String.format("PUSH%d at %d", push - PUSH1 + 1, offset), bytes));
      }
    }
    // 11 to 13 PUSH1 per chunk, entered with and without PUSH data carried in
    for (int pushes = 11; pushes <= 13; pushes++) {
      for (final int carried : new int[] {0, 1, 32}) {
        final byte[] bytes = filled(64 * 4, JUMPDEST);
        if (carried > 0) {
          // A PUSH32 whose last `carried` data bytes are in the next chunk
          bytes[31 + carried] = (byte) PUSH32;
        }
        for (int chunkStart = 64; chunkStart < bytes.length; chunkStart += 64) {
          for (int i = 0; i < pushes; i++) {
            bytes[chunkStart + 40 - 2 * i] = (byte) PUSH1;
          }
        }
        shapes.add(
            Arguments.of(
                String.format("%d PUSH1 per chunk, %d bytes carried in", pushes, carried), bytes));
      }
    }
    // Lengths around the entry boundaries, ending in a truncated PUSH
    for (final int length : new int[] {1, 2, 3, 63, 64, 65, 66, 127, 128, 129}) {
      final byte[] bytes = periodic(length, JUMPDEST, 0x01, 0x62);
      shapes.add(Arguments.of("truncated at length " + length, bytes));
    }
    return shapes.build();
  }

  static IntStream seeds() {
    return IntStream.range(0, 50);
  }

  private static void assertMatchesReference(final byte[] bytes) {
    final long[] actual = new Code(Bytes.wrap(bytes)).calculateJumpDestBitMask();

    assertThat(actual).isEqualTo(referenceBitMask(bytes));
  }

  /** Walks the code from offset 0, marking JUMPDEST and skipping PUSH1-PUSH32 immediate data. */
  private static long[] referenceBitMask(final byte[] bytes) {
    final long[] bitmap = new long[(bytes.length >> 6) + 1];
    int i = 0;
    while (i < bytes.length) {
      final int opcode = bytes[i] & 0xff;
      if (opcode == JUMPDEST) {
        bitmap[i >> 6] |= 1L << i;
      }
      i += opcode >= PUSH1 && opcode <= PUSH32 ? opcode - PUSH1 + 2 : 1;
    }
    return bitmap;
  }

  private static byte[] filled(final int length, final int opcode) {
    final byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) opcode);
    return bytes;
  }

  private static byte[] prefixed(final byte[] bytes, final int opcode) {
    bytes[0] = (byte) opcode;
    return bytes;
  }

  private static byte[] periodic(final int length, final int... pattern) {
    final byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) pattern[i % pattern.length];
    }
    return bytes;
  }

  private static byte[] randomOf(final Random random, final int length, final int... opcodes) {
    final byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) opcodes[random.nextInt(opcodes.length)];
    }
    return bytes;
  }

  private static byte[] randomBytes(final Random random, final int length) {
    final byte[] bytes = new byte[length];
    random.nextBytes(bytes);
    return bytes;
  }
}
