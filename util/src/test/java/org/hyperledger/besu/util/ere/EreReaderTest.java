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
package org.hyperledger.besu.util.ere;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.hyperledger.besu.util.snappy.SnappyFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.zip.CRC32C;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.xerial.snappy.Snappy;

class EreReaderTest {
  private final SnappyFactory snappy = new SnappyFactory();
  private final EreReader reader = new EreReader(snappy);
  @TempDir Path tempDir;

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7})
  void readsAllOptionalComponentCombinations(final int optionalComponents) throws IOException {
    final Archive archive = archive(optionalComponents, new byte[] {(byte) 0xc0});
    final List<Long> numbers = new ArrayList<>();
    read(
        archive.bytes(),
        1024,
        (number, header, body) -> {
          numbers.add(number);
          assertThat(header).containsExactly((byte) 0xc1, (byte) (number - 8192));
          assertThat(body).containsExactly((byte) 0xc0);
        });
    assertThat(numbers).containsExactly(8192L, 8193L);
  }

  @Test
  void acceptsGenesisRange() throws IOException {
    final Archive archive = archive(0, new byte[] {(byte) 0xc0});
    putLong(archive.bytes(), archive.indexOffset() + 8, 0);
    final List<Long> numbers = new ArrayList<>();
    read(archive.bytes(), 1024, (number, header, body) -> numbers.add(number));
    assertThat(numbers).containsExactly(0L, 1L);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 8192})
  void readsBoundaryBlockCounts(final int count) throws IOException {
    final Archive archive = archive(7, new byte[] {(byte) 0xc0}, count, false);
    final List<Long> numbers = new ArrayList<>();
    read(
        archive.bytes(),
        1024,
        (number, header, body) -> {
          assertThat(header).containsExactly((byte) 0xc1, (byte) (number - 8192));
          assertThat(body).containsExactly((byte) 0xc0);
          numbers.add(number);
        });
    assertThat(numbers).hasSize(count);
    assertThat(numbers.getFirst()).isEqualTo(8192L);
    assertThat(numbers.getLast()).isEqualTo(8192L + count - 1);
  }

  @Test
  void acceptsLastSupportedBlockNumber() throws IOException {
    final Archive archive = archive(0, new byte[] {1}, 1, false);
    putLong(archive.bytes(), archive.indexOffset() + 8, Long.MAX_VALUE);
    final List<Long> numbers = new ArrayList<>();
    read(archive.bytes(), 1024, (number, header, body) -> numbers.add(number));
    assertThat(numbers).containsExactly(Long.MAX_VALUE);
  }

  @Test
  void acceptsDecodedComponentExactlyAtLimit() throws IOException {
    final Archive archive = archive(0, new byte[1024]);
    read(archive.bytes(), 1024, (number, header, body) -> assertThat(body).hasSize(1024));
  }

  @Test
  void acceptsEncodedComponentExactlyAtLimit() throws IOException {
    final byte[] body = new byte[32];
    for (int i = 0; i < body.length; i++) {
      body[i] = (byte) i;
    }
    final int encodedLength = compress(body).length;
    assertThat(encodedLength).isGreaterThan(body.length);
    read(
        archive(0, body).bytes(),
        encodedLength,
        (number, header, actual) -> assertThat(actual).containsExactly(body));
  }

  @Test
  void rejectsNonVersionFirstEntry() throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    archive.bytes()[0] = 3;
    assertInvalid(archive.bytes(), "version");
  }

  @Test
  void rejectsNonzeroReservedBytes() throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    archive.bytes()[6] = 1;
    assertInvalid(archive.bytes(), "reserved");
  }

  @Test
  void rejectsMissingIndex() throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    assertInvalid(Arrays.copyOf(archive.bytes(), archive.indexOffset()), "Missing");
  }

  @Test
  void rejectsDataFollowingIndex() throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    assertInvalid(Arrays.copyOf(archive.bytes(), archive.bytes().length + 1), "final entry");
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 7, 9, 15})
  void rejectsTruncatedHeaders(final int length) throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    assertInvalid(Arrays.copyOf(archive.bytes(), length), "Truncated");
  }

  @Test
  void rejectsTruncatedPayload() throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    assertInvalid(Arrays.copyOf(archive.bytes(), archive.bytes().length - 1), "Truncated");
  }

  @Test
  void treatsPayloadLengthAsUnsignedBeforeAllocation() throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    Arrays.fill(archive.bytes(), 10, 14, (byte) 0xff);
    assertInvalid(archive.bytes(), "Truncated");
  }

  @ParameterizedTest
  @ValueSource(longs = {0, -1, 8193, Long.MAX_VALUE})
  void rejectsInvalidBlockCount(final long count) throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    putLong(archive.bytes(), archive.bytes().length - 8, count);
    assertInvalid(archive.bytes(), "count");
  }

  @ParameterizedTest
  @ValueSource(longs = {0, 1, 6, -1, Long.MAX_VALUE})
  void rejectsInvalidComponentCount(final long count) throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    putLong(archive.bytes(), archive.bytes().length - 16, count);
    assertInvalid(archive.bytes(), "count");
  }

  @Test
  void rejectsCountThatDoesNotMatchIndexSize() throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    putLong(archive.bytes(), archive.bytes().length - 8, 1);
    assertInvalid(archive.bytes(), "does not match");
  }

  @ParameterizedTest
  @ValueSource(longs = {-1, Long.MAX_VALUE})
  void rejectsUnsupportedBlockNumberRange(final long start) throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    putLong(archive.bytes(), archive.indexOffset() + 8, start);
    assertInvalid(archive.bytes(), "supported range");
  }

  @ParameterizedTest
  @ValueSource(longs = {0, 1, Long.MIN_VALUE, Long.MAX_VALUE})
  void rejectsOutOfRangeOffset(final long offset) throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    putLong(archive.bytes(), archive.indexOffset() + 16, offset);
    assertInvalid(archive.bytes(), "outside");
  }

  @Test
  void rejectsOffsetIntoPayload() throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    putLong(archive.bytes(), archive.indexOffset() + 16, 16L - archive.indexOffset());
    assertInvalid(archive.bytes(), "missing");
  }

  @Test
  void rejectsReusedComponent() throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    putLong(archive.bytes(), archive.indexOffset() + 24, 8L - archive.indexOffset());
    assertInvalid(archive.bytes(), "duplicate");
  }

  @Test
  void rejectsDuplicateComponentType() throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    final ByteBuffer data = ByteBuffer.wrap(archive.bytes()).order(ByteOrder.LITTLE_ENDIAN);
    // Point the first block's body slot at the second header entry, which is a real entry boundary.
    putLong(archive.bytes(), archive.indexOffset() + 24, data.getLong(archive.indexOffset() + 32));
    assertInvalid(archive.bytes(), "Duplicate component type");
  }

  @Test
  void rejectsInconsistentLayoutAcrossBlocks() throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    final ByteBuffer data = ByteBuffer.wrap(archive.bytes()).order(ByteOrder.LITTLE_ENDIAN);
    final long header = data.getLong(archive.indexOffset() + 32);
    final long body = data.getLong(archive.indexOffset() + 40);
    putLong(archive.bytes(), archive.indexOffset() + 32, body);
    putLong(archive.bytes(), archive.indexOffset() + 40, header);
    assertInvalid(archive.bytes(), "Inconsistent");
  }

  @Test
  void rejectsLegacyEra1Index() throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    archive.bytes()[archive.indexOffset()] = 0x66;
    assertInvalid(archive.bytes(), "legacy Era1");
  }

  @Test
  void rejectsEncodedComponentExceedingLimit() throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    assertThatIOException()
        .isThrownBy(() -> read(archive.bytes(), 1, (number, header, body) -> {}))
        .withMessageContaining("Encoded");
  }

  @Test
  void boundsSnappyExpansion() throws IOException {
    final Archive archive = archive(0, new byte[1024]);
    assertThatIOException()
        .isThrownBy(() -> read(archive.bytes(), 128, (number, header, body) -> {}))
        .withMessageContaining("Decoded");
  }

  @Test
  void rejectsCorruptSnappyStream() throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    archive.bytes()[16] = 0;
    assertThatIOException().isThrownBy(() -> read(archive.bytes(), 1024, (n, h, b) -> {}));
  }

  @Test
  void rejectsCorruptSnappyChecksum() throws IOException {
    final byte[] encoded = compress(new byte[] {1});
    encoded[14] ^= 1;
    assertInvalid(archive(0, encoded, 2, true).bytes(), "checksum");
  }

  @Test
  void rejectsOversizedSnappyChunkBeforeDecoding() throws IOException {
    final byte[] decoded = new byte[65537];
    final byte[] compressed = Snappy.compress(decoded);
    final int length = compressed.length + 4;
    final var checksum = new CRC32C();
    checksum.update(decoded);
    final ByteBuffer encoded =
        ByteBuffer.allocate(18 + compressed.length).order(ByteOrder.LITTLE_ENDIAN);
    encoded.put(HexFormat.of().parseHex("ff060000734e61507059"));
    encoded.put((byte) 0).put((byte) length).put((byte) (length >> 8)).put((byte) (length >> 16));
    encoded.putInt(Integer.rotateRight((int) checksum.getValue(), 15) + 0xa282ead8);
    encoded.put(compressed);
    final List<Long> delivered = new ArrayList<>();
    assertThatIOException()
        .isThrownBy(
            () ->
                read(
                    archive(0, encoded.array(), 2, true).bytes(),
                    decoded.length,
                    (number, header, body) -> delivered.add(number)))
        .withMessageContaining("Snappy chunk");
    assertThat(delivered).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "fe",
        "fe05000000",
        "ff060000734e61507058",
        "02000000",
        "00030000000000",
        "01030000000000",
        "0007000000000000818004"
      })
  void rejectsMalformedSnappyChunks(final String suffix) throws IOException {
    final byte[] encoded = HexFormat.of().parseHex("ff060000734e61507059" + suffix);
    assertThatIOException()
        .isThrownBy(() -> read(archive(0, encoded, 2, true).bytes(), 1024, (n, h, b) -> {}));
  }

  @Test
  void skipsManySnappyPaddingChunksWithoutRecursion() throws IOException {
    final byte[] stream = compress(new byte[] {1});
    final ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    encoded.write(stream, 0, 10);
    for (int i = 0; i < 10000; i++) {
      encoded.write(new byte[] {(byte) 0xfe, 0, 0, 0});
    }
    encoded.write(stream, 10, stream.length - 10);
    read(
        archive(0, encoded.toByteArray(), 2, true).bytes(),
        50000,
        (number, header, body) -> assertThat(body).containsExactly((byte) 1));
  }

  @Test
  void acceptsRepeatedSnappyIdentifiersAndSkippableChunks() throws IOException {
    final byte[] stream = compress(new byte[] {1});
    final ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    encoded.write(stream);
    encoded.write(new byte[] {(byte) 0x80, 1, 0, 0, 7});
    encoded.write(stream);
    read(
        archive(0, encoded.toByteArray(), 2, true).bytes(),
        1024,
        (number, header, body) -> assertThat(body).containsExactly((byte) 1, (byte) 1));
  }

  @Test
  void readsBodySpanningMultipleSnappyChunks() throws IOException {
    final byte[] body = new byte[131073];
    Arrays.fill(body, (byte) 1);
    read(
        archive(0, body).bytes(),
        body.length,
        (number, header, actual) -> assertThat(actual).containsExactly(body));
  }

  @Test
  void rejectsEveryTruncatedArchivePrefixBeforeCallbacks() throws IOException {
    final Archive archive = archive(7, new byte[] {1});
    for (int length = 0; length < archive.bytes().length; length++) {
      final byte[] truncated = Arrays.copyOf(archive.bytes(), length);
      final List<Long> delivered = new ArrayList<>();
      assertThatIOException()
          .as("truncation at byte %d", length)
          .isThrownBy(() -> read(truncated, 1024, (number, header, body) -> delivered.add(number)));
      assertThat(delivered).isEmpty();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, Integer.MAX_VALUE})
  void rejectsInvalidResourceLimit(final int limit) throws IOException {
    final Archive archive = archive(0, new byte[] {1});
    assertThatIllegalArgumentException()
        .isThrownBy(() -> read(archive.bytes(), limit, (number, header, body) -> {}));
  }

  @Test
  void leavesChannelOpenWhenConsumerFails() throws IOException {
    final Path file = tempDir.resolve("consumer.ere");
    Files.write(file, archive(0, new byte[] {1}).bytes());
    try (var channel = Files.newByteChannel(file, StandardOpenOption.READ)) {
      assertThatIOException()
          .isThrownBy(
              () ->
                  reader.read(
                      channel,
                      1024,
                      (number, header, body) -> {
                        throw new IOException("consumer failed");
                      }))
          .withMessage("consumer failed");
      assertThat(channel.isOpen()).isTrue();
    }
  }

  private void assertInvalid(final byte[] data, final String message) throws IOException {
    final List<Long> delivered = new ArrayList<>();
    assertThatIOException()
        .isThrownBy(() -> read(data, 1024, (number, header, body) -> delivered.add(number)))
        .withMessageContaining(message);
    assertThat(delivered).isEmpty();
  }

  private void read(final byte[] data, final int limit, final EreReader.BlockConsumer consumer)
      throws IOException {
    final Path file = tempDir.resolve("sample.ere");
    Files.write(file, data);
    try (var channel = Files.newByteChannel(file, StandardOpenOption.READ)) {
      reader.read(channel, limit, consumer);
      assertThat(channel.isOpen()).isTrue();
    }
  }

  // Synthetic e2store fixtures exercise framing and indexing, not Ethereum consensus validity.
  private Archive archive(final int optional, final byte[] body) throws IOException {
    return archive(optional, body, 2, false);
  }

  private Archive archive(
      final int optional, final byte[] body, final int count, final boolean bodyIsEncoded)
      throws IOException {
    final ByteArrayOutputStream file = new ByteArrayOutputStream();
    entry(file, 0x3265, new byte[0]);
    final List<Integer> types = new ArrayList<>(List.of(3, 4));
    if ((optional & 1) != 0) {
      types.add(0x0a);
    }
    if ((optional & 2) != 0) {
      types.add(6);
    }
    if ((optional & 4) != 0) {
      types.add(0x0b);
    }
    final long[][] offsets = new long[count][types.size()];
    // Physical order differs from index order when both proof and total difficulty are present.
    for (final int type : new int[] {3, 4, 0x0a, 0x0b, 6}) {
      final int slot = types.indexOf(type);
      if (slot < 0) {
        continue;
      }
      for (int block = 0; block < count; block++) {
        offsets[block][slot] = file.size();
        final byte[] payload =
            switch (type) {
              case 3 -> new byte[] {(byte) 0xc1, (byte) block};
              case 4 -> body;
              case 6 -> new byte[32];
              default -> new byte[] {(byte) 0xc0};
            };
        entry(file, type, type == 6 || (type == 4 && bodyIsEncoded) ? payload : compress(payload));
      }
    }
    entry(file, 0x80, new byte[] {1, 2, 3});
    if ((optional & 2) != 0) {
      entry(file, 7, new byte[32]);
    }
    final int indexOffset = file.size();
    final ByteBuffer index =
        ByteBuffer.allocate(24 + count * types.size() * 8).order(ByteOrder.LITTLE_ENDIAN);
    index.putLong(8192);
    for (final long[] block : offsets) {
      for (final long offset : block) {
        index.putLong(offset - indexOffset);
      }
    }
    index.putLong(types.size()).putLong(count);
    entry(file, 0x3267, index.array());
    return new Archive(file.toByteArray(), indexOffset);
  }

  private byte[] compress(final byte[] data) throws IOException {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (var stream = snappy.createFramedOutputStream(output)) {
      stream.write(data);
    }
    return output.toByteArray();
  }

  private void entry(final ByteArrayOutputStream file, final int type, final byte[] data)
      throws IOException {
    file.write(
        ByteBuffer.allocate(8)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putShort((short) type)
            .putInt(data.length)
            .putShort((short) 0)
            .array());
    file.write(data);
  }

  private void putLong(final byte[] bytes, final int position, final long value) {
    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putLong(position, value);
  }

  private record Archive(byte[] bytes, int indexOffset) {}
}
