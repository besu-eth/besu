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

import org.hyperledger.besu.util.snappy.SnappyFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.xerial.snappy.Snappy;

/**
 * Reads the header and body components of an Ere archive for full block import. RLP decoding, block
 * validation and authentication of the chain are the caller's responsibility. Receipts, proofs and
 * total difficulty are indexed but not decoded or cryptographically verified here.
 */
public class EreReader {
  private static final int VERSION = 0x3265;
  private static final int INDEX = 0x3267;
  private static final int HEADER = 0x03;
  private static final int BODY = 0x04;
  private static final int DIFFICULTY = 0x06;
  private static final int ACCUMULATOR = 0x07;
  private static final int RECEIPTS = 0x0a;
  private static final int PROOF = 0x0b;
  private static final int MAX_BLOCKS = 8192;
  private static final int ENTRY_HEADER_SIZE = 8;
  private static final int MAX_SNAPPY_CHUNK_SIZE = 65536;
  private static final byte[] SNAPPY_STREAM_HEADER = {
    (byte) 0xff, 6, 0, 0, 's', 'N', 'a', 'P', 'p', 'Y'
  };
  private final SnappyFactory snappyFactory;

  /**
   * Creates an Ere reader.
   *
   * @param snappyFactory the factory for framed Snappy decompression
   */
  public EreReader(final SnappyFactory snappyFactory) {
    this.snappyFactory = snappyFactory;
  }

  /**
   * Reads blocks in index order, retaining at most one decoded block at a time. The caller owns the
   * channel and must close it. The complete index is checked before any callbacks, but a later
   * decompression failure can occur after earlier blocks have been delivered.
   *
   * @param channel a seekable channel containing one complete Ere archive
   * @param maxComponentSize maximum encoded and decoded bytes per header or body; an operational
   *     resource limit, not a consensus limit
   * @param consumer receives the indexed block number and RLP-encoded header and body
   * @throws IOException for malformed, truncated or oversized input
   */
  public void read(
      final SeekableByteChannel channel, final int maxComponentSize, final BlockConsumer consumer)
      throws IOException {
    if (maxComponentSize <= 0 || maxComponentSize == Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Component size limit must be between 1 and 2147483646");
    }
    final long size = channel.size();
    final Entry version = readEntry(channel, 0, size);
    if (version.type() != VERSION || version.length() != 0) {
      throw new IOException("Ere archive must start with an empty version entry");
    }
    final Map<Long, Entry> components = new HashMap<>();
    boolean accumulatorSeen = false;
    long position = ENTRY_HEADER_SIZE;
    while (position < size) {
      final Entry entry = readEntry(channel, position, size);
      final long end = position + ENTRY_HEADER_SIZE + entry.length();
      if (entry.type() == INDEX) {
        if (end != size) {
          throw new IOException("Ere index must be the final entry");
        }
        final BlockIndex index = readIndex(channel, entry, components);
        for (int i = 0; i < index.blocks().size(); i++) {
          final BlockEntries block = index.blocks().get(i);
          consumer.accept(
              index.start() + i,
              decompress(channel, block.header(), maxComponentSize),
              decompress(channel, block.body(), maxComponentSize));
        }
        return;
      }
      switch (entry.type()) {
        case HEADER, BODY, RECEIPTS, PROOF, DIFFICULTY -> {
          if (components.size() >= MAX_BLOCKS * 5) {
            throw new IOException("Too many Ere block components");
          }
          if (entry.type() == DIFFICULTY && entry.length() != 32) {
            throw new IOException("Ere total difficulty must contain 32 bytes");
          }
          components.put(position, entry);
        }
        case ACCUMULATOR -> {
          if (accumulatorSeen || entry.length() != 32) {
            throw new IOException("Invalid Ere accumulator entry");
          }
          accumulatorSeen = true;
        }
        case VERSION, 0x05, 0x3266 ->
            throw new IOException("Unexpected version or legacy Era1 entry in Ere archive");
        default -> {
          // e2store permits unknown entries, which are skipped without allocating their payload.
        }
      }
      position = end;
    }
    throw new IOException("Missing Ere dynamic block index");
  }

  private BlockIndex readIndex(
      final SeekableByteChannel channel, final Entry index, final Map<Long, Entry> components)
      throws IOException {
    if (index.length() < 24 || index.length() > 24L + MAX_BLOCKS * 5L * Long.BYTES) {
      throw new IOException("Invalid Ere index length");
    }
    final ByteBuffer data =
        readBytes(channel, index.offset() + ENTRY_HEADER_SIZE, (int) index.length());
    final long start = data.getLong();
    final long componentCount = data.getLong(data.limit() - 16);
    final long count = data.getLong(data.limit() - 8);
    if (count < 1 || count > MAX_BLOCKS || componentCount < 2 || componentCount > 5) {
      throw new IOException("Invalid Ere block or component count");
    }
    if (start < 0 || start > Long.MAX_VALUE - (count - 1)) {
      throw new IOException("Ere block number is outside the supported range");
    }
    if (index.length() != 24 + count * componentCount * Long.BYTES
        || components.size() != count * componentCount) {
      throw new IOException("Ere index does not match the component entries");
    }
    final List<BlockEntries> blocks = new ArrayList<>((int) count);
    final var visited = new HashSet<Long>();
    final int[] layout = new int[(int) componentCount];
    for (int block = 0; block < count; block++) {
      final Map<Integer, Entry> row = new HashMap<>();
      for (int slot = 0; slot < componentCount; slot++) {
        // Offsets are two's-complement relative to the beginning of the index record, not each
        // slot.
        final long relative = data.getLong();
        if (relative >= 0 || relative < ENTRY_HEADER_SIZE - index.offset()) {
          throw new IOException("Ere component offset is outside the data section");
        }
        final long absolute = index.offset() + relative;
        final Entry component = components.get(absolute);
        if (component == null || !visited.add(absolute)) {
          throw new IOException("Ere index points to a missing or duplicate component");
        }
        if (row.put(component.type(), component) != null) {
          throw new IOException("Duplicate component type in Ere block index");
        }
        if (block == 0) {
          layout[slot] = component.type();
        } else if (layout[slot] != component.type()) {
          throw new IOException("Inconsistent component layout in Ere index");
        }
      }
      final Entry header = row.get(HEADER);
      final Entry body = row.get(BODY);
      if (header == null || body == null) {
        throw new IOException("Ere index requires a header and body for every block");
      }
      blocks.add(new BlockEntries(header, body));
    }
    return new BlockIndex(start, blocks);
  }

  private Entry readEntry(final SeekableByteChannel channel, final long position, final long size)
      throws IOException {
    if (position < 0 || size - position < ENTRY_HEADER_SIZE) {
      throw new EOFException("Truncated e2store entry header");
    }
    final ByteBuffer header = readBytes(channel, position, ENTRY_HEADER_SIZE);
    final int type = Short.toUnsignedInt(header.getShort());
    final long length = Integer.toUnsignedLong(header.getInt());
    if (header.getShort() != 0) {
      throw new IOException("Nonzero e2store reserved bytes");
    }
    if (length > size - position - ENTRY_HEADER_SIZE) {
      throw new EOFException("Truncated e2store entry payload");
    }
    return new Entry(type, position, length);
  }

  private byte[] decompress(
      final SeekableByteChannel channel, final Entry entry, final int maxComponentSize)
      throws IOException {
    if (entry.length() > maxComponentSize) {
      throw new IOException("Encoded Ere component exceeds size limit");
    }
    final byte[] encoded =
        readBytes(channel, entry.offset() + ENTRY_HEADER_SIZE, (int) entry.length()).array();
    try (var stream =
        snappyFactory.createFramedInputStream(checkSnappyFrames(encoded, maxComponentSize))) {
      final byte[] decoded = stream.readNBytes(maxComponentSize + 1);
      if (decoded.length > maxComponentSize) {
        throw new IOException("Decoded Ere component exceeds size limit");
      }
      return decoded;
    }
  }

  private byte[] checkSnappyFrames(final byte[] encoded, final int maxComponentSize)
      throws IOException {
    if (encoded.length < SNAPPY_STREAM_HEADER.length
        || !Arrays.equals(
            encoded,
            0,
            SNAPPY_STREAM_HEADER.length,
            SNAPPY_STREAM_HEADER,
            0,
            SNAPPY_STREAM_HEADER.length)) {
      throw new IOException("Invalid Ere Snappy stream identifier");
    }
    long decodedSize = 0;
    int retained = SNAPPY_STREAM_HEADER.length;
    int position = retained;
    while (position < encoded.length) {
      if (encoded.length - position < 4) {
        throw new EOFException("Truncated Ere Snappy chunk header");
      }
      final int type = Byte.toUnsignedInt(encoded[position]);
      final int length =
          Byte.toUnsignedInt(encoded[position + 1])
              | (Byte.toUnsignedInt(encoded[position + 2]) << 8)
              | (Byte.toUnsignedInt(encoded[position + 3]) << 16);
      if (length > encoded.length - position - 4) {
        throw new EOFException("Truncated Ere Snappy chunk payload");
      }
      if (type == 0 || type == 1) {
        if (length < (type == 0 ? 5 : 4)) {
          throw new IOException("Invalid Ere Snappy data chunk length");
        }
        // Check declared sizes before the framed decoder allocates its output buffers.
        final int decoded =
            type == 0 ? Snappy.uncompressedLength(encoded, position + 8, length - 4) : length - 4;
        if (decoded < 0 || decoded > MAX_SNAPPY_CHUNK_SIZE) {
          throw new IOException("Ere Snappy chunk exceeds the format size limit");
        }
        decodedSize += decoded;
        if (decodedSize > maxComponentSize) {
          throw new IOException("Decoded Ere component exceeds size limit");
        }
        // Remove skippable chunks and repeated identifiers iteratively so the framed decoder
        // cannot recurse through an attacker-controlled sequence of metadata chunks.
        System.arraycopy(encoded, position, encoded, retained, length + 4);
        retained += length + 4;
      } else if (type == 0xff) {
        if (length != 6
            || !Arrays.equals(
                encoded,
                position,
                position + 10,
                SNAPPY_STREAM_HEADER,
                0,
                SNAPPY_STREAM_HEADER.length)) {
          throw new IOException("Invalid Ere Snappy stream identifier");
        }
      } else if (type < 0x80) {
        throw new IOException("Unsupported unskippable Ere Snappy chunk");
      }
      position += length + 4;
    }
    return retained == encoded.length ? encoded : Arrays.copyOf(encoded, retained);
  }

  private ByteBuffer readBytes(
      final SeekableByteChannel channel, final long position, final int length) throws IOException {
    channel.position(position);
    final ByteBuffer buffer = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
    while (buffer.hasRemaining()) {
      if (channel.read(buffer) < 0) {
        throw new EOFException("Truncated Ere archive");
      }
    }
    return buffer.flip();
  }

  private record Entry(int type, long offset, long length) {}

  private record BlockEntries(Entry header, Entry body) {}

  private record BlockIndex(long start, List<BlockEntries> blocks) {}

  /** Receives decoded block components; implementations may reject them with an IOException. */
  @FunctionalInterface
  public interface BlockConsumer {
    /**
     * Consumes a block's RLP-encoded components.
     *
     * @param number the number from the file index, to be checked against the decoded header
     * @param header RLP-encoded execution header
     * @param body RLP-encoded execution body
     * @throws IOException if the consumer cannot process the block
     */
    void accept(long number, byte[] header, byte[] body) throws IOException;
  }
}
