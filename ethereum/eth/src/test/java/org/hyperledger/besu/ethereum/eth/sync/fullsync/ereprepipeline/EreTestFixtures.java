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
package org.hyperledger.besu.ethereum.eth.sync.fullsync.ereprepipeline;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.zip.CRC32C;

/** Creates Ere archives from block fixtures. */
final class EreTestFixtures {
  private EreTestFixtures() {}

  static Path write(final Path directory, final List<Block> blocks) throws IOException {
    final var file = new ByteArrayOutputStream();
    entry(file, 0x3265, new byte[0]);
    final long[][] positions = new long[blocks.size()][2];
    for (int component = 0; component < 2; component++) {
      for (int i = 0; i < blocks.size(); i++) {
        positions[i][component] = file.size();
        final Block block = blocks.get(i);
        final byte[] data =
            component == 0
                ? RLP.encode(block.getHeader()::writeTo).toArrayUnsafe()
                : RLP.encode(block.getBody()::writeWrappedBodyTo).toArrayUnsafe();
        entry(file, component + 3, frame(data));
      }
    }
    final int indexPosition = file.size();
    final ByteBuffer index =
        ByteBuffer.allocate(24 + blocks.size() * 16).order(ByteOrder.LITTLE_ENDIAN);
    final long start = blocks.getFirst().getHeader().getNumber();
    index.putLong(start);
    for (final long[] pair : positions) {
      index.putLong(pair[0] - indexPosition).putLong(pair[1] - indexPosition);
    }
    index.putLong(2).putLong(blocks.size());
    entry(file, 0x3267, index.array());
    final String name =
        "testnet-%05d-%s-noproofs-noreceipts.ere"
            .formatted(start / 8192, blocks.getLast().getHash().toHexString().substring(2, 10));
    return Files.write(directory.resolve(name), file.toByteArray());
  }

  private static byte[] frame(final byte[] data) throws IOException {
    final var output = new ByteArrayOutputStream();
    output.write(HexFormat.of().parseHex("ff060000734e61507059"));
    for (int offset = 0; offset < data.length; offset += 65536) {
      final int count = Math.min(65536, data.length - offset);
      final int length = count + 4;
      output.write(new byte[] {1, (byte) length, (byte) (length >>> 8), (byte) (length >>> 16)});
      final var crc = new CRC32C();
      crc.update(data, offset, count);
      final int masked = Integer.rotateRight((int) crc.getValue(), 15) + 0xa282ead8;
      output.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(masked).array());
      output.write(data, offset, count);
    }
    return output.toByteArray();
  }

  private static void entry(final ByteArrayOutputStream file, final int type, final byte[] data)
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
}
