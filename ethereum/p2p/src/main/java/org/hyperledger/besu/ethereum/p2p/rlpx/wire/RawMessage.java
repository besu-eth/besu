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
package org.hyperledger.besu.ethereum.p2p.rlpx.wire;

import org.hyperledger.besu.ethereum.p2p.rlpx.framing.SnappyCompressor;

import org.apache.tuweni.bytes.Bytes;

public final class RawMessage extends AbstractMessageData {

  private static final SnappyCompressor compressor = new SnappyCompressor();

  private final int code;
  private volatile byte[] compressedData;
  private volatile boolean decompressed;
  private volatile Bytes decompressedData;

  /** Constructor for uncompressed messages. */
  public RawMessage(final int code, final Bytes data) {
    super(data);
    this.code = code;
    this.compressedData = null;
    this.decompressed = true;
    this.decompressedData = data;
  }

  /** Constructor for compressed messages — decompression is deferred until getData() is called. */
  public RawMessage(final int code, final byte[] compressedData) {
    super(Bytes.EMPTY);
    this.code = code;
    this.compressedData = compressedData;
    this.decompressed = false;
    this.decompressedData = null;
  }

  @Override
  public int getCode() {
    return code;
  }

  @Override
  public Bytes getData() {
    if (!decompressed) {
      synchronized (this) {
        if (!decompressed) {
          decompressedData = Bytes.wrap(compressor.decompress(compressedData));
          decompressed = true;
          compressedData = null;
        }
      }
    }
    return decompressedData;
  }

  public byte[] getCompressedData() {
    return compressedData;
  }

  @Override
  public int getSize() {
    final byte[] compressed = compressedData;
    if (compressed != null) {
      return compressor.uncompressedLength(compressed);
    }
    return decompressedData.size();
  }
}
