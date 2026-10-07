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
package org.hyperledger.besu.ethereum.p2p.rlpx.framing;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.xerial.snappy.Snappy;

/**
 * A strategy for compressing and decompressing data with the Snappy algorithm.
 *
 * @see <a href="https://google.github.io/snappy/">Snappy algorithm</a>
 */
public class SnappyCompressor {

  public byte[] compress(final byte[] uncompressed) {
    checkNotNull(uncompressed, "input data must not be null");
    try {
      return Snappy.compress(uncompressed);
    } catch (final IOException e) {
      throw new FramingException("Snappy compression failed", e);
    }
  }

  public byte[] decompress(final byte[] compressed) {
    checkNotNull(compressed, "input data must not be null");
    try {
      return Snappy.uncompress(compressed);
    } catch (final IOException | RuntimeException e) {
      throw new FramingException("Snappy decompression failed", e);
    }
  }

  public int uncompressedLength(final byte[] compressed) {
    checkNotNull(compressed, "input data must not be null");
    try {
      return Snappy.uncompressedLength(compressed);
    } catch (final IOException e) {
      throw new FramingException("Snappy uncompressedLength failed", e);
    }
  }
}
