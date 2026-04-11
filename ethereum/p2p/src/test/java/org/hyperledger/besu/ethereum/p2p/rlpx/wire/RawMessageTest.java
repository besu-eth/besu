/*
 * Copyright contributors to Hyperledger Besu.
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.p2p.rlpx.framing.SnappyCompressor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class RawMessageTest {

  private static final SnappyCompressor compressor = new SnappyCompressor();
  private static final int CODE = 0x10;
  private static final byte[] ORIGINAL_DATA = "hello world, this is a test message".getBytes(UTF_8);
  private static final byte[] COMPRESSED_DATA = compressor.compress(ORIGINAL_DATA);

  @Test
  void uncompressedConstructorReturnsDataImmediately() {
    final Bytes data = Bytes.wrap(ORIGINAL_DATA);
    final RawMessage message = new RawMessage(CODE, data);

    assertThat(message.getCode()).isEqualTo(CODE);
    assertThat(message.getData()).isEqualTo(data);
    assertThat(message.getCompressedData()).isNull();
  }

  @Test
  void uncompressedConstructorGetSizeIsCorrect() {
    final Bytes data = Bytes.wrap(ORIGINAL_DATA);
    final RawMessage message = new RawMessage(CODE, data);

    assertThat(message.getSize()).isEqualTo(ORIGINAL_DATA.length);
  }

  @Test
  void compressedConstructorDefersDecompression() {
    final RawMessage message = new RawMessage(CODE, COMPRESSED_DATA.clone());

    assertThat(message.getCompressedData()).isNotNull();
  }

  @Test
  void compressedConstructorGetDataDecompressesCorrectly() {
    final RawMessage message = new RawMessage(CODE, COMPRESSED_DATA.clone());

    final Bytes result = message.getData();

    assertThat(result).isEqualTo(Bytes.wrap(ORIGINAL_DATA));
  }

  @Test
  void compressedConstructorGetSizeBeforeDecompression() {
    final RawMessage message = new RawMessage(CODE, COMPRESSED_DATA.clone());

    assertThat(message.getSize()).isEqualTo(ORIGINAL_DATA.length);
    // compressed data should still be present (getSize does not trigger decompression)
    assertThat(message.getCompressedData()).isNotNull();
  }

  @Test
  void compressedConstructorGetSizeAfterDecompression() {
    final RawMessage message = new RawMessage(CODE, COMPRESSED_DATA.clone());

    message.getData(); // trigger decompression
    assertThat(message.getSize()).isEqualTo(ORIGINAL_DATA.length);
  }

  @Test
  void compressedDataIsNulledAfterDecompression() {
    final RawMessage message = new RawMessage(CODE, COMPRESSED_DATA.clone());

    message.getData();
    assertThat(message.getCompressedData()).isNull();
  }

  @Test
  void getDataIsIdempotent() {
    final RawMessage message = new RawMessage(CODE, COMPRESSED_DATA.clone());

    final Bytes first = message.getData();
    final Bytes second = message.getData();

    assertThat(first).isEqualTo(Bytes.wrap(ORIGINAL_DATA));
    assertThat(first).isSameAs(second);
  }

  @Test
  void concurrentGetDataDecompressesOnlyOnce() throws Exception {
    final int threadCount = 16;
    final RawMessage message = new RawMessage(CODE, COMPRESSED_DATA.clone());
    final CyclicBarrier barrier = new CyclicBarrier(threadCount);
    final ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    try {
      final List<Future<Bytes>> futures = new ArrayList<>();
      for (int i = 0; i < threadCount; i++) {
        futures.add(
            executor.submit(
                () -> {
                  barrier.await();
                  return message.getData();
                }));
      }

      Bytes firstResult = null;
      for (final Future<Bytes> future : futures) {
        final Bytes result = future.get();
        assertThat(result).isEqualTo(Bytes.wrap(ORIGINAL_DATA));
        if (firstResult == null) {
          firstResult = result;
        } else {
          // all threads should get the exact same Bytes instance
          assertThat(result).isSameAs(firstResult);
        }
      }
    } finally {
      executor.shutdown();
    }
  }
}
