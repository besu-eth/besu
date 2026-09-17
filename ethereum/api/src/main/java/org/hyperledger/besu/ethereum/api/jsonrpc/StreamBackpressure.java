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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.vertx.core.streams.WriteStream;

/**
 * Blocks the calling thread until a Vertx {@link WriteStream} write queue is no longer full. This
 * prevents unbounded accumulation of Netty direct-memory buffers when the producer is faster than
 * the network can drain.
 *
 * <p>Safe to call from {@code executeBlocking} worker threads only — must never be called from the
 * Vertx event loop.
 */
public final class StreamBackpressure {

  private static final long DRAIN_TIMEOUT_SECONDS = 60;

  private StreamBackpressure() {}

  /**
   * If the write queue is full, blocks until it drains below the low watermark or the timeout
   * expires.
   *
   * @param stream the Vertx WriteStream to check
   * @throws IOException if the timeout expires or the thread is interrupted
   */
  public static void awaitDrain(final WriteStream<?> stream) throws IOException {
    if (stream.writeQueueFull()) {
      final CountDownLatch latch = new CountDownLatch(1);
      stream.drainHandler(v -> latch.countDown());
      // Re-check after setting handler to avoid race where queue drained
      // between the full-check and the handler registration
      if (stream.writeQueueFull()) {
        try {
          if (!latch.await(DRAIN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new IOException("Timed out waiting for write queue to drain");
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted waiting for write queue to drain", e);
        }
      }
    }
  }
}
