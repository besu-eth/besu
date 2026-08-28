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
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.vertx.core.Handler;
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

  private static final long DRAIN_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(60);

  /** Bounds how long a waiter can stay parked after the peer has gone away. */
  private static final long ABORT_POLL_MILLIS = 50;

  private StreamBackpressure() {}

  /** Signals that a response can no longer be delivered. */
  @FunctionalInterface
  public interface AbortCheck {

    /**
     * Implementations should throw {@link ClosedChannelException} for a lost peer and the
     * underlying failure for a write error, so the two stay distinguishable in the logs.
     *
     * @throws IOException if the response can no longer be delivered
     */
    void checkNotAborted() throws IOException;
  }

  /**
   * If the write queue is full, blocks until it drains below the low watermark, the peer goes away,
   * or the timeout expires.
   *
   * @param stream the Vertx WriteStream to check
   * @param abortCheck throws once the response can no longer be written
   * @throws IOException if {@code abortCheck} throws while waiting, if the timeout expires, or if
   *     the thread is interrupted
   */
  public static void awaitDrain(final WriteStream<?> stream, final AbortCheck abortCheck)
      throws IOException {
    awaitDrain(stream, abortCheck, DRAIN_TIMEOUT_MILLIS);
  }

  static void awaitDrain(
      final WriteStream<?> stream, final AbortCheck abortCheck, final long timeoutMillis)
      throws IOException {
    // A closed ServerWebSocket rejects every query about its write queue, so the abort has to be
    // detected before the stream is touched.
    abortCheck.checkNotAborted();

    if (!writeQueueFull(stream)) {
      return;
    }

    final CountDownLatch latch = new CountDownLatch(1);
    if (!setDrainHandler(stream, v -> latch.countDown())) {
      throw new ClosedChannelException();
    }
    try {
      // Guards against the queue draining between the full-check and the handler registration.
      if (!writeQueueFull(stream)) {
        return;
      }
      final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
      while (true) {
        // Nothing counts the latch down when the peer disappears, so the abort has to be polled.
        abortCheck.checkNotAborted();
        final long remainingNanos = deadline - System.nanoTime();
        if (remainingNanos <= 0) {
          throw new IOException("Timed out waiting for write queue to drain");
        }
        final long waitMillis =
            Math.min(ABORT_POLL_MILLIS, TimeUnit.NANOSECONDS.toMillis(remainingNanos) + 1);
        if (latch.await(waitMillis, TimeUnit.MILLISECONDS)) {
          return;
        }
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted waiting for write queue to drain", e);
    } finally {
      // Do not leave a handler pointing at a latch nobody waits on any more.
      setDrainHandler(stream, null);
    }
  }

  private static boolean writeQueueFull(final WriteStream<?> stream) throws IOException {
    try {
      return stream.writeQueueFull();
    } catch (final IllegalStateException e) {
      // ServerWebSocket throws instead of answering once the socket is closed
      final ClosedChannelException closed = new ClosedChannelException();
      closed.initCause(e);
      throw closed;
    }
  }

  /**
   * @return false if the stream refused the handler because it is already closed
   */
  private static boolean setDrainHandler(
      final WriteStream<?> stream, final Handler<Void> drainHandler) {
    try {
      stream.drainHandler(drainHandler);
      return true;
    } catch (final IllegalStateException e) {
      // ServerWebSocket.drainHandler() throws once the socket is closed, even when clearing the
      // handler with null. Swallowed so it cannot mask the abort the caller reports.
      return false;
    }
  }
}
