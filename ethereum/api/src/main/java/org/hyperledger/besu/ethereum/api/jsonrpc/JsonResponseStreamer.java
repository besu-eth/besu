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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.net.SocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonResponseStreamer extends OutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(JsonResponseStreamer.class);
  private static final long DRAIN_TIMEOUT_SECONDS = 60;

  private final HttpServerResponse response;
  private final SocketAddress remoteAddress;
  private final byte[] singleByteBuf = new byte[1];
  private boolean chunked = false;
  private boolean closed = false;
  private final AtomicReference<Throwable> failure = new AtomicReference<>();
  private volatile CountDownLatch pendingDrain;

  public JsonResponseStreamer(
      final HttpServerResponse response, final SocketAddress socketAddress) {
    this.response = response;
    this.remoteAddress = socketAddress;
    this.response.exceptionHandler(
        event -> {
          LOG.debug("Write to remote address {} failed", remoteAddress, event);
          failure.set(event);
          final CountDownLatch latch = pendingDrain;
          if (latch != null) latch.countDown();
        });
  }

  @Override
  public void write(final int b) throws IOException {
    singleByteBuf[0] = (byte) b;
    write(singleByteBuf, 0, 1);
  }

  @Override
  public void write(final byte[] bbuf, final int off, final int len) throws IOException {
    stopOnFailureOrClosed();

    if (!chunked) {
      response.setChunked(true);
      chunked = true;
    }

    awaitDrain();

    Buffer buf = Buffer.buffer(len);
    buf.appendBytes(bbuf, off, len);
    response.write(buf).onFailure(this::handleFailure);
  }

  private void awaitDrain() throws IOException {
    if (!response.writeQueueFull()) return;
    final CountDownLatch latch = new CountDownLatch(1);
    pendingDrain = latch;
    response.drainHandler(v -> latch.countDown());
    // Check failure after registering pendingDrain in case the connection closed
    // between the writeQueueFull check above and the pendingDrain assignment.
    stopOnFailureOrClosed();
    if (response.writeQueueFull()) {
      try {
        if (!latch.await(DRAIN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          throw new IOException("Timed out waiting for write queue to drain");
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted waiting for write queue to drain", e);
      } finally {
        pendingDrain = null;
      }
    } else {
      pendingDrain = null;
    }
    stopOnFailureOrClosed();
  }

  /**
   * Writes the entire response body and ends the response in a single Vert.x call, setting
   * Content-Length instead of chunked transfer encoding. Use this instead of write()+close() when
   * the full response is already buffered and chunked encoding overhead is undesirable.
   */
  public void writeAndClose(final byte[] data) throws IOException {
    stopOnFailureOrClosed();
    closed = true;
    response.end(Buffer.buffer(data)).onFailure(this::handleFailure);
  }

  /**
   * Variant of {@link #writeAndClose(byte[])} that accepts a pre-built Vert.x {@link Buffer},
   * avoiding the extra copy that {@code Buffer.buffer(byte[])} would introduce.
   */
  public void writeAndClose(final Buffer data) throws IOException {
    stopOnFailureOrClosed();
    closed = true;
    response.end(data).onFailure(this::handleFailure);
  }

  /**
   * An {@link java.io.OutputStream} that appends directly into a Vert.x {@link Buffer}, avoiding
   * the intermediate byte array that {@link java.io.ByteArrayOutputStream} requires before the
   * buffer can be handed to {@link io.vertx.core.http.HttpServerResponse#end(Buffer)}.
   */
  public static final class VertxBufferOutputStream extends java.io.OutputStream {
    private final Buffer buf;
    private final byte[] singleByte = new byte[1];

    public VertxBufferOutputStream(final Buffer buf) {
      this.buf = buf;
    }

    @Override
    public void write(final int b) {
      singleByte[0] = (byte) b;
      buf.appendBytes(singleByte);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) {
      buf.appendBytes(b, off, len);
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      if (chunked) {
        // Only end the response if data was actually written.  When nothing
        // was written the headers have not been flushed, leaving the caller
        // free to send a proper error response with the correct status code.
        response.end();
      }
    }
  }

  private void stopOnFailureOrClosed() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    Throwable t = failure.get();
    if (t != null) {
      LOG.debug("Stop writing to remote address {} due to a failure", remoteAddress, t);
      throw (t instanceof IOException ioException) ? ioException : new IOException(t);
    }
  }

  private void handleFailure(final Throwable t) {
    LOG.debug("Write to remote address {} failed", remoteAddress, t);
    failure.set(t);
  }
}
