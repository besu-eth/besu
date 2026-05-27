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
package org.hyperledger.besu.ethereum.api.jsonrpc.websocket;

import org.hyperledger.besu.ethereum.api.jsonrpc.StreamBackpressure;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicReference;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.http.WebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JsonResponseStreamer extends OutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(JsonResponseStreamer.class);
  private static final Buffer EMPTY_BUFFER = Buffer.buffer();

  private final ServerWebSocket response;
  private final byte[] singleByteBuf = new byte[1];
  private boolean firstFrame = true;
  private boolean closed = false;
  private Buffer buffer = EMPTY_BUFFER;
  private final AtomicReference<Throwable> failure = new AtomicReference<>();

  public JsonResponseStreamer(final ServerWebSocket response) {
    this.response = response;
    this.response.exceptionHandler(
        event -> {
          LOG.debug("Write to remote address {} failed", response.remoteAddress(), event);
          failure.set(event);
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

    if (buffer != EMPTY_BUFFER) {
      StreamBackpressure.awaitDrain(response);
      writeFrame(buffer, false);
    }
    Buffer buf = Buffer.buffer(len);
    buf.appendBytes(bbuf, off, len);
    buffer = buf;
  }

  private void writeFrame(final Buffer buf, final boolean isFinal) {
    if (firstFrame) {
      response
          .writeFrame(WebSocketFrame.textFrame(buf.toString(), isFinal))
          .onFailure(this::handleFailure);
      firstFrame = false;
    } else {
      response
          .writeFrame(WebSocketFrame.continuationFrame(buf, isFinal))
          .onFailure(this::handleFailure);
    }
  }

  @Override
  public void close() throws IOException {
    // write last buffer only if there were no previous failures and not already closed
    if (!closed && failure.get() == null) {
      writeFrame(buffer, true);
      closed = true;
    }
  }

  private void stopOnFailureOrClosed() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    Throwable t = failure.get();
    if (t != null) {
      LOG.debug("Stop writing to remote address {} due to a failure", response.remoteAddress(), t);
      throw (t instanceof IOException) ? (IOException) t : new IOException(t);
    }
  }

  private void handleFailure(final Throwable t) {
    LOG.debug("Write to remote address {} failed", response.remoteAddress(), t);
    failure.set(t);
  }
}
