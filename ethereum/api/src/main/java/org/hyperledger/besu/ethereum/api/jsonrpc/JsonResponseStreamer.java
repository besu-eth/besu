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
import java.util.concurrent.atomic.AtomicReference;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.net.SocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonResponseStreamer extends OutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(JsonResponseStreamer.class);

  private final HttpServerResponse response;
  private final SocketAddress remoteAddress;
  private final byte[] singleByteBuf = new byte[1];
  private boolean chunked = false;
  private boolean closed = false;
  private final AtomicReference<Throwable> failure = new AtomicReference<>();

  public JsonResponseStreamer(
      final HttpServerResponse response, final SocketAddress socketAddress) {
    this.response = response;
    this.remoteAddress = socketAddress;
    this.response.exceptionHandler(
        event -> {
          LOG.debug("Write to remote address {} failed", remoteAddress, event);
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

    if (!chunked) {
      response.setChunked(true);
      chunked = true;
    }

    StreamBackpressure.awaitDrain(response);

    Buffer buf = Buffer.buffer(len);
    buf.appendBytes(bbuf, off, len);
    response.write(buf).onFailure(this::handleFailure);
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
      throw (t instanceof IOException) ? (IOException) t : new IOException(t);
    }
  }

  private void handleFailure(final Throwable t) {
    LOG.debug("Write to remote address {} failed", remoteAddress, t);
    failure.set(t);
  }
}
