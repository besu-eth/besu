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

import org.hyperledger.besu.crypto.SecureRandomProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Optional;

/**
 * Raw-socket WebSocket client that completes the handshake, optionally sends one text frame, then
 * abortively closes (SO_LINGER 0 / TCP RST) without reading the response — reproduces #11277 Layer
 * B.
 */
public final class AbortiveWebSocketClient {

  private static final SecureRandom RANDOM = SecureRandomProvider.publicSecureRandom();

  private AbortiveWebSocketClient() {}

  public static void handshakeSendAndReset(
      final String host, final int port, final String jsonRpcPayload) throws IOException {
    handshakeSendAndReset(host, port, jsonRpcPayload, Optional.empty());
  }

  public static void handshakeSendAndReset(
      final String host,
      final int port,
      final String jsonRpcPayload,
      final Optional<String> bearerToken)
      throws IOException {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress(host, port), 2000);
    try {
      handshakeAndSend(socket, host, port, jsonRpcPayload, bearerToken);
      socket.setSoLinger(true, 0);
    } finally {
      socket.close();
    }
  }

  /**
   * Completes the handshake and sends one text frame, then leaves the socket open without ever
   * reading from it, so the server's writes back up once the small receive buffer fills.
   */
  public static Socket handshakeAndSendWithoutReading(
      final String host, final int port, final String jsonRpcPayload) throws IOException {
    final Socket socket = new Socket();
    socket.setReceiveBufferSize(4096);
    socket.connect(new InetSocketAddress(host, port), 2000);
    try {
      handshakeAndSend(socket, host, port, jsonRpcPayload, Optional.empty());
    } catch (final IOException | RuntimeException e) {
      socket.close();
      throw e;
    }
    return socket;
  }

  /** Abortively closes the socket (SO_LINGER 0 / TCP RST). */
  public static void reset(final Socket socket) throws IOException {
    socket.setSoLinger(true, 0);
    socket.close();
  }

  /** Sends a close frame with status 1000 and leaves the socket open, still not reading. */
  public static void sendCloseFrame(final Socket socket) throws IOException {
    writeMaskedFrame(socket.getOutputStream(), 0x88, new byte[] {0x03, (byte) 0xE8});
  }

  private static void handshakeAndSend(
      final Socket socket,
      final String host,
      final int port,
      final String jsonRpcPayload,
      final Optional<String> bearerToken)
      throws IOException {
    final byte[] payload = jsonRpcPayload.getBytes(StandardCharsets.UTF_8);
    if (payload.length > 125) {
      throw new IllegalArgumentException("payload must be <= 125 bytes for this helper");
    }

    final OutputStream out = socket.getOutputStream();
    final InputStream in = socket.getInputStream();

    final byte[] keyBytes = new byte[16];
    RANDOM.nextBytes(keyBytes);
    final String key = Base64.getEncoder().encodeToString(keyBytes);

    final StringBuilder request = new StringBuilder();
    request
        .append("GET / HTTP/1.1\r\n")
        .append("Host: ")
        .append(host)
        .append(':')
        .append(port)
        .append("\r\n")
        .append("Upgrade: websocket\r\n")
        .append("Connection: Upgrade\r\n")
        .append("Sec-WebSocket-Key: ")
        .append(key)
        .append("\r\n")
        .append("Sec-WebSocket-Version: 13\r\n");
    bearerToken.ifPresent(
        token -> request.append("Authorization: Bearer ").append(token).append("\r\n"));
    request.append("\r\n");
    out.write(request.toString().getBytes(StandardCharsets.UTF_8));
    out.flush();

    final byte[] buf = new byte[4096];
    final int n = in.read(buf);
    if (n <= 0) {
      throw new IOException("empty handshake response");
    }
    final String resp = new String(buf, 0, n, StandardCharsets.UTF_8);
    if (!resp.split("\r\n", 2)[0].contains(" 101 ")) {
      throw new IOException("handshake failed/rejected: " + resp.substring(0, Math.min(120, n)));
    }

    writeMaskedFrame(out, 0x81, payload);
  }

  private static void writeMaskedFrame(
      final OutputStream out, final int firstByte, final byte[] payload) throws IOException {
    final byte[] mask = new byte[4];
    RANDOM.nextBytes(mask);
    final byte[] frame = new byte[2 + 4 + payload.length];
    frame[0] = (byte) firstByte;
    frame[1] = (byte) (0x80 | payload.length);
    System.arraycopy(mask, 0, frame, 2, 4);
    for (int i = 0; i < payload.length; i++) {
      frame[6 + i] = (byte) (payload[i] ^ mask[i % 4]);
    }
    out.write(frame);
    out.flush();
  }
}
