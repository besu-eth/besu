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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.impl.future.FailedFuture;
import io.vertx.core.impl.future.SucceededFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class JsonResponseStreamerTest {

  @Mock private ServerWebSocket response;

  @Mock private ServerWebSocket failedResponse;

  @BeforeEach
  public void before() {
    when(response.writeFrame(any(WebSocketFrame.class)))
        .thenReturn(new SucceededFuture<>(null, null));
    when(failedResponse.writeFrame(any(WebSocketFrame.class)))
        .thenReturn(new FailedFuture<Void>(new IOException()));
  }

  @Test
  public void writeSingleChar() throws IOException {
    try (JsonResponseStreamer streamer = new JsonResponseStreamer(response)) {
      streamer.write('x');
    }

    verify(response).writeFrame(argThat(frameContains("x", true)));
  }

  @Test
  public void writeString() throws IOException {
    try (JsonResponseStreamer streamer = new JsonResponseStreamer(response)) {
      streamer.write("xyz".getBytes(StandardCharsets.UTF_8), 0, 3);
    }

    verify(response).writeFrame(argThat(frameContains("xyz", true)));
  }

  @Test
  public void writeSubString() throws IOException {
    try (JsonResponseStreamer streamer = new JsonResponseStreamer(response)) {
      streamer.write("abcxyz".getBytes(StandardCharsets.UTF_8), 1, 3);
    }

    verify(response).writeFrame(argThat(frameContains("bcx", true)));
  }

  @Test
  public void writeTwice() throws IOException {
    try (JsonResponseStreamer streamer = new JsonResponseStreamer(response)) {
      streamer.write("xyz".getBytes(StandardCharsets.UTF_8));
      streamer.write('\n');
    }

    verify(response).writeFrame(argThat(frameContains("xyz", false)));
    verify(response).writeFrame(argThat(frameContains("\n", true)));
  }

  @Test
  public void stopOnError() throws IOException {
    try (JsonResponseStreamer streamer = new JsonResponseStreamer(failedResponse)) {
      streamer.write("xyz".getBytes(StandardCharsets.UTF_8));
      streamer.write('\n');
      assertThatThrownBy(() -> streamer.write('\n')).isInstanceOf(IOException.class);
    }

    verify(failedResponse).writeFrame(argThat(frameContains("xyz", false)));
    verify(failedResponse, never()).writeFrame(argThat(frameContains("\n", true)));
  }

  @Test
  public void writeAbortsWhenWebSocketClosesWhileQueueIsFull() throws IOException {
    when(response.writeQueueFull()).thenReturn(true);
    final AtomicBoolean webSocketClosed = new AtomicBoolean(false);
    when(response.isClosed()).thenAnswer(invocation -> webSocketClosed.get());

    JsonResponseStreamer streamer = new JsonResponseStreamer(response);
    // the first write only buffers; the second is the one that has to wait for the queue
    streamer.write("xyz".getBytes(StandardCharsets.UTF_8));
    webSocketClosed.set(true);

    assertThatThrownBy(() -> streamer.write('\n')).isInstanceOf(ClosedChannelException.class);
    verify(response, never()).writeFrame(any(WebSocketFrame.class));
  }

  @Test
  public void writeAbortsWhenWebSocketIsAlreadyClosed() {
    // a closed ServerWebSocket throws IllegalStateException from writeQueueFull() and
    // drainHandler(), so the abort has to be detected without touching either
    when(response.isClosed()).thenReturn(true);
    when(response.writeQueueFull()).thenThrow(new IllegalStateException("WebSocket is closed"));
    when(response.drainHandler(any())).thenThrow(new IllegalStateException("WebSocket is closed"));

    JsonResponseStreamer streamer = new JsonResponseStreamer(response);

    assertThatThrownBy(() -> streamer.write("xyz".getBytes(StandardCharsets.UTF_8)))
        .isInstanceOf(ClosedChannelException.class);
    verify(response, never()).writeFrame(any(WebSocketFrame.class));
  }

  private ArgumentMatcher<WebSocketFrame> frameContains(final String text, final boolean isFinal) {
    return frame -> frame.textData().equals(text) && frame.isFinal() == isFinal;
  }
}
