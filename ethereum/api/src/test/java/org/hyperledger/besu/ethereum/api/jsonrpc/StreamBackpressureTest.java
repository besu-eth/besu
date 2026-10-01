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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.jsonrpc.StreamBackpressure.AbortCheck;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class StreamBackpressureTest {

  private static final long TIMEOUT_MILLIS = 30_000;

  private static final AbortCheck NEVER_ABORTS = () -> {};
  private static final AbortCheck ALWAYS_ABORTED =
      () -> {
        throw new ClosedChannelException();
      };

  @Mock private WriteStream<Buffer> stream;

  private final AtomicReference<Handler<Void>> drainHandler = new AtomicReference<>();

  @BeforeEach
  void captureDrainHandler() {
    when(stream.drainHandler(any()))
        .thenAnswer(
            invocation -> {
              drainHandler.set(invocation.getArgument(0));
              return stream;
            });
  }

  @Test
  void returnsImmediatelyWhenQueueIsNotFull() throws IOException {
    when(stream.writeQueueFull()).thenReturn(false);

    StreamBackpressure.awaitDrain(stream, NEVER_ABORTS, TIMEOUT_MILLIS);

    verify(stream, never()).drainHandler(any());
  }

  @Test
  void returnsWhenQueueDrainsAfterHandlerRegistration() {
    when(stream.writeQueueFull()).thenReturn(true, false);

    assertThatCode(() -> StreamBackpressure.awaitDrain(stream, NEVER_ABORTS, TIMEOUT_MILLIS))
        .doesNotThrowAnyException();
  }

  @Test
  void returnsWhenDrainHandlerFires() {
    when(stream.writeQueueFull()).thenReturn(true);
    when(stream.drainHandler(any()))
        .thenAnswer(
            invocation -> {
              final Handler<Void> handler = invocation.getArgument(0);
              if (handler != null) {
                handler.handle(null);
              }
              return stream;
            });

    assertThatCode(() -> StreamBackpressure.awaitDrain(stream, NEVER_ABORTS, TIMEOUT_MILLIS))
        .doesNotThrowAnyException();
  }

  @Test
  void abortsPromptlyWhenPeerGoesAwayWhileWaiting() {
    when(stream.writeQueueFull()).thenReturn(true);
    final AtomicBoolean aborted = new AtomicBoolean(false);
    // the dead-peer case: the drain callback never fires, so nothing counts the latch down
    new Thread(
            () -> {
              try {
                Thread.sleep(200);
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              aborted.set(true);
            })
        .start();

    final AbortCheck abortCheck =
        () -> {
          if (aborted.get()) {
            throw new ClosedChannelException();
          }
        };

    final long start = System.nanoTime();
    assertThatThrownBy(() -> StreamBackpressure.awaitDrain(stream, abortCheck, TIMEOUT_MILLIS))
        .isInstanceOf(ClosedChannelException.class);
    final long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

    // Must not wait out the full backpressure timeout.
    assertThat(elapsedMillis).isLessThan(TIMEOUT_MILLIS / 2);
  }

  @Test
  void abortsBeforeWaitingWhenPeerIsAlreadyGone() {
    when(stream.writeQueueFull()).thenReturn(true);

    assertThatThrownBy(() -> StreamBackpressure.awaitDrain(stream, ALWAYS_ABORTED, TIMEOUT_MILLIS))
        .isInstanceOf(ClosedChannelException.class);
  }

  @Test
  void abortsWithoutQueryingAClosedStream() {
    // ServerWebSocket.writeQueueFull() throws rather than answering once the socket is closed
    when(stream.writeQueueFull()).thenThrow(new IllegalStateException("WebSocket is closed"));

    assertThatThrownBy(() -> StreamBackpressure.awaitDrain(stream, ALWAYS_ABORTED, TIMEOUT_MILLIS))
        .isInstanceOf(ClosedChannelException.class);

    verify(stream, never()).writeQueueFull();
  }

  @Test
  void reportsAStreamClosedDuringTheFullCheckAsAClosedChannel() {
    when(stream.writeQueueFull()).thenThrow(new IllegalStateException("WebSocket is closed"));

    assertThatThrownBy(() -> StreamBackpressure.awaitDrain(stream, NEVER_ABORTS, TIMEOUT_MILLIS))
        .isInstanceOf(ClosedChannelException.class)
        .hasCauseInstanceOf(IllegalStateException.class);
  }

  @Test
  void reportsAStreamThatRefusesTheDrainHandlerAsAClosedChannel() {
    // the socket closed between the full-check and the handler registration
    when(stream.writeQueueFull()).thenReturn(true);
    when(stream.drainHandler(any())).thenThrow(new IllegalStateException("WebSocket is closed"));

    assertThatThrownBy(() -> StreamBackpressure.awaitDrain(stream, NEVER_ABORTS, TIMEOUT_MILLIS))
        .isInstanceOf(ClosedChannelException.class);
  }

  @Test
  void clearingTheDrainHandlerOnAClosedStreamDoesNotMaskTheAbort() {
    // ServerWebSocket.drainHandler() throws once the socket is closed, even for a null handler
    when(stream.writeQueueFull()).thenReturn(true);
    when(stream.drainHandler(isNull())).thenThrow(new IllegalStateException("WebSocket is closed"));

    assertThatThrownBy(() -> StreamBackpressure.awaitDrain(stream, abortAfterFirstCheck(), 150))
        .isInstanceOf(ClosedChannelException.class);
  }

  @Test
  void propagatesTheAbortCheckFailureUnchanged() {
    when(stream.writeQueueFull()).thenReturn(true);
    final IOException cause = new IOException("SSL write failed");

    assertThatThrownBy(
            () ->
                StreamBackpressure.awaitDrain(
                    stream,
                    () -> {
                      throw cause;
                    },
                    TIMEOUT_MILLIS))
        .isSameAs(cause);
  }

  @Test
  void throwsOnTimeoutWhenPeerIsAliveButNeverDrains() {
    when(stream.writeQueueFull()).thenReturn(true);

    assertThatThrownBy(() -> StreamBackpressure.awaitDrain(stream, NEVER_ABORTS, 150))
        .isInstanceOf(IOException.class)
        .isNotInstanceOf(ClosedChannelException.class)
        .hasMessageContaining("Timed out waiting for write queue to drain");
  }

  @Test
  void clearsDrainHandlerOnAbort() {
    when(stream.writeQueueFull()).thenReturn(true);

    assertThatThrownBy(() -> StreamBackpressure.awaitDrain(stream, abortAfterFirstCheck(), 150))
        .isInstanceOf(ClosedChannelException.class);

    assertThat(drainHandler.get()).isNull();
  }

  /**
   * Passes the pre-registration check and aborts on the first in-loop check, so the drain handler
   * is registered before the abort is reported.
   */
  private AbortCheck abortAfterFirstCheck() {
    final AtomicBoolean firstCheck = new AtomicBoolean(true);
    return () -> {
      if (!firstCheck.compareAndSet(true, false)) {
        throw new ClosedChannelException();
      }
    };
  }
}
