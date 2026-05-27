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

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.impl.future.FailedFuture;
import io.vertx.core.impl.future.SucceededFuture;
import io.vertx.core.net.SocketAddress;
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
  private final SocketAddress testAddress = SocketAddress.domainSocketAddress("test");

  @Mock private HttpServerResponse httpResponse;

  @Mock private HttpServerResponse failedResponse;

  @BeforeEach
  public void before() {
    when(httpResponse.write(any(Buffer.class))).thenReturn(new SucceededFuture<>(null, null));
    when(failedResponse.write(any(Buffer.class)))
        .thenReturn(new FailedFuture<Void>(new IOException()));
  }

  @Test
  public void writeSingleChar() throws IOException {
    JsonResponseStreamer streamer = new JsonResponseStreamer(httpResponse, testAddress);
    streamer.write('x');

    verify(httpResponse).write(argThat(bufferContains("x")));
  }

  @Test
  public void writeString() throws IOException {
    JsonResponseStreamer streamer = new JsonResponseStreamer(httpResponse, testAddress);
    streamer.write("xyz".getBytes(StandardCharsets.UTF_8));

    verify(httpResponse).write(argThat(bufferContains("xyz")));
  }

  @Test
  public void writeSubString() throws IOException {
    JsonResponseStreamer streamer = new JsonResponseStreamer(httpResponse, testAddress);
    streamer.write("abcxyz".getBytes(StandardCharsets.UTF_8), 1, 3);

    verify(httpResponse).write(argThat(bufferContains("bcx")));
  }

  @Test
  public void writeTwice() throws IOException {
    JsonResponseStreamer streamer = new JsonResponseStreamer(httpResponse, testAddress);
    streamer.write("xyz".getBytes(StandardCharsets.UTF_8));
    streamer.write('\n');

    verify(httpResponse).write(argThat(bufferContains("xyz")));
    verify(httpResponse).write(argThat(bufferContains("\n")));
  }

  @Test
  public void writeStringAndClose() throws IOException {
    try (JsonResponseStreamer streamer = new JsonResponseStreamer(httpResponse, testAddress)) {
      streamer.write("xyz".getBytes(StandardCharsets.UTF_8));
    }

    verify(httpResponse).write(argThat(bufferContains("xyz")));
    verify(httpResponse).end();
  }

  @Test
  public void closeWithoutWriteDoesNotEndResponse() throws IOException {
    try (JsonResponseStreamer streamer = new JsonResponseStreamer(httpResponse, testAddress)) {
      // no writes
    }

    verify(httpResponse, never()).write(any(Buffer.class));
    verify(httpResponse, never()).end();
  }

  @Test
  public void stopOnError() throws IOException {
    try (JsonResponseStreamer streamer = new JsonResponseStreamer(failedResponse, testAddress)) {
      streamer.write("xyz".getBytes(StandardCharsets.UTF_8));
      assertThatThrownBy(() -> streamer.write("abc".getBytes(StandardCharsets.UTF_8)))
          .isInstanceOf(IOException.class);
    }

    verify(failedResponse).write(argThat(bufferContains("xyz")));
    verify(failedResponse, never()).write(argThat(bufferContains("abc")));
    verify(failedResponse).end();
  }

  private ArgumentMatcher<Buffer> bufferContains(final String text) {
    return buf -> buf.toString().equals(text);
  }
}
