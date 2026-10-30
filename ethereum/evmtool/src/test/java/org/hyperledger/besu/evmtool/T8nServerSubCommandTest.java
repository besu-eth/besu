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
package org.hyperledger.besu.evmtool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import picocli.CommandLine;

@ExtendWith(MockitoExtension.class)
class T8nServerSubCommandTest {

  @Mock HttpServerRequest httpServerRequest;

  @Mock(answer = Answers.RETURNS_SELF)
  HttpServerResponse httpServerResponse;

  @Test
  void exceptionEncodedProperlyInJSON() {
    T8nServerSubCommand subject = new T8nServerSubCommand();
    ObjectMapper objectMapper = new ObjectMapper();

    when(httpServerRequest.response()).thenReturn(httpServerResponse);

    // Should trigger a NPE within the try block.
    subject.handleT8nRequest(httpServerRequest, objectMapper, null, null);

    ArgumentCaptor<Integer> responseCodeCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<String> responseStringCaptor = ArgumentCaptor.forClass(String.class);

    verify(httpServerResponse).setStatusCode(responseCodeCaptor.capture());
    verify(httpServerResponse).end(responseStringCaptor.capture());

    assertThat(responseCodeCaptor.getValue()).isEqualTo(500);
    assertThat(responseStringCaptor.getValue()).doesNotContain("\\t");
  }

  @Test
  void runKeepsServingAfterServerStarts() throws Exception {
    final int port = freePort();
    final T8nServerSubCommand subject = new T8nServerSubCommand();
    new CommandLine(subject).parseArgs("--host", "127.0.0.1", "--port", String.valueOf(port));
    final Thread serverThread = new Thread(subject::call);
    serverThread.start();
    try {
      awaitListening(port);

      // EvmTool.main exits the JVM when call() returns, which would stop the server.
      assertThat(serverThread.isAlive()).isTrue();
    } finally {
      serverThread.interrupt();
      serverThread.join(TimeUnit.SECONDS.toMillis(10));
    }
    assertThat(serverThread.isAlive()).isFalse();
  }

  @Test
  void callReturnsNonZeroWhenPortIsTaken() throws Exception {
    try (ServerSocket taken = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
      final T8nServerSubCommand subject = new T8nServerSubCommand();
      new CommandLine(subject)
          .parseArgs("--host", "127.0.0.1", "--port", String.valueOf(taken.getLocalPort()));

      assertThat(subject.call()).isEqualTo(1);
    }
  }

  private static int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }

  private static void awaitListening(final int port) throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (System.nanoTime() < deadline) {
      try (Socket ignored = new Socket(InetAddress.getLoopbackAddress(), port)) {
        return;
      } catch (IOException e) {
        Thread.sleep(50);
      }
    }
    throw new AssertionError("t8n-server did not start listening on port " + port);
  }
}
