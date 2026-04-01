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
package org.hyperledger.besu.ethereum.api.jsonrpc.ipc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.jsonrpc.execution.CombinedJsonRpcProcessor;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.JsonRpcExecutor;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.methods.WebSocketMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@EnabledOnOs({OS.LINUX, OS.MAC})
@ExtendWith(VertxExtension.class)
class JsonRpcIpcServiceTest {

  @TempDir private Path tempDir;
  private Vertx vertx;
  private VertxTestContext testContext;

  @BeforeEach
  public void setUp() {
    vertx = Vertx.vertx(new VertxOptions().setPreferNativeTransport(true));
    testContext = new VertxTestContext();
  }

  @AfterEach
  public void after() throws Throwable {
    assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS))
        .describedAs("Test completed on time")
        .isTrue();
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void successfulExecution() {
    final Path socketPath = tempDir.resolve("besu-test.ipc");
    final JsonRpcMethod testMethod = mock(JsonRpcMethod.class);
    when(testMethod.response(any())).thenReturn(new JsonRpcSuccessResponse(1, "TEST OK"));
    final JsonRpcIpcService service =
        new JsonRpcIpcService(
            vertx,
            socketPath,
            new JsonRpcExecutor(new CombinedJsonRpcProcessor(new NoOpMetricsSystem()), Map.of("test_method", testMethod)));
    final String expectedResponse = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"TEST OK\"}\n";

    assertSocketCall(
        service,
        socketPath,
        expectedResponse,
        new JsonObject().put("id", 1).put("method", "test_method").toBuffer());
  }

  @Test
  void successfulBatchExecution() {
    final Path socketPath = tempDir.resolve("besu-test.ipc");
    final JsonRpcMethod fooMethod = mock(JsonRpcMethod.class);
    when(fooMethod.response(any())).thenReturn(new JsonRpcSuccessResponse(1, "FOO OK"));
    final JsonRpcMethod barMethod = mock(JsonRpcMethod.class);
    when(barMethod.response(any())).thenReturn(new JsonRpcSuccessResponse(2, "BAR OK"));
    final JsonRpcIpcService service =
        new JsonRpcIpcService(
            vertx,
            socketPath,
            new JsonRpcExecutor(
                new CombinedJsonRpcProcessor(new NoOpMetricsSystem()),
                Map.of("foo_method", fooMethod, "bar_method", barMethod)));

    assertSocketCall(
        service,
        socketPath,
        "[{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"FOO OK\"},{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":\"BAR OK\"}]\n",
        new JsonArray(
                Arrays.asList(
                    new JsonObject().put("id", 1).put("method", "foo_method"),
                    new JsonObject().put("id", 2).put("method", "bar_method")))
            .toBuffer());
  }

  @Test
  void validJsonButNotRpcShouldReturnInvalidRequest() {
    final Path socketPath = tempDir.resolve("besu-test.ipc");
    final JsonRpcIpcService service =
        new JsonRpcIpcService(
            vertx,
            socketPath,
            new JsonRpcExecutor(new CombinedJsonRpcProcessor(new NoOpMetricsSystem()), Collections.emptyMap()));
    final String expectedResponse =
        "{\"jsonrpc\":\"2.0\",\"id\":null,\"error\":{\"code\":-32600,\"message\":\"Invalid Request\"}}\n";

    assertSocketCall(service, socketPath, expectedResponse, Buffer.buffer("{\"foo\":\"bar\"}"));
  }

  @Test
  void nonJsonRequestShouldReturnParseError() {
    final Path socketPath = tempDir.resolve("besu-test.ipc");
    final JsonRpcIpcService service =
        new JsonRpcIpcService(vertx, socketPath, mock(JsonRpcExecutor.class));
    final String expectedResponse =
        "{\"jsonrpc\":\"2.0\",\"id\":null,\"error\":{\"code\":-32700,\"message\":\"Parse error\"}}\n";

    assertSocketCall(service, socketPath, expectedResponse, Buffer.buffer("bad request"));
  }

  @Test
  void concatenatedJsonRequestsShouldBeHandledIndependently() {
    final Path socketPath = tempDir.resolve("besu-test.ipc");
    final JsonRpcMethod fooMethod = mock(JsonRpcMethod.class);
    when(fooMethod.response(any())).thenReturn(new JsonRpcSuccessResponse(1, "FOO OK"));
    final JsonRpcMethod barMethod = mock(JsonRpcMethod.class);
    when(barMethod.response(any())).thenReturn(new JsonRpcSuccessResponse(2, "BAR OK"));
    final JsonRpcMethod bazMethod = mock(JsonRpcMethod.class);
    when(bazMethod.response(any())).thenReturn(new JsonRpcSuccessResponse(3, "BAZ OK"));
    final JsonRpcIpcService service =
        new JsonRpcIpcService(
            vertx,
            socketPath,
            new JsonRpcExecutor(
                new CombinedJsonRpcProcessor(new NoOpMetricsSystem()),
                Map.of("foo_method", fooMethod, "bar_method", barMethod, "baz_method", bazMethod)));

    // Simulate concurrent requests concatenated in a single buffer
    // This mimics what happens when multiple requests arrive at the same time
    final String concatenatedJson =
        "{\"id\":1,\"method\":\"foo_method\"}"
            + "{\"id\":2,\"method\":\"bar_method\"}"
            + "{\"id\":3,\"method\":\"baz_method\"}";

    service
        .start()
        .onComplete(
            testContext.succeeding(
                server ->
                    vertx
                        .createNetClient()
                        .connect(SocketAddress.domainSocketAddress(socketPath.toString()))
                        .onComplete(
                            testContext.succeeding(
                                socket -> {
                                  final StringBuilder receivedResponses = new StringBuilder();
                                  socket
                                      .handler(
                                          buffer -> {
                                            receivedResponses.append(buffer.toString());
                                            // Wait for all 3 responses (each ends with \n)
                                            if (receivedResponses.toString().split("\n").length
                                                == 3) {
                                              testContext.verify(
                                                  () -> {
                                                    String responses = receivedResponses.toString();
                                                    // Verify all three responses are present
                                                    assertThat(responses)
                                                        .contains(
                                                            "\"id\":1", "\"result\":\"FOO OK\"");
                                                    assertThat(responses)
                                                        .contains(
                                                            "\"id\":2", "\"result\":\"BAR OK\"");
                                                    assertThat(responses)
                                                        .contains(
                                                            "\"id\":3", "\"result\":\"BAZ OK\"");
                                                    service
                                                        .stop()
                                                        .onComplete(
                                                            testContext.succeedingThenComplete());
                                                  });
                                            }
                                          })
                                      .write(Buffer.buffer(concatenatedJson));
                                }))));
  }

  @Test
  void shouldDeleteSocketFileOnStop() {
    final Path socketPath = tempDir.resolve("besu-test.ipc");
    final JsonRpcIpcService service =
        new JsonRpcIpcService(vertx, socketPath, mock(JsonRpcExecutor.class));
    service
        .start()
        .onComplete(
            testContext.succeeding(
                server ->
                    service
                        .stop()
                        .onComplete(
                            testContext.succeeding(
                                handler ->
                                    testContext.verify(
                                        () -> {
                                          assertThat(socketPath).doesNotExist();
                                          testContext.completeNow();
                                        })))));
  }

  private void assertSocketCall(
      final JsonRpcIpcService service,
      final Path socketPath,
      final String expectedResponse,
      final Buffer request) {
    service
        .start()
        .onComplete(
            testContext.succeeding(
                server ->
                    vertx
                        .createNetClient()
                        .connect(SocketAddress.domainSocketAddress(socketPath.toString()))
                        .onComplete(
                            testContext.succeeding(
                                socket ->
                                    socket
                                        .handler(
                                            buffer ->
                                                testContext.verify(
                                                    () -> {
                                                      assertThat(buffer)
                                                          .hasToString(expectedResponse);
                                                      service
                                                          .stop()
                                                          .onComplete(
                                                              testContext.succeedingThenComplete());
                                                    }))
                                        .write(request)))));
  }

  @Test
  void subscriptionRequestSuccessful() {
    final Path socketPath = tempDir.resolve("besu-test.ipc");
    final SubscriptionManager subscriptionManager =
        new SubscriptionManager(new NoOpMetricsSystem());
    vertx.deployVerticle(subscriptionManager);

    final Map<String, JsonRpcMethod> methods =
        new WebSocketMethodsFactory(subscriptionManager, new HashMap<>()).methods();

    final JsonRpcIpcService service =
        new JsonRpcIpcService(
            vertx,
            socketPath,
            new JsonRpcExecutor(new CombinedJsonRpcProcessor(new NoOpMetricsSystem()), methods),
            Optional.of(subscriptionManager));

    final String request = "{\"id\":1,\"method\":\"eth_subscribe\",\"params\":[\"newHeads\"]}\n";
    final String expectedResponse = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0x1\"}";

    service
        .start()
        .onComplete(
            testContext.succeeding(
                server ->
                    vertx
                        .createNetClient()
                        .connect(SocketAddress.domainSocketAddress(socketPath.toString()))
                        .onComplete(
                            testContext.succeeding(
                                socket ->
                                    socket
                                        .handler(
                                            buffer ->
                                                testContext.verify(
                                                    () -> {
                                                      assertThat(buffer.toString().trim())
                                                          .isEqualTo(expectedResponse);
                                                      service
                                                          .stop()
                                                          .onComplete(
                                                              testContext.succeedingThenComplete());
                                                    }))
                                        .write(Buffer.buffer(request))))));
  }

  @Test
  void unsubscribeRequestSuccessful() {
    final Path socketPath = tempDir.resolve("besu-test.ipc");
    final SubscriptionManager subscriptionManager =
        new SubscriptionManager(new NoOpMetricsSystem());
    vertx.deployVerticle(subscriptionManager);

    final Map<String, JsonRpcMethod> methods =
        new WebSocketMethodsFactory(subscriptionManager, new HashMap<>()).methods();

    final JsonRpcIpcService service =
        new JsonRpcIpcService(
            vertx,
            socketPath,
            new JsonRpcExecutor(new CombinedJsonRpcProcessor(new NoOpMetricsSystem()), methods),
            Optional.of(subscriptionManager));

    final String subscribeRequest =
        "{\"id\":1,\"method\":\"eth_subscribe\",\"params\":[\"newHeads\"]}\n";
    final String unsubscribeRequest =
        "{\"id\":2,\"method\":\"eth_unsubscribe\",\"params\":[\"0x1\"]}\n";
    final AtomicInteger messageCount = new AtomicInteger(0);

    service
        .start()
        .onComplete(
            testContext.succeeding(
                server ->
                    vertx
                        .createNetClient()
                        .connect(SocketAddress.domainSocketAddress(socketPath.toString()))
                        .onComplete(
                            testContext.succeeding(
                                socket -> {
                                  socket.handler(
                                      buffer -> {
                                        final int count = messageCount.incrementAndGet();
                                        if (count == 1) {
                                          // First response is subscribe
                                          socket.write(Buffer.buffer(unsubscribeRequest));
                                        } else if (count == 2) {
                                          // Second response is unsubscribe
                                          testContext.verify(
                                              () -> {
                                                assertThat(buffer.toString().trim())
                                                    .contains("\"result\":true");
                                                service
                                                    .stop()
                                                    .onComplete(
                                                        testContext.succeedingThenComplete());
                                              });
                                        }
                                      });
                                  socket.write(Buffer.buffer(subscribeRequest));
                                }))));
  }

  @Test
  void batchRequestDoesNotSupportSubscriptions() {
    final Path socketPath = tempDir.resolve("besu-test.ipc");
    final SubscriptionManager subscriptionManager =
        new SubscriptionManager(new NoOpMetricsSystem());
    vertx.deployVerticle(subscriptionManager);

    final Map<String, JsonRpcMethod> methods =
        new WebSocketMethodsFactory(subscriptionManager, new HashMap<>()).methods();

    final JsonRpcIpcService service =
        new JsonRpcIpcService(
            vertx,
            socketPath,
            new JsonRpcExecutor(new CombinedJsonRpcProcessor(new NoOpMetricsSystem()), methods),
            Optional.of(subscriptionManager));

    final String batchRequest =
        "[{\"id\":1,\"method\":\"eth_subscribe\",\"params\":[\"newHeads\"]},"
            + "{\"id\":2,\"method\":\"eth_subscribe\",\"params\":[\"logs\"]}]";

    service
        .start()
        .onComplete(
            testContext.succeeding(
                server ->
                    vertx
                        .createNetClient()
                        .connect(SocketAddress.domainSocketAddress(socketPath.toString()))
                        .onComplete(
                            testContext.succeeding(
                                socket ->
                                    socket
                                        .handler(
                                            buffer ->
                                                testContext.verify(
                                                    () -> {
                                                      // Batch requests with subscriptions should
                                                      // fail
                                                      assertThat(buffer.toString())
                                                          .contains("\"error\"");
                                                      service
                                                          .stop()
                                                          .onComplete(
                                                              testContext.succeedingThenComplete());
                                                    }))
                                        .write(Buffer.buffer(batchRequest))))));
  }
}
