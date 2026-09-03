/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.JsonRpcResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscribeRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscriptionType;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.response.SubscriptionResponse;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class SubscriptionManagerSendMessageTest {

  private static final int VERTX_AWAIT_TIMEOUT_MILLIS = 10000;

  private Vertx vertx;
  private VertxTestContext testContext;
  private SubscriptionManager subscriptionManager;

  @BeforeEach
  public void before() {
    vertx = Vertx.vertx();
    testContext = new VertxTestContext();
    subscriptionManager = new SubscriptionManager(new NoOpMetricsSystem());
    vertx.deployVerticle(subscriptionManager).onComplete(testContext.succeedingThenComplete());
  }

  @Test
  @Disabled
  public void shouldSendMessageOnTheConnectionIdEventBusAddressForExistingSubscription()
      throws InterruptedException {
    final String connectionId = UUID.randomUUID().toString();
    final SubscribeRequest subscribeRequest =
        new SubscribeRequest(SubscriptionType.SYNCING, null, null, connectionId);

    final JsonRpcResult expectedResult = mock(JsonRpcResult.class);
    final Subscription subscription =
        new Subscription(1L, connectionId, SubscriptionType.SYNCING, false);
    final SubscriptionResponse expectedResponse =
        new SubscriptionResponse(subscription, expectedResult);

    final Long subscriptionId = subscriptionManager.subscribe(subscribeRequest);

    vertx
        .eventBus()
        .consumer(connectionId)
        .handler(
            msg -> {
              assertEquals(Json.encode(expectedResponse), msg.body());
              testContext.completeNow();
            })
        .completion()
        .onComplete(v -> subscriptionManager.sendMessage(subscriptionId, expectedResult));

    assertThat(testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
        .as("test context should complete before the timeout")
        .isTrue();
    if (testContext.failed()) {
      throw new AssertionError(testContext.causeOfFailure());
    }
  }

  @Test
  public void shouldNotSendMessageOnTheConnectionIdEventBusAddressForAbsentSubscription()
      throws InterruptedException {
    final String connectionId = UUID.randomUUID().toString();

    vertx
        .eventBus()
        .consumer(connectionId)
        .handler(
            msg -> {
              Assertions.fail("Shouldn't receive message");
              testContext.completeNow();
            })
        .completion()
        .onComplete(v -> subscriptionManager.sendMessage(1L, mock(JsonRpcResult.class)));

    // if it doesn't receive the message in 5 seconds we assume it won't receive anymore
    vertx.setPeriodic(5000, v -> testContext.completeNow());

    assertThat(testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
        .as("test context should complete before the timeout")
        .isTrue();
    if (testContext.failed()) {
      throw new AssertionError(testContext.causeOfFailure());
    }
  }
}
