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

import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcObjectMapperFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.JsonRpcResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscribeRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscriptionType;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.UnsubscribeRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.response.SubscriptionResponse;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SubscriptionManager is responsible for managing subscriptions and sending messages to the
 * clients that have an active subscription.
 */
public class SubscriptionManager extends AbstractVerticle {

  private static final Logger LOG = LoggerFactory.getLogger(SubscriptionManager.class);
  private static final ObjectMapper jsonObjectMapper =
      JsonRpcObjectMapperFactory.getResponseMapper();

  public static final String EVENTBUS_REMOVE_SUBSCRIPTIONS_ADDRESS =
      "SubscriptionManager::removeSubscriptions";

  private final AtomicLong subscriptionCounter = new AtomicLong(0);
  private final AtomicInteger activeSubscriptionCount = new AtomicInteger(0);
  private final Map<Long, Subscription> subscriptions = new ConcurrentHashMap<>();
  private final SubscriptionBuilder subscriptionBuilder = new SubscriptionBuilder();
  private final LabelledMetric<Counter> subscribeCounter;
  private final LabelledMetric<Counter> unsubscribeCounter;
  private final int maxActiveSubscriptions;

  @VisibleForTesting
  public SubscriptionManager(final MetricsSystem metricsSystem) {
    this(metricsSystem, WebSocketConfiguration.createDefault());
  }

  public SubscriptionManager(
      final MetricsSystem metricsSystem, final WebSocketConfiguration config) {
    subscribeCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.RPC,
            "subscription_subscribe_total",
            "Total number of subscriptions",
            "type");
    unsubscribeCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.RPC,
            "subscription_unsubscribe_total",
            "Total number of unsubscriptions",
            "type");
    maxActiveSubscriptions = config.getMaxActiveSubscriptions();
  }

  @Override
  public void start() {
    vertx.eventBus().consumer(EVENTBUS_REMOVE_SUBSCRIPTIONS_ADDRESS, this::removeSubscriptions);
  }

  public Long subscribe(final SubscribeRequest request) {
    LOG.debug("Subscribe request {}", request);

    reserveActiveSubscriptionSlot();

    subscribeCounter.labels(request.getSubscriptionType().getCode()).inc();
    final long subscriptionId = subscriptionCounter.incrementAndGet();
    final Subscription subscription =
        subscriptionBuilder.build(subscriptionId, request.getConnectionId(), request);
    subscriptions.put(subscription.getSubscriptionId(), subscription);

    return subscription.getSubscriptionId();
  }

  /**
   * Atomically reserves a slot against {@code maxActiveSubscriptions}. Increment-then-check avoids
   * the TOCTOU window a separate "check subscriptions.size() then put" would have under concurrent
   * subscribe calls: each thread gets a distinct reserved count, so only as many threads as there
   * are free slots can pass, and any others roll their reservation back before throwing.
   */
  private void reserveActiveSubscriptionSlot() {
    if (maxActiveSubscriptions <= 0) {
      return;
    }
    final int reservedCount = activeSubscriptionCount.incrementAndGet();
    if (reservedCount > maxActiveSubscriptions) {
      activeSubscriptionCount.decrementAndGet();
      LOG.atWarn()
          .setMessage("Maximum number of active subscriptions reached {} / {}")
          .addArgument(reservedCount - 1)
          .addArgument(maxActiveSubscriptions)
          .log();
      throw new MaxSubscriptionsExceededException(maxActiveSubscriptions);
    }
  }

  public boolean unsubscribe(final UnsubscribeRequest request) {
    final Long subscriptionId = request.getSubscriptionId();
    final String connectionId = request.getConnectionId();

    LOG.debug("Unsubscribe request subscriptionId = {}", subscriptionId);

    final Subscription subscription = subscriptions.get(subscriptionId);
    if (subscription == null || !subscription.getConnectionId().equals(connectionId)) {
      throw new SubscriptionNotFoundException(subscriptionId);
    }

    destroySubscription(subscriptionId);

    return true;
  }

  private void destroySubscription(final long subscriptionId) {
    final Subscription removed = subscriptions.remove(subscriptionId);
    if (removed != null) {
      unsubscribeCounter.labels(removed.getSubscriptionType().getCode()).inc();
      if (maxActiveSubscriptions > 0) {
        activeSubscriptionCount.decrementAndGet();
      }
    }
  }

  private void removeSubscriptions(final Message<String> message) {
    final String connectionId = message.body();
    if (connectionId == null || connectionId.isEmpty()) {
      LOG.warn("Received invalid connectionId ({}). No subscriptions removed.", connectionId);
    }

    LOG.debug("Removing subscription for connectionId {}", connectionId);

    subscriptions.values().stream()
        .filter(subscription -> subscription.getConnectionId().equals(connectionId))
        .forEach(subscription -> destroySubscription(subscription.getSubscriptionId()));
  }

  public Subscription getSubscriptionById(final Long subscriptionId) {
    return subscriptions.get(subscriptionId);
  }

  public <T> List<T> subscriptionsOfType(final SubscriptionType type, final Class<T> clazz) {
    return subscriptions.values().stream()
        .filter(subscription -> subscription.isType(type))
        .map(subscriptionBuilder.mapToSubscriptionClass(clazz))
        .collect(Collectors.toList());
  }

  public void sendMessage(final Long subscriptionId, final JsonRpcResult msg) {
    final Subscription subscription = subscriptions.get(subscriptionId);

    if (subscription != null) {
      final SubscriptionResponse response = new SubscriptionResponse(subscription, msg);
      try {
        vertx
            .eventBus()
            .send(subscription.getConnectionId(), jsonObjectMapper.writeValueAsString(response));
      } catch (JsonProcessingException e) {
        LOG.error("Error streaming websocket JSON-RPC response", e);
      }
    }
  }

  public <T> void notifySubscribersOnWorkerThread(
      final SubscriptionType subscriptionType,
      final Class<T> clazz,
      final Consumer<List<T>> runnable) {
    vertx
        .<Void>executeBlocking(
            () -> {
              final List<T> syncingSubscriptions = subscriptionsOfType(subscriptionType, clazz);
              runnable.accept(syncingSubscriptions);
              return null;
            })
        .onFailure(t -> LOG.error("Failed to notify subscribers.", t));
  }
}
