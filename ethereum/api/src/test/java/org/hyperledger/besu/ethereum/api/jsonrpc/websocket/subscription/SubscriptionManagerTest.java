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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.blockheaders.NewBlockHeadersSubscription;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscribeRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscriptionType;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.UnsubscribeRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.syncing.SyncingSubscription;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SubscriptionManagerTest {

  private SubscriptionManager subscriptionManager;
  private final String CONNECTION_ID = "test-connection-id";

  @BeforeEach
  public void before() {
    subscriptionManager = new SubscriptionManager(new NoOpMetricsSystem());
  }

  @Test
  public void subscribeShouldCreateSubscription() {
    final SubscribeRequest subscribeRequest = subscribeRequest(CONNECTION_ID);

    final Long subscriptionId = subscriptionManager.subscribe(subscribeRequest);

    final SyncingSubscription expectedSubscription =
        new SyncingSubscription(
            subscriptionId, CONNECTION_ID, subscribeRequest.getSubscriptionType());
    final Subscription createdSubscription =
        subscriptionManager.getSubscriptionById(subscriptionId);

    assertThat(subscriptionId).isEqualTo(1L);
    assertThat(createdSubscription).isEqualTo(expectedSubscription);
  }

  @Test
  public void unsubscribeExistingSubscriptionShouldDestroySubscription() {
    final SubscribeRequest subscribeRequest = subscribeRequest(CONNECTION_ID);
    final Long subscriptionId = subscriptionManager.subscribe(subscribeRequest);

    assertThat(subscriptionManager.getSubscriptionById(subscriptionId)).isNotNull();

    final UnsubscribeRequest unsubscribeRequest =
        new UnsubscribeRequest(subscriptionId, CONNECTION_ID);
    final boolean unsubscribed = subscriptionManager.unsubscribe(unsubscribeRequest);

    assertThat(unsubscribed).isTrue();
    assertThat(subscriptionManager.getSubscriptionById(subscriptionId)).isNull();
  }

  @Test
  public void unsubscribeAbsentSubscriptionShouldThrowSubscriptionNotFoundException() {
    final UnsubscribeRequest unsubscribeRequest = new UnsubscribeRequest(1L, CONNECTION_ID);
    assertThatThrownBy(() -> subscriptionManager.unsubscribe(unsubscribeRequest))
        .isInstanceOf(SubscriptionNotFoundException.class)
        .hasMessage("Subscription not found (id=1)");
  }

  @Test
  public void shouldAddSubscriptionToNewConnection() {
    final SubscribeRequest subscribeRequest = subscribeRequest(CONNECTION_ID);

    final Long subscriptionId = subscriptionManager.subscribe(subscribeRequest);

    final Subscription subscription = subscriptionManager.getSubscriptionById(subscriptionId);
    assertThat(subscription.getConnectionId()).isEqualTo(CONNECTION_ID);
  }

  @Test
  public void shouldAddSubscriptionToExistingConnection() {
    final SubscribeRequest subscribeRequest = subscribeRequest(CONNECTION_ID);

    final Long subscriptionId1 = subscriptionManager.subscribe(subscribeRequest);
    final Long subscriptionId2 = subscriptionManager.subscribe(subscribeRequest);

    assertThat(subscriptionManager.getSubscriptionById(subscriptionId1).getConnectionId())
        .isEqualTo(CONNECTION_ID);
    assertThat(subscriptionManager.getSubscriptionById(subscriptionId2).getConnectionId())
        .isEqualTo(CONNECTION_ID);
  }

  @Test
  public void shouldRemoveSubscriptionFromExistingConnection() {
    final SubscribeRequest subscribeRequest = subscribeRequest(CONNECTION_ID);

    final Long subscriptionId1 = subscriptionManager.subscribe(subscribeRequest);
    final Long subscriptionId2 = subscriptionManager.subscribe(subscribeRequest);

    assertThat(subscriptionManager.getSubscriptionById(subscriptionId1).getConnectionId())
        .isEqualTo(CONNECTION_ID);
    assertThat(subscriptionManager.getSubscriptionById(subscriptionId2).getConnectionId())
        .isEqualTo(CONNECTION_ID);

    final UnsubscribeRequest unsubscribeRequest =
        new UnsubscribeRequest(subscriptionId1, CONNECTION_ID);
    subscriptionManager.unsubscribe(unsubscribeRequest);

    assertThat(subscriptionManager.getSubscriptionById(subscriptionId1)).isNull();
    assertThat(subscriptionManager.getSubscriptionById(subscriptionId2).getConnectionId())
        .isEqualTo(CONNECTION_ID);
  }

  @Test
  public void shouldRemoveConnectionWithSingleSubscriptions() {
    final SubscribeRequest subscribeRequest = subscribeRequest(CONNECTION_ID);

    final Long subscriptionId1 = subscriptionManager.subscribe(subscribeRequest);

    assertThat(subscriptionManager.getSubscriptionById(subscriptionId1).getConnectionId())
        .isEqualTo(CONNECTION_ID);

    final UnsubscribeRequest unsubscribeRequest =
        new UnsubscribeRequest(subscriptionId1, CONNECTION_ID);
    subscriptionManager.unsubscribe(unsubscribeRequest);

    assertThat(subscriptionManager.getSubscriptionById(subscriptionId1)).isNull();
  }

  @Test
  public void getSubscriptionsOfCorrectTypeReturnExpectedSubscriptions() {
    final SubscribeRequest subscribeRequest =
        new SubscribeRequest(SubscriptionType.NEW_BLOCK_HEADERS, null, true, CONNECTION_ID);

    subscriptionManager.subscribe(subscribeRequest);

    final List<NewBlockHeadersSubscription> subscriptions =
        subscriptionManager.subscriptionsOfType(
            SubscriptionType.NEW_BLOCK_HEADERS, NewBlockHeadersSubscription.class);

    assertThat(subscriptions).hasSize(1);
    assertThat(subscriptions.get(0)).isInstanceOf(NewBlockHeadersSubscription.class);
  }

  @Test
  public void getSubscriptionsOfWrongTypeReturnEmptyList() {
    final SubscribeRequest subscribeRequest =
        new SubscribeRequest(SubscriptionType.NEW_BLOCK_HEADERS, null, true, CONNECTION_ID);

    subscriptionManager.subscribe(subscribeRequest);

    final List<NewBlockHeadersSubscription> subscriptions =
        subscriptionManager.subscriptionsOfType(
            SubscriptionType.SYNCING, NewBlockHeadersSubscription.class);

    assertThat(subscriptions).hasSize(0);
  }

  @Test
  public void unsubscribeOthersSubscriptionsNotHavingOwnSubscriptionShouldReturnNotFound() {
    final SubscribeRequest subscribeRequest = subscribeRequest(CONNECTION_ID);
    final Long subscriptionId = subscriptionManager.subscribe(subscribeRequest);

    final UnsubscribeRequest unsubscribeRequest =
        new UnsubscribeRequest(subscriptionId, UUID.randomUUID().toString());

    final Throwable thrown =
        catchThrowable(() -> subscriptionManager.unsubscribe(unsubscribeRequest));
    assertThat(thrown).isInstanceOf(SubscriptionNotFoundException.class);
  }

  @Test
  public void unsubscribeOthersSubscriptionsHavingOwnSubscriptionShouldReturnNotFound() {
    final String ownConnectionId = UUID.randomUUID().toString();
    final SubscribeRequest ownSubscribeRequest = subscribeRequest(ownConnectionId);
    subscriptionManager.subscribe(ownSubscribeRequest);

    final String otherConnectionId = UUID.randomUUID().toString();
    final SubscribeRequest otherSubscribeRequest = subscribeRequest(otherConnectionId);
    final Long otherSubscriptionId = subscriptionManager.subscribe(otherSubscribeRequest);

    final UnsubscribeRequest unsubscribeRequest =
        new UnsubscribeRequest(otherSubscriptionId, ownConnectionId);

    final Throwable thrown =
        catchThrowable(() -> subscriptionManager.unsubscribe(unsubscribeRequest));
    assertThat(thrown).isInstanceOf(SubscriptionNotFoundException.class);
  }

  @Test
  public void subscribeShouldThrowWhenMaxActiveSubscriptionsReached() {
    final SubscriptionManager manager = managerWithMaxActiveSubscriptions(2);

    manager.subscribe(subscribeRequest(CONNECTION_ID));
    manager.subscribe(subscribeRequest(CONNECTION_ID));

    assertThatThrownBy(() -> manager.subscribe(subscribeRequest(CONNECTION_ID)))
        .isInstanceOf(MaxSubscriptionsExceededException.class);
  }

  @Test
  public void subscribeShouldNotLimitWhenMaxActiveSubscriptionsIsZero() {
    final SubscriptionManager manager = managerWithMaxActiveSubscriptions(0);

    Long lastSubscriptionId = null;
    for (int i = 0; i < 50; i++) {
      lastSubscriptionId = manager.subscribe(subscribeRequest(CONNECTION_ID));
    }

    assertThat(manager.getSubscriptionById(lastSubscriptionId)).isNotNull();
  }

  @Test
  public void unsubscribeShouldFreeSlotWhenMaxActiveSubscriptionsReached() {
    final SubscriptionManager manager = managerWithMaxActiveSubscriptions(1);

    final Long subscriptionId = manager.subscribe(subscribeRequest(CONNECTION_ID));
    assertThatThrownBy(() -> manager.subscribe(subscribeRequest(CONNECTION_ID)))
        .isInstanceOf(MaxSubscriptionsExceededException.class);

    manager.unsubscribe(new UnsubscribeRequest(subscriptionId, CONNECTION_ID));

    assertThat(manager.subscribe(subscribeRequest(CONNECTION_ID))).isNotNull();
  }

  private SubscriptionManager managerWithMaxActiveSubscriptions(final int maxActiveSubscriptions) {
    final WebSocketConfiguration configuration = WebSocketConfiguration.createDefault();
    configuration.setMaxActiveSubscriptions(maxActiveSubscriptions);
    return new SubscriptionManager(new NoOpMetricsSystem(), configuration);
  }

  private SubscribeRequest subscribeRequest(final String connectionId) {
    return new SubscribeRequest(SubscriptionType.SYNCING, null, null, connectionId);
  }
}
