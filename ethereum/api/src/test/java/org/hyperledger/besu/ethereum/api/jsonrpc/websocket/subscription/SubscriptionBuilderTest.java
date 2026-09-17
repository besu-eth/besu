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
import static org.assertj.core.api.Assertions.catchThrowable;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.FilterParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.blockheaders.NewBlockHeadersSubscription;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.logs.LogsSubscription;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscribeRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscriptionType;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.syncing.SyncingSubscription;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.transactionreceipts.TransactionReceiptsSubscription;

import java.util.List;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SubscriptionBuilderTest {

  private static final String CONNECTION_ID = "connectionId";
  private SubscriptionBuilder subscriptionBuilder;

  @BeforeEach
  public void before() {
    subscriptionBuilder = new SubscriptionBuilder();
  }

  @Test
  public void shouldBuildLogsSubscriptionWhenSubscribeRequestTypeIsLogs() {
    final FilterParameter filterParameter = filterParameter();
    final SubscribeRequest subscribeRequest =
        new SubscribeRequest(SubscriptionType.LOGS, filterParameter, null, CONNECTION_ID);
    final LogsSubscription expectedSubscription =
        new LogsSubscription(1L, CONNECTION_ID, filterParameter);

    final Subscription builtSubscription =
        subscriptionBuilder.build(1L, CONNECTION_ID, subscribeRequest);

    assertThat(builtSubscription).usingRecursiveComparison().isEqualTo(expectedSubscription);
  }

  @Test
  public void shouldBuildNewBlockHeadsSubscriptionWhenSubscribeRequestTypeIsNewBlockHeads() {
    final SubscribeRequest subscribeRequest =
        new SubscribeRequest(SubscriptionType.NEW_BLOCK_HEADERS, null, true, CONNECTION_ID);
    final NewBlockHeadersSubscription expectedSubscription =
        new NewBlockHeadersSubscription(1L, CONNECTION_ID, true);

    final Subscription builtSubscription =
        subscriptionBuilder.build(1L, CONNECTION_ID, subscribeRequest);

    assertThat(builtSubscription).usingRecursiveComparison().isEqualTo(expectedSubscription);
  }

  @Test
  public void shouldBuildSubscriptionWhenSubscribeRequestTypeIsNewPendingTransactions() {
    final SubscribeRequest subscribeRequest =
        new SubscribeRequest(SubscriptionType.NEW_PENDING_TRANSACTIONS, null, null, CONNECTION_ID);
    final Subscription expectedSubscription =
        new Subscription(1L, CONNECTION_ID, SubscriptionType.NEW_PENDING_TRANSACTIONS, null);

    final Subscription builtSubscription =
        subscriptionBuilder.build(1L, CONNECTION_ID, subscribeRequest);

    assertThat(builtSubscription).usingRecursiveComparison().isEqualTo(expectedSubscription);
  }

  @Test
  public void shouldBuildSubscriptionWhenSubscribeRequestTypeIsSyncing() {
    final SubscribeRequest subscribeRequest =
        new SubscribeRequest(SubscriptionType.SYNCING, null, null, CONNECTION_ID);
    final SyncingSubscription expectedSubscription =
        new SyncingSubscription(1L, CONNECTION_ID, SubscriptionType.SYNCING);

    final Subscription builtSubscription =
        subscriptionBuilder.build(1L, CONNECTION_ID, subscribeRequest);

    assertThat(builtSubscription).usingRecursiveComparison().isEqualTo(expectedSubscription);
  }

  @Test
  public void shouldReturnLogsSubscriptionWhenMappingLogsSubscription() {
    final Function<Subscription, LogsSubscription> function =
        subscriptionBuilder.mapToSubscriptionClass(LogsSubscription.class);
    final Subscription subscription = new LogsSubscription(1L, CONNECTION_ID, filterParameter());

    assertThat(function.apply(subscription)).isInstanceOf(LogsSubscription.class);
  }

  @Test
  public void shouldReturnNewBlockHeadsSubscriptionWhenMappingNewBlockHeadsSubscription() {
    final Function<Subscription, NewBlockHeadersSubscription> function =
        subscriptionBuilder.mapToSubscriptionClass(NewBlockHeadersSubscription.class);
    final Subscription subscription = new NewBlockHeadersSubscription(1L, CONNECTION_ID, true);

    assertThat(function.apply(subscription)).isInstanceOf(NewBlockHeadersSubscription.class);
  }

  @Test
  public void shouldReturnSubscriptionWhenMappingNewPendingTransactionsSubscription() {
    final Function<Subscription, Subscription> function =
        subscriptionBuilder.mapToSubscriptionClass(Subscription.class);
    final Subscription logsSubscription =
        new Subscription(
            1L, CONNECTION_ID, SubscriptionType.NEW_PENDING_TRANSACTIONS, Boolean.FALSE);

    assertThat(function.apply(logsSubscription)).isInstanceOf(Subscription.class);
  }

  @Test
  public void shouldReturnSubscriptionWhenMappingSyncingSubscription() {
    final Function<Subscription, SyncingSubscription> function =
        subscriptionBuilder.mapToSubscriptionClass(SyncingSubscription.class);
    final Subscription subscription =
        new SyncingSubscription(1L, CONNECTION_ID, SubscriptionType.SYNCING);

    assertThat(function.apply(subscription)).isInstanceOf(SyncingSubscription.class);
  }

  @Test
  public void
      shouldBuildTransactionReceiptsSubscriptionWhenSubscribeRequestTypeIsTransactionReceipts() {
    final List<Hash> txHashes =
        List.of(
            Hash.fromHexString(
                "0x0000000000000000000000000000000000000000000000000000000000000001"));
    final SubscribeRequest subscribeRequest =
        new SubscribeRequest(
            SubscriptionType.TRANSACTION_RECEIPTS, null, null, txHashes, CONNECTION_ID);
    final TransactionReceiptsSubscription expectedSubscription =
        new TransactionReceiptsSubscription(1L, CONNECTION_ID, txHashes);

    final Subscription builtSubscription =
        subscriptionBuilder.build(1L, CONNECTION_ID, subscribeRequest);

    assertThat(builtSubscription).usingRecursiveComparison().isEqualTo(expectedSubscription);
  }

  @Test
  public void shouldBuildTransactionReceiptsSubscriptionWithNoFilter() {
    final SubscribeRequest subscribeRequest =
        new SubscribeRequest(
            SubscriptionType.TRANSACTION_RECEIPTS, null, null, null, CONNECTION_ID);
    final TransactionReceiptsSubscription expectedSubscription =
        new TransactionReceiptsSubscription(1L, CONNECTION_ID, null);

    final Subscription builtSubscription =
        subscriptionBuilder.build(1L, CONNECTION_ID, subscribeRequest);

    assertThat(builtSubscription).usingRecursiveComparison().isEqualTo(expectedSubscription);
  }

  @Test
  public void
      shouldReturnTransactionReceiptsSubscriptionWhenMappingTransactionReceiptsSubscription() {
    final Function<Subscription, TransactionReceiptsSubscription> function =
        subscriptionBuilder.mapToSubscriptionClass(TransactionReceiptsSubscription.class);
    final Subscription subscription = new TransactionReceiptsSubscription(1L, CONNECTION_ID, null);

    assertThat(function.apply(subscription)).isInstanceOf(TransactionReceiptsSubscription.class);
  }

  @Test
  @SuppressWarnings("ReturnValueIgnored")
  public void shouldThrownIllegalArgumentExceptionWhenMappingWrongSubscriptionType() {
    final Function<Subscription, LogsSubscription> function =
        subscriptionBuilder.mapToSubscriptionClass(LogsSubscription.class);
    final NewBlockHeadersSubscription subscription =
        new NewBlockHeadersSubscription(1L, CONNECTION_ID, true);

    final Throwable thrown = catchThrowable(() -> function.apply(subscription));
    assertThat(thrown)
        .hasNoCause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "NewBlockHeadersSubscription instance can't be mapped to type LogsSubscription");
  }

  private FilterParameter filterParameter() {
    return new FilterParameter(
        BlockParameter.EARLIEST, BlockParameter.LATEST, null, null, null, null, null, null, null);
  }
}
