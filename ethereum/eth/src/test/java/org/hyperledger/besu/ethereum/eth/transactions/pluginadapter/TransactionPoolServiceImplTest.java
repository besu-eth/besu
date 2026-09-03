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
package org.hyperledger.besu.ethereum.eth.transactions.pluginadapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactionAddedListener;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactionDroppedListener;
import org.hyperledger.besu.ethereum.eth.transactions.RemovalReason;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.plugin.services.Subscription;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TransactionPoolServiceImplTest {

  @Mock private TransactionPool transactionPool;
  private TransactionPoolServiceImpl service;

  @BeforeEach
  void setUp() {
    service = new TransactionPoolServiceImpl(transactionPool);
  }

  @Test
  void transactionAddedIsDeliveredAndClosedOnce() {
    when(transactionPool.subscribePendingTransactions(any())).thenReturn(7L);
    final AtomicReference<Transaction> received = new AtomicReference<>();

    final Subscription subscription = service.subscribeTransactionAdded(received::set);

    final ArgumentCaptor<PendingTransactionAddedListener> captor =
        ArgumentCaptor.forClass(PendingTransactionAddedListener.class);
    verify(transactionPool).subscribePendingTransactions(captor.capture());
    final org.hyperledger.besu.ethereum.core.Transaction tx =
        mock(org.hyperledger.besu.ethereum.core.Transaction.class);
    captor.getValue().onTransactionAdded(tx);
    assertThat(received.get()).isSameAs(tx);

    verify(transactionPool, never()).unsubscribePendingTransactions(7L);
    subscription.close();
    subscription.close();
    verify(transactionPool, times(1)).unsubscribePendingTransactions(7L);
  }

  @Test
  void transactionDroppedIsDeliveredWithReasonLabelAndClosedOnce() {
    when(transactionPool.subscribeDroppedTransactions(any())).thenReturn(3L);
    final AtomicReference<Transaction> received = new AtomicReference<>();
    final AtomicReference<String> reason = new AtomicReference<>();

    final Subscription subscription =
        service.subscribeTransactionDropped(
            (tx, why) -> {
              received.set(tx);
              reason.set(why);
            });

    final ArgumentCaptor<PendingTransactionDroppedListener> captor =
        ArgumentCaptor.forClass(PendingTransactionDroppedListener.class);
    verify(transactionPool).subscribeDroppedTransactions(captor.capture());
    final org.hyperledger.besu.ethereum.core.Transaction tx =
        mock(org.hyperledger.besu.ethereum.core.Transaction.class);
    final RemovalReason removalReason = mock(RemovalReason.class);
    when(removalReason.label()).thenReturn("replaced");
    captor.getValue().onTransactionDropped(tx, removalReason);
    assertThat(received.get()).isSameAs(tx);
    assertThat(reason.get()).isEqualTo("replaced");

    subscription.close();
    subscription.close();
    verify(transactionPool, times(1)).unsubscribeDroppedTransactions(3L);
  }
}
