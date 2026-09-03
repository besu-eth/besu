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
package org.hyperledger.besu.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.plugin.data.SyncStatus;
import org.hyperledger.besu.plugin.services.BesuEvents;
import org.hyperledger.besu.plugin.services.Subscription;
import org.hyperledger.besu.plugin.services.sync.spi.InitialSyncCompletionListener;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SynchronizationServiceImplTest {

  @Mock private Synchronizer synchronizer;
  @Mock private ProtocolContext protocolContext;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private SyncState syncState;
  private SynchronizationServiceImpl service;

  @BeforeEach
  void setUp() {
    service =
        new SynchronizationServiceImpl(
            synchronizer, protocolContext, protocolSchedule, syncState, null);
  }

  @Test
  void syncStatusIsDeliveredAndClosedOnce() {
    when(syncState.subscribeSyncStatus(any())).thenReturn(5L);
    final AtomicReference<Optional<SyncStatus>> received = new AtomicReference<>();

    final Subscription subscription = service.subscribeSyncStatus(received::set);

    final ArgumentCaptor<BesuEvents.SyncStatusListener> captor =
        ArgumentCaptor.forClass(BesuEvents.SyncStatusListener.class);
    verify(syncState).subscribeSyncStatus(captor.capture());
    captor.getValue().onSyncStatusChanged(Optional.empty());
    assertThat(received.get()).isEqualTo(Optional.empty());

    verify(syncState, never()).unsubscribeSyncStatus(5L);
    subscription.close();
    subscription.close();
    verify(syncState, times(1)).unsubscribeSyncStatus(5L);
  }

  @Test
  void initialSyncCompletionForwardsBothCallbacksAndCanBeClosed() {
    when(syncState.subscribeCompletionReached(any())).thenReturn(9L);
    final AtomicInteger completed = new AtomicInteger();
    final AtomicInteger restarted = new AtomicInteger();

    final Subscription subscription =
        service.subscribeInitialSyncCompletion(
            new InitialSyncCompletionListener() {
              @Override
              public void onInitialSyncCompleted() {
                completed.incrementAndGet();
              }

              @Override
              public void onInitialSyncRestart() {
                restarted.incrementAndGet();
              }
            });

    final ArgumentCaptor<BesuEvents.InitialSyncCompletionListener> captor =
        ArgumentCaptor.forClass(BesuEvents.InitialSyncCompletionListener.class);
    verify(syncState).subscribeCompletionReached(captor.capture());
    captor.getValue().onInitialSyncCompleted();
    captor.getValue().onInitialSyncRestart();
    assertThat(completed.get()).isEqualTo(1);
    assertThat(restarted.get()).isEqualTo(1);

    subscription.close();
    subscription.close();
    verify(syncState, times(1)).unsubscribeInitialConditionReached(9L);
  }
}
