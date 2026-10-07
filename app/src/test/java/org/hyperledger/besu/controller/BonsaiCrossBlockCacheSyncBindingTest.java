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
package org.hyperledger.besu.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.VersionedFlatDbCacheManager;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.BesuEvents.InitialSyncCompletionListener;

import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

/** The Bonsai cross-block cache is off during the initial sync and on once it completes. */
class BonsaiCrossBlockCacheSyncBindingTest {

  private final VersionedFlatDbCacheManager cacheManager =
      new VersionedFlatDbCacheManager(10, 10, new NoOpMetricsSystem());

  @AfterEach
  void tearDown() {
    cacheManager.close();
  }

  @Test
  void withInitialSyncPhase_disabledUntilSyncCompletesAndAgainOnRestart() {
    final SyncState syncState =
        new SyncState(mock(Blockchain.class), mock(EthPeers.class), true, Optional.empty());

    BesuControllerBuilder.bindCrossBlockCacheToInitialSync(cacheManager, syncState);
    assertThat(cacheManager.isEnabled()).isFalse();

    syncState.markInitialSyncPhaseAsDone();
    assertThat(cacheManager.isEnabled()).isTrue();

    syncState.markInitialSyncRestart();
    assertThat(cacheManager.isEnabled()).isFalse();

    syncState.markInitialSyncPhaseAsDone();
    assertThat(cacheManager.isEnabled()).isTrue();
  }

  @Test
  void withoutInitialSyncPhase_enablesTheCache() {
    cacheManager.disable();
    final SyncState syncState =
        new SyncState(mock(Blockchain.class), mock(EthPeers.class), false, Optional.empty());

    BesuControllerBuilder.bindCrossBlockCacheToInitialSync(cacheManager, syncState);

    assertThat(cacheManager.isEnabled()).isTrue();
  }

  @Test
  void events_applyTheCurrentSyncStateNotTheEventType() {
    final SyncState syncState = mock(SyncState.class);
    when(syncState.isInitialSyncPhaseDone()).thenReturn(false);
    BesuControllerBuilder.bindCrossBlockCacheToInitialSync(cacheManager, syncState);
    final InitialSyncCompletionListener listener = registeredListener(syncState);

    // a late "completed" event while the sync has already restarted
    listener.onInitialSyncCompleted();
    assertThat(cacheManager.isEnabled()).isFalse();

    // a late "restart" event while the sync has already completed again
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);
    listener.onInitialSyncRestart();
    assertThat(cacheManager.isEnabled()).isTrue();
  }

  @Test
  void syncStateIsReadOnlyAfterTheListenerIsRegistered() {
    final SyncState syncState = mock(SyncState.class);
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);
    cacheManager.disable();

    BesuControllerBuilder.bindCrossBlockCacheToInitialSync(cacheManager, syncState);

    // reading the state first could miss an event fired before the listener is registered
    final InOrder inOrder = inOrder(syncState);
    inOrder.verify(syncState).subscribeCompletionReached(any());
    inOrder.verify(syncState).isInitialSyncPhaseDone();
    assertThat(cacheManager.isEnabled()).isTrue();
  }

  @Test
  void noOpCache_isNotBoundToTheSyncState() {
    final SyncState syncState = mock(SyncState.class);

    BesuControllerBuilder.bindCrossBlockCacheToInitialSync(
        FlatDbCacheManager.NO_OP_CACHE, syncState);

    verifyNoInteractions(syncState);
  }

  private static InitialSyncCompletionListener registeredListener(final SyncState syncState) {
    final ArgumentCaptor<InitialSyncCompletionListener> captor =
        ArgumentCaptor.forClass(InitialSyncCompletionListener.class);
    verify(syncState).subscribeCompletionReached(captor.capture());
    return captor.getValue();
  }
}
