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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.VersionedFlatDbCacheManager;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The Bonsai cross-block cache is off during the initial sync and on once it completes. */
class BonsaiCrossBlockCacheSyncBindingTest {

  private final VersionedFlatDbCacheManager cacheManager =
      new VersionedFlatDbCacheManager(10, 10, new NoOpMetricsSystem());

  @AfterEach
  void tearDown() {
    cacheManager.close();
  }

  private static SyncState syncState(final boolean hasInitialSyncPhase) {
    return new SyncState(
        mock(Blockchain.class), mock(EthPeers.class), hasInitialSyncPhase, Optional.empty());
  }

  @Test
  void withInitialSyncPhase_disabledUntilSyncCompletesAndAgainOnRestart() {
    final SyncState syncState = syncState(true);

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
  void withoutInitialSyncPhase_enabledFromTheStart() {
    BesuControllerBuilder.bindCrossBlockCacheToInitialSync(cacheManager, syncState(false));

    assertThat(cacheManager.isEnabled()).isTrue();
  }

  @Test
  void noOpCache_isLeftUntouched() {
    final SyncState syncState = syncState(true);

    assertThatCode(
            () -> {
              BesuControllerBuilder.bindCrossBlockCacheToInitialSync(
                  FlatDbCacheManager.NO_OP_CACHE, syncState);
              syncState.markInitialSyncPhaseAsDone();
              syncState.markInitialSyncRestart();
            })
        .doesNotThrowAnyException();
    assertThat(FlatDbCacheManager.NO_OP_CACHE.isEnabled()).isFalse();
  }
}
