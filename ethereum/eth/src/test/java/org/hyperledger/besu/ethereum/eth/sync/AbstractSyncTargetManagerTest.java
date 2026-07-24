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
package org.hyperledger.besu.ethereum.eth.sync;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class AbstractSyncTargetManagerTest {

  private EthScheduler ethScheduler;

  @AfterEach
  public void tearDown() {
    if (ethScheduler != null) {
      ethScheduler.stop();
    }
  }

  // Regression test for besu-eth/besu#10864. FullSyncTargetManager's retry loop could deadlock
  // forever when the sole connected peer's outstanding-request budget was momentarily exhausted
  // (e.g. by BlockPropagationManager's backward parent walk) at the exact moment
  // waitForPeerAndThenSetSyncTarget() ran: EthPeers.waitForPeer() only wakes on a *new* peer
  // connecting, never on an existing peer's capacity freeing back up, and had no timeout - so if
  // no new peer ever connects, the retry chain hangs silently forever. This simulates "no new peer
  // will ever connect" by stubbing waitForPeer() to return a future that never completes on its
  // own, and asserts findSyncTarget() still gets retried thanks to the orTimeout guard.
  @Test
  public void findSyncTargetRetriesEvenWhenWaitForPeerNeverCompletes() {
    ethScheduler = new EthScheduler(1, 1, 1, 1, new NoOpMetricsSystem());
    final EthPeers ethPeers = mock(EthPeers.class);
    when(ethPeers.waitForPeer(any())).thenAnswer(invocation -> new CompletableFuture<EthPeer>());

    final EthContext ethContext = mock(EthContext.class);
    when(ethContext.getScheduler()).thenReturn(ethScheduler);
    when(ethContext.getEthPeers()).thenReturn(ethPeers);

    final AtomicInteger selectionAttempts = new AtomicInteger();
    final AbstractSyncTargetManager syncTargetManager =
        new AbstractSyncTargetManager(
            mock(SynchronizerConfiguration.class),
            mock(ProtocolSchedule.class),
            mock(ProtocolContext.class),
            ethContext,
            new NoOpMetricsSystem()) {
          @Override
          protected CompletableFuture<Optional<EthPeer>> selectBestAvailableSyncTarget() {
            selectionAttempts.incrementAndGet();
            return completedFuture(Optional.empty());
          }

          @Override
          public boolean shouldContinueDownloading() {
            return true;
          }
        };

    syncTargetManager.findSyncTarget();

    // The first attempt happens synchronously; a second attempt only happens if the retry
    // chain survives waitForPeer() never completing on its own - proving the orTimeout guard
    // added in AbstractSyncTargetManager.waitForPeerAndThenSetSyncTarget() actually unsticks it.
    Awaitility.await()
        .atMost(20, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(selectionAttempts.get()).isGreaterThanOrEqualTo(2));
  }
}
