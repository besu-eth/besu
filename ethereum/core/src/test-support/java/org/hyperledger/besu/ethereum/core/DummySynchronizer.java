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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.plugin.data.SyncStatus;
import org.hyperledger.besu.plugin.services.BesuEvents;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Naive implementation of Synchronizer used by retesteth. Because retesteth is not implemented in
 * the test module, it has no access to mockito. This class provides a minimum implementation needed
 * to run RPC methods which may require a Synchronizer.
 */
public class DummySynchronizer implements Synchronizer {
  @Override
  public CompletableFuture<Void> start() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void stop() {}

  @Override
  public void awaitStop() throws InterruptedException {}

  @Override
  public Optional<SyncStatus> getSyncStatus() {
    return Optional.empty();
  }

  @Override
  public boolean isInSync() {
    return false;
  }

  @Override
  public Optional<Long> getBestPeerChainHead() {
    return Optional.empty();
  }

  @Override
  public boolean resyncWorldState() {
    return false;
  }

  @Override
  public long subscribeSyncStatus(final BesuEvents.SyncStatusListener listener) {
    return 0;
  }

  @Override
  public boolean unsubscribeSyncStatus(final long observerId) {
    return false;
  }

  @Override
  public long subscribeInSync(final InSyncListener listener) {
    return 0;
  }

  @Override
  public long subscribeInSync(final InSyncListener listener, final long syncTolerance) {
    return 0;
  }

  @Override
  public boolean unsubscribeInSync(final long listenerId) {
    return false;
  }

  @Override
  public long subscribeInitialSync(final BesuEvents.InitialSyncCompletionListener listener) {
    return 0;
  }

  @Override
  public boolean unsubscribeInitialSync(final long listenerId) {
    return false;
  }
}
