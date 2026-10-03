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

import static org.hyperledger.besu.ethereum.core.plugins.Subscriptions.unsubscribeOnClose;

import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.plugin.services.BesuEvents;
import org.hyperledger.besu.plugin.services.Subscription;
import org.hyperledger.besu.plugin.services.sync.SyncEventService;
import org.hyperledger.besu.plugin.services.sync.spi.InitialSyncCompletionListener;
import org.hyperledger.besu.plugin.services.sync.spi.SyncStatusListener;

/** Delivers the sync state's events to plugins. */
public class SyncEventServiceImpl implements SyncEventService {

  private final SyncState syncState;

  /**
   * Creates the service.
   *
   * @param syncState the sync state whose events are delivered
   */
  public SyncEventServiceImpl(final SyncState syncState) {
    this.syncState = syncState;
  }

  @Override
  public Subscription subscribeSyncStatus(final SyncStatusListener listener) {
    final long id = syncState.subscribeSyncStatus(listener::onSyncStatusChanged);
    return unsubscribeOnClose(() -> syncState.unsubscribeSyncStatus(id));
  }

  @Override
  public Subscription subscribeInitialSyncCompletion(final InitialSyncCompletionListener listener) {
    final long id =
        syncState.subscribeCompletionReached(
            new BesuEvents.InitialSyncCompletionListener() {
              @Override
              public void onInitialSyncCompleted() {
                listener.onInitialSyncCompleted();
              }

              @Override
              public void onInitialSyncRestart() {
                listener.onInitialSyncRestart();
              }
            });
    return unsubscribeOnClose(() -> syncState.unsubscribeInitialConditionReached(id));
  }
}
