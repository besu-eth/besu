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
package org.hyperledger.besu.plugin.services.sync;

import org.hyperledger.besu.plugin.StartService;
import org.hyperledger.besu.plugin.services.Subscription;
import org.hyperledger.besu.plugin.services.sync.spi.InitialSyncCompletionListener;
import org.hyperledger.besu.plugin.services.sync.spi.SyncStatusListener;

/**
 * Subscriptions to synchronization events: sync status changes, and initial sync completion and
 * restart.
 *
 * <p>This is a start-phase service, separate from {@link SynchronizationService}, because a
 * subscription has to be in place before the synchronizer starts producing events, and the
 * synchronizer starts with the main loop. On a full-sync node, for example, initial sync completion
 * is announced the moment the synchronizer starts, and only once. Querying and controlling the
 * synchronizer is {@link SynchronizationService}, available once the node is running.
 */
public interface SyncEventService extends StartService {

  /**
   * Subscribes to sync status changes.
   *
   * @param listener the listener that receives each status change
   * @return the subscription; close it to stop receiving events
   */
  Subscription subscribeSyncStatus(SyncStatusListener listener);

  /**
   * Subscribes to initial sync completion and restart.
   *
   * @param listener the listener that receives completion and restart callbacks
   * @return the subscription; close it to stop receiving events
   */
  Subscription subscribeInitialSyncCompletion(InitialSyncCompletionListener listener);
}
