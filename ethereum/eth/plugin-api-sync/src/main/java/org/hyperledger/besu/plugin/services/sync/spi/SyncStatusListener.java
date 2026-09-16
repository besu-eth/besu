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
package org.hyperledger.besu.plugin.services.sync.spi;

import org.hyperledger.besu.plugin.data.SyncStatus;

import java.util.Optional;

/** Receives sync status events. Plugins implement this; Besu calls it. */
@FunctionalInterface
public interface SyncStatusListener {

  /**
   * Invoked when the synchronizer status changes.
   *
   * @param syncStatus the sync status, empty when the node is not syncing
   */
  void onSyncStatusChanged(Optional<SyncStatus> syncStatus);
}
