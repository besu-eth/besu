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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.request;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.manager.task.EthTask;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;

import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Context needed by snap requests to enqueue follow-up work and report progress. */
public interface SnapRequestContext {

  void enqueueRequest(SnapDataRequest request);

  void enqueueRequests(Stream<SnapDataRequest> requests);

  void addOutstandingTask(EthTask<?> task);

  void removeOutstandingTask(EthTask<?> task);

  void setRootNodeData(Bytes rootNodeData);

  SnapSyncMetricsManager getMetricsManager();

  void addAccountToHealingList(Bytes account);

  /**
   * An account's storage root learned by a storage root refresh.
   *
   * @param stateRoot the state root the account was fetched for
   * @param storageRoot the account's storage root at that state root
   */
  record RefreshedStorageRoot(Hash stateRoot, Bytes32 storageRoot) {}

  /**
   * Returns the latest storage root learned by a refresh for the account, if any.
   *
   * @param accountHash the account
   * @return the refreshed storage root, or empty if the account was never refreshed
   */
  Optional<RefreshedStorageRoot> getRefreshedStorageRoot(Hash accountHash);

  /**
   * Requests a refresh of the account's storage root, unless one is already pending or was made at
   * the current pivot.
   *
   * @param accountHash the account whose storage root may be out of date
   */
  void requestStorageRootRefresh(Hash accountHash);

  /**
   * Records the result of a storage root refresh and clears the pending marker.
   *
   * @param accountHash the account
   * @param stateRoot the state root the account was fetched for
   * @param storageRoot the account's storage root at that state root
   */
  void completeStorageRootRefresh(Hash accountHash, Hash stateRoot, Bytes32 storageRoot);
}
