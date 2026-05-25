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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import org.hyperledger.besu.ethereum.core.SealableBlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the state of the SnapSync process, including the current progress, healing status, and
 * other relevant information.
 */
public class SnapSyncProcessState extends PivotSyncState {
  private static final Logger LOG = LoggerFactory.getLogger(SnapSyncProcessState.class);

  private boolean isHealTrieInProgress;
  private boolean isHealFlatDatabaseInProgress;
  private boolean isWaitingBlockchain;

  public SnapSyncProcessState(final PivotSyncState fastSyncState) {
    super(
        fastSyncState.getPivotBlockNumber(),
        fastSyncState.getPivotBlockHash(),
        fastSyncState.getPivotBlockHeader(),
        fastSyncState.isSourceTrusted());
  }

  public boolean isHealTrieInProgress() {
    return isHealTrieInProgress;
  }

  public void setHealTrieStatus(final boolean healTrieStatus) {
    isHealTrieInProgress = healTrieStatus;
  }

  public boolean isHealFlatDatabaseInProgress() {
    return isHealFlatDatabaseInProgress;
  }

  public void setHealFlatDatabaseInProgress(final boolean healFlatDatabaseInProgress) {
    isHealFlatDatabaseInProgress = healFlatDatabaseInProgress;
  }

  public boolean isWaitingBlockchain() {
    return isWaitingBlockchain;
  }

  public void setWaitingBlockchain(final boolean waitingBlockchain) {
    LOG.debug("Set waiting blockchain to {}", waitingBlockchain);
    isWaitingBlockchain = waitingBlockchain;
  }

  public boolean isExpired(final SnapDataRequest request) {
    return getPivotBlockHeader()
        .map(SealableBlockHeader::getStateRoot)
        .filter(hash -> hash.equals(request.getRootHash()))
        .isEmpty();
  }
}
