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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SealableBlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSelectorFromSafeBlock;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the state of the SnapSync process, including the current progress, healing status, and
 * other relevant information.
 */
public class SnapSyncProcessState {
  private static final Logger LOG = LoggerFactory.getLogger(SnapSyncProcessState.class);

  private OptionalLong pivotBlockNumber;
  private Optional<Hash> pivotBlockHash;
  private Optional<BlockHeader> pivotBlockHeader;
  private boolean sourceIsTrusted = false;
  private boolean isHealTrieInProgress;
  private boolean isHealFlatDatabaseInProgress;
  private boolean isWaitingBlockchain;

  public SnapSyncProcessState() {
    pivotBlockNumber = OptionalLong.empty();
    pivotBlockHash = Optional.empty();
    pivotBlockHeader = Optional.empty();
  }

  public SnapSyncProcessState(final long pivotBlockNumber, final boolean sourceIsTrusted) {
    this(OptionalLong.of(pivotBlockNumber), Optional.empty(), Optional.empty(), sourceIsTrusted);
  }

  public SnapSyncProcessState(final Hash pivotBlockHash, final boolean sourceIsTrusted) {
    this(OptionalLong.empty(), Optional.of(pivotBlockHash), Optional.empty(), sourceIsTrusted);
  }

  public SnapSyncProcessState(final BlockHeader pivotBlockHeader, final boolean sourceIsTrusted) {
    this(
        OptionalLong.of(pivotBlockHeader.getNumber()),
        Optional.of(pivotBlockHeader.getHash()),
        Optional.of(pivotBlockHeader),
        sourceIsTrusted);
  }

  private SnapSyncProcessState(
      final OptionalLong pivotBlockNumber,
      final Optional<Hash> pivotBlockHash,
      final Optional<BlockHeader> pivotBlockHeader,
      final boolean sourceIsTrusted) {
    this.pivotBlockNumber = pivotBlockNumber;
    this.pivotBlockHash = pivotBlockHash;
    this.pivotBlockHeader = pivotBlockHeader;
    this.sourceIsTrusted = sourceIsTrusted;
  }

  public OptionalLong getPivotBlockNumber() {
    return pivotBlockNumber;
  }

  public Optional<Hash> getPivotBlockHash() {
    return pivotBlockHash;
  }

  public Optional<BlockHeader> getPivotBlockHeader() {
    return pivotBlockHeader;
  }

  public boolean hasPivotBlockHeader() {
    return pivotBlockHeader.isPresent();
  }

  public boolean hasPivotBlockHash() {
    return pivotBlockHash.isPresent();
  }

  /**
   * Returns true if the source of the pivot block is fully trusted. In practice this means that it
   * comes from the Consensus client through the engine API and the {@link
   * PivotSelectorFromSafeBlock} is used for the pivot.
   *
   * @return true if the source is fully trusted, false otherwise
   */
  public boolean isSourceTrusted() {
    return sourceIsTrusted;
  }

  void setCurrentHeader(final BlockHeader header) {
    pivotBlockNumber = OptionalLong.of(header.getNumber());
    pivotBlockHash = Optional.of(header.getHash());
    pivotBlockHeader = Optional.of(header);
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

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SnapSyncProcessState that = (SnapSyncProcessState) o;
    return sourceIsTrusted == that.sourceIsTrusted
        && Objects.equals(pivotBlockNumber, that.pivotBlockNumber)
        && Objects.equals(pivotBlockHash, that.pivotBlockHash)
        && Objects.equals(pivotBlockHeader, that.pivotBlockHeader);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pivotBlockNumber, pivotBlockHash, pivotBlockHeader, sourceIsTrusted);
  }

  @Override
  public String toString() {
    return "SnapSyncProcessState{"
        + "pivotBlockNumber="
        + pivotBlockNumber
        + ", pivotBlockHash="
        + pivotBlockHash
        + ", pivotBlockHeader="
        + pivotBlockHeader
        + ", sourceIsTrusted="
        + sourceIsTrusted
        + '}';
  }
}
