/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.eth.sync.common;

import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Objects;

/**
 * Immutable state for the chain synchronization in two-stages. This state is managed exclusively by
 * the SnapSyncChainDownloader.
 *
 * <p>Updates create new instances.
 *
 * @param pivotBlockHeader header of the pivot block
 * @param bodyCheckpoint the lowest block from which Stage 2 must download bodies
 * @param headerDownloadAnchor the block header at which Stage 1 stops downloading
 * @param headersDownloadComplete true if the header download has finished
 * @param headerDownloadProgress lowest header successfully imported so far (resume point)
 */
public record ChainSyncState(
    BlockHeader pivotBlockHeader,
    BlockHeader bodyCheckpoint,
    BlockHeader headerDownloadAnchor,
    boolean headersDownloadComplete,
    BlockHeader headerDownloadProgress) {

  /**
   * Creates a new state with an initial pivot block.
   *
   * @param pivotBlockHeader the pivot block header
   * @param bodyCheckpoint the checkpoint block to start bodies download from
   * @param headerDownloadAnchor the block header at which Stage 1 stops downloading
   * @return new ChainSyncState
   */
  public static ChainSyncState initialSync(
      final BlockHeader pivotBlockHeader,
      final BlockHeader bodyCheckpoint,
      final BlockHeader headerDownloadAnchor) {
    return new ChainSyncState(pivotBlockHeader, bodyCheckpoint, headerDownloadAnchor, false, null);
  }

  /**
   * Creates a new state for continuing sync to an updated pivot once the previous pivot block has
   * been reached. The body checkpoint is preserved; the previous pivot becomes the new {@code
   * headerDownloadAnchor} so Stage 1 downloads from the new pivot down to the old one.
   *
   * @param newPivotHeader the new pivot block header
   * @param previousPivotHeader the previous pivot, used as the Stage 1 stop point
   * @return new ChainSyncState for continuation
   */
  public ChainSyncState continueToNewPivot(
      final BlockHeader newPivotHeader, final BlockHeader previousPivotHeader) {
    return new ChainSyncState(
        newPivotHeader, this.bodyCheckpoint, previousPivotHeader, false, null);
  }

  /**
   * Creates a new state with headers download marked as complete.
   *
   * @return new ChainSyncState instance
   */
  public ChainSyncState withHeadersDownloadComplete() {
    return new ChainSyncState(
        this.pivotBlockHeader, this.bodyCheckpoint, this.headerDownloadAnchor, true, null);
  }

  /**
   * Updates the pivot to a new header whose headers are already available locally, skipping Stage
   * 1. The body checkpoint and header download anchor are preserved; {@code
   * headersDownloadComplete} is set to {@code true} so Stage 2 starts immediately.
   *
   * @param newPivotHeader the new pivot block header
   * @return new ChainSyncState with Stage 1 marked complete for the new pivot
   */
  public ChainSyncState withNewPivotSkippingStage1(final BlockHeader newPivotHeader) {
    return new ChainSyncState(
        newPivotHeader, this.bodyCheckpoint, this.headerDownloadAnchor, true, null);
  }

  /**
   * Creates a new state that restarts Stage 1 with a new pivot and a specific header download
   * anchor, while preserving the existing body checkpoint. {@code headersDownloadComplete} is reset
   * to {@code false} and {@code headerDownloadProgress} is cleared.
   *
   * @param newPivotHeader the new pivot block header
   * @param headerAnchor the block header at which Stage 1 should stop downloading
   * @return new ChainSyncState ready to restart Stage 1
   */
  public ChainSyncState restartHeaderDownload(
      final BlockHeader newPivotHeader, final BlockHeader headerAnchor) {
    return new ChainSyncState(newPivotHeader, this.bodyCheckpoint, headerAnchor, false, null);
  }

  /**
   * Creates a new state with updated header download progress. The given header is recorded as the
   * lowest successfully imported header so that a pipeline restart can resume from this point.
   *
   * @param lowestImportedHeader the lowest header that was successfully imported
   * @return new ChainSyncState instance with updated header download progress
   */
  public ChainSyncState withHeaderProgress(final BlockHeader lowestImportedHeader) {
    return new ChainSyncState(
        this.pivotBlockHeader,
        this.bodyCheckpoint,
        this.headerDownloadAnchor,
        this.headersDownloadComplete,
        lowestImportedHeader);
  }

  /**
   * Creates a new state after Stage 1 anchor recovery matched a canonical stored ancestor. {@code
   * headerDownloadAnchor} is always set to {@code matchedAncestor}.
   *
   * @param matchedAncestor the canonical stored header that recovery matched
   * @return new ChainSyncState with updated anchors
   */
  public ChainSyncState withRecoveryMatch(final BlockHeader matchedAncestor) {
    return new ChainSyncState(
        this.pivotBlockHeader,
        this.bodyCheckpoint,
        matchedAncestor,
        this.headersDownloadComplete,
        this.headerDownloadProgress);
  }

  @Override
  public String toString() {
    return "ChainSyncState{"
        + "pivotBlockNumber="
        + pivotBlockHeader.getNumber()
        + ", pivotBlockHash="
        + pivotBlockHeader.getHash()
        + ", bodyCheckpointNumber="
        + bodyCheckpoint.getNumber()
        + ", headerDownloadAnchorNumber="
        + headerDownloadAnchor.getNumber()
        + ", headerDownloadProgressNumber="
        + (headerDownloadProgress != null ? headerDownloadProgress.getNumber() : "null")
        + ", headersDownloadComplete="
        + headersDownloadComplete
        + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ChainSyncState that = (ChainSyncState) o;
    return headersDownloadComplete == that.headersDownloadComplete
        && Objects.equals(pivotBlockHeader, that.pivotBlockHeader)
        && Objects.equals(bodyCheckpoint, that.bodyCheckpoint)
        && Objects.equals(headerDownloadAnchor, that.headerDownloadAnchor)
        && Objects.equals(headerDownloadProgress, that.headerDownloadProgress);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        pivotBlockHeader,
        bodyCheckpoint,
        headerDownloadAnchor,
        headersDownloadComplete,
        headerDownloadProgress);
  }
}
