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
 */
public record ChainSyncState(
    BlockHeader pivotBlockHeader,
    BlockHeader bodyCheckpoint,
    BlockHeader headerDownloadAnchor,
    boolean headersDownloadComplete) {

  public ChainSyncState(
      final BlockHeader pivotBlockHeader,
      final BlockHeader bodyCheckpoint,
      final BlockHeader headerDownloadAnchor,
      final boolean headersDownloadComplete) {
    this.pivotBlockHeader = Objects.requireNonNull(pivotBlockHeader, "pivotBlockHeader");
    this.bodyCheckpoint = Objects.requireNonNull(bodyCheckpoint, "bodyCheckpoint");
    this.headerDownloadAnchor =
        Objects.requireNonNull(headerDownloadAnchor, "headerDownloadAnchor");
    this.headersDownloadComplete = headersDownloadComplete;
  }

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
    return new ChainSyncState(pivotBlockHeader, bodyCheckpoint, headerDownloadAnchor, false);
  }

  /**
   * Creates a new state with headers download marked as complete.
   *
   * @return new ChainSyncState instance
   */
  public ChainSyncState withHeadersDownloadComplete() {
    return new ChainSyncState(
        this.pivotBlockHeader, this.bodyCheckpoint, this.headerDownloadAnchor, true);
  }

  /**
   * Updates the pivot to a header that is already on the local canonical chain, so all headers are
   * already present. The body checkpoint and header download anchor are preserved; {@code
   * headersDownloadComplete} is set to {@code true}.
   *
   * @param newPivotHeader the new pivot block header, which must be on the local canonical chain
   * @return new ChainSyncState with Stage 1 marked complete for the new pivot
   */
  public ChainSyncState withCanonicalPivot(final BlockHeader newPivotHeader) {
    return new ChainSyncState(newPivotHeader, this.bodyCheckpoint, this.headerDownloadAnchor, true);
  }

  /**
   * Creates a new state that restarts Stage 1 with a new pivot and a specific header download
   * anchor, while preserving the existing body checkpoint. {@code headersDownloadComplete} is reset
   * to {@code false}. Used both when advancing to a new pivot (anchor = previous pivot) and when
   * restarting after a reorg (anchor = highest stored header below the new pivot).
   *
   * @param newPivotHeader the new pivot block header
   * @param headerAnchor the block header at which Stage 1 should stop downloading
   * @return new ChainSyncState ready to restart Stage 1
   */
  public ChainSyncState restartHeaderDownload(
      final BlockHeader newPivotHeader, final BlockHeader headerAnchor) {
    return new ChainSyncState(newPivotHeader, this.bodyCheckpoint, headerAnchor, false);
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
        this.pivotBlockHeader, this.bodyCheckpoint, matchedAncestor, this.headersDownloadComplete);
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
        + ", headersDownloadComplete="
        + headersDownloadComplete
        + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    // All three headers are guaranteed non-null by the constructor, so compare them directly.
    final ChainSyncState that = (ChainSyncState) o;
    return headersDownloadComplete == that.headersDownloadComplete
        && pivotBlockHeader.equals(that.pivotBlockHeader)
        && bodyCheckpoint.equals(that.bodyCheckpoint)
        && headerDownloadAnchor.equals(that.headerDownloadAnchor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        pivotBlockHeader, bodyCheckpoint, headerDownloadAnchor, headersDownloadComplete);
  }
}
