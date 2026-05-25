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
 * @param firstPivotBlockHeader header of the first pivot block
 * @param pivotBlockHeader header of the pivot block
 * @param blockDownloadAnchor header of the checkpoint block
 * @param headerDownloadAnchor set if the anchor is different from the checkpoint block header
 * @param headersDownloadComplete true if the header download has finished
 * @param headerDownloadProgress lowest header successfully imported so far (resume point)
 */
public record ChainSyncState(
    BlockHeader firstPivotBlockHeader,
    BlockHeader pivotBlockHeader,
    BlockHeader blockDownloadAnchor,
    BlockHeader headerDownloadAnchor,
    boolean headersDownloadComplete,
    BlockHeader headerDownloadProgress) {

  /**
   * Creates a new state with an initial pivot block.
   *
   * @param pivotBlockHeader the pivot block header
   * @param blockDownloadAnchor the checkpoint block to start bodies download from
   * @param headerDownloadAnchor set if the anchor is different from the checkpoint block header
   * @return new ChainSyncState
   */
  public static ChainSyncState initialSync(
      final BlockHeader pivotBlockHeader,
      final BlockHeader blockDownloadAnchor,
      final BlockHeader headerDownloadAnchor) {
    return new ChainSyncState(
        pivotBlockHeader, pivotBlockHeader, blockDownloadAnchor, headerDownloadAnchor, false, null);
  }

  /**
   * Creates a new state for continuing sync to an updated pivot that is used once the previous
   * pivot block has been reached (previous pivot becomes the new block download anchor).
   *
   * @param newPivotHeader the new pivot block header
   * @param previousPivotHeader the previous pivot block header
   * @return new ChainSyncState for continuation
   */
  public ChainSyncState continueToNewPivot(
      final BlockHeader newPivotHeader, final BlockHeader previousPivotHeader) {
    return new ChainSyncState(
        firstPivotBlockHeader, newPivotHeader, previousPivotHeader, null, false, null);
  }

  /**
   * Creates a new state with headers download marked as complete.
   *
   * @return new ChainSyncState instance
   */
  public ChainSyncState withHeadersDownloadComplete() {
    return new ChainSyncState(
        firstPivotBlockHeader, this.pivotBlockHeader, this.blockDownloadAnchor, null, true, null);
  }

  /**
   * Replaces the pivot, preserves the existing block-download anchor, and resets header-download
   * progress. Caller has explicit control over {@code headersDownloadComplete}.
   *
   * @param newPivotHeader the new pivot block header
   * @param headersDownloadComplete whether Stage 1 should be considered already done
   * @return new ChainSyncState with the new pivot, the supplied completion flag, and progress reset
   *     to null
   */
  public ChainSyncState withPivot(
      final BlockHeader newPivotHeader, final boolean headersDownloadComplete) {
    return new ChainSyncState(
        firstPivotBlockHeader,
        newPivotHeader,
        blockDownloadAnchor,
        headerDownloadAnchor,
        headersDownloadComplete,
        null);
  }

  /**
   * Creates a new state when we restart the sync from the current chain head.
   *
   * @param chainHeadHeader the current head of our local chain
   * @return new ChainSyncState instance
   */
  public ChainSyncState fromHead(final BlockHeader chainHeadHeader) {
    return new ChainSyncState(
        firstPivotBlockHeader,
        this.pivotBlockHeader,
        chainHeadHeader,
        this.headerDownloadAnchor,
        this.headersDownloadComplete,
        this.headerDownloadProgress);
  }

  /**
   * Creates a new state with updated header download progress. The given header becomes the new
   * anchor for the backward header download so that a pipeline restart resumes from this point.
   *
   * @param lowestImportedHeader the lowest header that was successfully imported
   * @return new ChainSyncState instance with updated header download anchor
   */
  public ChainSyncState withHeaderProgress(final BlockHeader lowestImportedHeader) {
    return new ChainSyncState(
        firstPivotBlockHeader,
        this.pivotBlockHeader,
        this.blockDownloadAnchor,
        this.headerDownloadAnchor,
        this.headersDownloadComplete,
        lowestImportedHeader);
  }

  /**
   * Creates a new state with the body download anchor advanced to a block that has already been
   * imported in a previous session. All other fields are preserved.
   *
   * @param newAnchor the highest block whose body is confirmed present under the current canonical
   *     chain's hash
   * @return new ChainSyncState with the advanced anchor
   */
  public ChainSyncState withAdvancedBodyAnchor(final BlockHeader newAnchor) {
    return new ChainSyncState(
        firstPivotBlockHeader,
        pivotBlockHeader,
        newAnchor,
        headerDownloadAnchor,
        headersDownloadComplete,
        headerDownloadProgress);
  }

  /**
   * Creates a new state for the case where Stage 1 anchor recovery walked below the original anchor
   * and matched a canonical stored ancestor. Both anchors are replaced with the matched ancestor so
   * Stage 2 (bodies and receipts) starts from there, not from the now-side-chain chain head.
   *
   * @param matchedAncestor the canonical stored header that recovery matched
   * @return new ChainSyncState with both anchors replaced
   */
  public ChainSyncState withRecoveryMatch(final BlockHeader matchedAncestor) {
    return new ChainSyncState(
        firstPivotBlockHeader,
        pivotBlockHeader,
        matchedAncestor,
        matchedAncestor,
        headersDownloadComplete,
        headerDownloadProgress);
  }

  @Override
  public String toString() {
    return "ChainSyncState{"
        + "firstPivotBlockNumber="
        + firstPivotBlockHeader.getNumber()
        + ", firstPivotBlockHash="
        + firstPivotBlockHeader.getHash()
        + ", pivotBlockNumber="
        + pivotBlockHeader.getNumber()
        + ", pivotBlockHash="
        + pivotBlockHeader.getHash()
        + ", checkpointBlockNumber="
        + blockDownloadAnchor.getNumber()
        + ", headerDownloadAnchorNumber="
        + (headerDownloadAnchor != null ? headerDownloadAnchor.getNumber() : "null")
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
        && Objects.equals(blockDownloadAnchor, that.blockDownloadAnchor)
        && Objects.equals(headerDownloadAnchor, that.headerDownloadAnchor)
        && Objects.equals(firstPivotBlockHeader, that.firstPivotBlockHeader)
        && Objects.equals(headerDownloadProgress, that.headerDownloadProgress);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        firstPivotBlockHeader,
        pivotBlockHeader,
        blockDownloadAnchor,
        headerDownloadAnchor,
        headersDownloadComplete,
        headerDownloadProgress);
  }
}
