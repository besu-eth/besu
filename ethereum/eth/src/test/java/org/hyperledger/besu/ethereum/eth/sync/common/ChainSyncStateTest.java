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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ChainSyncStateTest {

  private BlockHeader pivotBlockHeader;
  private BlockHeader checkpointBlockHeader;
  private BlockHeader genesisBlockHeader;

  @BeforeEach
  public void setUp() {
    pivotBlockHeader = new BlockHeaderTestFixture().number(1000).buildHeader();
    checkpointBlockHeader = new BlockHeaderTestFixture().number(500).buildHeader();
    genesisBlockHeader = new BlockHeaderTestFixture().number(0).buildHeader();
  }

  // Factory Method Tests

  @Test
  public void initialSyncShouldCreateStateWithAllFields() {
    ChainSyncState state =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);

    assertThat(state.pivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(state.bodyCheckpoint()).isEqualTo(checkpointBlockHeader);
    assertThat(state.headerDownloadAnchor()).isEqualTo(genesisBlockHeader);
    assertThat(state.headerDownloadProgress()).isNull();
    assertThat(state.headersDownloadComplete()).isFalse();
  }

  @Test
  public void continueToNewPivotShouldUpdatePivotAndAnchor() {
    ChainSyncState initialState =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);
    BlockHeader newPivot = new BlockHeaderTestFixture().number(2000).buildHeader();

    ChainSyncState continuedState = initialState.continueToNewPivot(newPivot, pivotBlockHeader);

    assertThat(continuedState.pivotBlockHeader()).isEqualTo(newPivot);
    // bodyCheckpoint is preserved; the previous pivot becomes the Stage 1 header anchor.
    assertThat(continuedState.bodyCheckpoint()).isEqualTo(checkpointBlockHeader);
    assertThat(continuedState.headerDownloadAnchor()).isEqualTo(pivotBlockHeader);
    assertThat(continuedState.headerDownloadProgress()).isNull();
    assertThat(continuedState.headersDownloadComplete()).isFalse();
  }

  @Test
  public void withHeadersDownloadCompleteShouldMarkComplete() {
    ChainSyncState initialState =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);

    ChainSyncState completedState = initialState.withHeadersDownloadComplete();

    assertThat(completedState.pivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(completedState.bodyCheckpoint()).isEqualTo(checkpointBlockHeader);
    // headerDownloadAnchor is preserved when marking the header download complete.
    assertThat(completedState.headerDownloadAnchor()).isEqualTo(genesisBlockHeader);
    assertThat(completedState.headerDownloadProgress()).isNull();
    assertThat(completedState.headersDownloadComplete()).isTrue();
  }

  @Test
  public void withHeaderProgressShouldSetProgressAndPreserveAnchor() {
    ChainSyncState initialState =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);
    BlockHeader progressHeader = new BlockHeaderTestFixture().number(800).buildHeader();

    ChainSyncState updatedState = initialState.withHeaderProgress(progressHeader);

    assertThat(updatedState.pivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(updatedState.bodyCheckpoint()).isEqualTo(checkpointBlockHeader);
    assertThat(updatedState.headerDownloadAnchor()).isEqualTo(genesisBlockHeader);
    assertThat(updatedState.headerDownloadProgress()).isEqualTo(progressHeader);
    assertThat(updatedState.headersDownloadComplete()).isFalse();
  }

  @Test
  public void continueToNewPivotShouldClearProgress() {
    ChainSyncState initialState =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);
    BlockHeader progressHeader = new BlockHeaderTestFixture().number(800).buildHeader();
    ChainSyncState withProgress = initialState.withHeaderProgress(progressHeader);

    BlockHeader newPivot = new BlockHeaderTestFixture().number(2000).buildHeader();
    ChainSyncState continued = withProgress.continueToNewPivot(newPivot, pivotBlockHeader);

    assertThat(continued.headerDownloadProgress()).isNull();
    assertThat(continued.headerDownloadAnchor()).isEqualTo(pivotBlockHeader);
  }

  @Test
  public void withHeadersDownloadCompleteShouldClearProgress() {
    ChainSyncState initialState =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);
    ChainSyncState withProgress =
        initialState.withHeaderProgress(new BlockHeaderTestFixture().number(800).buildHeader());

    ChainSyncState completed = withProgress.withHeadersDownloadComplete();

    assertThat(completed.headerDownloadProgress()).isNull();
    assertThat(completed.headerDownloadAnchor()).isEqualTo(genesisBlockHeader);
  }

  @Test
  public void shouldHandleStateTransitionWorkflow() {
    // Initial sync
    ChainSyncState state1 =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);
    assertThat(state1.headersDownloadComplete()).isFalse();
    assertThat(state1.headerDownloadAnchor()).isEqualTo(genesisBlockHeader);

    // Mark headers complete
    ChainSyncState state2 = state1.withHeadersDownloadComplete();
    assertThat(state2.headersDownloadComplete()).isTrue();
    assertThat(state2.headerDownloadAnchor()).isEqualTo(genesisBlockHeader);

    // Continue to new pivot
    BlockHeader newPivot = new BlockHeaderTestFixture().number(2000).buildHeader();
    ChainSyncState state3 = state2.continueToNewPivot(newPivot, pivotBlockHeader);
    assertThat(state3.pivotBlockHeader()).isEqualTo(newPivot);
    // bodyCheckpoint is preserved across the continuation.
    assertThat(state3.bodyCheckpoint()).isEqualTo(checkpointBlockHeader);
    assertThat(state3.headersDownloadComplete()).isFalse();
  }

  // Equality and HashCode Tests

  @Test
  public void shouldBeEqualWhenAllFieldsMatch() {
    ChainSyncState state1 =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);
    ChainSyncState state2 =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);

    assertThat(state1).isEqualTo(state2);
    assertThat(state1.hashCode()).isEqualTo(state2.hashCode());
  }

  @Test
  public void shouldNotBeEqualWhenPivotDiffers() {
    ChainSyncState state1 =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);
    BlockHeader differentPivot = new BlockHeaderTestFixture().number(2000).buildHeader();
    ChainSyncState state2 =
        ChainSyncState.initialSync(differentPivot, checkpointBlockHeader, genesisBlockHeader);

    assertThat(state1).isNotEqualTo(state2);
  }

  @Test
  public void shouldNotBeEqualWhenBlockDownloadAnchorDiffers() {
    ChainSyncState state1 =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);
    BlockHeader differentAnchor = new BlockHeaderTestFixture().number(600).buildHeader();
    ChainSyncState state2 =
        ChainSyncState.initialSync(pivotBlockHeader, differentAnchor, genesisBlockHeader);

    assertThat(state1).isNotEqualTo(state2);
  }

  @Test
  public void shouldNotBeEqualWhenHeaderDownloadAnchorDiffers() {
    ChainSyncState state1 =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);
    BlockHeader differentHeader = new BlockHeaderTestFixture().number(10).buildHeader();
    ChainSyncState state2 =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, differentHeader);

    assertThat(state1).isNotEqualTo(state2);
  }

  @Test
  public void shouldNotBeEqualWhenHeadersDownloadCompleteDiffers() {
    ChainSyncState state1 =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);
    ChainSyncState state2 = state1.withHeadersDownloadComplete();

    assertThat(state1).isNotEqualTo(state2);
  }

  @Test
  public void shouldBeEqualWhenBothHeaderDownloadAnchorsAreNull() {
    ChainSyncState state1 =
        new ChainSyncState(pivotBlockHeader, checkpointBlockHeader, null, true, null);
    ChainSyncState state2 =
        new ChainSyncState(pivotBlockHeader, checkpointBlockHeader, null, true, null);

    assertThat(state1).isEqualTo(state2);
    assertThat(state1.hashCode()).isEqualTo(state2.hashCode());
  }

  @Test
  public void shouldNotBeEqualWhenOneHeaderDownloadAnchorIsNull() {
    ChainSyncState state1 =
        new ChainSyncState(pivotBlockHeader, checkpointBlockHeader, null, true, null);
    ChainSyncState state2 =
        new ChainSyncState(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader, true, null);

    assertThat(state1).isNotEqualTo(state2);
  }

  @Test
  public void shouldNotBeEqualToNull() {
    ChainSyncState state =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);

    assertThat(state.equals(null)).isFalse();
  }

  // ToString Tests

  @Test
  public void toStringShouldIncludeAllFields() {
    ChainSyncState state =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);

    String result = state.toString();

    assertThat(result).contains("pivotBlockNumber=" + pivotBlockHeader.getNumber());
    assertThat(result).contains("pivotBlockHash=" + pivotBlockHeader.getHash());
    assertThat(result).contains("bodyCheckpointNumber=" + checkpointBlockHeader.getNumber());
    assertThat(result).contains("headerDownloadAnchorNumber=" + genesisBlockHeader.getNumber());
    assertThat(result).contains("headersDownloadComplete=false");
  }

  @Test
  public void toStringShouldIncludeHeaderDownloadAnchorNumber() {
    ChainSyncState state =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader)
            .withHeadersDownloadComplete();

    String result = state.toString();

    assertThat(result).contains("headerDownloadAnchorNumber=" + genesisBlockHeader.getNumber());
    assertThat(result).contains("headersDownloadComplete=true");
  }

  @Test
  public void factoryMethodsShouldReturnNewInstances() {
    ChainSyncState state1 =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);
    ChainSyncState state2 = state1.withHeadersDownloadComplete();

    // All should be different instances
    assertThat(state1).isNotSameAs(state2);

    // Original state should be unchanged
    assertThat(state1.headersDownloadComplete()).isFalse();
    assertThat(state1.headerDownloadAnchor()).isEqualTo(genesisBlockHeader);
  }

  @Test
  public void shouldHandleMultipleContinuations() {
    ChainSyncState state =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);

    BlockHeader pivot2 = new BlockHeaderTestFixture().number(2000).buildHeader();
    ChainSyncState continued1 = state.continueToNewPivot(pivot2, pivotBlockHeader);

    BlockHeader pivot3 = new BlockHeaderTestFixture().number(3000).buildHeader();
    ChainSyncState continued2 = continued1.continueToNewPivot(pivot3, pivot2);

    assertThat(continued2.pivotBlockHeader()).isEqualTo(pivot3);
    // bodyCheckpoint is preserved across continuations; the anchor tracks the previous pivot.
    assertThat(continued2.bodyCheckpoint()).isEqualTo(checkpointBlockHeader);
    assertThat(continued2.headerDownloadAnchor()).isEqualTo(pivot2);
    assertThat(continued2.headersDownloadComplete()).isFalse();
  }

  @Test
  public void shouldHandleSamePivotAndCheckpoint() {
    ChainSyncState state =
        ChainSyncState.initialSync(pivotBlockHeader, pivotBlockHeader, genesisBlockHeader);

    assertThat(state.pivotBlockHeader()).isEqualTo(state.bodyCheckpoint());
  }

  @Test
  public void shouldHandleVeryLargeBlockNumbers() {
    BlockHeader largePivot =
        new BlockHeaderTestFixture().number(Long.MAX_VALUE - 100).buildHeader();
    BlockHeader largeCheckpoint =
        new BlockHeaderTestFixture().number(Long.MAX_VALUE - 500).buildHeader();

    ChainSyncState state =
        ChainSyncState.initialSync(largePivot, largeCheckpoint, genesisBlockHeader);

    assertThat(state.pivotBlockHeader().getNumber()).isEqualTo(Long.MAX_VALUE - 100);
    assertThat(state.bodyCheckpoint().getNumber()).isEqualTo(Long.MAX_VALUE - 500);
  }

  @Test
  public void withRecoveryMatchShouldNotRaiseBlockDownloadAnchorAboveCurrentProgress() {
    // matchedAncestor is ABOVE the existing bodyCheckpoint. This happens in Case B crash
    // recovery when the body anchor was set to the chain-head (e.g. 300) but recovery found a
    // canonical match higher up the chain (e.g. 800). bodyCheckpoint must not be raised.
    final BlockHeader newPivot = new BlockHeaderTestFixture().number(2000).buildHeader();
    final BlockHeader lowAnchor = new BlockHeaderTestFixture().number(300).buildHeader();
    final BlockHeader matchedAncestor = new BlockHeaderTestFixture().number(800).buildHeader();

    final ChainSyncState before = new ChainSyncState(newPivot, lowAnchor, null, false, null);
    final ChainSyncState after = before.withRecoveryMatch(matchedAncestor);

    assertThat(after.bodyCheckpoint()).isEqualTo(lowAnchor);
    assertThat(after.headerDownloadAnchor()).isEqualTo(matchedAncestor);
    assertThat(after.pivotBlockHeader()).isEqualTo(newPivot);
    assertThat(after.headersDownloadComplete()).isFalse();
    assertThat(after.headerDownloadProgress()).isNull();
  }

  @Test
  public void withRecoveryMatchUpdatesHeaderAnchorAndPreservesBodyCheckpoint() {
    // Recovery sets the header download anchor to the matched ancestor while preserving the body
    // checkpoint (the lowest block Stage 2 downloads bodies from), even when the matched ancestor
    // is below the current body checkpoint.
    final BlockHeader newPivot = new BlockHeaderTestFixture().number(2000).buildHeader();
    final BlockHeader bodyCheckpoint = new BlockHeaderTestFixture().number(1500).buildHeader();
    final BlockHeader matchedAncestor = new BlockHeaderTestFixture().number(1400).buildHeader();

    final ChainSyncState before = new ChainSyncState(newPivot, bodyCheckpoint, null, false, null);

    final ChainSyncState after = before.withRecoveryMatch(matchedAncestor);

    assertThat(after.pivotBlockHeader()).isEqualTo(newPivot);
    assertThat(after.bodyCheckpoint()).isEqualTo(bodyCheckpoint);
    assertThat(after.headerDownloadAnchor()).isEqualTo(matchedAncestor);
    assertThat(after.headersDownloadComplete()).isFalse();
    assertThat(after.headerDownloadProgress()).isNull();
  }
}
