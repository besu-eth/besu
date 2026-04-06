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
  private BlockHeader chainHeadHeader;

  @BeforeEach
  public void setUp() {
    pivotBlockHeader = new BlockHeaderTestFixture().number(1000).buildHeader();
    checkpointBlockHeader = new BlockHeaderTestFixture().number(500).buildHeader();
    genesisBlockHeader = new BlockHeaderTestFixture().number(0).buildHeader();
    chainHeadHeader = new BlockHeaderTestFixture().number(600).buildHeader();
  }

  // Factory Method Tests

  @Test
  public void initialSyncShouldCreateStateWithAllFields() {
    ChainSyncState state =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);

    assertThat(state.pivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(state.firstPivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(state.blockDownloadAnchor()).isEqualTo(checkpointBlockHeader);
    assertThat(state.headerDownloadAnchor()).isEqualTo(genesisBlockHeader);
    assertThat(state.headersDownloadComplete()).isFalse();
  }

  @Test
  public void continueToNewPivotShouldUpdatePivotAndAnchor() {
    ChainSyncState initialState =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);
    BlockHeader newPivot = new BlockHeaderTestFixture().number(2000).buildHeader();

    ChainSyncState continuedState = initialState.continueToNewPivot(newPivot, pivotBlockHeader);

    assertThat(continuedState.pivotBlockHeader()).isEqualTo(newPivot);
    assertThat(continuedState.firstPivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(continuedState.blockDownloadAnchor()).isEqualTo(pivotBlockHeader);
    assertThat(continuedState.headerDownloadAnchor()).isNull();
    assertThat(continuedState.headersDownloadComplete()).isFalse();
  }

  @Test
  public void withHeadersDownloadCompleteShouldMarkComplete() {
    ChainSyncState initialState =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);

    ChainSyncState completedState = initialState.withHeadersDownloadComplete();

    assertThat(completedState.pivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(completedState.firstPivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(completedState.blockDownloadAnchor()).isEqualTo(checkpointBlockHeader);
    assertThat(completedState.headerDownloadAnchor()).isNull();
    assertThat(completedState.headersDownloadComplete()).isTrue();
  }

  @Test
  public void fromHeadShouldUpdateBlockDownloadAnchor() {
    ChainSyncState initialState =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);

    ChainSyncState restartedState = initialState.fromHead(chainHeadHeader);

    assertThat(restartedState.pivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(restartedState.firstPivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(restartedState.blockDownloadAnchor()).isEqualTo(chainHeadHeader);
    assertThat(restartedState.headerDownloadAnchor()).isEqualTo(genesisBlockHeader);
    assertThat(restartedState.headersDownloadComplete()).isFalse();
  }

  @Test
  public void fromHeadShouldPreserveHeadersDownloadComplete() {
    ChainSyncState initialState =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);
    ChainSyncState completedState = initialState.withHeadersDownloadComplete();

    ChainSyncState restartedState = completedState.fromHead(chainHeadHeader);

    assertThat(restartedState.headersDownloadComplete()).isTrue();
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
    assertThat(state2.headerDownloadAnchor()).isNull();

    // Continue to new pivot
    BlockHeader newPivot = new BlockHeaderTestFixture().number(2000).buildHeader();
    ChainSyncState state3 = state2.continueToNewPivot(newPivot, pivotBlockHeader);
    assertThat(state3.pivotBlockHeader()).isEqualTo(newPivot);
    assertThat(state3.blockDownloadAnchor()).isEqualTo(pivotBlockHeader);
    assertThat(state3.headersDownloadComplete()).isFalse();

    // Restart from head
    ChainSyncState state4 = state3.fromHead(chainHeadHeader);
    assertThat(state4.blockDownloadAnchor()).isEqualTo(chainHeadHeader);
    assertThat(state4.pivotBlockHeader()).isEqualTo(newPivot);
    assertThat(state4.firstPivotBlockHeader()).isEqualTo(pivotBlockHeader);
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
        new ChainSyncState(pivotBlockHeader, pivotBlockHeader, checkpointBlockHeader, null, true);
    ChainSyncState state2 =
        new ChainSyncState(pivotBlockHeader, pivotBlockHeader, checkpointBlockHeader, null, true);

    assertThat(state1).isEqualTo(state2);
    assertThat(state1.hashCode()).isEqualTo(state2.hashCode());
  }

  @Test
  public void shouldNotBeEqualWhenOneHeaderDownloadAnchorIsNull() {
    ChainSyncState state1 =
        new ChainSyncState(pivotBlockHeader, pivotBlockHeader, checkpointBlockHeader, null, true);
    ChainSyncState state2 =
        new ChainSyncState(
            pivotBlockHeader, pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader, true);

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
    assertThat(result).contains("firstPivotBlockNumber=" + pivotBlockHeader.getNumber());
    assertThat(result).contains("pivotBlockHash=" + pivotBlockHeader.getHash());
    assertThat(result).contains("checkpointBlockNumber=" + checkpointBlockHeader.getNumber());
    assertThat(result).contains("headerDownloadAnchorNumber=" + genesisBlockHeader.getNumber());
    assertThat(result).contains("headersDownloadComplete=false");
  }

  @Test
  public void toStringShouldHandleNullHeaderDownloadAnchor() {
    // withHeadersDownloadComplete() sets headerDownloadAnchor to null
    ChainSyncState state =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader)
            .withHeadersDownloadComplete();

    String result = state.toString();

    assertThat(result).contains("headerDownloadAnchorNumber=null");
    assertThat(result).contains("headersDownloadComplete=true");
  }

  @Test
  public void factoryMethodsShouldReturnNewInstances() {
    ChainSyncState state1 =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, genesisBlockHeader);
    ChainSyncState state2 = state1.withHeadersDownloadComplete();
    ChainSyncState state3 = state2.fromHead(chainHeadHeader);

    // All should be different instances
    assertThat(state1).isNotSameAs(state2);
    assertThat(state2).isNotSameAs(state3);
    assertThat(state1).isNotSameAs(state3);

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
    assertThat(continued2.firstPivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(continued2.blockDownloadAnchor()).isEqualTo(pivot2);
    assertThat(continued2.headerDownloadAnchor()).isNull();
    assertThat(continued2.headersDownloadComplete()).isFalse();
  }

  @Test
  public void shouldHandleSamePivotAndCheckpoint() {
    ChainSyncState state =
        ChainSyncState.initialSync(pivotBlockHeader, pivotBlockHeader, genesisBlockHeader);

    assertThat(state.pivotBlockHeader()).isEqualTo(state.blockDownloadAnchor());
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
    assertThat(state.blockDownloadAnchor().getNumber()).isEqualTo(Long.MAX_VALUE - 500);
  }
}
