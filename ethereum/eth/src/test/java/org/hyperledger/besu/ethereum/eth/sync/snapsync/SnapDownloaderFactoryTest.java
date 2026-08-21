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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.ImmutableCheckpoint;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Covers the "can snap sync still start from scratch?" check. Snap sync refuses to run over a
 * non-empty database, and with checkpoint sync the trusted checkpoint header — which {@link
 * SnapSyncChainDownloader} writes before it persists its chain sync state — must count as empty.
 * Otherwise a crash in that window permanently disables snap sync for the data directory.
 */
@ExtendWith(MockitoExtension.class)
class SnapDownloaderFactoryTest {

  @Mock private Blockchain blockchain;
  @Mock private SyncState syncState;

  private BlockHeader checkpointHeader;
  private Checkpoint checkpoint;

  @BeforeEach
  void setUp() {
    checkpointHeader = new BlockHeaderTestFixture().number(500).buildHeader();
    checkpoint =
        ImmutableCheckpoint.builder()
            .blockNumber(checkpointHeader.getNumber())
            .blockHash(checkpointHeader.getHash())
            .totalDifficulty(Difficulty.ONE)
            .build();
  }

  @Test
  void anEmptyChainIsAtGenesis() {
    when(blockchain.getChainHeadHeader())
        .thenReturn(new BlockHeaderTestFixture().number(0).buildHeader());

    assertThat(SnapDownloaderFactory.holdsNothingButTheTrustAnchor(blockchain, syncState)).isTrue();
  }

  @Test
  void aChainHoldingOnlyTheTrustedCheckpointHeaderCountsAsEmpty() {
    // The state a snap sync leaves behind when it is killed between storing the checkpoint header
    // and persisting its ChainSyncState.
    when(blockchain.getChainHeadHeader()).thenReturn(checkpointHeader);
    when(syncState.getCheckpoint()).thenReturn(Optional.of(checkpoint));
    when(blockchain.getBlockBody(checkpointHeader.getHash())).thenReturn(Optional.empty());

    assertThat(SnapDownloaderFactory.holdsNothingButTheTrustAnchor(blockchain, syncState)).isTrue();
  }

  @Test
  void aChainWithABodyAtTheCheckpointIsNotEmpty() {
    // Same height and hash, but the block was actually imported: this is real data, not the bare
    // checkpoint header, so snap sync must not run over it.
    when(blockchain.getChainHeadHeader()).thenReturn(checkpointHeader);
    when(syncState.getCheckpoint()).thenReturn(Optional.of(checkpoint));
    when(blockchain.getBlockBody(checkpointHeader.getHash()))
        .thenReturn(Optional.of(mock(BlockBody.class)));

    assertThat(SnapDownloaderFactory.holdsNothingButTheTrustAnchor(blockchain, syncState))
        .isFalse();
  }

  @Test
  void aChainHeadAboveTheCheckpointIsNotEmpty() {
    final BlockHeader syncedHead = new BlockHeaderTestFixture().number(900).buildHeader();
    when(blockchain.getChainHeadHeader()).thenReturn(syncedHead);
    when(syncState.getCheckpoint()).thenReturn(Optional.of(checkpoint));
    lenient().when(blockchain.getBlockBody(syncedHead.getHash())).thenReturn(Optional.empty());

    assertThat(SnapDownloaderFactory.holdsNothingButTheTrustAnchor(blockchain, syncState))
        .isFalse();
  }

  @Test
  void aNonEmptyChainWithoutAConfiguredCheckpointIsNotEmpty() {
    when(blockchain.getChainHeadHeader())
        .thenReturn(new BlockHeaderTestFixture().number(900).buildHeader());
    when(syncState.getCheckpoint()).thenReturn(Optional.empty());

    assertThat(SnapDownloaderFactory.holdsNothingButTheTrustAnchor(blockchain, syncState))
        .isFalse();
  }
}
