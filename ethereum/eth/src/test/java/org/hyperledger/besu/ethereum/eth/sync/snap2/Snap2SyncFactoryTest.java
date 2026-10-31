/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.SyncFixture.checkpoint;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.SNAPSYNC_FLAT_ACCOUNTS;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.SNAPSYNC_FLAT_STORAGE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.PivotBlockSelector;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncController;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Snap2SyncFactoryTest {

  private static final BlockHeader GENESIS = header(0);
  private static final BlockHeader CHECKPOINT = header(10);

  @TempDir Path dataDirectory;

  private final SyncState syncState = mock(SyncState.class);
  private final MutableBlockchain blockchain = mock(MutableBlockchain.class);
  private final WorldStateStorageCoordinator worldState = mock(WorldStateStorageCoordinator.class);
  private final StorageProvider storageProvider = mock(StorageProvider.class);

  @BeforeEach
  void aNewNode() {
    head(GENESIS);
  }

  @Test
  void syncsANodeThatHoldsNothingButGenesisOrTheCheckpointHeader() {
    when(syncState.getCheckpoint()).thenReturn(Optional.of(checkpoint(10, CHECKPOINT.getHash())));
    assertThat(needsSync()).isTrue();

    head(CHECKPOINT);

    assertThat(needsSync()).isTrue();
  }

  @Test
  void keepsTheChainOfANodeBelowTheCheckpoint() {
    when(syncState.getCheckpoint()).thenReturn(Optional.of(checkpoint(10, CHECKPOINT.getHash())));
    // down while a release raised the checkpoint
    head(header(5));
    assertThat(needsSync()).isFalse();

    // the checkpoint with its body, imported
    head(CHECKPOINT);
    when(blockchain.getBlockBody(CHECKPOINT.getHash())).thenReturn(Optional.of(BlockBody.empty()));

    assertThat(needsSync()).isFalse();
  }

  @Test
  void syncsAgainWhenASyncDidNotFinish() throws IOException {
    head(header(11));
    Files.createFile(marker());

    assertThat(needsSync()).isTrue();
  }

  @Test
  void syncsTheStateAgainOnAResync() {
    head(header(11));
    when(syncState.isResyncNeeded()).thenReturn(true);

    assertThat(needsSync()).isTrue();
  }

  @Test
  void createsNoSyncOnASyncedChain() {
    head(header(1));

    assertThat(create()).isEmpty();
  }

  @Test
  void stagesTheFlatStateOfASyncOnForestInItsOwnSegments() {
    when(worldState.getDataStorageFormat()).thenReturn(DataStorageFormat.FOREST);

    assertThat(create()).isPresent();
    verify(storageProvider)
        .getStorageBySegmentIdentifiers(List.of(SNAPSYNC_FLAT_ACCOUNTS, SNAPSYNC_FLAT_STORAGE));
  }

  @Test
  void refusesAnArchiveDatabase() {
    when(worldState.getDataStorageFormat()).thenReturn(DataStorageFormat.X_BONSAI_ARCHIVE);

    assertThatThrownBy(this::create)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("X_BONSAI_ARCHIVE");
  }

  private Optional<SnapSyncController> create() {
    final ProtocolContext protocolContext = mock(ProtocolContext.class);
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(syncState.getCheckpoint()).thenReturn(Optional.empty());
    return Snap2SyncFactory.create(
        SynchronizerConfiguration.builder().build(),
        dataDirectory,
        ProtocolScheduleFixture.MAINNET,
        protocolContext,
        new NoOpMetricsSystem(),
        mock(EthPeers.class),
        worldState,
        storageProvider,
        syncState,
        mock(PivotBlockSelector.class),
        Optional.empty(),
        scheduler -> {});
  }

  private boolean needsSync() {
    return Snap2SyncFactory.needsSync(marker(), syncState, blockchain);
  }

  private void head(final BlockHeader header) {
    when(blockchain.getChainHeadHeader()).thenReturn(header);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(header.getNumber());
  }

  private static BlockHeader header(final long number) {
    return new BlockHeaderTestFixture().number(number).buildHeader();
  }

  private Path marker() {
    return Snap2SyncController.markerFile(dataDirectory);
  }
}
