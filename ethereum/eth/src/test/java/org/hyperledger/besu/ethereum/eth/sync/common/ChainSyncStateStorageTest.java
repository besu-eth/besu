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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ChainSyncStateStorageTest {
  @TempDir private Path tempDir;

  private ChainSyncStateStorage storage;
  private BlockHeader pivotBlockHeader;
  private BlockHeader checkpointBlockHeader;
  private BlockHeader headerDownloadAnchor;

  @BeforeEach
  public void setUp() {
    storage = new ChainSyncStateStorage(tempDir);
    pivotBlockHeader = new BlockHeaderTestFixture().number(1000).buildHeader();
    checkpointBlockHeader = new BlockHeaderTestFixture().number(500).buildHeader();
    headerDownloadAnchor = new BlockHeaderTestFixture().number(600).buildHeader();
  }

  @Test
  public void shouldReturnNullWhenNoStateFileExists() {
    assertThat(
            storage.loadState(rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions())))
        .isNull();
  }

  @Test
  public void shouldStoreAndLoadStateWithNullHeaderDownloadAnchor() {
    final ChainSyncState state =
        new ChainSyncState(
            pivotBlockHeader, pivotBlockHeader, checkpointBlockHeader, null, false, null);

    storage.storeState(state);

    final ChainSyncState loadedState =
        storage.loadState(rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions()));

    assertThat(loadedState).isNotNull();
    assertThat(loadedState.firstPivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(loadedState.pivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(loadedState.blockDownloadAnchor()).isEqualTo(checkpointBlockHeader);
    assertThat(loadedState.headerDownloadAnchor()).isNull();
    assertThat(loadedState.headersDownloadComplete()).isFalse();
  }

  @Test
  public void shouldStoreAndLoadStateWithNonNullHeaderDownloadAnchor() {
    final ChainSyncState state =
        new ChainSyncState(
            pivotBlockHeader,
            pivotBlockHeader,
            checkpointBlockHeader,
            headerDownloadAnchor,
            false,
            null);

    storage.storeState(state);

    final ChainSyncState loadedState =
        storage.loadState(rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions()));

    assertThat(loadedState).isNotNull();
    assertThat(loadedState.firstPivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(loadedState.pivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(loadedState.blockDownloadAnchor()).isEqualTo(checkpointBlockHeader);
    assertThat(loadedState.headerDownloadAnchor()).isEqualTo(headerDownloadAnchor);
    assertThat(loadedState.headersDownloadComplete()).isFalse();
  }

  @Test
  public void shouldStoreAndLoadStateWithHeadersDownloadCompleteTrue() {
    final ChainSyncState state =
        new ChainSyncState(
            pivotBlockHeader, pivotBlockHeader, checkpointBlockHeader, null, true, null);

    storage.storeState(state);

    final ChainSyncState loadedState =
        storage.loadState(rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions()));

    assertThat(loadedState).isNotNull();
    assertThat(loadedState.firstPivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(loadedState.pivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(loadedState.blockDownloadAnchor()).isEqualTo(checkpointBlockHeader);
    assertThat(loadedState.headerDownloadAnchor()).isNull();
    assertThat(loadedState.headersDownloadComplete()).isTrue();
  }

  @Test
  public void shouldStoreAndLoadStateWithHeadersDownloadCompleteFalse() {
    final ChainSyncState state =
        new ChainSyncState(
            pivotBlockHeader,
            pivotBlockHeader,
            checkpointBlockHeader,
            headerDownloadAnchor,
            false,
            null);

    storage.storeState(state);

    final ChainSyncState loadedState =
        storage.loadState(rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions()));

    assertThat(loadedState).isNotNull();
    assertThat(loadedState.headersDownloadComplete()).isFalse();
  }

  @Test
  public void shouldRoundTripStateWithNewStorageInstance() {
    final ChainSyncState state =
        new ChainSyncState(
            pivotBlockHeader,
            pivotBlockHeader,
            checkpointBlockHeader,
            headerDownloadAnchor,
            true,
            null);

    storage.storeState(state);

    // Create a new storage instance pointing to the same directory
    final ChainSyncStateStorage newStorage = new ChainSyncStateStorage(tempDir);
    final ChainSyncState loadedState =
        newStorage.loadState(rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions()));

    assertThat(loadedState).isNotNull();
    assertThat(loadedState.firstPivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(loadedState.pivotBlockHeader()).isEqualTo(pivotBlockHeader);
    assertThat(loadedState.blockDownloadAnchor()).isEqualTo(checkpointBlockHeader);
    assertThat(loadedState.headerDownloadAnchor()).isEqualTo(headerDownloadAnchor);
    assertThat(loadedState.headersDownloadComplete()).isTrue();
  }

  @Test
  public void shouldHandleWrongFormatVersion() throws IOException {
    // Create a file with an unsupported version
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    output.startList();
    output.writeByte((byte) 99); // Use version 99 which is not supported
    pivotBlockHeader.writeTo(output);
    checkpointBlockHeader.writeTo(output);
    output.writeByte((byte) 0); // headersDownloadComplete = false
    output.endList();

    Files.write(tempDir.resolve("chain-sync-state.rlp"), output.encoded().toArrayUnsafe());

    // Load should return null for wrong version
    final ChainSyncState loadedState =
        storage.loadState(rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions()));
    assertThat(loadedState).isNull();
  }

  @Test
  public void shouldDeleteStateFiles() {
    final ChainSyncState state =
        new ChainSyncState(
            pivotBlockHeader,
            pivotBlockHeader,
            checkpointBlockHeader,
            headerDownloadAnchor,
            false,
            null);

    storage.storeState(state);

    // Verify state file exists
    assertThat(tempDir.resolve("chain-sync-state.rlp").toFile().exists()).isTrue();

    // Delete the state
    storage.deleteState();

    // Verify state file is deleted
    assertThat(tempDir.resolve("chain-sync-state.rlp").toFile().exists()).isFalse();

    // Load should return null after deletion
    assertThat(
            storage.loadState(rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions())))
        .isNull();
  }

  @Test
  public void shouldDeleteBothStateAndTempFiles() throws IOException {
    final ChainSyncState state =
        new ChainSyncState(
            pivotBlockHeader, pivotBlockHeader, checkpointBlockHeader, null, false, null);

    storage.storeState(state);

    // Manually create a temp file to simulate interrupted write
    final Path tempFile = tempDir.resolve("chain-sync-state.rlp.tmp");
    Files.write(tempFile, new byte[] {1, 2, 3});

    // Verify both files exist
    assertThat(tempDir.resolve("chain-sync-state.rlp").toFile().exists()).isTrue();
    assertThat(tempFile.toFile().exists()).isTrue();

    // Delete the state
    storage.deleteState();

    // Verify both files are deleted
    assertThat(tempDir.resolve("chain-sync-state.rlp").toFile().exists()).isFalse();
    assertThat(tempFile.toFile().exists()).isFalse();
  }

  @Test
  public void shouldCleanupLeftoverTempFileOnStore() throws IOException {
    // Manually create a leftover temp file
    final Path tempFile = tempDir.resolve("chain-sync-state.rlp.tmp");
    Files.write(tempFile, new byte[] {1, 2, 3});
    assertThat(tempFile.toFile().exists()).isTrue();

    // Store a new state (should clean up the temp file)
    final ChainSyncState state =
        new ChainSyncState(
            pivotBlockHeader, pivotBlockHeader, checkpointBlockHeader, null, false, null);
    storage.storeState(state);

    // Verify the state file exists and temp file was cleaned up
    assertThat(tempDir.resolve("chain-sync-state.rlp").toFile().exists()).isTrue();
    // After successful store, temp file should not exist
    assertThat(tempFile.toFile().exists()).isFalse();
  }

  @Test
  public void shouldThrowExceptionWhenLoadingCorruptedFile() throws IOException {
    // Create a corrupted file
    Files.write(tempDir.resolve("chain-sync-state.rlp"), new byte[] {1, 2, 3, 4, 5});

    // Load should throw RLPException when trying to parse corrupted data
    assertThatThrownBy(
            () ->
                storage.loadState(
                    rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions())))
        .isInstanceOf(org.hyperledger.besu.ethereum.rlp.RLPException.class);
  }

  @Test
  public void shouldOverwriteExistingStateFile() {
    final ChainSyncState state1 =
        new ChainSyncState(
            pivotBlockHeader, pivotBlockHeader, checkpointBlockHeader, null, false, null);
    storage.storeState(state1);

    final ChainSyncState loadedState1 =
        storage.loadState(rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions()));
    assertThat(loadedState1.headersDownloadComplete()).isFalse();

    // Store a new state with different values
    final ChainSyncState state2 =
        new ChainSyncState(
            pivotBlockHeader,
            pivotBlockHeader,
            checkpointBlockHeader,
            headerDownloadAnchor,
            true,
            null);
    storage.storeState(state2);

    final ChainSyncState loadedState2 =
        storage.loadState(rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions()));
    assertThat(loadedState2.headersDownloadComplete()).isTrue();
    assertThat(loadedState2.headerDownloadAnchor()).isEqualTo(headerDownloadAnchor);
  }

  @Test
  public void shouldHandleCompleteStateLifecycle() {
    // 1. Initially no state exists
    assertThat(
            storage.loadState(rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions())))
        .isNull();

    // 2. Store initial sync state
    final ChainSyncState initialState =
        ChainSyncState.initialSync(pivotBlockHeader, checkpointBlockHeader, headerDownloadAnchor);
    storage.storeState(initialState);

    ChainSyncState loaded =
        storage.loadState(rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions()));
    assertThat(loaded.headersDownloadComplete()).isFalse();
    assertThat(loaded.headerDownloadAnchor()).isEqualTo(headerDownloadAnchor);

    // 3. Update to headers complete
    final ChainSyncState updatedState = loaded.withHeadersDownloadComplete();
    storage.storeState(updatedState);

    loaded = storage.loadState(rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions()));
    assertThat(loaded.headersDownloadComplete()).isTrue();
    assertThat(loaded.headerDownloadAnchor()).isNull();

    // 4. Delete the state
    storage.deleteState();

    assertThat(
            storage.loadState(rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions())))
        .isNull();
  }

  @Test
  public void shouldRoundTripAllCombinationsOfOptionalHeaders() {
    final BlockHeader progressHeader = new BlockHeaderTestFixture().number(800).buildHeader();

    // Case 1: both anchor and progress set
    assertRoundTrip(
        new ChainSyncState(
            pivotBlockHeader,
            pivotBlockHeader,
            checkpointBlockHeader,
            headerDownloadAnchor,
            false,
            progressHeader),
        headerDownloadAnchor,
        progressHeader);

    // Case 2: anchor set, progress null
    assertRoundTrip(
        new ChainSyncState(
            pivotBlockHeader,
            pivotBlockHeader,
            checkpointBlockHeader,
            headerDownloadAnchor,
            false,
            null),
        headerDownloadAnchor,
        null);

    // Case 3: anchor null, progress set (continuation round with saved progress)
    assertRoundTrip(
        new ChainSyncState(
            pivotBlockHeader, pivotBlockHeader, checkpointBlockHeader, null, false, progressHeader),
        null,
        progressHeader);

    // Case 4: both null
    assertRoundTrip(
        new ChainSyncState(
            pivotBlockHeader, pivotBlockHeader, checkpointBlockHeader, null, true, null),
        null,
        null);
  }

  private void assertRoundTrip(
      final ChainSyncState state,
      final BlockHeader expectedAnchor,
      final BlockHeader expectedProgress) {
    storage.storeState(state);
    final ChainSyncState loaded =
        storage.loadState(rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions()));
    assertThat(loaded).isNotNull();
    assertThat(loaded.headerDownloadAnchor()).isEqualTo(expectedAnchor);
    assertThat(loaded.headerDownloadProgress()).isEqualTo(expectedProgress);
    assertThat(loaded.headersDownloadComplete()).isEqualTo(state.headersDownloadComplete());
  }
}
