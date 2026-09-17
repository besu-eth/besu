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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.metrics.noop.NoOpMetricsSystem.NO_OP_COUNTER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.AccountTrieNodeHealingRequest;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.pipeline.Pipe;
import org.hyperledger.besu.services.tasks.Task;

import java.util.HashSet;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class LoadLocalDataStepTest {

  private static final Bytes DATA = Bytes.of(1, 2, 3);
  private static final Hash HASH = Hash.hash(DATA);

  private final BlockHeader blockHeader =
      new BlockHeaderTestFixture().stateRoot(HASH).buildHeader();
  private final AccountTrieNodeHealingRequest request =
      SnapDataRequest.createAccountTrieNodeDataRequest(
          HASH, Bytes.fromHexString("0x01"), new HashSet<>());
  private final Task<SnapDataRequest> task = new StubTask(request);

  private final Pipe<Task<SnapDataRequest>> completedTasks =
      new Pipe<>(10, NO_OP_COUNTER, NO_OP_COUNTER, NO_OP_COUNTER, "test_pipe");

  private final SnapSyncProcessState snapSyncState = mock(SnapSyncProcessState.class);
  private final SnapWorldDownloadState downloadState = mock(SnapWorldDownloadState.class);
  private final WorldStateStorageCoordinator worldStateStorageCoordinator =
      mock(WorldStateStorageCoordinator.class);
  private final WorldStateKeyValueStorage.Updater updater =
      mock(WorldStateKeyValueStorage.Updater.class);

  private final SnapSyncConfiguration snapSyncConfiguration = mock(SnapSyncConfiguration.class);

  private final LoadLocalDataStep loadLocalDataStep =
      new LoadLocalDataStep(
          worldStateStorageCoordinator,
          downloadState,
          snapSyncConfiguration,
          new NoOpMetricsSystem(),
          snapSyncState);

  @BeforeEach
  public void setup() {
    when(snapSyncState.hasPivotBlockHeader()).thenReturn(true);
    when(snapSyncState.getPivotBlockHeader()).thenReturn(Optional.of(blockHeader));
  }

  @Test
  public void shouldReturnStreamWithUnchangedTaskWhenDataNotPresent() {

    final Stream<Task<SnapDataRequest>> output =
        loadLocalDataStep.loadLocalDataTrieNode(task, completedTasks);

    assertThat(completedTasks.poll()).isNull();
    assertThat(output).containsExactly(task);
  }

  @Test
  public void shouldReturnStreamWithSameRootHashTaskWhenDataArePresent() {

    task.getData().setRootHash(blockHeader.getStateRoot());

    when(worldStateStorageCoordinator.getAccountStateTrieNode(any(), any()))
        .thenReturn(Optional.of(DATA));
    when(worldStateStorageCoordinator.updater())
        .thenReturn(mock(WorldStateKeyValueStorage.Updater.class));

    final BlockHeader newBlockHeader =
        new BlockHeaderTestFixture().stateRoot(Hash.EMPTY).buildHeader();
    when(snapSyncState.getPivotBlockHeader()).thenReturn(Optional.of(newBlockHeader));

    loadLocalDataStep.loadLocalDataTrieNode(task, completedTasks);

    assertThat(completedTasks.poll()).isEqualTo(task);
    assertThat(task.getData().getRootHash()).isEqualTo(blockHeader.getStateRoot());
  }

  @Test
  public void shouldReturnEmptyStreamAndSendTaskToCompletedPipeWhenDataIsPresent() {
    when(worldStateStorageCoordinator.getAccountStateTrieNode(any(), any()))
        .thenReturn(Optional.of(DATA));
    when(worldStateStorageCoordinator.updater()).thenReturn(updater);

    final Stream<Task<SnapDataRequest>> output =
        loadLocalDataStep.loadLocalDataTrieNode(task, completedTasks);

    assertThat(completedTasks.poll()).isSameAs(task);
    assertThat(request.isResponseReceived()).isTrue();
    assertThat(output).isEmpty();

    verify(updater).commit();
    Mockito.reset(updater);

    // Should not require persisting.
    request.persist(
        worldStateStorageCoordinator, updater, downloadState, snapSyncState, snapSyncConfiguration);
    verifyNoInteractions(updater);
  }
}
