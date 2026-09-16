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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.StubTask;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2BytecodeRequest;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldDownloadState;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnapV2CompleteTaskStepTest {

  private static final BlockHeader PIVOT = new BlockHeaderTestFixture().buildHeader();

  private final SnapSyncProcessState snapSyncState = mock(SnapSyncProcessState.class);

  @SuppressWarnings("unchecked")
  private final WorldDownloadState<SnapDataRequest> downloadState = mock(WorldDownloadState.class);

  private final SnapV2CompleteTaskStep completeTaskStep =
      new SnapV2CompleteTaskStep(snapSyncState, new NoOpMetricsSystem());

  @BeforeEach
  void setup() {
    when(snapSyncState.getPivotBlockHash()).thenReturn(Optional.of(PIVOT.getHash()));
    when(snapSyncState.getPivotBlockHeader()).thenReturn(Optional.of(PIVOT));
  }

  @Test
  void completesTaskWithResponseAndChecksCompletion() {
    final SnapV2BytecodeRequest request = codeRequest();
    request.setCode(Bytes.of(1, 2, 3));
    final StubTask task = new StubTask(request);

    completeTaskStep.markAsCompleteOrFailed(downloadState, task);

    assertThat(task.isCompleted()).isTrue();
    assertThat(task.isFailed()).isFalse();
    verify(downloadState).checkCompletion(PIVOT);
    verify(downloadState).notifyTaskAvailable();
  }

  @Test
  void failsTaskWithoutResponse() {
    final StubTask task = new StubTask(codeRequest());

    completeTaskStep.markAsCompleteOrFailed(downloadState, task);

    assertThat(task.isCompleted()).isFalse();
    assertThat(task.isFailed()).isTrue();
    verify(downloadState, never()).checkCompletion(PIVOT);
    verify(downloadState).notifyTaskAvailable();
  }

  @Test
  void rejectsRequestBoundToAnotherPivot() {
    when(snapSyncState.getPivotBlockHash()).thenReturn(Optional.of(Hash.hash(Bytes.of(9, 9, 9))));

    final StubTask task = new StubTask(codeRequest());

    assertThatThrownBy(() -> completeTaskStep.markAsCompleteOrFailed(downloadState, task))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Expired snap/2 request");
    assertThat(task.isCompleted()).isFalse();
    assertThat(task.isFailed()).isFalse();
  }

  private static SnapV2BytecodeRequest codeRequest() {
    return new SnapV2BytecodeRequest(
        PIVOT, Bytes32.fromHexString("0xaa"), Bytes32.fromHexString("0xbb"), Bytes32.ZERO);
  }
}
