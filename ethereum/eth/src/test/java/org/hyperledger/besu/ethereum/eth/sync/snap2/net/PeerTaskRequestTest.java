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
package org.hyperledger.besu.ethereum.eth.sync.snap2.net;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskValidationResponse;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncBlockBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.eth.messages.BlockBodiesMessage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

class PeerTaskRequestTest {

  // a peer that pruned the bodies answers with an empty list, which is no protocol breach
  @Test
  void treatsAnEmptyBodiesAnswerAsEmpty() {
    final BlockHeader header = new BlockHeaderTestFixture().number(1).buildHeader();
    final PeerTaskRequest<List<SyncBlock>> request =
        new PeerTaskRequest<>(
            () ->
                new GetSyncBlockBodiesFromPeerTask(
                    List.of(header), ProtocolScheduleFixture.MAINNET),
            Priority.HEADERS,
            "bodies");
    final EthPeer peer = mock(EthPeer.class);
    when(peer.getAgreedCapabilities()).thenReturn(Set.of(EthProtocol.LATEST));
    request.message(peer);

    assertThatThrownBy(() -> request.decode(BlockBodiesMessage.create(List.of()), peer))
        .isInstanceOf(SyncRequest.EmptyResponseException.class);
  }

  @Test
  void treatsAResultWithNothingInItAsEmpty() throws Exception {
    final PeerTaskRequest<String> request =
        requestValidatedAs(PeerTaskValidationResponse.NO_RESULTS_RETURNED);

    assertThatThrownBy(() -> request.decode(mock(MessageData.class), peer()))
        .isInstanceOf(SyncRequest.EmptyResponseException.class);
  }

  @Test
  void rejectsAResultTheTaskFindsWrongWithItsDisconnectReason() throws Exception {
    final PeerTaskRequest<String> request =
        requestValidatedAs(PeerTaskValidationResponse.NON_SEQUENTIAL_HEADERS_RETURNED);

    assertThatThrownBy(() -> request.decode(mock(MessageData.class), peer()))
        .isInstanceOfSatisfying(
            SyncRequest.InvalidResponseException.class,
            e ->
                assertThat(e.disconnectReason())
                    .contains(DisconnectReason.BREACH_OF_PROTOCOL_NON_SEQUENTIAL_HEADERS));
  }

  @Test
  @SuppressWarnings("unchecked")
  void refusesATaskOfAnotherProtocol() {
    final PeerTask<String> task = mock(PeerTask.class);
    when(task.getSubProtocol()).thenReturn(SnapProtocol.get());

    assertThatThrownBy(() -> new PeerTaskRequest<>(() -> task, Priority.HEADERS, "task"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // a task whose response decodes to a result the task then validates as given
  @SuppressWarnings("unchecked")
  private static PeerTaskRequest<String> requestValidatedAs(
      final PeerTaskValidationResponse validation) throws Exception {
    final PeerTask<String> task = mock(PeerTask.class);
    when(task.getSubProtocol()).thenReturn(EthProtocol.get());
    when(task.processResponse(any(), any())).thenReturn("result");
    when(task.validateResult("result")).thenReturn(validation);
    final PeerTaskRequest<String> request =
        new PeerTaskRequest<>(() -> task, Priority.HEADERS, "task");
    request.message(peer());
    return request;
  }

  private static EthPeer peer() {
    final EthPeer peer = mock(EthPeer.class);
    when(peer.getAgreedCapabilities()).thenReturn(Set.of(EthProtocol.LATEST));
    return peer;
  }
}
