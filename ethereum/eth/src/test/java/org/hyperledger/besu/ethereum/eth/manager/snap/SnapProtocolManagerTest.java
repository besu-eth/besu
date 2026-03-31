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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.MockPeerConnection;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.DefaultMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;

import java.util.Collections;
import java.util.HashSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SnapProtocolManagerTest {

  @Mock private WorldStateStorageCoordinator worldStateStorageCoordinator;
  @Mock private SnapSyncConfiguration snapConfig;
  @Mock private EthPeers ethPeers;
  @Mock private EthMessages snapMessages;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolContext protocolContext;
  @Mock private Synchronizer synchronizer;
  @Mock private EthScheduler ethScheduler;
  @Mock private EthPeer ethPeer;

  private SnapProtocolManager snapProtocolManager;

  @BeforeEach
  void setUp() {
    when(snapConfig.isSnapServerEnabled()).thenReturn(false);
    snapProtocolManager =
        new SnapProtocolManager(
            worldStateStorageCoordinator,
            snapConfig,
            ethPeers,
            snapMessages,
            ethScheduler,
            protocolSchedule,
            protocolContext,
            synchronizer);
  }

  @Test
  void disconnectsPeerOnDecompressionFailure() {
    final MockPeerConnection peerConnection =
        new MockPeerConnection(
            new HashSet<>(Collections.singletonList(SnapProtocol.SNAP1)), (cap, msg, conn) -> {});
    when(ethPeers.peer(peerConnection)).thenReturn(ethPeer);
    when(ethPeer.validateReceivedMessage(any(), any())).thenReturn(true);

    // Create a RawMessage with invalid compressed data that will throw FramingException
    final RawMessage badMessage = new RawMessage(0x00, new byte[] {0x01, 0x02, 0x03});
    snapProtocolManager.processMessage(
        SnapProtocol.SNAP1, new DefaultMessage(peerConnection, badMessage));

    assertThat(peerConnection.isDisconnected()).isFalse();
    // ethPeer (mock) receives the disconnect call
    org.mockito.Mockito.verify(ethPeer)
        .disconnect(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);
  }
}
