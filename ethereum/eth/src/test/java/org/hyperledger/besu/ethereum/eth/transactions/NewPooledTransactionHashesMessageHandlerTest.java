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
package org.hyperledger.besu.ethereum.eth.transactions;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthMessage;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NewPooledTransactionHashesMessageHandlerTest {

  @Mock private NewPooledTransactionHashesMessageProcessor processor;
  @Mock private EthPeer peer;
  @Mock private PeerConnection peerConnection;

  private NewPooledTransactionHashesMessageHandler handler;

  @BeforeEach
  void setUp() {
    final DeterministicEthScheduler scheduler = new DeterministicEthScheduler();
    handler = new NewPooledTransactionHashesMessageHandler(scheduler, processor, 300);
    handler.setEnabled();
    when(peer.getConnection()).thenReturn(peerConnection);
    when(peerConnection.capability(EthProtocol.NAME)).thenReturn(EthProtocol.ETH68);
  }

  @Test
  void disconnectsPeerOnDecompressionFailure() {
    // Create a RawMessage with invalid compressed data that will throw FramingException
    final RawMessage badMessage = new RawMessage(0x08, new byte[] {0x01, 0x02, 0x03});
    final EthMessage ethMessage = new EthMessage(peer, badMessage);

    handler.exec(ethMessage);

    verify(peer).disconnect(eq(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED));
    verifyNoInteractions(processor);
  }
}
