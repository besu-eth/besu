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
package org.hyperledger.besu.services;

import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.plugin.services.p2p.P2PService.ConnectionListener;
import org.hyperledger.besu.plugin.services.p2p.P2PService.DisconnectionListener;
import org.hyperledger.besu.plugin.services.p2p.P2PService.MessageListener;
import org.hyperledger.besu.plugin.services.p2p.PeerEventService;

/** Delivers the peer-to-peer network's events to plugins. */
public class PeerEventServiceImpl implements PeerEventService {

  private final P2PNetwork p2PNetwork;

  /**
   * Creates the service.
   *
   * @param p2PNetwork the network whose events are delivered
   */
  public PeerEventServiceImpl(final P2PNetwork p2PNetwork) {
    this.p2PNetwork = p2PNetwork;
  }

  @Override
  public void subscribeConnect(final ConnectionListener connectionListener) {
    p2PNetwork.subscribeConnect(connectionListener::onConnect);
  }

  @Override
  public void subscribeDisconnect(final DisconnectionListener networkSubscriber) {
    p2PNetwork.subscribeDisconnect(
        (peerConnection, disconnectReason, initiatedByPeer) ->
            networkSubscriber.onDisconnect(
                peerConnection,
                disconnectReason.getCode(),
                disconnectReason.getMessage(),
                initiatedByPeer));
  }

  @Override
  public void subscribeMessage(
      final org.hyperledger.besu.plugin.data.p2p.Capability capability,
      final MessageListener networkSubscriber) {
    final Capability wireCap = Capability.create(capability.getName(), capability.getVersion());
    p2PNetwork.subscribe(wireCap, networkSubscriber::onMessage);
  }
}
