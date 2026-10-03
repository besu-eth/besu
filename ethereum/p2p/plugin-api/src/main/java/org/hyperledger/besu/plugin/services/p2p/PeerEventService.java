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
package org.hyperledger.besu.plugin.services.p2p;

import org.hyperledger.besu.plugin.StartService;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.data.p2p.Capability;

/**
 * Subscriptions to peer-to-peer network events: peers connecting, peers disconnecting, and messages
 * received on a capability.
 *
 * <p>This is a start-phase service, separate from {@link P2PService}, because a subscription has to
 * be in place before the network starts connecting to peers, and the network starts with the main
 * loop. Querying peers, sending messages and controlling the network is {@link P2PService},
 * available once the node is running.
 */
@Unstable
public interface PeerEventService extends StartService {

  /**
   * Subscribe to connection events.
   *
   * @param networkSubscriber the subscriber to receive connection events
   */
  void subscribeConnect(P2PService.ConnectionListener networkSubscriber);

  /**
   * Subscribe to disconnection events.
   *
   * @param networkSubscriber the subscriber to receive disconnection events
   */
  void subscribeDisconnect(P2PService.DisconnectionListener networkSubscriber);

  /**
   * Subscribe to messages on a specific capability.
   *
   * @param capability the capability to subscribe to
   * @param networkSubscriber the subscriber to receive messages for the specified capability
   */
  void subscribeMessage(
      final Capability capability, final P2PService.MessageListener networkSubscriber);
}
