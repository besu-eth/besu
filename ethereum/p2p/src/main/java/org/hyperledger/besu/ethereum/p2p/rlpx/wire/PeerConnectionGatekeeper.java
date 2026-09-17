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
package org.hyperledger.besu.ethereum.p2p.rlpx.wire;

import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;

import java.util.Optional;

/**
 * Decides whether a peer connection attempt should be allowed or rejected.
 *
 * <p>The return value uses the following convention:
 *
 * <ul>
 *   <li>{@code Optional.empty()} &mdash; allow the connection to proceed.
 *   <li>{@code Optional.of(reason)} &mdash; reject the connection. For inbound connection attempts
 *       ({@code incoming == true}), a disconnect message will be sent to the remote peer with the
 *       provided {@link DisconnectMessage.DisconnectReason} before the session is closed. For
 *       outbound connection attempts ({@code incoming == false}), the connection will not be
 *       established and no disconnect message is sent because no RLPx session exists yet.
 * </ul>
 */
@FunctionalInterface
public interface PeerConnectionGatekeeper {

  /**
   * Check whether the connection with the given peer should be allowed.
   *
   * @param peer the remote peer for which a connection is being considered
   * @param incoming {@code true} if this is an inbound connection attempt initiated by the remote
   *     peer, {@code false} if this node is attempting to establish an outbound connection
   * @return {@code Optional.empty()} to allow the connection, or an {@code Optional} containing a
   *     {@link DisconnectMessage.DisconnectReason} to reject it
   */
  Optional<DisconnectMessage.DisconnectReason> checkPeerConnection(
      final Peer peer, final boolean incoming);
}
