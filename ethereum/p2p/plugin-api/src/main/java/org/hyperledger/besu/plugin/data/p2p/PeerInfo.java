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
package org.hyperledger.besu.plugin.data.p2p;

import org.hyperledger.besu.datatypes.Address;

import org.apache.tuweni.bytes.Bytes;

/** Peer information for plugin use. */
public interface PeerInfo {

  /**
   * Get the Ethereum address of this peer, derived from its node public key. This is not a network
   * address; see {@link PeerConnection#getRemoteAddress()} for the socket address.
   *
   * @return the Ethereum address derived from the peer's node public key
   */
  Address getAddress();

  /**
   * Get the node ID of this peer.
   *
   * @return node ID
   */
  Bytes getNodeId();

  /**
   * Get the client ID of this peer.
   *
   * @return client ID
   */
  String getClientId();
}
