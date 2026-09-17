/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.p2p.peers;

import org.hyperledger.besu.crypto.SecureRandomProvider;
import org.hyperledger.besu.ethereum.forkid.ForkId;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.ethereum.beacon.discovery.schema.NodeRecord;

public interface Peer extends org.hyperledger.besu.plugin.data.p2p.Peer, PeerId {

  /**
   * ENode URL of this peer.
   *
   * @return The enode representing the location of this peer.
   */
  @Override
  EnodeURLImpl getEnodeURL();

  /**
   * Generates a random peer ID in a secure manner.
   *
   * @return The generated peer ID.
   */
  static Bytes randomId() {
    final byte[] id = new byte[EnodeURLImpl.NODE_ID_SIZE];
    SecureRandomProvider.publicSecureRandom().nextBytes(id);
    return Bytes.wrap(id);
  }

  /**
   * Returns this peer's enode URL.
   *
   * @return The enode URL as a String.
   */
  default String getEnodeURLString() {
    return this.getEnodeURL().toString();
  }

  /**
   * Returns the node record
   *
   * @return The node record wrapped in an Optional
   */
  default Optional<NodeRecord> getNodeRecord() {
    return Optional.empty();
  }

  /**
   * Returns the fork id
   *
   * @return The fork id wrapped in an Optional
   */
  default Optional<ForkId> getForkId() {
    return Optional.empty();
  }

  /**
   * Sets the fork id
   *
   * @param forkId The fork id
   */
  void setForkId(ForkId forkId);
}
