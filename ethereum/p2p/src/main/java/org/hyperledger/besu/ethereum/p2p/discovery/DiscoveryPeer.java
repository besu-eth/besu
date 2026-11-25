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
package org.hyperledger.besu.ethereum.p2p.discovery;

import org.hyperledger.besu.ethereum.forkid.ForkId;
import org.hyperledger.besu.ethereum.p2p.peers.DefaultPeer;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;

import java.util.Optional;

import org.ethereum.beacon.discovery.schema.NodeRecord;

public class DiscoveryPeer extends DefaultPeer {
  private long lastAttemptedConnection = 0;
  // The local-node DiscoveryPeer is shared across the DiscV4/DiscV5 threads via NodeRecordManager;
  // volatile gives readers the JMM visibility guarantee for writes made under its lock.
  private volatile NodeRecord nodeRecord;
  private volatile Optional<ForkId> forkId = Optional.empty();
  private volatile boolean forkIdMalformed = false;

  protected DiscoveryPeer(final EnodeURLImpl enodeURL) {
    super(enodeURL);
  }

  public long getLastAttemptedConnection() {
    return lastAttemptedConnection;
  }

  public void setLastAttemptedConnection(final long lastAttemptedConnection) {
    this.lastAttemptedConnection = lastAttemptedConnection;
  }

  @Override
  public Optional<NodeRecord> getNodeRecord() {
    return Optional.ofNullable(nodeRecord);
  }

  public void setNodeRecord(final NodeRecord nodeRecord) {
    this.nodeRecord = nodeRecord;
    final Object rawForkId = nodeRecord.get("eth");
    this.forkId = ForkId.fromRawForkId(rawForkId);
    this.forkIdMalformed = rawForkId != null && forkId.isEmpty();
  }

  @Override
  public Optional<ForkId> getForkId() {
    return this.forkId;
  }

  @Override
  public void setForkId(final ForkId forkId) {
    this.forkId = Optional.ofNullable(forkId);
    this.forkIdMalformed = false;
  }

  /**
   * Indicates whether the peer's node record has an {@code eth} entry that doesn't decode to a fork
   * ID. A peer with no {@code eth} entry may be on any chain, but a peer whose entry is malformed
   * is treated as incompatible, as geth's discovery filter does.
   *
   * @return true if the {@code eth} entry is present but can't be decoded
   */
  public boolean hasMalformedForkId() {
    return forkIdMalformed;
  }

  /**
   * Indicates whether the peer is ready to accept RLPx connections.
   *
   * @return true if the peer is ready for connections, false otherwise
   */
  public boolean isReadyForConnections() {
    return isListening();
  }

  public boolean isListening() {
    return getEnodeURL().isListening();
  }
}
