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

import org.hyperledger.besu.ethereum.p2p.EthProtocolHelper;

import java.util.Arrays;

import org.apache.tuweni.bytes.Bytes;

public class PeerTestHelper {

  public static Peer createPeer() {
    return DefaultPeer.fromEnodeURL(enode());
  }

  public static Peer createPeer(final EnodeURLImpl enodeURL) {
    return DefaultPeer.fromEnodeURL(enodeURL);
  }

  public static Peer createPeer(final Bytes nodeId) {
    return DefaultPeer.fromEnodeURL(enodeBuilder().nodeId(nodeId).build());
  }

  public static EnodeURLImpl enode() {
    return enodeBuilder().build();
  }

  public static EnodeURLImpl.Builder enodeBuilder() {
    return EnodeURLImpl.builder().ipAddress("127.0.0.1").useDefaultPorts().nodeId(Peer.randomId());
  }

  /**
   * @return A LocalNode that is setup and ready.
   */
  public static LocalNode createLocalNode() {
    final MutableLocalNode localNode = createMutableLocalNode();
    localNode.setEnode(enode());
    return localNode;
  }

  /**
   * @return A MutableLocalNode that is not ready (the enode has not been set).
   */
  public static MutableLocalNode createMutableLocalNode() {
    return MutableLocalNode.create("clientId", 5, Arrays.asList(EthProtocolHelper.LATEST));
  }
}
