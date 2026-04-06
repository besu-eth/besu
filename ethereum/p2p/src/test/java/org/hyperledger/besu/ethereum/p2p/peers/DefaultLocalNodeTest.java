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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.p2p.EthProtocolHelper;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerInfo;

import java.util.Arrays;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class DefaultLocalNodeTest {
  private final String clientId = "test client";
  private final int p2pVersion = 5;
  private final List<Capability> supportedCapabilities = Arrays.asList(EthProtocolHelper.LATEST);
  private final Bytes nodeId = Bytes.of(new byte[64]);
  private final int port = 30303;
  private final EnodeURLImpl enode =
      EnodeURLImpl.builder()
          .ipAddress("127.0.0.1")
          .discoveryAndListeningPorts(port)
          .nodeId(nodeId)
          .build();
  private final PeerInfo peerInfo =
      new PeerInfo(p2pVersion, clientId, supportedCapabilities, port, nodeId);
  private final Peer peer = DefaultPeer.fromEnodeURL(enode);

  @Test
  public void create() {
    final LocalNode localNode = createLocalNode();
    assertThat(localNode.isReady()).isFalse();
    assertThatThrownBy(localNode::getPeerInfo).isInstanceOf(LocalNode.NodeNotReadyException.class);
    assertThatThrownBy(localNode::getPeer).isInstanceOf(LocalNode.NodeNotReadyException.class);
  }

  @Test
  public void setEnode() {
    final MutableLocalNode localNode = createLocalNode();
    localNode.setEnode(enode);

    assertThat(localNode.isReady()).isTrue();
    validateReadyNode(localNode);

    // Verify we can't set the enode a second time
    assertThatThrownBy(() -> localNode.setEnode(enode))
        .isInstanceOf(MutableLocalNode.NodeAlreadySetException.class);
  }

  private MutableLocalNode createLocalNode() {
    return DefaultLocalNode.create(clientId, p2pVersion, supportedCapabilities);
  }

  private void validateReadyNode(final LocalNode localNode) {
    Assertions.assertThat(localNode.getPeerInfo()).isEqualTo(peerInfo);
    assertThat(localNode.getPeer()).isEqualTo(peer);
  }
}
