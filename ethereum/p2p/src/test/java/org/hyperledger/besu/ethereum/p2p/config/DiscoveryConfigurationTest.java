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
package org.hyperledger.besu.ethereum.p2p.config;

import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;

import java.util.Collections;

import org.junit.jupiter.api.Test;

public class DiscoveryConfigurationTest {

  @Test
  public void setEnodeBootnodes_withDiscoveryDisabled() {
    final EnodeURLImpl invalidBootnode =
        EnodeURLImpl.builder()
            .nodeId(Peer.randomId())
            .ipAddress("127.0.0.1")
            .listeningPort(30303)
            .disableDiscovery()
            .build();

    DiscoveryConfiguration config = DiscoveryConfiguration.create();

    assertThatThrownBy(() -> config.setEnodeBootnodes(Collections.singletonList(invalidBootnode)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid bootnodes")
        .hasMessageContaining("Bootnodes must have discovery enabled");
  }

  @Test
  public void setEnodeBootnodes_withListeningDisabled() {
    final EnodeURLImpl invalidBootnode =
        EnodeURLImpl.builder()
            .nodeId(Peer.randomId())
            .ipAddress("127.0.0.1")
            .discoveryAndListeningPorts(0)
            .build();

    DiscoveryConfiguration config = DiscoveryConfiguration.create();

    assertThatThrownBy(() -> config.setEnodeBootnodes(Collections.singletonList(invalidBootnode)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid bootnodes")
        .hasMessageContaining("Bootnodes must have discovery enabled");
  }
}
