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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.EthProtocolVersion;
import org.hyperledger.besu.ethereum.eth.SnapProtocolVersion;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.PeerRequest;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Map;
import java.util.Set;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class GetTrieNodeFromPeerTaskTest {

  private final EthPeers ethPeers = mock(EthPeers.class);
  private PeerRequest request;

  @BeforeEach
  void sendRequest() {
    final EthContext ethContext = mock(EthContext.class);
    when(ethContext.getEthPeers()).thenReturn(ethPeers);
    GetTrieNodeFromPeerTask.forTrieNodes(
            ethContext,
            Map.of(),
            new BlockHeaderTestFixture().buildHeader(),
            new NoOpMetricsSystem())
        .sendRequest();

    final ArgumentCaptor<PeerRequest> captor = ArgumentCaptor.forClass(PeerRequest.class);
    verify(ethPeers).executePeerRequest(captor.capture(), anyLong(), any());
    request = captor.getValue();
  }

  @Test
  void sendsTheRequestToAPeerThatAgreedOnSnap1() {
    assertThat(request.isEthPeerSuitable(peer(true, SnapProtocolVersion.V1.getCapability())))
        .isTrue();
  }

  @Test
  void neverSendsTheRequestToAPeerThatAgreedOnSnap2() {
    assertThat(request.isEthPeerSuitable(peer(true, SnapProtocolVersion.V2.getCapability())))
        .isFalse();
  }

  @Test
  void neverSendsTheRequestToAPeerThatServesNoSnap() {
    assertThat(request.isEthPeerSuitable(peer(false, SnapProtocolVersion.V1.getCapability())))
        .isFalse();
  }

  private static EthPeerImmutableAttributes peer(
      final boolean servingSnap, final Capability snapCapability) {
    final EthPeer ethPeer = mock(EthPeer.class);
    when(ethPeer.getAgreedCapabilities())
        .thenReturn(Set.of(EthProtocolVersion.V68.getCapability(), snapCapability));
    return new EthPeerImmutableAttributes(
        UInt256.ZERO, true, 1L, 0, 0, 0L, false, true, servingSnap, true, false, ethPeer);
  }
}
