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
package org.hyperledger.besu.ethereum.permissioning.node;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.rlpx.ConnectCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.DisconnectCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.plugin.data.EnodeURL;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class InsufficientPeersPermissioningProviderTest {
  @Mock private P2PNetwork p2pNetwork;
  private final EnodeURLImpl SELF_ENODE =
      EnodeURLImpl.fromString(
          "enode://00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001@192.168.0.1:30303");
  private final EnodeURLImpl ENODE_2 =
      EnodeURLImpl.fromString(
          "enode://00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002@192.168.0.2:30303");
  private final EnodeURLImpl ENODE_3 =
      EnodeURLImpl.fromString(
          "enode://00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003@192.168.0.3:30303");
  private final EnodeURLImpl ENODE_4 =
      EnodeURLImpl.fromString(
          "enode://00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004@192.168.0.4:30303");
  private final EnodeURLImpl ENODE_5 =
      EnodeURLImpl.fromString(
          "enode://00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005@192.168.0.5:30303");

  @BeforeEach
  public void setup() {
    when(p2pNetwork.getLocalEnode()).thenReturn(Optional.of(SELF_ENODE));
  }

  @Test
  public void noResultWhenNoBootnodes() {
    final Collection<EnodeURLImpl> bootnodes = Collections.emptyList();

    when(p2pNetwork.getPeers()).thenReturn(Collections.emptyList());

    final InsufficientPeersPermissioningProvider provider =
        new InsufficientPeersPermissioningProvider(p2pNetwork, bootnodes);

    assertThat(provider.isPermitted(SELF_ENODE, ENODE_2)).isEmpty();
  }

  @Test
  public void noResultWhenOtherConnections() {
    final PeerConnection neverMatchPeerConnection = mock(PeerConnection.class);
    when(neverMatchPeerConnection.getRemoteEnode()).thenReturn(ENODE_5);
    when(p2pNetwork.getPeers()).thenReturn(Collections.singletonList(neverMatchPeerConnection));

    final Collection<EnodeURLImpl> bootnodes = Collections.singletonList(ENODE_2);

    final InsufficientPeersPermissioningProvider provider =
        new InsufficientPeersPermissioningProvider(p2pNetwork, bootnodes);

    assertThat(provider.isPermitted(SELF_ENODE, ENODE_3)).isEmpty();
    assertThat(provider.isPermitted(SELF_ENODE, ENODE_2)).isEmpty();
  }

  @Test
  public void allowsConnectionIfBootnodeAndNoConnections() {
    final Collection<EnodeURLImpl> bootnodes = Collections.singletonList(ENODE_2);

    when(p2pNetwork.getPeers()).thenReturn(Collections.emptyList());

    final InsufficientPeersPermissioningProvider provider =
        new InsufficientPeersPermissioningProvider(p2pNetwork, bootnodes);

    assertThat(provider.isPermitted(SELF_ENODE, ENODE_2)).contains(true);
    assertThat(provider.isPermitted(SELF_ENODE, ENODE_3)).isEmpty();
  }

  @Test
  public void noResultWhenLocalNodeNotReady() {
    final Collection<EnodeURLImpl> bootnodes = Collections.singletonList(ENODE_2);

    when(p2pNetwork.getPeers()).thenReturn(Collections.emptyList());
    when(p2pNetwork.getLocalEnode()).thenReturn(Optional.empty());

    final InsufficientPeersPermissioningProvider provider =
        new InsufficientPeersPermissioningProvider(p2pNetwork, bootnodes);

    assertThat(provider.isPermitted(SELF_ENODE, ENODE_2)).isEmpty();
    assertThat(provider.isPermitted(SELF_ENODE, ENODE_3)).isEmpty();
  }

  @Test
  public void allowsConnectionIfBootnodeAndOnlyBootnodesConnected() {
    final Collection<EnodeURLImpl> bootnodes = Collections.singletonList(ENODE_2);

    final PeerConnection bootnodeMatchPeerConnection = mock(PeerConnection.class);
    when(bootnodeMatchPeerConnection.getRemoteEnode()).thenReturn(ENODE_2);
    when(p2pNetwork.getPeers()).thenReturn(Collections.singletonList(bootnodeMatchPeerConnection));

    final InsufficientPeersPermissioningProvider provider =
        new InsufficientPeersPermissioningProvider(p2pNetwork, bootnodes);

    assertThat(provider.isPermitted(SELF_ENODE, ENODE_2)).contains(true);
    assertThat(provider.isPermitted(SELF_ENODE, ENODE_3)).isEmpty();
  }

  private PeerConnection peerConnectionMatching(final EnodeURL enode) {
    final PeerConnection pc = mock(PeerConnection.class);
    when(pc.getRemoteEnode()).thenReturn(enode);
    return pc;
  }

  @Test
  public void firesUpdateWhenDisconnectLastNonBootnode() {
    final Collection<EnodeURLImpl> bootnodes = Collections.singletonList(ENODE_2);
    final Collection<PeerConnection> pcs =
        Arrays.asList(
            peerConnectionMatching(ENODE_2),
            peerConnectionMatching(ENODE_3),
            peerConnectionMatching(ENODE_4));
    when(p2pNetwork.getPeers()).thenReturn(pcs);

    final InsufficientPeersPermissioningProvider provider =
        new InsufficientPeersPermissioningProvider(p2pNetwork, bootnodes);

    final ArgumentCaptor<DisconnectCallback> callbackCaptor =
        ArgumentCaptor.forClass(DisconnectCallback.class);
    verify(p2pNetwork).subscribeDisconnect(callbackCaptor.capture());
    final DisconnectCallback disconnectCallback = callbackCaptor.getValue();

    final Runnable updatePermsCallback = mock(Runnable.class);

    provider.subscribeToUpdates(updatePermsCallback);

    disconnectCallback.onDisconnect(peerConnectionMatching(ENODE_3), null, true);
    verify(updatePermsCallback, times(0)).run();
    disconnectCallback.onDisconnect(peerConnectionMatching(ENODE_4), null, true);
    verify(updatePermsCallback, times(1)).run();
    disconnectCallback.onDisconnect(peerConnectionMatching(ENODE_2), null, true);
    verify(updatePermsCallback, times(1)).run();
  }

  @Test
  public void firesUpdateWhenNonBootnodeConnects() {
    final Collection<EnodeURLImpl> bootnodes = Arrays.asList(ENODE_2, ENODE_3);
    final Collection<PeerConnection> pcs = Collections.emptyList();

    when(p2pNetwork.getPeers()).thenReturn(pcs);

    final InsufficientPeersPermissioningProvider provider =
        new InsufficientPeersPermissioningProvider(p2pNetwork, bootnodes);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<ConnectCallback> callbackCaptor =
        ArgumentCaptor.forClass(ConnectCallback.class);

    verify(p2pNetwork).subscribeConnect(callbackCaptor.capture());
    final ConnectCallback incomingConnectCallback = callbackCaptor.getValue();

    final Runnable updatePermsCallback = mock(Runnable.class);

    provider.subscribeToUpdates(updatePermsCallback);

    incomingConnectCallback.onConnect(peerConnectionMatching(ENODE_2));
    verify(updatePermsCallback, times(0)).run();
    incomingConnectCallback.onConnect(peerConnectionMatching(ENODE_3));
    verify(updatePermsCallback, times(0)).run();
    incomingConnectCallback.onConnect(peerConnectionMatching(ENODE_4));
    verify(updatePermsCallback, times(1)).run();
    incomingConnectCallback.onConnect(peerConnectionMatching(ENODE_5));
    verify(updatePermsCallback, times(1)).run();
    incomingConnectCallback.onConnect(peerConnectionMatching(ENODE_3));
    verify(updatePermsCallback, times(1)).run();
  }

  @Test
  public void firesUpdateWhenGettingAndLosingConnection() {
    final Collection<EnodeURLImpl> bootnodes = Arrays.asList(ENODE_2, ENODE_3);
    final Collection<PeerConnection> pcs = Collections.emptyList();

    when(p2pNetwork.getPeers()).thenReturn(pcs);

    final InsufficientPeersPermissioningProvider provider =
        new InsufficientPeersPermissioningProvider(p2pNetwork, bootnodes);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<ConnectCallback> connectCallbackCaptor =
        ArgumentCaptor.forClass(ConnectCallback.class);
    verify(p2pNetwork).subscribeConnect(connectCallbackCaptor.capture());
    final ConnectCallback incomingConnectCallback = connectCallbackCaptor.getValue();

    final ArgumentCaptor<DisconnectCallback> disconnectCallbackCaptor =
        ArgumentCaptor.forClass(DisconnectCallback.class);
    verify(p2pNetwork).subscribeDisconnect(disconnectCallbackCaptor.capture());
    final DisconnectCallback disconnectCallback = disconnectCallbackCaptor.getValue();

    final Runnable updatePermsCallback = mock(Runnable.class);

    provider.subscribeToUpdates(updatePermsCallback);

    incomingConnectCallback.onConnect(peerConnectionMatching(ENODE_2));
    verify(updatePermsCallback, times(0)).run();
    incomingConnectCallback.onConnect(peerConnectionMatching(ENODE_3));
    verify(updatePermsCallback, times(0)).run();
    incomingConnectCallback.onConnect(peerConnectionMatching(ENODE_4));
    verify(updatePermsCallback, times(1)).run();
    incomingConnectCallback.onConnect(peerConnectionMatching(ENODE_5));
    verify(updatePermsCallback, times(1)).run();
    disconnectCallback.onDisconnect(peerConnectionMatching(ENODE_2), null, true);
    verify(updatePermsCallback, times(1)).run();
    disconnectCallback.onDisconnect(peerConnectionMatching(ENODE_4), null, true);
    verify(updatePermsCallback, times(1)).run();
    disconnectCallback.onDisconnect(peerConnectionMatching(ENODE_5), null, true);
    verify(updatePermsCallback, times(2)).run();
  }
}
