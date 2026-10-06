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
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.p2p.peers.DefaultPeer;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions.Action;
import org.hyperledger.besu.ethereum.permissioning.AllowlistPersistor;
import org.hyperledger.besu.ethereum.permissioning.LocalPermissioningConfiguration;
import org.hyperledger.besu.ethereum.permissioning.NodeLocalConfigPermissioningController;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.permissioning.NodeConnectionPermissioningProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

public class PeerPermissionsAdapterTest {

  private final Peer localNode = createPeer();
  private final Peer remoteNode = createPeer();
  private final NodePermissioningController nodePermissioningController =
      mock(NodePermissioningController.class);
  private final BlockDataGenerator gen = new BlockDataGenerator();
  private final MutableBlockchain blockchain =
      InMemoryKeyValueStorageProvider.createInMemoryBlockchain(gen.genesisBlock());
  private final PeerPermissionsAdapter adapter =
      new PeerPermissionsAdapter(nodePermissioningController, blockchain);

  @Test
  public void allowInPeerTable() {
    final Action action = Action.DISCOVERY_ALLOW_IN_PEER_TABLE;

    mockControllerPermissions(true, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();

    mockControllerPermissions(false, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();

    mockControllerPermissions(false, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();

    mockControllerPermissions(true, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();
  }

  @Test
  public void allowOutboundBonding() {

    final Action action = Action.DISCOVERY_ALLOW_OUTBOUND_BONDING;

    mockControllerPermissions(true, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();

    mockControllerPermissions(false, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();

    mockControllerPermissions(true, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();

    mockControllerPermissions(false, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();
  }

  @Test
  public void allowInboundBonding() {
    final Action action = Action.DISCOVERY_ACCEPT_INBOUND_BONDING;

    mockControllerPermissions(true, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();

    mockControllerPermissions(false, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();

    mockControllerPermissions(true, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();

    mockControllerPermissions(false, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();
  }

  @Test
  public void allowOutboundNeighborsRequest() {
    final Action action = Action.DISCOVERY_ALLOW_OUTBOUND_NEIGHBORS_REQUEST;

    mockControllerPermissions(true, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();

    mockControllerPermissions(false, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();

    mockControllerPermissions(true, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();

    mockControllerPermissions(false, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();
  }

  @Test
  public void allowInboundNeighborsRequest() {
    final Action action = Action.DISCOVERY_SERVE_INBOUND_NEIGHBORS_REQUEST;

    mockControllerPermissions(true, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();

    mockControllerPermissions(false, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();

    mockControllerPermissions(true, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();

    mockControllerPermissions(false, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();
  }

  @Test
  public void allowLocallyInitiatedConnection() {
    final Action action = Action.RLPX_ALLOW_NEW_OUTBOUND_CONNECTION;

    mockControllerPermissions(true, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();

    mockControllerPermissions(false, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();

    mockControllerPermissions(true, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();

    mockControllerPermissions(false, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();
  }

  @Test
  public void allowRemotelyInitiatedConnection() {
    final Action action = Action.RLPX_ALLOW_NEW_INBOUND_CONNECTION;

    mockControllerPermissions(true, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();

    mockControllerPermissions(false, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();

    mockControllerPermissions(true, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();

    mockControllerPermissions(false, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();
  }

  @Test
  public void allowOngoingLocallyInitiatedConnection() {
    final Action action = Action.RLPX_ALLOW_ONGOING_LOCALLY_INITIATED_CONNECTION;

    mockControllerPermissions(true, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();

    mockControllerPermissions(false, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();

    mockControllerPermissions(true, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();

    mockControllerPermissions(false, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();
  }

  @Test
  public void allowOngoingRemotelyInitiatedConnection() {
    final Action action = Action.RLPX_ALLOW_ONGOING_REMOTELY_INITIATED_CONNECTION;

    mockControllerPermissions(true, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();

    mockControllerPermissions(false, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();

    mockControllerPermissions(true, true);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isTrue();

    mockControllerPermissions(false, false);
    assertThat(adapter.isPermitted(localNode, remoteNode, action)).isFalse();
  }

  @Test
  public void subscribeUpdate_firesWhenBlockAdded() {
    final AtomicBoolean updateDispatched = new AtomicBoolean(false);
    adapter.subscribeUpdate((restricted, peers) -> updateDispatched.set(true));

    final Block newBlock = gen.nextBlock(blockchain.getGenesisBlock());
    blockchain.appendBlock(newBlock, gen.receipts(newBlock));

    assertThat(updateDispatched).isTrue();
  }

  /**
   * Revoking permission for a connected peer must dispatch a permissions update immediately, not
   * wait for the next block import. Deliberately appends no block: the block-added observer is the
   * accidental trigger that masks this in a block-producing network.
   */
  @Test
  public void subscribeUpdate_firesWhenNodeRemovedFromLocalAllowlist() {
    final String allowedEnode =
        "enode://6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@192.168.0.10:4567";
    final NodeLocalConfigPermissioningController localConfigController =
        localConfigController(allowedEnode);
    final PeerPermissionsAdapter adapterUnderTest =
        new PeerPermissionsAdapter(
            new NodePermissioningController(
                List.<NodeConnectionPermissioningProvider>of(localConfigController)),
            blockchain);

    final AtomicBoolean updateDispatched = new AtomicBoolean(false);
    adapterUnderTest.subscribeUpdate((restricted, peers) -> updateDispatched.set(true));

    localConfigController.removeNodes(List.of(allowedEnode));

    assertThat(updateDispatched).isTrue();
  }

  @Test
  public void subscribeUpdate_firesWhenNodeAddedToLocalAllowlist() {
    final String newEnode =
        "enode://5f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@192.168.0.11:4567";
    final NodeLocalConfigPermissioningController localConfigController =
        localConfigController(
            "enode://6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@192.168.0.10:4567");
    final PeerPermissionsAdapter adapterUnderTest =
        new PeerPermissionsAdapter(
            new NodePermissioningController(
                List.<NodeConnectionPermissioningProvider>of(localConfigController)),
            blockchain);

    final AtomicBoolean updateDispatched = new AtomicBoolean(false);
    adapterUnderTest.subscribeUpdate((restricted, peers) -> updateDispatched.set(true));

    localConfigController.addNodes(List.of(newEnode));

    assertThat(updateDispatched).isTrue();
  }

  private NodeLocalConfigPermissioningController localConfigController(
      final String... allowedNodes) {
    final LocalPermissioningConfiguration config = LocalPermissioningConfiguration.createDefault();
    config.setNodeAllowlist(
        Arrays.stream(allowedNodes).map(EnodeURLImpl::fromString).collect(Collectors.toList()));

    return new NodeLocalConfigPermissioningController(
        config,
        Collections.emptyList(),
        localNode.getId(),
        mock(AllowlistPersistor.class),
        new NoOpMetricsSystem());
  }

  private void mockControllerPermissions(
      final boolean allowLocalToRemote, final boolean allowRemoteToLocal) {
    when(nodePermissioningController.isPermitted(
            ArgumentMatchers.eq(localNode.getEnodeURL()),
            ArgumentMatchers.eq(remoteNode.getEnodeURL())))
        .thenReturn(allowLocalToRemote);
    when(nodePermissioningController.isPermitted(
            ArgumentMatchers.eq(remoteNode.getEnodeURL()),
            ArgumentMatchers.eq(localNode.getEnodeURL())))
        .thenReturn(allowRemoteToLocal);
  }

  private Peer createPeer() {
    return DefaultPeer.fromEnodeURL(
        EnodeURLImpl.builder()
            .ipAddress("127.0.0.1")
            .nodeId(Peer.randomId())
            .useDefaultPorts()
            .build());
  }
}
