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
package org.hyperledger.besu.ethereum.permissioning;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.p2p.discovery.NodeIdentifier;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeDnsConfiguration;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.ImmutableEnodeDnsConfiguration;
import org.hyperledger.besu.ethereum.permissioning.NodeLocalConfigPermissioningController.NodesAllowlistResult;
import org.hyperledger.besu.ethereum.permissioning.node.NodeAllowlistUpdatedEvent;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.data.EnodeURL;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class NodeLocalConfigPermissioningControllerTest {

  @Mock private AllowlistPersistor allowlistPersistor;
  private final List<NodeIdentifier> bootnodesList = new ArrayList<>();
  private NodeLocalConfigPermissioningController controller;

  private final String enode1 =
      "enode://6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@192.168.0.10:4567";
  private final String enode2 =
      "enode://5f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@192.168.0.10:4567";
  private final EnodeURL selfEnode =
      EnodeURLImpl.fromString(
          "enode://5f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@192.168.0.10:1111");
  private final String enodeDns =
      "enode://6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@localhost:4567";

  @Mock private MetricsSystem metricsSystem;
  @Mock private Counter checkCounter;
  @Mock private Counter checkPermittedCounter;
  @Mock private Counter checkUnpermittedCounter;

  @BeforeEach
  public void setUp() {
    bootnodesList.clear();

    when(metricsSystem.createCounter(
            BesuMetricCategory.PERMISSIONING,
            "node_local_check_count",
            "Number of times the node local permissioning provider has been checked"))
        .thenReturn(checkCounter);

    when(metricsSystem.createCounter(
            BesuMetricCategory.PERMISSIONING,
            "node_local_check_count_permitted",
            "Number of times the node local permissioning provider has been checked and returned permitted"))
        .thenReturn(checkPermittedCounter);

    when(metricsSystem.createCounter(
            BesuMetricCategory.PERMISSIONING,
            "node_local_check_count_unpermitted",
            "Number of times the node local permissioning provider has been checked and returned unpermitted"))
        .thenReturn(checkUnpermittedCounter);

    controller =
        new NodeLocalConfigPermissioningController(
            LocalPermissioningConfiguration.createDefault(),
            bootnodesList,
            selfEnode.getNodeId(),
            allowlistPersistor,
            metricsSystem);
  }

  @Test
  public void whenAddNodesWithValidInputShouldReturnSuccess() {
    NodesAllowlistResult expected = new NodesAllowlistResult(AllowlistOperationResult.SUCCESS);
    NodesAllowlistResult actualResult = controller.addNodes(Lists.newArrayList(enode1));

    assertThat(actualResult)
        .usingRecursiveComparison()
        .comparingOnlyFields("result")
        .isEqualTo(expected);
    assertThat(controller.getNodesAllowlist()).containsExactly(enode1);
  }

  @Test
  public void whenAddNodesInputHasExistingNodeShouldReturnAddErrorExistingEntry() {
    controller.addNodes(List.of(enode1));

    NodesAllowlistResult expected =
        new NodesAllowlistResult(AllowlistOperationResult.ERROR_EXISTING_ENTRY);
    NodesAllowlistResult actualResult = controller.addNodes(Lists.newArrayList(enode1, enode2));

    assertThat(actualResult)
        .usingRecursiveComparison()
        .comparingOnlyFields("result")
        .isEqualTo(expected);
  }

  @Test
  public void whenAddNodesInputHasDuplicatedNodesShouldReturnDuplicatedEntryError() {
    NodesAllowlistResult expected =
        new NodesAllowlistResult(AllowlistOperationResult.ERROR_DUPLICATED_ENTRY);

    NodesAllowlistResult actualResult = controller.addNodes(Arrays.asList(enode1, enode1));

    assertThat(actualResult)
        .usingRecursiveComparison()
        .comparingOnlyFields("result")
        .isEqualTo(expected);
  }

  @Test
  public void whenAddNodesInputHasEmptyListOfNodesShouldReturnErrorEmptyEntry() {
    NodesAllowlistResult expected =
        new NodesAllowlistResult(AllowlistOperationResult.ERROR_EMPTY_ENTRY);
    NodesAllowlistResult actualResult = controller.removeNodes(new ArrayList<>());

    assertThat(actualResult)
        .usingRecursiveComparison()
        .comparingOnlyFields("result")
        .isEqualTo(expected);
  }

  @Test
  public void whenAddNodesInputHasNullListOfNodesShouldReturnErrorEmptyEntry() {
    NodesAllowlistResult expected =
        new NodesAllowlistResult(AllowlistOperationResult.ERROR_EMPTY_ENTRY);
    NodesAllowlistResult actualResult = controller.removeNodes(null);

    assertThat(actualResult)
        .usingRecursiveComparison()
        .comparingOnlyFields("result")
        .isEqualTo(expected);
  }

  @Test
  public void whenRemoveNodesInputHasAbsentNodeShouldReturnRemoveErrorAbsentEntry() {
    NodesAllowlistResult expected =
        new NodesAllowlistResult(AllowlistOperationResult.ERROR_ABSENT_ENTRY);
    NodesAllowlistResult actualResult = controller.removeNodes(Lists.newArrayList(enode1, enode2));

    assertThat(actualResult)
        .usingRecursiveComparison()
        .comparingOnlyFields("result")
        .isEqualTo(expected);
  }

  @Test
  public void whenRemoveNodesInputHasDuplicateNodesShouldReturnErrorDuplicatedEntry() {
    NodesAllowlistResult expected =
        new NodesAllowlistResult(AllowlistOperationResult.ERROR_DUPLICATED_ENTRY);
    NodesAllowlistResult actualResult = controller.removeNodes(Lists.newArrayList(enode1, enode1));

    assertThat(actualResult)
        .usingRecursiveComparison()
        .comparingOnlyFields("result")
        .isEqualTo(expected);
  }

  @Test
  public void whenRemoveNodesInputHasEmptyListOfNodesShouldReturnErrorEmptyEntry() {
    NodesAllowlistResult expected =
        new NodesAllowlistResult(AllowlistOperationResult.ERROR_EMPTY_ENTRY);
    NodesAllowlistResult actualResult = controller.removeNodes(new ArrayList<>());

    assertThat(actualResult)
        .usingRecursiveComparison()
        .comparingOnlyFields("result")
        .isEqualTo(expected);
  }

  @Test
  public void whenRemoveNodesInputHasNullListOfNodesShouldReturnErrorEmptyEntry() {
    NodesAllowlistResult expected =
        new NodesAllowlistResult(AllowlistOperationResult.ERROR_EMPTY_ENTRY);
    NodesAllowlistResult actualResult = controller.removeNodes(null);

    assertThat(actualResult)
        .usingRecursiveComparison()
        .comparingOnlyFields("result")
        .isEqualTo(expected);
  }

  @Test
  public void whenNodeIdsAreDifferentItShouldNotBePermitted() {
    String peer1 =
        "enode://aaaa80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:30303";
    String peer2 =
        "enode://bbbb80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.2:30303";

    controller.addNodes(List.of(peer1));
    assertThat(controller.isPermitted(peer2)).isFalse();

    verifyCountersUntouched();

    assertThat(
            controller.isConnectionPermitted(
                EnodeURLImpl.fromString(peer2), EnodeURLImpl.fromString(peer1)))
        .isFalse();

    verifyCountersUnpermitted();
  }

  @Test
  public void whenNodesHostsAreDifferentItShouldNotBePermitted() {
    String peer1 =
        "enode://aaaa80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:30303";
    String peer2 =
        "enode://aaaa80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.2:30303";

    controller.addNodes(List.of(peer1));

    assertThat(controller.isPermitted(peer2)).isFalse();
  }

  @Test
  public void whenNodesListeningPortsAreDifferentItShouldNotBePermitted() {
    String peer1 =
        "enode://aaaa80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:30301";
    String peer2 =
        "enode://aaaa80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:30302";

    controller.addNodes(List.of(peer1));

    assertThat(controller.isPermitted(peer2)).isFalse();
  }

  @Test
  public void
      whenCheckingIfNodeIsPermittedDiscoveryPortShouldNotBeConsidered_implicitDiscPortMismatch() {
    String peerWithDiscoveryPortSet =
        "enode://aaaa80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:30303?discport=10001";
    String peerWithoutDiscoveryPortSet =
        "enode://aaaa80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:30303";

    controller.addNodes(List.of(peerWithDiscoveryPortSet));

    assertThat(controller.isPermitted(peerWithoutDiscoveryPortSet)).isTrue();
  }

  @Test
  public void
      whenCheckingIfNodeIsPermittedDiscoveryPortShouldBeNotConsidered_explicitDiscPortMismatch() {
    String peerWithDiscoveryPortSet =
        "enode://aaaa80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:30303?discport=10001";
    String peerWithDifferentDiscoveryPortSet =
        "enode://aaaa80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:30303?discport=10002";

    controller.addNodes(List.of(peerWithDifferentDiscoveryPortSet));

    assertThat(controller.isPermitted(peerWithDiscoveryPortSet)).isTrue();
  }

  @Test
  public void
      whenCheckingIfNodeIsPermittedDiscoveryPortShouldNotBeConsidered_nodeToCheckHasDiscDisabled() {
    String peerWithDiscoveryPortSet =
        "enode://aaaa80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:30303?discport=10001";
    String peerWithoutDiscoveryPortSet =
        "enode://aaaa80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:30303?discport=0";

    controller.addNodes(List.of(peerWithDiscoveryPortSet));

    assertThat(controller.isPermitted(peerWithoutDiscoveryPortSet)).isTrue();
  }

  @Test
  public void
      whenCheckingIfNodeIsPermittedDiscoveryPortShouldNotBeConsidered_allowedNodeHasDiscDisabled() {
    String peerWithDiscoveryPortSet =
        "enode://aaaa80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:30303?discport=0";
    String peerWithoutDiscoveryPortSet =
        "enode://aaaa80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:30303?discport=123";

    controller.addNodes(List.of(peerWithDiscoveryPortSet));

    assertThat(controller.isPermitted(peerWithoutDiscoveryPortSet)).isTrue();
  }

  @Test
  public void
      whenCheckingIfNodeIsPermittedDiscoveryPortShouldNotBeConsidered_allowlistAndNodeHaveDiscDisabled() {
    String peerWithDiscoveryPortSet =
        "enode://aaaa80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:30303?discport=0";
    String peerWithoutDiscoveryPortSet =
        "enode://aaaa80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:30303?discport=0";

    controller.addNodes(List.of(peerWithDiscoveryPortSet));

    assertThat(controller.isPermitted(peerWithoutDiscoveryPortSet)).isTrue();
  }

  @Test
  public void whenCheckingIfNodeIsPermittedOrderDoesNotMatter() {
    controller.addNodes(List.of(enode1));

    verifyCountersUntouched();

    assertThat(controller.isConnectionPermitted(EnodeURLImpl.fromString(enode1), selfEnode))
        .isTrue();

    verifyCountersPermitted();

    assertThat(controller.isConnectionPermitted(selfEnode, EnodeURLImpl.fromString(enode1)))
        .isTrue();
  }

  @Test
  public void whenCallingIsPermittedAndRemovingEntryInAnotherThreadShouldNotThrowException()
      throws InterruptedException {
    // Add a node to the allowlist
    controller.addNodes(Lists.newArrayList(enode1));

    // Atomic flag to detect exceptions
    AtomicBoolean exceptionOccurred = new AtomicBoolean(false);

    // Create a thread to call isPermitted
    Thread isPermittedThread =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < 1000; i++) {
                  controller.isPermitted(enode1);
                }
              } catch (Exception e) {
                exceptionOccurred.set(true);
                e.printStackTrace();
              }
            });

    // Create a thread to modify the allowlist
    Thread modifyAllowlistThread =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < 1000; i++) {
                  controller.removeNodes(Lists.newArrayList(enode1));
                  controller.addNodes(Lists.newArrayList(enode1));
                }
              } catch (Exception e) {
                exceptionOccurred.set(true);
                e.printStackTrace();
              }
            });

    // Start both threads
    isPermittedThread.start();
    modifyAllowlistThread.start();

    // Wait for both threads to complete
    isPermittedThread.join();
    modifyAllowlistThread.join();

    // Assert no exceptions were thrown
    assert (!exceptionOccurred.get());
  }

  @Test
  public void stateShouldRevertIfAllowlistPersistFails()
      throws IOException, AllowlistFileSyncException {
    List<String> newNode1 = singletonList(EnodeURLImpl.fromString(enode1).toString());
    List<String> newNode2 = singletonList(EnodeURLImpl.fromString(enode2).toString());

    assertThat(controller.getNodesAllowlist().size()).isEqualTo(0);

    controller.addNodes(newNode1);
    assertThat(controller.getNodesAllowlist().size()).isEqualTo(1);

    doThrow(new IOException()).when(allowlistPersistor).updateConfig(any(), any());
    controller.addNodes(newNode2);

    assertThat(controller.getNodesAllowlist().size()).isEqualTo(1);
    assertThat(controller.getNodesAllowlist()).isEqualTo(newNode1);

    verify(allowlistPersistor, times(3)).verifyConfigFileMatchesState(any(), any());
    verify(allowlistPersistor, times(2)).updateConfig(any(), any());
    verifyNoMoreInteractions(allowlistPersistor);
  }

  @Test
  public void reloadNodeAllowlistWithValidConfigFileShouldUpdateAllowlist() throws Exception {
    final String expectedEnodeURL =
        "enode://6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@192.168.0.9:4567";
    final Path permissionsFile = createPermissionsFileWithNode(expectedEnodeURL);
    final LocalPermissioningConfiguration permissioningConfig =
        mock(LocalPermissioningConfiguration.class);

    when(permissioningConfig.getNodePermissioningConfigFilePath())
        .thenReturn(permissionsFile.toAbsolutePath().toString());
    when(permissioningConfig.isNodeAllowlistEnabled()).thenReturn(true);
    when(permissioningConfig.getNodeAllowlist())
        .thenReturn(singletonList(EnodeURLImpl.fromString(expectedEnodeURL)));
    when(permissioningConfig.getEnodeDnsConfiguration())
        .thenReturn(EnodeDnsConfiguration.dnsDisabled());

    controller =
        new NodeLocalConfigPermissioningController(
            permissioningConfig, bootnodesList, selfEnode.getNodeId(), metricsSystem);

    controller.reload();

    assertThat(controller.getNodesAllowlist()).containsExactly(expectedEnodeURL);
  }

  @Test
  public void reloadNodeAllowlistWithErrorReadingConfigFileShouldKeepOldAllowlist() {
    final String expectedEnodeURI =
        "enode://aaaa80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@192.168.0.9:4567";
    final LocalPermissioningConfiguration permissioningConfig =
        mock(LocalPermissioningConfiguration.class);

    when(permissioningConfig.getNodePermissioningConfigFilePath()).thenReturn("foo");
    when(permissioningConfig.isNodeAllowlistEnabled()).thenReturn(true);
    when(permissioningConfig.getNodeAllowlist())
        .thenReturn(singletonList(EnodeURLImpl.fromString(expectedEnodeURI)));
    controller =
        new NodeLocalConfigPermissioningController(
            permissioningConfig, bootnodesList, selfEnode.getNodeId(), metricsSystem);

    final Throwable thrown = catchThrowable(() -> controller.reload());

    assertThat(thrown)
        .isInstanceOf(RuntimeException.class)
        .hasRootCauseMessage(
            "Unable to read permissioning TOML config file : foo Configuration file does not exist: foo");

    assertThat(controller.getNodesAllowlist()).containsExactly(expectedEnodeURI);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void whenAddingNodeShouldNotifyAllowlistModifiedSubscribers() {
    final Consumer<NodeAllowlistUpdatedEvent> consumer = mock(Consumer.class);
    final NodeAllowlistUpdatedEvent expectedEvent =
        new NodeAllowlistUpdatedEvent(
            Lists.newArrayList(EnodeURLImpl.fromString(enode1)), Collections.emptyList());

    controller.subscribeToListUpdatedEvent(consumer);
    controller.addNodes(Lists.newArrayList(enode1));

    verify(consumer).accept(eq(expectedEvent));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void whenAddingNodeDoesNotAddShouldNotNotifyAllowlistModifiedSubscribers() {
    // adding node before subscribing to allowlist modified events
    controller.addNodes(Lists.newArrayList(enode1));
    final Consumer<NodeAllowlistUpdatedEvent> consumer = mock(Consumer.class);

    controller.subscribeToListUpdatedEvent(consumer);
    // won't add duplicate node
    controller.addNodes(Lists.newArrayList(enode1));

    verifyNoInteractions(consumer);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void whenRemovingNodeShouldNotifyAllowlistModifiedSubscribers() {
    // adding node before subscribing to allowlist modified events
    controller.addNodes(Lists.newArrayList(enode1));

    final Consumer<NodeAllowlistUpdatedEvent> consumer = mock(Consumer.class);
    final NodeAllowlistUpdatedEvent expectedEvent =
        new NodeAllowlistUpdatedEvent(
            Collections.emptyList(), Lists.newArrayList(EnodeURLImpl.fromString(enode1)));

    controller.subscribeToListUpdatedEvent(consumer);
    controller.removeNodes(Lists.newArrayList(enode1));

    verify(consumer).accept(eq(expectedEvent));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void whenRemovingNodeDoesNotRemoveShouldNotifyAllowlistModifiedSubscribers() {
    final Consumer<NodeAllowlistUpdatedEvent> consumer = mock(Consumer.class);

    controller.subscribeToListUpdatedEvent(consumer);
    // won't remove absent node
    controller.removeNodes(Lists.newArrayList(enode1));

    verifyNoInteractions(consumer);
  }

  @Test
  public void whenRemovingBootnodeShouldReturnRemoveBootnodeError() {
    NodesAllowlistResult expected =
        new NodesAllowlistResult(AllowlistOperationResult.ERROR_FIXED_NODE_CANNOT_BE_REMOVED);
    bootnodesList.add(EnodeURLImpl.fromString(enode1));
    controller.addNodes(Lists.newArrayList(enode1, enode2));

    NodesAllowlistResult actualResult = controller.removeNodes(Lists.newArrayList(enode1));

    assertThat(actualResult)
        .usingRecursiveComparison()
        .comparingOnlyFields("result")
        .isEqualTo(expected);
    assertThat(controller.getNodesAllowlist()).containsExactly(enode1, enode2);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void whenReloadingAllowlistShouldNotifyAllowlistModifiedSubscribers() throws Exception {
    final Path permissionsFile = createPermissionsFileWithNode(enode2);
    final LocalPermissioningConfiguration permissioningConfig =
        mock(LocalPermissioningConfiguration.class);
    final Consumer<NodeAllowlistUpdatedEvent> consumer = mock(Consumer.class);
    final NodeAllowlistUpdatedEvent expectedEvent =
        new NodeAllowlistUpdatedEvent(
            Lists.newArrayList(EnodeURLImpl.fromString(enode2)),
            Lists.newArrayList(EnodeURLImpl.fromString(enode1)));

    when(permissioningConfig.getNodePermissioningConfigFilePath())
        .thenReturn(permissionsFile.toAbsolutePath().toString());
    when(permissioningConfig.isNodeAllowlistEnabled()).thenReturn(true);
    when(permissioningConfig.getNodeAllowlist())
        .thenReturn(singletonList(EnodeURLImpl.fromString(enode1)));
    when(permissioningConfig.getEnodeDnsConfiguration())
        .thenReturn(EnodeDnsConfiguration.dnsDisabled());

    controller =
        new NodeLocalConfigPermissioningController(
            permissioningConfig, bootnodesList, selfEnode.getNodeId(), metricsSystem);
    controller.subscribeToListUpdatedEvent(consumer);

    controller.reload();

    verify(consumer).accept(eq(expectedEvent));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void whenReloadingAllowlistAndNothingChangesShouldNotNotifyAllowlistModifiedSubscribers()
      throws Exception {
    final Path permissionsFile = createPermissionsFileWithNode(enode1);
    final LocalPermissioningConfiguration permissioningConfig =
        mock(LocalPermissioningConfiguration.class);
    final Consumer<NodeAllowlistUpdatedEvent> consumer = mock(Consumer.class);

    when(permissioningConfig.getNodePermissioningConfigFilePath())
        .thenReturn(permissionsFile.toAbsolutePath().toString());
    when(permissioningConfig.isNodeAllowlistEnabled()).thenReturn(true);
    when(permissioningConfig.getNodeAllowlist())
        .thenReturn(singletonList(EnodeURLImpl.fromString(enode1)));
    when(permissioningConfig.getEnodeDnsConfiguration())
        .thenReturn(EnodeDnsConfiguration.dnsDisabled());

    controller =
        new NodeLocalConfigPermissioningController(
            permissioningConfig, bootnodesList, selfEnode.getNodeId(), metricsSystem);
    controller.subscribeToListUpdatedEvent(consumer);

    controller.reload();

    verifyNoInteractions(consumer);
  }

  @Test
  public void getNodeAllowlistShouldWorkWhenOnlyDnsEnabled() throws Exception {
    final Path permissionsFile = createPermissionsFileWithNode(enodeDns);
    final LocalPermissioningConfiguration permissioningConfig =
        mock(LocalPermissioningConfiguration.class);

    final EnodeDnsConfiguration enodeDnsConfiguration =
        ImmutableEnodeDnsConfiguration.builder().dnsEnabled(true).updateEnabled(false).build();

    when(permissioningConfig.getNodePermissioningConfigFilePath())
        .thenReturn(permissionsFile.toAbsolutePath().toString());
    when(permissioningConfig.isNodeAllowlistEnabled()).thenReturn(true);
    when(permissioningConfig.getNodeAllowlist())
        .thenReturn(singletonList(EnodeURLImpl.fromString(enodeDns, enodeDnsConfiguration)));
    controller =
        new NodeLocalConfigPermissioningController(
            permissioningConfig, bootnodesList, selfEnode.getNodeId(), metricsSystem);
    assertThat(controller.getNodesAllowlist()).hasSize(1);
    assertThat(controller.getNodesAllowlist())
        .contains(
            "enode://6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:4567");
  }

  @Test
  public void getNodeAllowlistShouldWorkWhenDnsAndUpdateEnabled() throws Exception {
    final Path permissionsFile = createPermissionsFileWithNode(enodeDns);
    final LocalPermissioningConfiguration permissioningConfig =
        mock(LocalPermissioningConfiguration.class);

    final EnodeDnsConfiguration enodeDnsConfiguration =
        ImmutableEnodeDnsConfiguration.builder().dnsEnabled(true).updateEnabled(true).build();

    when(permissioningConfig.getNodePermissioningConfigFilePath())
        .thenReturn(permissionsFile.toAbsolutePath().toString());
    when(permissioningConfig.isNodeAllowlistEnabled()).thenReturn(true);
    when(permissioningConfig.getNodeAllowlist())
        .thenReturn(singletonList(EnodeURLImpl.fromString(enodeDns, enodeDnsConfiguration)));
    controller =
        new NodeLocalConfigPermissioningController(
            permissioningConfig, bootnodesList, selfEnode.getNodeId(), metricsSystem);
    assertThat(controller.getNodesAllowlist()).hasSize(1);
    assertThat(controller.getNodesAllowlist())
        .contains(
            "enode://6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@"
                + InetAddress.getLocalHost().getHostName()
                + ":4567");
  }

  private Path createPermissionsFileWithNode(final String node) throws IOException {
    final String nodePermissionsFileContent = "nodes-allowlist=[\"" + node + "\"]";
    final Path permissionsFile = Files.createTempFile("node_permissions", "");
    permissionsFile.toFile().deleteOnExit();
    Files.writeString(permissionsFile, nodePermissionsFileContent);
    return permissionsFile;
  }

  private void verifyCountersUntouched() {
    verify(checkCounter, times(0)).inc();
    verify(checkPermittedCounter, times(0)).inc();
    verify(checkUnpermittedCounter, times(0)).inc();
  }

  private void verifyCountersPermitted() {
    verify(checkCounter, times(1)).inc();
    verify(checkPermittedCounter, times(1)).inc();
    verify(checkUnpermittedCounter, times(0)).inc();
  }

  private void verifyCountersUnpermitted() {
    verify(checkCounter, times(1)).inc();
    verify(checkPermittedCounter, times(0)).inc();
    verify(checkUnpermittedCounter, times(1)).inc();
  }
}
