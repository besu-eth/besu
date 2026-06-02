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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlockAccessList;
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.PeerRequest;
import org.hyperledger.besu.ethereum.eth.manager.PendingPeerRequest;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.ProtocolViolationException;
import org.hyperledger.besu.ethereum.eth.messages.snap.BlockAccessListsMessage;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class GetBlockAccessListsFromPeerTaskTest {

  private static final BigInteger REQUEST_ID = BigInteger.ONE;

  private final BlockDataGenerator dataGenerator = new BlockDataGenerator(1);

  @Test
  void shouldProcessValidBlockAccessLists() {
    final BlockAccessList firstBal = dataGenerator.blockAccessList();
    final BlockAccessList secondBal = dataGenerator.blockAccessList();

    final GetBlockAccessListsFromPeerTask task =
        taskFor(List.of(headerForBal(1, firstBal), headerForBal(2, secondBal)));

    final Optional<List<SyncBlockAccessList>> result =
        task.processResponse(
            false,
            responseWith(Optional.of(firstBal), Optional.of(secondBal)),
            mock(EthPeer.class));

    assertThat(result).contains(List.of(syncBal(firstBal), syncBal(secondBal)));
  }

  @Test
  void shouldProcessUnavailableBlockAccessListPlaceholder() {
    final BlockAccessList expectedBal = dataGenerator.blockAccessList();
    final GetBlockAccessListsFromPeerTask task = taskFor(List.of(headerForBal(1, expectedBal)));

    final Optional<List<SyncBlockAccessList>> result =
        task.processResponse(false, responseWith(Optional.empty()), mock(EthPeer.class));

    assertThat(result).contains(List.of(unavailableBal()));
    assertThat(result.orElseThrow().getFirst().isUnavailable()).isTrue();
  }

  @Test
  void shouldRejectBlockAccessListWithInvalidHash() {
    final BlockAccessList expectedBal = new BlockAccessList(List.of());
    final BlockAccessList mismatchingBal = dataGenerator.blockAccessList();
    final GetBlockAccessListsFromPeerTask task = taskFor(List.of(headerForBal(1, expectedBal)));

    assertThatThrownBy(
            () ->
                task.processResponse(
                    false, responseWith(Optional.of(mismatchingBal)), mock(EthPeer.class)))
        .isInstanceOf(ProtocolViolationException.class)
        .hasMessageContaining("invalid hash");
  }

  @Test
  void shouldRejectMoreBlockAccessListsThanRequested() {
    final BlockAccessList firstBal = dataGenerator.blockAccessList();
    final BlockAccessList unexpectedBal = dataGenerator.blockAccessList();
    final GetBlockAccessListsFromPeerTask task = taskFor(List.of(headerForBal(1, firstBal)));

    assertThatThrownBy(
            () ->
                task.processResponse(
                    false,
                    responseWith(Optional.of(firstBal), Optional.of(unexpectedBal)),
                    mock(EthPeer.class)))
        .isInstanceOf(ProtocolViolationException.class)
        .hasMessageContaining("more block access lists than requested");
  }

  @Test
  void shouldAcceptTruncatedBlockAccessListResponse() {
    final BlockAccessList firstBal = dataGenerator.blockAccessList();
    final BlockAccessList secondBal = dataGenerator.blockAccessList();

    final GetBlockAccessListsFromPeerTask task =
        taskFor(List.of(headerForBal(1, firstBal), headerForBal(2, secondBal)));

    final Optional<List<SyncBlockAccessList>> result =
        task.processResponse(false, responseWith(Optional.of(firstBal)), mock(EthPeer.class));

    assertThat(result).contains(List.of(syncBal(firstBal)));
  }

  @Test
  void shouldWaitForResponseWhenStreamClosesBeforeBlockAccessListsArrive() {
    final GetBlockAccessListsFromPeerTask task =
        taskFor(List.of(headerForBal(1, dataGenerator.blockAccessList())));

    final Optional<List<SyncBlockAccessList>> result =
        task.processResponse(true, null, mock(EthPeer.class));

    assertThat(result).isEmpty();
  }

  @Test
  void shouldRetryUnavailableAndTruncatedBlockAccessListEntries() {
    final BlockAccessList firstBal = dataGenerator.blockAccessList();
    final BlockAccessList secondBal = dataGenerator.blockAccessList();
    final BlockAccessList thirdBal = dataGenerator.blockAccessList();
    final RetryingGetBlockAccessListsFromPeerTask task =
        retryingTaskFor(
            List.of(
                headerForBal(1, firstBal), headerForBal(2, secondBal), headerForBal(3, thirdBal)));

    task.processBlockAccessLists(List.of(0, 1, 2), List.of(syncBal(firstBal), unavailableBal()));

    assertThat(task.pendingBlockAccessLists()).isEqualTo(2);

    task.processBlockAccessLists(List.of(1, 2), List.of(syncBal(secondBal), syncBal(thirdBal)));

    assertThat(task.pendingBlockAccessLists()).isEqualTo(0);
    assertThat(task.executeTaskOnCurrentPeer(mock(EthPeer.class)).join())
        .containsExactly(syncBal(firstBal), syncBal(secondBal), syncBal(thirdBal));
  }

  @Test
  void shouldMapRetriedResponsesToOriginalRequestIndexes() {
    final BlockAccessList firstBal = dataGenerator.blockAccessList();
    final BlockAccessList secondBal = dataGenerator.blockAccessList();
    final BlockAccessList thirdBal = dataGenerator.blockAccessList();
    final RetryingGetBlockAccessListsFromPeerTask task =
        retryingTaskFor(
            List.of(
                headerForBal(1, firstBal), headerForBal(2, secondBal), headerForBal(3, thirdBal)));

    task.processBlockAccessLists(List.of(0, 1, 2), List.of(unavailableBal(), syncBal(secondBal)));

    assertThat(task.pendingBlockAccessLists()).isEqualTo(2);

    task.processBlockAccessLists(List.of(0, 2), List.of(syncBal(firstBal), syncBal(thirdBal)));

    assertThat(task.executeTaskOnCurrentPeer(mock(EthPeer.class)).join())
        .containsExactly(syncBal(firstBal), syncBal(secondBal), syncBal(thirdBal));
  }

  @Test
  void shouldRequestBlockAccessListsFromSelectedSwitchingPeer() {
    final BlockAccessList blockAccessList = dataGenerator.blockAccessList();
    final EthContext ethContext = mock(EthContext.class);
    final EthPeers ethPeers = mock(EthPeers.class);
    final PendingPeerRequest pendingPeerRequest = mock(PendingPeerRequest.class);
    final EthPeer selectedPeer = mock(EthPeer.class);
    final RetryingGetBlockAccessListsFromPeerTask task =
        new RetryingGetBlockAccessListsFromPeerTask(
            ethContext, List.of(headerForBal(1, blockAccessList)), new NoOpMetricsSystem());

    when(ethContext.getEthPeers()).thenReturn(ethPeers);
    when(ethPeers.executePeerRequest(any(PeerRequest.class), eq(1L), eq(Optional.of(selectedPeer))))
        .thenReturn(pendingPeerRequest);

    task.executeTaskOnCurrentPeer(selectedPeer);

    verify(ethPeers)
        .executePeerRequest(any(PeerRequest.class), eq(1L), eq(Optional.of(selectedPeer)));
  }

  @Test
  void shouldSelectOnlySnap2ServingPeers() {
    final RetryingGetBlockAccessListsFromPeerTask task = retryingTask();

    assertThat(task.isSuitablePeer(peerAttributes(true, Set.of(SnapProtocol.SNAP2)))).isTrue();
    assertThat(task.isSuitablePeer(peerAttributes(false, Set.of(SnapProtocol.SNAP2)))).isFalse();
    assertThat(task.isSuitablePeer(peerAttributes(true, Set.of(SnapProtocol.SNAP1)))).isFalse();
  }

  private GetBlockAccessListsFromPeerTask taskFor(final List<BlockHeader> blockHeaders) {
    return new GetBlockAccessListsFromPeerTask(
        mock(EthContext.class), blockHeaders, new NoOpMetricsSystem());
  }

  private RetryingGetBlockAccessListsFromPeerTask retryingTask() {
    return retryingTaskFor(List.of(headerForBal(1, dataGenerator.blockAccessList())));
  }

  private RetryingGetBlockAccessListsFromPeerTask retryingTaskFor(
      final List<BlockHeader> blockHeaders) {
    return new RetryingGetBlockAccessListsFromPeerTask(
        mock(EthContext.class), blockHeaders, new NoOpMetricsSystem());
  }

  private BlockHeader headerForBal(final long number, final BlockAccessList blockAccessList) {
    final Hash hash = dataGenerator.hash();
    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getNumber()).thenReturn(number);
    when(blockHeader.getHash()).thenReturn(hash);
    when(blockHeader.getBlockHash()).thenReturn(hash);
    when(blockHeader.getBalHash()).thenReturn(Optional.of(BodyValidation.balHash(blockAccessList)));
    return blockHeader;
  }

  @SafeVarargs
  private final MessageData responseWith(final Optional<BlockAccessList>... blockAccessLists) {
    return BlockAccessListsMessage.create(List.of(blockAccessLists)).wrapMessageData(REQUEST_ID);
  }

  private SyncBlockAccessList syncBal(final BlockAccessList blockAccessList) {
    return new SyncBlockAccessList(RLP.encode(blockAccessList::writeTo));
  }

  private SyncBlockAccessList unavailableBal() {
    return new SyncBlockAccessList(RLP.NULL);
  }

  private EthPeerImmutableAttributes peerAttributes(
      final boolean isServingSnap, final Set<Capability> agreedCapabilities) {
    final EthPeer ethPeer = mock(EthPeer.class);
    when(ethPeer.getAgreedCapabilities()).thenReturn(agreedCapabilities);
    return new EthPeerImmutableAttributes(
        UInt256.ZERO, true, 1L, 0, 0, 0L, false, true, isServingSnap, true, false, ethPeer);
  }
}
