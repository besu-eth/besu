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
package org.hyperledger.besu.ethereum.eth.manager;

import static com.google.common.base.Preconditions.checkArgument;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.ChainHead;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.EthProtocolVersion;
import org.hyperledger.besu.ethereum.eth.manager.snap.SnapProtocolManager;
import org.hyperledger.besu.ethereum.eth.peervalidation.PeerValidator;
import org.hyperledger.besu.ethereum.eth.sync.ChainHeadTracker;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.DefaultMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import org.mockito.Mockito;

public class EthProtocolManagerTestUtil {

  public static ChainHeadTracker getChainHeadTrackerMock() {
    final ChainHeadTracker chtMock = mock(ChainHeadTracker.class);
    final BlockHeader blockHeaderMock = mock(BlockHeader.class);
    Mockito.lenient()
        .when(chtMock.getBestHeaderFromPeer(any()))
        .thenReturn(CompletableFuture.completedFuture(blockHeaderMock));
    Mockito.lenient().when(blockHeaderMock.getNumber()).thenReturn(0L);
    Mockito.lenient().when(blockHeaderMock.getStateRoot()).thenReturn(Hash.ZERO);
    return chtMock;
  }

  // Utility to prevent scheduler from automatically running submitted tasks
  public static void disableEthSchedulerAutoRun(final EthProtocolManager ethProtocolManager) {
    final EthScheduler scheduler = ethProtocolManager.ethContext().getScheduler();
    checkArgument(
        scheduler instanceof DeterministicEthScheduler,
        "EthProtocolManager must be set up with "
            + DeterministicEthScheduler.class.getSimpleName()
            + " in order to disable auto run.");
    ((DeterministicEthScheduler) scheduler).disableAutoRun();
  }

  // Manually runs any pending tasks submitted to the EthScheduler
  // Works with {@code disableEthSchedulerAutoRun} - tasks will only be pending if
  // autoRun has been disabled.
  public static void runPendingFutures(final EthProtocolManager ethProtocolManager) {
    final EthScheduler scheduler = ethProtocolManager.ethContext().getScheduler();
    checkArgument(
        scheduler instanceof DeterministicEthScheduler,
        "EthProtocolManager must be set up with "
            + DeterministicEthScheduler.class.getSimpleName()
            + " in order to manually run pending futures.");
    ((DeterministicEthScheduler) scheduler).runPendingFutures();
  }

  /**
   * Expires any pending timeouts tracked by {@code DeterministicEthScheduler}
   *
   * @param ethProtocolManager The {@code EthProtocolManager} managing the scheduler holding the
   *     timeouts to be expired.
   */
  public static void expirePendingTimeouts(final EthProtocolManager ethProtocolManager) {
    final EthScheduler scheduler = ethProtocolManager.ethContext().getScheduler();
    checkArgument(
        scheduler instanceof DeterministicEthScheduler,
        "EthProtocolManager must be set up with "
            + DeterministicEthScheduler.class.getSimpleName()
            + " in order to manually expire pending timeouts.");
    ((DeterministicEthScheduler) scheduler).expirePendingTimeouts();
  }

  /**
   * Gets the number of pending tasks submitted to the EthScheduler.
   *
   * <p>Works with {@code disableEthSchedulerAutoRun} - tasks will only be pending if autoRun has
   * been disabled.
   */
  public static long getPendingFuturesCount(final EthProtocolManager ethProtocolManager) {
    final EthScheduler scheduler = ethProtocolManager.ethContext().getScheduler();
    checkArgument(
        scheduler instanceof DeterministicEthScheduler,
        "EthProtocolManager must be set up with "
            + DeterministicEthScheduler.class.getSimpleName()
            + " in order to manually run pending futures.");
    return ((DeterministicEthScheduler) scheduler).getPendingFuturesCount();
  }

  public static void broadcastMessage(
      final EthProtocolManager ethProtocolManager,
      final RespondingEthPeer peer,
      final MessageData message) {
    ethProtocolManager.processMessage(
        EthProtocol.LATEST, new DefaultMessage(peer.getPeerConnection(), message));
  }

  public static RespondingEthPeer.Builder peerBuilder() {
    return RespondingEthPeer.builder();
  }

  public static RespondingEthPeer createPeer(
      final EthProtocolManager ethProtocolManager, final Difficulty td) {
    return RespondingEthPeer.builder()
        .ethProtocolManager(ethProtocolManager)
        .totalDifficulty(td)
        .capability(EthProtocolVersion.V68.getCapability())
        .build();
  }

  public static RespondingEthPeer createPeer(
      final EthProtocolManager ethProtocolManager,
      final Difficulty td,
      final long estimatedHeight) {
    return RespondingEthPeer.builder()
        .ethProtocolManager(ethProtocolManager)
        .totalDifficulty(td)
        .estimatedHeight(estimatedHeight)
        .capability(EthProtocolVersion.V68.getCapability())
        .build();
  }

  public static RespondingEthPeer createPeer(
      final EthProtocolManager ethProtocolManager,
      final Difficulty td,
      final OptionalLong estimatedHeight) {
    return RespondingEthPeer.builder()
        .ethProtocolManager(ethProtocolManager)
        .totalDifficulty(td)
        .estimatedHeight(estimatedHeight)
        .capability(EthProtocolVersion.V68.getCapability())
        .build();
  }

  public static RespondingEthPeer createPeer(
      final EthProtocolManager ethProtocolManager,
      final OptionalLong estimatedHeight,
      final PeerValidator... validators) {
    return RespondingEthPeer.builder()
        .ethProtocolManager(ethProtocolManager)
        .estimatedHeight(estimatedHeight)
        .peerValidators(validators)
        .build();
  }

  public static RespondingEthPeer createPeer(
      final EthProtocolManager ethProtocolManager,
      final Difficulty td,
      final OptionalLong estimatedHeight,
      final PeerValidator... validators) {
    return RespondingEthPeer.builder()
        .ethProtocolManager(ethProtocolManager)
        .totalDifficulty(td)
        .estimatedHeight(estimatedHeight)
        .capability(EthProtocolVersion.V68.getCapability())
        .peerValidators(validators)
        .build();
  }

  public static RespondingEthPeer createPeer(final EthProtocolManager ethProtocolManager) {
    return RespondingEthPeer.builder().ethProtocolManager(ethProtocolManager).build();
  }

  public static RespondingEthPeer createPeer(
      final EthProtocolManager ethProtocolManager, final long estimatedHeight) {
    return RespondingEthPeer.builder()
        .ethProtocolManager(ethProtocolManager)
        .estimatedHeight(estimatedHeight)
        .build();
  }

  public static RespondingEthPeer createPeer(
      final EthProtocolManager ethProtocolManager,
      final SnapProtocolManager snapProtocolManager,
      final long estimatedHeight) {
    return RespondingEthPeer.builder()
        .ethProtocolManager(ethProtocolManager)
        .estimatedHeight(estimatedHeight)
        .snapProtocolManager(snapProtocolManager)
        .build();
  }

  public static RespondingEthPeer createPeer(
      final EthProtocolManager ethProtocolManager,
      final long estimatedHeight,
      final PeerValidator... validators) {
    return RespondingEthPeer.builder()
        .ethProtocolManager(ethProtocolManager)
        .estimatedHeight(estimatedHeight)
        .peerValidators(validators)
        .build();
  }

  public static RespondingEthPeer createPeer(
      final EthProtocolManager ethProtocolManager, final Blockchain blockchain) {
    final ChainHead head = blockchain.getChainHead();
    return RespondingEthPeer.builder()
        .ethProtocolManager(ethProtocolManager)
        .totalDifficulty(head.getTotalDifficulty())
        .chainHeadHash(head.getHash())
        .estimatedHeight(blockchain.getChainHeadBlockNumber())
        .build();
  }

  public static RespondingEthPeer createPeer(
      final EthProtocolManager ethProtocolManager,
      final Difficulty td,
      final int estimatedHeight,
      final boolean isServingSnap,
      final boolean addToEthPeers) {
    return RespondingEthPeer.builder()
        .ethProtocolManager(ethProtocolManager)
        .totalDifficulty(td)
        .estimatedHeight(estimatedHeight)
        .isServingSnap(isServingSnap)
        .addToEthPeers(addToEthPeers)
        .build();
  }
}
