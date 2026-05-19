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
package org.hyperledger.besu.ethereum.eth.manager.task;

import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.NoAvailablePeersException;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRetryingSwitchingPeerTask<T> extends AbstractRetryingPeerTask<T> {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractRetryingSwitchingPeerTask.class);

  private final Set<EthPeer> triedPeers = new HashSet<>();
  private final Set<EthPeer> failedPeers = new HashSet<>();

  protected AbstractRetryingSwitchingPeerTask(
      final EthContext ethContext,
      final MetricsSystem metricsSystem,
      final Predicate<T> isEmptyResponse,
      final int maxRetries) {
    super(ethContext, maxRetries, isEmptyResponse, metricsSystem);
  }

  @Override
  public boolean assignPeer(final EthPeer peer) {
    if (super.assignPeer(peer)) {
      triedPeers.add(peer);
      return true;
    }
    return false;
  }

  protected abstract CompletableFuture<T> executeTaskOnCurrentPeer(final EthPeer peer);

  @Override
  protected CompletableFuture<T> executePeerTask(final Optional<EthPeer> assignedPeer) {

    final Optional<EthPeer> maybePeer =
        assignedPeer
            .filter(u -> getRetryCount() == 1) // first try with the assigned peer if present
            .or(this::selectNextPeer); // otherwise select a new one from the pool

    if (maybePeer.isEmpty()) {
      LOG.atTrace()
          .setMessage("No peer found to try to execute task at attempt {}, tried peers {}")
          .addArgument(this::getRetryCount)
          .addArgument(triedPeers)
          .log();
      return CompletableFuture.failedFuture(NoAvailablePeersException.WITHOUT_STACKTRACE);
    }

    final EthPeer peerToUse = maybePeer.get();
    assignPeer(peerToUse);

    LOG.atTrace()
        .setMessage("Trying to execute task on peer {}, attempt {}")
        .addArgument(this::getAssignedPeer)
        .addArgument(this::getRetryCount)
        .log();

    return executeTaskOnCurrentPeer(peerToUse)
        .thenApply(
            peerResult -> {
              LOG.atTrace()
                  .setMessage("Got result {} from peer {}, attempt {}")
                  .addArgument(peerResult)
                  .addArgument(peerToUse)
                  .addArgument(this::getRetryCount)
                  .log();
              result.complete(peerResult);
              return peerResult;
            });
  }

  @Override
  protected void handleTaskError(final Throwable error) {
    if (isPeerFailure(error)) {
      getAssignedPeer().ifPresent(failedPeers::add);
    }
    super.handleTaskError(error);
  }

  @Override
  protected boolean isRetryableError(final Throwable error) {
    return error instanceof TimeoutException || isPeerFailure(error);
  }

  private Optional<EthPeer> selectNextPeer() {
    final Optional<EthPeer> maybeNextPeer = nextPeerToTry();

    if (maybeNextPeer.isEmpty()) {
      // tried all the peers, restart from the best one but excluding the failed ones
      refreshPeers();
      triedPeers.retainAll(failedPeers);
      return nextPeerToTry();
    }

    return maybeNextPeer;
  }

  protected Optional<EthPeer> nextPeerToTry() {
    return getEthContext()
        .getEthPeers()
        .streamBestPeers()
        .filter((peer) -> isSuitablePeer(peer) && !triedPeers.contains(peer.ethPeer()))
        .map(EthPeerImmutableAttributes::ethPeer)
        .findFirst();
  }

  private void refreshPeers() {
    final EthPeers peers = getEthContext().getEthPeers();
    // If we are at max connections, then refresh peers disconnecting one of the failed peers,
    // or the least useful

    if (peers.peerCount() >= peers.getMaxPeers()) {
      failedPeers.stream()
          .map(EthPeerImmutableAttributes::from)
          .filter(peer -> !peer.isDisconnected())
          .min(EthPeers.MOST_USEFUL_PEER)
          .or(() -> peers.streamAvailablePeers().min(EthPeers.MOST_USEFUL_PEER))
          .ifPresent(
              peer -> {
                LOG.atDebug()
                    .setMessage(
                        "Refresh peers disconnecting peer {} Waiting for better peers. Current {} of max {}")
                    .addArgument(peer.ethPeer().getLoggableId())
                    .addArgument(peers::peerCount)
                    .addArgument(peers::getMaxPeers)
                    .log();
                peer.ethPeer().disconnect(DisconnectReason.USELESS_PEER_BY_REPUTATION);
              });
    }
  }
}
