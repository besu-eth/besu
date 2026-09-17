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

import org.hyperledger.besu.ethereum.eth.manager.RequestManager.ResponseStream;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.NoAvailablePeersException;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.PeerDisconnectedException;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection.PeerNotConnected;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

public class PendingPeerRequest {
  private final EthPeers ethPeers;
  private final PeerRequest request;
  private final CompletableFuture<ResponseStream> result = new CompletableFuture<>();
  private final long minimumBlockNumber;
  private final Optional<EthPeer> peer;

  PendingPeerRequest(
      final EthPeers ethPeers,
      final PeerRequest request,
      final long minimumBlockNumber,
      final Optional<EthPeer> peer) {
    this.ethPeers = ethPeers;
    this.request = request;
    this.minimumBlockNumber = minimumBlockNumber;
    this.peer = peer;
  }

  /**
   * Attempts to find an available peer and execute the peer request.
   *
   * @return true if the request should be removed from the pending list, otherwise false.
   */
  public boolean attemptExecution() {
    if (result.isDone()) {
      return true;
    }
    final Optional<EthPeer> maybePeer = getPeerToUse();
    if (maybePeer.isEmpty()) {
      // No peers have the required height.
      result.completeExceptionally(NoAvailablePeersException.WITHOUT_STACKTRACE);
      return true;
    } else {
      // At least one peer has the required height, but we are not able to use it if it's busy
      final Optional<EthPeer> maybePeerWithCapacity =
          maybePeer.filter(EthPeer::hasAvailableRequestCapacity);

      maybePeerWithCapacity.ifPresent(this::sendRequest);
      return maybePeerWithCapacity.isPresent();
    }
  }

  private synchronized void sendRequest(final EthPeer peer) {
    // Recheck if we should send the request now we're inside the synchronized block
    if (!result.isDone()) {
      try {
        final ResponseStream responseStream = request.sendRequest(peer);
        result.complete(responseStream);
      } catch (final PeerNotConnected e) {
        result.completeExceptionally(new PeerDisconnectedException(peer));
      }
    }
  }

  private Optional<EthPeer> getPeerToUse() {
    // return the assigned peer if still valid, otherwise switch to another peer
    return peer.filter(p -> !p.isDisconnected()).isPresent()
        ? peer
        : ethPeers
            .streamAvailablePeers()
            .filter(peer -> peer.estimatedChainHeight() >= minimumBlockNumber)
            .filter(request::isEthPeerSuitable)
            .min(EthPeers.LEAST_TO_MOST_BUSY)
            .map(EthPeerImmutableAttributes::ethPeer);
  }

  /**
   * Register callbacks for when the request is made or
   *
   * @param onSuccess handler for when a peer becomes available and the request is sent
   * @param onError handler for when there is no peer with sufficient height or the request fails to
   *     send
   */
  public void then(final Consumer<ResponseStream> onSuccess, final Consumer<Throwable> onError) {
    result.whenComplete(
        (result, error) -> {
          if (error != null) {
            onError.accept(error);
          } else {
            onSuccess.accept(result);
          }
        });
  }

  /**
   * Abort this request.
   *
   * @return the response stream if the request has already been sent, otherwise empty.
   */
  public synchronized Optional<ResponseStream> abort() {
    try {
      result.cancel(false);
      return Optional.ofNullable(result.getNow(null));
    } catch (final CancellationException | CompletionException e) {
      return Optional.empty();
    }
  }

  public Optional<EthPeer> getAssignedPeer() {
    return peer;
  }
}
