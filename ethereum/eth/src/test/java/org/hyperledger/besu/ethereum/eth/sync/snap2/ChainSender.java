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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.ChainState;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.RespondingEthPeer;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

/**
 * Answers the eth requests of the sync on the calling thread from a chain, as a peer that holds it
 * does.
 */
final class ChainSender implements RequestSender {

  private final RespondingEthPeer.Responder responder;
  private final EthPeer peer = mock(EthPeer.class);
  private final List<MessageData> requests = new ArrayList<>();
  // the requests that fail instead of being answered, and how
  private Predicate<MessageData> failing = message -> false;
  private RuntimeException failure = new IllegalStateException("request failed");

  ChainSender(final Blockchain chain) {
    responder = RespondingEthPeer.blockchainResponder(chain);
    when(peer.getAgreedCapabilities()).thenReturn(Set.of(EthProtocol.LATEST));
    // the tasks report the height they see to the peer
    when(peer.chainState()).thenReturn(new ChainState());
  }

  /** Fails the requests the predicate matches, from now on. */
  void fail(final Predicate<MessageData> requests) {
    fail(requests, new IllegalStateException("request failed"));
  }

  /** Fails the requests the predicate matches with this failure, from now on. */
  void fail(final Predicate<MessageData> requests, final RuntimeException failure) {
    failing = requests;
    this.failure = failure;
  }

  /** Returns every request answered or failed, in order. */
  List<MessageData> requests() {
    return requests;
  }

  @Override
  public <T> CompletableFuture<Response<T>> submit(
      final SyncRequest<T> request, final Set<EthPeer> excluded) {
    final MessageData message = request.message(peer);
    requests.add(message);
    if (failing.test(message)) {
      return CompletableFuture.failedFuture(failure);
    }
    // an invalid answer fails the request, as with the scheduler
    try {
      final MessageData answer = responder.respond(EthProtocol.LATEST, peer, message).orElseThrow();
      return CompletableFuture.completedFuture(new Response<>(request.decode(answer, peer), peer));
    } catch (final RuntimeException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  @Override
  public <T> CompletableFuture<Response<T>> submitWithRetries(
      final SyncRequest<T> request, final int attempts) {
    return submit(request);
  }

  @Override
  public int peerCount(final Protocol protocol) {
    return 1;
  }

  @Override
  public int capacity(final Protocol protocol) {
    return 1;
  }

  @Override
  public Executor workers() {
    return Runnable::run;
  }
}
