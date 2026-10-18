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
package org.hyperledger.besu.ethereum.eth.sync.snap2.net;

import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Sends sync requests to peers. */
public interface RequestSender {

  /**
   * A decoded response and the peer that sent it.
   *
   * @param value the decoded response
   * @param peer the peer
   * @param <T> the response type
   */
  record Response<T>(T value, EthPeer peer) {}

  /** Sends a request to one peer, never one of {@code excluded}. */
  <T> CompletableFuture<Response<T>> submit(SyncRequest<T> request, Set<EthPeer> excluded);

  /** Sends a request to one peer. */
  default <T> CompletableFuture<Response<T>> submit(final SyncRequest<T> request) {
    return submit(request, Set.of());
  }

  /** Sends a request until a peer answers it correctly, switching peer after each failure. */
  <T> CompletableFuture<Response<T>> submitWithRetries(SyncRequest<T> request, int attempts);

  /** Sends a request the sync cannot do without, with the default number of attempts. */
  default <T> CompletableFuture<Response<T>> submitWithRetries(final SyncRequest<T> request) {
    return submitWithRetries(request, 10);
  }

  /** Returns the number of usable peers for a protocol. */
  int peerCount(Protocol protocol);

  /** Returns the highest chain height announced by a connected peer, 0 if unknown. */
  default long bestPeerHeight() {
    return 0;
  }

  /** Returns the request capacity of the usable peers of a protocol. */
  int capacity(Protocol protocol);

  /** Returns the executor for CPU work that follows a response. */
  Executor workers();

  /** Stops sending requests. */
  default void stop() {}
}
