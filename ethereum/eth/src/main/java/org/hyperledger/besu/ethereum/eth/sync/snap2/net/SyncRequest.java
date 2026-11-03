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
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;

import java.util.Optional;

/**
 * A request sent to one peer through the {@link RequestScheduler}.
 *
 * @param <T> the decoded response
 */
public interface SyncRequest<T> {

  /** Protocol a request is sent on. */
  enum Protocol {
    ETH,
    /** Any snap version: the range requests are the same in snap/1 and snap/2. */
    SNAP,
    /** Only snap/2, as the block access lists; on the snap lane of a peer. */
    SNAP2
  }

  /** Dispatch priority, most urgent first. */
  enum Priority {
    /** Block access lists and headers that block a pivot move. */
    PIVOT(0),
    /** World state ranges and bytecodes. */
    STATE(1),
    /** Block headers below the pivot, which the bodies wait for. */
    HEADERS(2),
    /** Block bodies and receipts, which nothing else waits for. */
    BODIES(3);

    private final int rank;

    Priority(final int rank) {
      this.rank = rank;
    }

    /** Returns the rank of the priority, the lowest dispatched first. */
    public int rank() {
      return rank;
    }
  }

  Protocol protocol();

  /** Returns the protocol used with a given peer, by default {@link #protocol()}. */
  default Protocol protocolFor(final EthPeer peer) {
    return protocol();
  }

  Priority priority();

  /** Short name used in logs and metrics. */
  String name();

  /** Builds the request message for the peer it is dispatched to. */
  MessageData message(EthPeer peer);

  /**
   * Decodes and validates the response. Runs on a worker thread.
   *
   * @throws InvalidResponseException if the response is wrong
   * @throws EmptyResponseException if the peer had nothing to serve
   */
  T decode(MessageData response, EthPeer peer);

  /** Returns the chain height a peer must have announced to serve this request, 0 if any. */
  default long minimumPeerHeight() {
    return 0;
  }

  /** Returns whether the peer can serve this request, beyond protocol support. */
  default boolean canServe(final EthPeer peer) {
    return true;
  }

  /** The peer had nothing to serve; it may have pruned the data. */
  final class EmptyResponseException extends RuntimeException {
    public EmptyResponseException(final String message) {
      super(message, null, false, false);
    }
  }

  /** The response is malformed or does not match what was requested. */
  final class InvalidResponseException extends RuntimeException {
    private final DisconnectReason disconnect;

    /** A wrong response, also a malformed message that disconnects the peer if so asked. */
    public InvalidResponseException(final String message, final boolean disconnect) {
      this(
          message,
          disconnect ? DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED : null);
    }

    /** A wrong response that disconnects the peer for the given reason, unless null. */
    public InvalidResponseException(final String message, final DisconnectReason disconnect) {
      super(message, null, false, false);
      this.disconnect = disconnect;
    }

    /** Returns why the peer deserves a disconnection, or empty for a lower score only. */
    public Optional<DisconnectReason> disconnectReason() {
      return Optional.ofNullable(disconnect);
    }
  }
}
