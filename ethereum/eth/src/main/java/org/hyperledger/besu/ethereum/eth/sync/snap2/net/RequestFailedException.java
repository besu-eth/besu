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

import java.util.Optional;

/** A request attempt that did not produce a valid response. */
public final class RequestFailedException extends RuntimeException {

  /** Why the attempt failed. */
  public enum Reason {
    /** No connected peer could serve the request in time, or only peers it left out. */
    NO_PEER,
    /** The peer did not answer in time. */
    TIMEOUT,
    /** The peer disconnected or the message could not be sent. */
    DISCONNECTED,
    /** The peer had nothing to serve. */
    EMPTY,
    /** The response was wrong. */
    INVALID,
    /** The scheduler stopped. */
    STOPPED
  }

  private final Reason reason;
  private final transient EthPeer peer;

  public RequestFailedException(final Reason reason, final EthPeer peer, final String message) {
    super(message, null, false, false);
    this.reason = reason;
    this.peer = peer;
  }

  public Reason reason() {
    return reason;
  }

  /** Returns the peer the attempt was sent to, if any. */
  public Optional<EthPeer> peer() {
    return Optional.ofNullable(peer);
  }

  /** Extracts the failure from a completion exception. */
  public static Optional<RequestFailedException> from(final Throwable error) {
    Throwable current = error;
    while (current != null) {
      if (current instanceof RequestFailedException failure) {
        return Optional.of(failure);
      }
      current = current.getCause();
    }
    return Optional.empty();
  }
}
