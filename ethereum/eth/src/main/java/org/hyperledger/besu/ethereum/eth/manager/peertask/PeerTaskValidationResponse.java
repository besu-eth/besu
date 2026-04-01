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
package org.hyperledger.besu.ethereum.eth.manager.peertask;

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;

import java.util.Optional;

public enum PeerTaskValidationResponse {
  NO_RESULTS_RETURNED(null, false),
  TOO_MANY_RESULTS_RETURNED(null, true),
  RESULTS_DO_NOT_MATCH_QUERY(null, true),
  NON_SEQUENTIAL_HEADERS_RETURNED(
      DisconnectMessage.DisconnectReason.BREACH_OF_PROTOCOL_NON_SEQUENTIAL_HEADERS, true),
  RESULTS_VALID_AND_GOOD(null, false),
  INVALID_RECEIPT_RETURNED(DisconnectMessage.DisconnectReason.INVALID_RECEIPT_RECEIVED, true);
  private final Optional<DisconnectMessage.DisconnectReason> disconnectReason;
  private final boolean recordUselessResponse;

  PeerTaskValidationResponse(
      final DisconnectMessage.DisconnectReason disconnectReason,
      final boolean recordUselessResponse) {
    this.disconnectReason = Optional.ofNullable(disconnectReason);
    this.recordUselessResponse = recordUselessResponse;
  }

  public Optional<DisconnectMessage.DisconnectReason> getDisconnectReason() {
    return disconnectReason;
  }

  public boolean recordUselessResponse() {
    return recordUselessResponse;
  }
}
