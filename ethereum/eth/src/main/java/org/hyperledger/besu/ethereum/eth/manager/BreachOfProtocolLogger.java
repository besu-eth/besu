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
package org.hyperledger.besu.ethereum.eth.manager;

import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs inbound breach-of-protocol disconnects. Isolated in its own class so that operators can
 * selectively enable DEBUG logging for this class without affecting other log output:
 *
 * <pre>{@code
 * <Logger name="org.hyperledger.besu.ethereum.eth.manager.BreachOfProtocolLogger" level="DEBUG"/>
 * }</pre>
 */
public class BreachOfProtocolLogger {
  private static final Logger LOG = LoggerFactory.getLogger(BreachOfProtocolLogger.class);

  private BreachOfProtocolLogger() {}

  /**
   * Logs an inbound breach-of-protocol disconnect at DEBUG level. Enable DEBUG on this class in
   * log4j configuration to surface these events.
   *
   * @param connection the peer connection that was disconnected
   * @param reason the disconnect reason
   */
  public static void log(final PeerConnection connection, final DisconnectReason reason) {
    LOG.atDebug()
        .setMessage("Inbound breach-of-protocol disconnect - reason: {} - peer: {} ({})")
        .addArgument(reason::toString)
        .addArgument(() -> connection.getPeer().getLoggableId())
        .addArgument(() -> connection.getPeerInfo().getClientId())
        .log();
  }
}
