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
package org.hyperledger.besu.nat.core.domain;

import org.jspecify.annotations.Nullable;

/** This class describes a NAT configuration. */
public class NatPortMapping {

  private final NatServiceType natServiceType;
  private final NetworkProtocol protocol;
  private final String internalHost;
  private final @Nullable String remoteHost;
  private final int externalPort;
  private final int internalPort;

  /**
   * Instantiates a new Nat port mapping.
   *
   * @param natServiceType the nat service type
   * @param protocol the protocol
   * @param internalHost the internal host
   * @param remoteHost the remote host
   * @param externalPort the external port
   * @param internalPort the internal port
   */
  public NatPortMapping(
      final NatServiceType natServiceType,
      final NetworkProtocol protocol,
      final String internalHost,
      final @Nullable String remoteHost,
      final int externalPort,
      final int internalPort) {
    this.natServiceType = natServiceType;
    this.protocol = protocol;
    this.internalHost = internalHost;
    this.remoteHost = remoteHost;
    this.externalPort = externalPort;
    this.internalPort = internalPort;
  }

  /**
   * Gets nat service type.
   *
   * @return the nat service type
   */
  public NatServiceType getNatServiceType() {
    return natServiceType;
  }

  /**
   * Gets protocol.
   *
   * @return the protocol
   */
  public NetworkProtocol getProtocol() {
    return protocol;
  }

  /**
   * Gets internal host.
   *
   * @return the internal host
   */
  public String getInternalHost() {
    return internalHost;
  }

  /**
   * Gets remote host.
   *
   * @return the remote host
   */
  public @Nullable String getRemoteHost() {
    return remoteHost;
  }

  /**
   * Gets external port.
   *
   * @return the external port
   */
  public int getExternalPort() {
    return externalPort;
  }

  /**
   * Gets internal port.
   *
   * @return the internal port
   */
  public int getInternalPort() {
    return internalPort;
  }

  @Override
  public String toString() {
    return String.format(
        "[%s - %s] %s:%d ==> %s:%d",
        natServiceType, protocol, internalHost, internalPort, remoteHost, externalPort);
  }
}
