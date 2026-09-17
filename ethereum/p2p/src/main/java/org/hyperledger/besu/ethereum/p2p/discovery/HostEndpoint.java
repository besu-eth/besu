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
package org.hyperledger.besu.ethereum.p2p.discovery;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.util.NetworkUtility;

/**
 * Groups the network endpoint information (host address and ports) for a discovery node.
 *
 * <p>Instances are effectively immutable once constructed. Use {@link #withDiscoveryPort(int)} to
 * obtain a copy with a different UDP discovery port.
 */
public class HostEndpoint {
  private final String host;
  private final int discoveryPort;
  private final int tcpPort;
  private final boolean isIpv4;

  /**
   * Creates a new {@code HostEndpoint}, deriving the address family from the given host.
   *
   * @param host the advertised IP address (IPv4 or IPv6); hostnames are rejected
   * @param discoveryPort the UDP discovery port (0 for ephemeral)
   * @param tcpPort the TCP listening port
   * @throws IllegalArgumentException if {@code host} is not a valid IP address
   */
  public HostEndpoint(final String host, final int discoveryPort, final int tcpPort) {
    this(host, discoveryPort, tcpPort, NetworkUtility.isIpV4Address(host));
  }

  private HostEndpoint(
      final String host, final int discoveryPort, final int tcpPort, final boolean isIpv4) {
    // host is already meant to be filtered at cli validation level to be an IP address
    checkArgument(
        NetworkUtility.isIpV4Address(host) || NetworkUtility.isIpV6Address(host),
        "host must be an IP address, not a hostname: %s",
        host);
    this.host = host;
    this.discoveryPort = discoveryPort;
    this.tcpPort = tcpPort;
    this.isIpv4 = isIpv4;
  }

  /**
   * Returns a copy of this endpoint with the given UDP discovery port, preserving all other fields.
   *
   * @param discoveryPort the new UDP discovery port
   * @return a new {@code HostEndpoint} with the updated port
   */
  public HostEndpoint withDiscoveryPort(final int discoveryPort) {
    return new HostEndpoint(this.host, discoveryPort, this.tcpPort, this.isIpv4);
  }

  /** Returns the UDP discovery port. */
  public int discoveryPort() {
    return discoveryPort;
  }

  /** Returns the TCP listening port. */
  public int tcpPort() {
    return tcpPort;
  }

  /** Returns the advertised IP address. */
  public String host() {
    return host;
  }

  /** Returns {@code true} if the host address is IPv4, {@code false} if IPv6. */
  public boolean isIpv4() {
    return isIpv4;
  }
}
