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
package org.hyperledger.besu.ethereum.p2p.config;

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;
import org.hyperledger.besu.util.NetworkUtility;
import org.hyperledger.besu.util.number.ByteUnits;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class RlpxConfiguration {
  public static final int DEFAULT_MAX_MESSAGE_SIZE = 10 * ByteUnits.MEGABYTE;
  public static final float DEFAULT_FRACTION_REMOTE_CONNECTIONS_ALLOWED = 0.6f;
  private String clientId = "TestClient/1.0.0";
  private String bindHost = NetworkUtility.INADDR_ANY;
  private int bindPort = 30303;
  private Optional<String> bindHostIpv6 = Optional.empty();
  private Optional<Integer> bindPortIpv6 = Optional.empty();
  private int maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
  private List<SubProtocol> supportedProtocols = Collections.emptyList();

  public static RlpxConfiguration create() {
    return new RlpxConfiguration();
  }

  public RlpxConfiguration setSupportedProtocols(final SubProtocol... supportedProtocols) {
    this.supportedProtocols = Arrays.asList(supportedProtocols);
    return this;
  }

  public RlpxConfiguration setSupportedProtocols(final List<SubProtocol> supportedProtocols) {
    this.supportedProtocols = supportedProtocols;
    return this;
  }

  public List<SubProtocol> getSupportedProtocols() {
    return supportedProtocols;
  }

  public String getBindHost() {
    return bindHost;
  }

  public RlpxConfiguration setBindHost(final String bindHost) {
    this.bindHost = bindHost;
    return this;
  }

  public int getBindPort() {
    return bindPort;
  }

  public RlpxConfiguration setBindPort(final int bindPort) {
    this.bindPort = bindPort;
    return this;
  }

  public Optional<String> getBindHostIpv6() {
    return bindHostIpv6;
  }

  public RlpxConfiguration setBindHostIpv6(final Optional<String> bindHostIpv6) {
    this.bindHostIpv6 = bindHostIpv6;
    return this;
  }

  public Optional<Integer> getBindPortIpv6() {
    return bindPortIpv6;
  }

  public RlpxConfiguration setBindPortIpv6(final Optional<Integer> bindPortIpv6) {
    this.bindPortIpv6 = bindPortIpv6;
    return this;
  }

  public boolean isDualStackEnabled() {
    return bindHostIpv6.isPresent() && bindPortIpv6.isPresent();
  }

  public int getMaxMessageSize() {
    return maxMessageSize;
  }

  public RlpxConfiguration setMaxMessageSize(final int maxMessageSize) {
    this.maxMessageSize = maxMessageSize;
    return this;
  }

  public String getClientId() {
    return clientId;
  }

  public RlpxConfiguration setClientId(final String clientId) {
    this.clientId = clientId;
    return this;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RlpxConfiguration that = (RlpxConfiguration) o;
    return bindPort == that.bindPort
        && Objects.equals(bindHost, that.bindHost)
        && Objects.equals(bindHostIpv6, that.bindHostIpv6)
        && Objects.equals(bindPortIpv6, that.bindPortIpv6);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bindHost, bindPort, bindHostIpv6, bindPortIpv6);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("RlpxConfiguration{");
    sb.append("bindHost='").append(bindHost).append('\'');
    sb.append(", bindPort=").append(bindPort);
    bindHostIpv6.ifPresent(h -> sb.append(", bindHostIpv6='").append(h).append('\''));
    bindPortIpv6.ifPresent(p -> sb.append(", bindPortIpv6=").append(p));
    sb.append('}');
    return sb.toString();
  }
}
