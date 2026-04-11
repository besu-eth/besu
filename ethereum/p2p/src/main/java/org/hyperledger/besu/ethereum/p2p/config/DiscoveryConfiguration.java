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

import org.hyperledger.besu.ethereum.p2p.discovery.NodeIdentifier;
import org.hyperledger.besu.ethereum.p2p.discovery.dns.EthereumNodeRecord;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.util.NetworkUtility;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class DiscoveryConfiguration {

  private boolean enabled = true;
  private String bindHost = NetworkUtility.INADDR_ANY;
  private int bindPort = 30303;
  private String advertisedHost = "127.0.0.1";
  private int bucketSize = 16;
  private List<EnodeURLImpl> enodeBootnodes = new ArrayList<>();
  private List<EthereumNodeRecord> enrBootnodes = new ArrayList<>();
  private String dnsDiscoveryURL;
  private boolean discoveryV5Enabled = false;
  private boolean filterOnEnrForkId = NetworkingConfiguration.DEFAULT_FILTER_ON_ENR_FORK_ID;
  private boolean includeBootnodesOnPeerRefresh = true;
  private Optional<String> bindHostIpv6 = Optional.empty();
  private int bindPortIpv6 = EnodeURLImpl.DEFAULT_LISTENING_PORT_IPV6;
  private Optional<String> advertisedHostIpv6 = Optional.empty();
  private boolean preferIpv6Outbound = false;
  private int discV5DiscoveryIntervalSeconds = 1;
  private int discV5DiscoveryTimeoutSeconds = 30;
  private double discV5MinimumPeerRatio = 0.8;

  public static DiscoveryConfiguration create() {
    return new DiscoveryConfiguration();
  }

  public static void assertValidBootnodes(final List<? extends NodeIdentifier> bootnodes) {
    final List<? extends NodeIdentifier> invalidEnodes =
        bootnodes.stream()
            .filter(e -> e.getUdpDiscoveryPort().isEmpty() && e.getIpV6UdpDiscoveryPort().isEmpty())
            .toList();

    if (!invalidEnodes.isEmpty()) {
      final String invalidBootnodes =
          invalidEnodes.stream().map(NodeIdentifier::toString).collect(Collectors.joining(","));
      final String errorMsg =
          "Bootnodes must have discovery enabled. Invalid bootnodes: " + invalidBootnodes + ".";
      throw new IllegalArgumentException(errorMsg);
    }
  }

  public String getBindHost() {
    return bindHost;
  }

  public DiscoveryConfiguration setBindHost(final String bindHost) {
    this.bindHost = bindHost;
    return this;
  }

  public int getBindPort() {
    return bindPort;
  }

  public DiscoveryConfiguration setBindPort(final int bindPort) {
    this.bindPort = bindPort;
    return this;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public DiscoveryConfiguration setEnabled(final boolean enabled) {
    this.enabled = enabled;
    return this;
  }

  public List<EnodeURLImpl> getEnodeBootnodes() {
    return enodeBootnodes;
  }

  public DiscoveryConfiguration setEnodeBootnodes(final List<EnodeURLImpl> enodeBootnodes) {
    assertValidBootnodes(enodeBootnodes);
    this.enodeBootnodes = enodeBootnodes;
    return this;
  }

  public List<EthereumNodeRecord> getEnrBootnodes() {
    return enrBootnodes;
  }

  public DiscoveryConfiguration setEnrBootnodes(final List<EthereumNodeRecord> enrBootnodes) {
    assertValidBootnodes(enrBootnodes);
    this.enrBootnodes = enrBootnodes;
    return this;
  }

  public List<? extends NodeIdentifier> getBootnodeIdentifiers() {
    return discoveryV5Enabled ? enrBootnodes : enodeBootnodes;
  }

  public boolean getIncludeBootnodesOnPeerRefresh() {
    return includeBootnodesOnPeerRefresh;
  }

  public DiscoveryConfiguration setIncludeBootnodesOnPeerRefresh(
      final boolean includeBootnodesOnPeerRefresh) {
    this.includeBootnodesOnPeerRefresh = includeBootnodesOnPeerRefresh;
    return this;
  }

  public String getAdvertisedHost() {
    return advertisedHost;
  }

  public DiscoveryConfiguration setAdvertisedHost(final String advertisedHost) {
    this.advertisedHost = advertisedHost;
    return this;
  }

  public int getBucketSize() {
    return bucketSize;
  }

  public DiscoveryConfiguration setBucketSize(final int bucketSize) {
    this.bucketSize = bucketSize;
    return this;
  }

  public String getDNSDiscoveryURL() {
    return dnsDiscoveryURL;
  }

  public DiscoveryConfiguration setDnsDiscoveryURL(final String dnsDiscoveryURL) {
    this.dnsDiscoveryURL = dnsDiscoveryURL;
    return this;
  }

  public void setDiscoveryV5Enabled(final boolean discoveryV5Enabled) {
    this.discoveryV5Enabled = discoveryV5Enabled;
  }

  public boolean isDiscoveryV5Enabled() {
    return discoveryV5Enabled;
  }

  public void setFilterOnEnrForkId(final boolean filterOnEnrForkId) {
    this.filterOnEnrForkId = filterOnEnrForkId;
  }

  public boolean isFilterOnEnrForkIdEnabled() {
    return filterOnEnrForkId;
  }

  public Optional<String> getBindHostIpv6() {
    return bindHostIpv6;
  }

  public DiscoveryConfiguration setBindHostIpv6(final Optional<String> bindHostIpv6) {
    this.bindHostIpv6 = bindHostIpv6;
    return this;
  }

  public int getBindPortIpv6() {
    return bindPortIpv6;
  }

  public DiscoveryConfiguration setBindPortIpv6(final int bindPortIpv6) {
    this.bindPortIpv6 = bindPortIpv6;
    return this;
  }

  public Optional<String> getAdvertisedHostIpv6() {
    return advertisedHostIpv6;
  }

  public DiscoveryConfiguration setAdvertisedHostIpv6(final Optional<String> advertisedHostIpv6) {
    this.advertisedHostIpv6 = advertisedHostIpv6;
    return this;
  }

  public boolean isDualStackEnabled() {
    return bindHostIpv6.isPresent();
  }

  public boolean isPreferIpv6Outbound() {
    return preferIpv6Outbound;
  }

  public DiscoveryConfiguration setPreferIpv6Outbound(final boolean preferIpv6Outbound) {
    this.preferIpv6Outbound = preferIpv6Outbound;
    return this;
  }

  public int getDiscV5DiscoveryIntervalSeconds() {
    return discV5DiscoveryIntervalSeconds;
  }

  public DiscoveryConfiguration setDiscV5DiscoveryIntervalSeconds(
      final int discV5DiscoveryIntervalSeconds) {
    this.discV5DiscoveryIntervalSeconds = discV5DiscoveryIntervalSeconds;
    return this;
  }

  public int getDiscV5DiscoveryTimeoutSeconds() {
    return discV5DiscoveryTimeoutSeconds;
  }

  public DiscoveryConfiguration setDiscV5DiscoveryTimeoutSeconds(
      final int discV5DiscoveryTimeoutSeconds) {
    this.discV5DiscoveryTimeoutSeconds = discV5DiscoveryTimeoutSeconds;
    return this;
  }

  public double getDiscV5MinimumPeerRatio() {
    return discV5MinimumPeerRatio;
  }

  public DiscoveryConfiguration setDiscV5MinimumPeerRatio(final double discV5MinimumPeerRatio) {
    this.discV5MinimumPeerRatio = discV5MinimumPeerRatio;
    return this;
  }

  @Override
  public boolean equals(final Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof DiscoveryConfiguration)) {
      return false;
    }
    final DiscoveryConfiguration that = (DiscoveryConfiguration) o;
    return enabled == that.enabled
        && bindPort == that.bindPort
        && bucketSize == that.bucketSize
        && bindPortIpv6 == that.bindPortIpv6
        && Objects.equals(bindHost, that.bindHost)
        && Objects.equals(advertisedHost, that.advertisedHost)
        && Objects.equals(enodeBootnodes, that.enodeBootnodes)
        && Objects.equals(enrBootnodes, that.enrBootnodes)
        && Objects.equals(dnsDiscoveryURL, that.dnsDiscoveryURL)
        && Objects.equals(bindHostIpv6, that.bindHostIpv6)
        && Objects.equals(advertisedHostIpv6, that.advertisedHostIpv6)
        && preferIpv6Outbound == that.preferIpv6Outbound;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        enabled,
        bindHost,
        bindPort,
        advertisedHost,
        bucketSize,
        enodeBootnodes,
        enrBootnodes,
        dnsDiscoveryURL,
        bindHostIpv6,
        bindPortIpv6,
        advertisedHostIpv6,
        preferIpv6Outbound);
  }

  @Override
  public String toString() {
    return "DiscoveryConfiguration{"
        + "enabled="
        + enabled
        + ", bindHost='"
        + bindHost
        + '\''
        + ", bindPort="
        + bindPort
        + ", advertisedHost='"
        + advertisedHost
        + '\''
        + ", bucketSize="
        + bucketSize
        + ", enodeBootnodes="
        + enodeBootnodes
        + ", enrBootnodes="
        + enrBootnodes
        + ", dnsDiscoveryURL="
        + dnsDiscoveryURL
        + ", isDiscoveryV5Enabled="
        + discoveryV5Enabled
        + ", isFilterOnEnrForkIdEnabled="
        + filterOnEnrForkId
        + ", bindHostIpv6="
        + bindHostIpv6
        + ", bindPortIpv6="
        + bindPortIpv6
        + ", advertisedHostIpv6="
        + advertisedHostIpv6
        + ", preferIpv6Outbound="
        + preferIpv6Outbound
        + '}';
  }
}
