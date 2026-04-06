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
package org.hyperledger.besu.ethereum.p2p.peers;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import org.hyperledger.besu.ethereum.p2p.discovery.NodeIdentifier;
import org.hyperledger.besu.plugin.data.EnodeURL;
import org.hyperledger.besu.util.NetworkUtility;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.net.InetAddresses;
import com.google.common.primitives.Ints;
import org.apache.tuweni.bytes.Bytes;

public class EnodeURLImpl implements EnodeURL, NodeIdentifier {

  public static final int DEFAULT_LISTENING_PORT = 30303;
  public static final int DEFAULT_LISTENING_PORT_IPV6 = 30404;
  public static final int NODE_ID_SIZE = 64;
  private static final Pattern DISCPORT_QUERY_STRING_REGEX =
      Pattern.compile("^discport=([0-9]{1,5})$");
  private static final Pattern NODE_ID_PATTERN = Pattern.compile("^[0-9a-fA-F]{128}$");

  private final Bytes nodeId;
  private InetAddress ip;
  private final Optional<String> maybeHostname;
  private final Optional<Integer> listeningPort;
  private final Optional<Integer> discoveryPort;

  private EnodeURLImpl(
      final Bytes nodeId,
      final InetAddress address,
      final Optional<String> maybeHostname,
      final Optional<Integer> listeningPort,
      final Optional<Integer> discoveryPort) {
    checkArgument(
        nodeId.size() == NODE_ID_SIZE,
        "Invalid node id of length " + nodeId.size() + ".  Expected id of length: 64 bytes.");
    listeningPort.ifPresent(port -> NetworkUtility.checkPort(port, "listening"));
    discoveryPort.ifPresent(port -> NetworkUtility.checkPort(port, "discovery"));

    this.nodeId = nodeId;
    this.ip = address;
    this.maybeHostname = maybeHostname;
    this.listeningPort = listeningPort;
    this.discoveryPort = discoveryPort;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static EnodeURLImpl fromString(final String value) {
    return fromString(value, EnodeDnsConfiguration.dnsDisabled());
  }

  public static EnodeURLImpl fromString(
      final String value, final EnodeDnsConfiguration enodeDnsConfiguration) {
    try {
      checkStringArgumentNotEmpty(value, "Invalid empty value.");
      return fromURI(URI.create(value), enodeDnsConfiguration);
    } catch (final IllegalArgumentException e) {
      String message = "";
      if (enodeDnsConfiguration.dnsEnabled() && !enodeDnsConfiguration.updateEnabled()) {
        message =
            String.format(
                "Invalid IP address '%s' (or DNS query resolved an invalid IP). --Xdns-enabled is true but --Xdns-update-enabled flag is false.",
                value);
      } else {
        message =
            String.format(
                "Invalid enode URL syntax '%s'. Enode URL should have the following format 'enode://<node_id>@<ip>:<listening_port>[?discport=<discovery_port>]'.",
                value);
        if (e.getMessage() != null) {
          message += " " + e.getMessage();
        }
      }

      throw new IllegalArgumentException(message, e);
    }
  }

  public static EnodeURLImpl fromURI(final URI uri) {
    return fromURI(uri, EnodeDnsConfiguration.dnsDisabled());
  }

  public static EnodeURLImpl fromURI(
      final URI uri, final EnodeDnsConfiguration enodeDnsConfiguration) {
    checkArgument(uri != null, "URI cannot be null");
    checkStringArgumentNotEmpty(uri.getScheme(), "Missing 'enode' scheme.");
    checkStringArgumentNotEmpty(uri.getHost(), "Missing or invalid host or ip address.");
    checkStringArgumentNotEmpty(uri.getUserInfo(), "Missing node ID.");

    checkArgument(
        uri.getScheme().equalsIgnoreCase("enode"), "Invalid URI scheme (must equal \"enode\").");
    checkArgument(
        NODE_ID_PATTERN.matcher(uri.getUserInfo()).matches(),
        "Invalid node ID: node ID must have exactly 128 hexadecimal characters and should not include any '0x' hex prefix.");

    final Bytes id = Bytes.fromHexString(uri.getUserInfo());
    String host = uri.getHost();
    int tcpPort = uri.getPort();

    // Parse discport if it exists
    Optional<Integer> discoveryPort = Optional.empty();
    String query = uri.getQuery();
    if (query != null) {
      final Matcher discPortMatcher = DISCPORT_QUERY_STRING_REGEX.matcher(query);
      if (discPortMatcher.matches()) {
        discoveryPort = Optional.ofNullable(Ints.tryParse(discPortMatcher.group(1)));
      }
      checkArgument(discoveryPort.isPresent(), "Invalid discovery port: '" + query + "'.");
    } else {
      discoveryPort = Optional.of(tcpPort);
    }

    return builder()
        .ipAddress(host, enodeDnsConfiguration)
        .nodeId(id)
        .listeningPort(tcpPort)
        .discoveryPort(discoveryPort)
        .build();
  }

  private static void checkStringArgumentNotEmpty(final String argument, final String message) {
    checkArgument(argument != null && !argument.trim().isEmpty(), message);
  }

  public static boolean sameListeningEndpoint(
      final EnodeURL enodeA, final NodeIdentifier otherNode) {
    if (enodeA == null || (otherNode == null || otherNode.getIpV4Address().isEmpty())) {
      return false;
    }

    return Objects.equals(enodeA.getIp(), otherNode.getIpV4Address().get())
        && Objects.equals(enodeA.getListeningPort(), otherNode.getTcpListeningPort());
  }

  public static Bytes parseNodeId(final String nodeId) {
    int expectedSize = EnodeURLImpl.NODE_ID_SIZE * 2;
    if (nodeId.toLowerCase(Locale.ROOT).startsWith("0x")) {
      expectedSize += 2;
    }
    checkArgument(
        nodeId.length() == expectedSize,
        "Expected " + EnodeURLImpl.NODE_ID_SIZE + " bytes in " + nodeId);
    return Bytes.fromHexString(nodeId, NODE_ID_SIZE);
  }

  @Override
  public URI toURI() {

    final String uri =
        String.format(
            "enode://%s@%s:%d",
            nodeId.toUnprefixedHexString(),
            maybeHostname.orElse(InetAddresses.toUriString(getIp())),
            getListeningPortOrZero());
    final OptionalInt discPort = getDiscPortQueryParam();
    if (discPort.isPresent()) {
      return URI.create(uri + String.format("?discport=%d", discPort.getAsInt()));
    } else {
      return URI.create(uri);
    }
  }

  @Override
  public URI toURIWithoutDiscoveryPort() {
    final String uri =
        String.format(
            "enode://%s@%s:%d",
            nodeId.toUnprefixedHexString(),
            maybeHostname.orElse(InetAddresses.toUriString(getIp())),
            getListeningPortOrZero());

    return URI.create(uri);
  }

  /**
   * Returns the discovery port only if it differs from the listening port
   *
   * @return The port, as an optional.
   */
  private OptionalInt getDiscPortQueryParam() {
    final int listeningPort = getListeningPortOrZero();
    final int discoveryPort = getDiscoveryPortOrZero();
    if (listeningPort == discoveryPort) {
      return OptionalInt.empty();
    }
    return OptionalInt.of(discoveryPort);
  }

  public static URI asURI(final String url) {
    return asURI(url, EnodeDnsConfiguration.dnsDisabled());
  }

  public static URI asURI(final String url, final EnodeDnsConfiguration enodeDnsConfiguration) {
    return fromString(url, enodeDnsConfiguration).toURI();
  }

  @Override
  public Bytes getNodeId() {
    return nodeId;
  }

  @Override
  public String getIpAsString() {
    return getIp().getHostAddress();
  }

  /**
   * Get IP of the EnodeURL
   *
   * <p>If "dns" and "dns-update" are enabled -&gt; DNS lookup every time to have the IP up to date
   * and not to rely on an invalid cache
   *
   * <p>If the "dns" is enabled but "dns-update" is disabled -&gt; IP is retrieved only one time and
   * the hostname is no longer stored (maybeHostname is empty).
   *
   * @return ip
   */
  @Override
  public InetAddress getIp() {
    this.ip =
        maybeHostname
            .map(
                hostname -> {
                  try {
                    return InetAddress.getByName(hostname);
                  } catch (final UnknownHostException e) {
                    return ip;
                  }
                })
            .orElse(ip);
    return ip;
  }

  @Override
  public boolean isListening() {
    return listeningPort.isPresent();
  }

  @Override
  public boolean isRunningDiscovery() {
    return discoveryPort.isPresent();
  }

  @Override
  public Optional<Integer> getListeningPort() {
    return listeningPort;
  }

  @Override
  public int getListeningPortOrZero() {
    return listeningPort.orElse(0);
  }

  @Override
  public Optional<Integer> getDiscoveryPort() {
    return discoveryPort;
  }

  @Override
  public int getDiscoveryPortOrZero() {
    return discoveryPort.orElse(0);
  }

  @Override
  public String getHost() {
    final URI uriWithoutDiscoveryPort = toURIWithoutDiscoveryPort();
    String host = uriWithoutDiscoveryPort.getHost();
    if (host == null) {
      host = "";
      final String uriString = uriWithoutDiscoveryPort.toString();
      int indexOfAt = uriString.indexOf("@");
      if (indexOfAt > -1) {
        int lastIndexOfColon = uriString.lastIndexOf(":");
        if (lastIndexOfColon > indexOfAt) {
          host = uriString.substring(indexOfAt + 1, lastIndexOfColon);
        }
      }
    }
    return host;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final EnodeURL enodeURL = (EnodeURL) o;
    return Objects.equals(getNodeId(), enodeURL.getNodeId())
        && Objects.equals(getIp(), enodeURL.getIp())
        && Objects.equals(getListeningPort(), enodeURL.getListeningPort())
        && Objects.equals(getDiscoveryPort(), enodeURL.getDiscoveryPort());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getNodeId(), getIp(), getListeningPort(), getDiscoveryPort());
  }

  @Override
  public String toString() {
    return this.toURI().toString();
  }

  @Override
  public Optional<InetAddress> getIpV4Address() {
    return Optional.ofNullable(getIp());
  }

  @Override
  public Optional<Integer> getTcpListeningPort() {
    return getListeningPort();
  }

  @Override
  public Optional<Integer> getUdpDiscoveryPort() {
    return getDiscoveryPort();
  }

  @Override
  public Optional<InetAddress> getIpV6Address() {
    return Optional.empty();
  }

  @Override
  public Optional<Integer> getIpV6TcpListeningPort() {
    return Optional.empty();
  }

  @Override
  public Optional<Integer> getIpV6UdpDiscoveryPort() {
    return Optional.empty();
  }

  public static class Builder {

    private Bytes nodeId;
    private Optional<Integer> listeningPort;
    private Optional<Integer> discoveryPort;
    private Optional<String> maybeHostname = Optional.empty();
    private InetAddress ip;

    private Builder() {}

    public EnodeURLImpl build() {
      validate();
      return new EnodeURLImpl(nodeId, ip, maybeHostname, listeningPort, discoveryPort);
    }

    private void validate() {
      checkState(nodeId != null, "Node id must be configured.");
      checkState(listeningPort != null, "Listening port must be configured.");
      checkState(discoveryPort != null, "Discovery port must be configured.");
      checkState(ip != null, "Ip address must be configured.");
    }

    public Builder configureFromEnode(final EnodeURL enode) {
      return this.nodeId(enode.getNodeId())
          .listeningPort(enode.getListeningPort())
          .discoveryPort(enode.getDiscoveryPort())
          .ipAddress(enode.getIp());
    }

    public Builder nodeId(final Bytes nodeId) {
      this.nodeId = nodeId;
      return this;
    }

    public Builder nodeId(final byte[] nodeId) {
      this.nodeId = Bytes.wrap(nodeId);
      return this;
    }

    public Builder nodeId(final String nodeId) {
      this.nodeId = Bytes.fromHexString(nodeId);
      return this;
    }

    public Builder ipAddress(final InetAddress ip) {
      this.ip = ip;
      return this;
    }

    public Builder ipAddress(final String ip) {
      return ipAddress(ip, EnodeDnsConfiguration.dnsDisabled());
    }

    public Builder ipAddress(
        final String hostField, final EnodeDnsConfiguration enodeDnsConfiguration) {
      if (enodeDnsConfiguration.dnsEnabled()) {
        try {
          this.ip = InetAddress.getByName(hostField);
          if (enodeDnsConfiguration.updateEnabled()) {
            if (this.ip.isLoopbackAddress()) {
              this.ip = InetAddress.getLocalHost();
            }
            this.maybeHostname = Optional.of(this.ip.getHostName());
          }
        } catch (final UnknownHostException e) {
          if (!enodeDnsConfiguration.updateEnabled()) {
            throw new IllegalArgumentException("Invalid ip address or hostname.");
          } else {
            this.ip = InetAddresses.forString("127.0.0.1");
          }
        }
      } else if (InetAddresses.isUriInetAddress(hostField)) {
        this.ip = InetAddresses.forUriString(hostField);
      } else if (InetAddresses.isInetAddress(hostField)) {
        this.ip = InetAddresses.forString(hostField);
      } else {
        throw new IllegalArgumentException("Invalid ip address.");
      }
      return this;
    }

    public Builder discoveryAndListeningPorts(final int listeningAndDiscoveryPorts) {
      listeningPort(listeningAndDiscoveryPorts);
      discoveryPort(listeningAndDiscoveryPorts);
      return this;
    }

    public Builder disableListening() {
      this.listeningPort = Optional.empty();
      return this;
    }

    public Builder disableDiscovery() {
      this.discoveryPort = Optional.empty();
      return this;
    }

    public Builder useDefaultPorts() {
      discoveryAndListeningPorts(EnodeURLImpl.DEFAULT_LISTENING_PORT);
      return this;
    }

    /**
     * An optional listening port value. If the value is empty of equal to 0, the listening port
     * will be empty - indicating the corresponding node is not listening.
     *
     * @param maybeListeningPort If non-empty represents the port to listen on, if empty means the
     *     node is not listening
     * @return The modified builder
     */
    public Builder listeningPort(final Optional<Integer> maybeListeningPort) {
      this.listeningPort = maybeListeningPort.filter(port -> port != 0);
      return this;
    }

    /**
     * An listening port value. A value of 0 means the node is not listening.
     *
     * @param listeningPort If non-zero, represents the port on which to listen for connections. A
     *     value of 0 means the node is not listening for connections.
     * @return The modified builder
     */
    public Builder listeningPort(final int listeningPort) {
      return listeningPort(Optional.of(listeningPort));
    }

    /**
     * The port on which to listen for discovery messages. A value that is empty or equal to 0,
     * indicates that the node is not listening for discovery messages.
     *
     * @param maybeDiscoveryPort If non-empty and non-zero, represents the port on which to listen
     *     for discovery messages. Otherwise, indicates that the node is not running discovery.
     * @return The modified builder
     */
    public Builder discoveryPort(final Optional<Integer> maybeDiscoveryPort) {
      this.discoveryPort = maybeDiscoveryPort.filter(port -> port != 0);
      return this;
    }

    /**
     * The port on which to listen for discovery messages. A value that is equal to 0, indicates
     * that the node is not listening for discovery messages.
     *
     * @param discoveryPort If non-zero, represents the port on which to listen for discovery
     *     messages. Otherwise, indicates that the node is not running discovery.
     * @return The modified builder
     */
    public Builder discoveryPort(final int discoveryPort) {
      return discoveryPort(Optional.of(discoveryPort));
    }
  }
}
