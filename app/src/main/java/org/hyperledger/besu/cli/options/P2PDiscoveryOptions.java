/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.cli.options;

import org.hyperledger.besu.cli.DefaultCommandValues;
import org.hyperledger.besu.cli.converter.PercentageConverter;
import org.hyperledger.besu.cli.converter.SubnetCidrConverter;
import org.hyperledger.besu.cli.util.CommandLineUtils;
import org.hyperledger.besu.ethereum.p2p.discovery.P2PDiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.util.NetworkUtility;
import org.hyperledger.besu.util.number.Fraction;
import org.hyperledger.besu.util.number.Percentage;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.net.InetAddresses;
import inet.ipaddr.IPAddress;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/** Command line options for configuring P2P discovery on the node. */
public class P2PDiscoveryOptions implements CLIOptions<P2PDiscoveryConfiguration> {

  private static final Logger LOG = LoggerFactory.getLogger(P2PDiscoveryOptions.class);

  /** Functional interface for checking if a network interface is available. */
  @FunctionalInterface
  public interface NetworkInterfaceChecker {

    /**
     * Checks if the provided network interface is available.
     *
     * @param p2pInterface The network interface to check.
     * @return True if the network interface is available, false otherwise.
     * @throws UnknownHostException If the host is unknown.
     * @throws SocketException If there is an error with the socket.
     */
    boolean isNetworkInterfaceAvailable(final String p2pInterface)
        throws UnknownHostException, SocketException;
  }

  /** Default constructor */
  public P2PDiscoveryOptions() {}

  // Public IP stored to prevent having to research it each time we need it.
  private InetAddress autoDiscoveredDefaultIP = null;

  /** Completely disables P2P within Besu. */
  @CommandLine.Option(
      names = {"--p2p-enabled"},
      description = "Enable P2P functionality (default: ${DEFAULT-VALUE})",
      arity = "1")
  public final Boolean p2pEnabled = true;

  /** Boolean option to indicate if peers should be discovered. */
  @CommandLine.Option(
      names = {"--discovery-enabled"},
      description = "Enable P2P discovery (default: ${DEFAULT-VALUE})",
      arity = "1")
  public final Boolean peerDiscoveryEnabled = true;

  /**
   * A list of bootstrap nodes can be passed and a hardcoded list will be used otherwise by the
   * Runner.
   */
  // NOTE: we have no control over default value here.
  @CommandLine.Option(
      names = {"--bootnodes"},
      paramLabel = "<enode://id@host:port>|<enr:base64Enr>",
      description =
          "Comma separated enode or ENR URLs for P2P discovery bootstrap. "
              + "Must be either all enode URLs (discovery V4) or all ENR URLs (discovery V5). "
              + "Default is a predefined list.",
      split = ",",
      arity = "0..*")
  public final List<String> bootNodes = null;

  // ===================== IPv4 / General Network Options =====================

  /** The IP address the node advertises to peers for P2P communication. */
  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @CommandLine.Option(
      names = {"--p2p-host"},
      paramLabel = DefaultCommandValues.MANDATORY_HOST_FORMAT_HELP,
      description = "IP address this node advertises to its peers (default: ${DEFAULT-VALUE})")
  public String p2pHost = autoDiscoverDefaultIP().getHostAddress();

  /** The network interface address on which this node listens for P2P communication. */
  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @CommandLine.Option(
      names = {"--p2p-interface"},
      paramLabel = DefaultCommandValues.MANDATORY_HOST_FORMAT_HELP,
      description = "Network interface address to listen on (default: ${DEFAULT-VALUE})")
  public String p2pInterface = NetworkUtility.INADDR_ANY;

  /** The port on which this node listens for P2P communication. */
  @CommandLine.Option(
      names = {"--p2p-port"},
      paramLabel = DefaultCommandValues.MANDATORY_PORT_FORMAT_HELP,
      description = "Port on which to listen for P2P communication (default: ${DEFAULT-VALUE})")
  public Integer p2pPort = EnodeURLImpl.DEFAULT_LISTENING_PORT;

  // ===================== IPv6 Network Options =====================

  /** The IPv6 address the node advertises to peers for P2P communication. */
  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @CommandLine.Option(
      names = {"--p2p-host-ipv6"},
      paramLabel = DefaultCommandValues.MANDATORY_HOST_FORMAT_HELP,
      description = "IPv6 address this node advertises to its peers (default: none)")
  public String p2pHostIpv6 = null;

  /** The IPv6 network interface address on which this node listens for P2P communication. */
  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @CommandLine.Option(
      names = {"--p2p-interface-ipv6"},
      paramLabel = DefaultCommandValues.MANDATORY_HOST_FORMAT_HELP,
      description = "IPv6 network interface address to listen on (default: none)")
  public String p2pInterfaceIpv6 = null;

  /** The IPv6 P2P port. */
  @CommandLine.Option(
      names = {"--p2p-port-ipv6"},
      paramLabel = DefaultCommandValues.MANDATORY_PORT_FORMAT_HELP,
      description =
          "Port on which to listen for IPv6 P2P communication (default: ${DEFAULT-VALUE})")
  public Integer p2pPortIpv6 = EnodeURLImpl.DEFAULT_LISTENING_PORT_IPV6;

  // ===================== IP Version Preference =====================

  /**
   * When a discovered peer advertises both IPv4 and IPv6 addresses, prefer IPv6 for outbound
   * connections. By default, IPv4 is preferred. If the peer only advertises one address family, it
   * is always used regardless of this setting.
   */
  @CommandLine.Option(
      names = {"--p2p-ipv6-outbound-enabled"},
      description =
          """
          Prefer IPv6 addresses for outbound P2P connections when peers advertise both IPv4 and IPv6.
          When false (default), IPv4 is preferred.
          If a peer only advertises one address family, it is always used.
          (default: ${DEFAULT-VALUE})""")
  public boolean preferIpv6Outbound = false;

  /** The maximum number of peers this node can connect to. */
  @CommandLine.Option(
      names = {"--max-peers", "--p2p-peer-upper-bound"},
      paramLabel = DefaultCommandValues.MANDATORY_INTEGER_FORMAT_HELP,
      description = "Maximum P2P connections that can be established (default: ${DEFAULT-VALUE})")
  public final Integer maxPeers = DefaultCommandValues.DEFAULT_MAX_PEERS;

  /** Boolean option to limit the number of P2P connections initiated remotely. */
  @CommandLine.Option(
      names = {"--remote-connections-limit-enabled"},
      description =
          "Whether to limit the number of P2P connections initiated remotely. (default: ${DEFAULT-VALUE})")
  public final Boolean isLimitRemoteWireConnectionsEnabled = true;

  /** The maximum percentage of P2P connections that can be initiated remotely. */
  @CommandLine.Option(
      names = {"--remote-connections-max-percentage"},
      paramLabel = DefaultCommandValues.MANDATORY_DOUBLE_FORMAT_HELP,
      description =
          "The maximum percentage of P2P connections that can be initiated remotely. Must be between 0 and 100 inclusive. (default: ${DEFAULT-VALUE})",
      converter = PercentageConverter.class)
  public final Percentage maxRemoteConnectionsPercentage =
      Fraction.fromFloat(DefaultCommandValues.DEFAULT_FRACTION_REMOTE_WIRE_CONNECTIONS_ALLOWED)
          .toPercentage();

  /** The URL to use for DNS discovery. */
  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @CommandLine.Option(
      names = {"--discovery-dns-url"},
      description = "Specifies the URL to use for DNS discovery")
  public String discoveryDnsUrl = null;

  /** Boolean option to allow for incoming connections to be prioritized randomly. */
  @CommandLine.Option(
      names = {"--random-peer-priority-enabled"},
      description =
          "Allow for incoming connections to be prioritized randomly. This will prevent (typically small, stable) networks from forming impenetrable peer cliques. (default: ${DEFAULT-VALUE})")
  public final Boolean randomPeerPriority = Boolean.FALSE;

  /** A list of node IDs to ban from the P2P network. */
  @CommandLine.Option(
      names = {"--banned-node-ids", "--banned-node-id"},
      paramLabel = DefaultCommandValues.MANDATORY_NODE_ID_FORMAT_HELP,
      description = "A list of node IDs to ban from the P2P network.",
      split = ",",
      arity = "1..*")
  void setBannedNodeIds(final List<String> values) {
    try {
      bannedNodeIds =
          values.stream()
              .filter(value -> !value.isEmpty())
              .map(EnodeURLImpl::parseNodeId)
              .collect(Collectors.toList());
    } catch (final IllegalArgumentException e) {
      throw new CommandLine.ParameterException(
          new CommandLine(this), "Invalid ids supplied to '--banned-node-ids'. " + e.getMessage());
    }
  }

  // Boolean option to set that in a PoA network the bootnodes should always be queried during
  // peer table refresh. If this flag is disabled bootnodes are only sent FINDN requests on first
  // startup, meaning that an offline bootnode or network outage at the client can prevent it
  // discovering any peers without a restart.
  @CommandLine.Option(
      names = {"--poa-discovery-retry-bootnodes"},
      description =
          "Always use of bootnodes for discovery in PoA networks. Disabling this reverts "
              + " to the same behaviour as non-PoA networks, where neighbours are only discovered from bootnodes on first startup."
              + "(default: ${DEFAULT-VALUE})",
      arity = "1")
  private final Boolean poaDiscoveryRetryBootnodes = true;

  private Collection<Bytes> bannedNodeIds = new ArrayList<>();

  /**
   * Auto-discovers the default IP of the client.
   *
   * @return machine loopback address
   */
  // Loopback IP is used by default as this is how smokeTests require it to be
  // and it's probably a good security behaviour to default only on the localhost.
  public InetAddress autoDiscoverDefaultIP() {
    autoDiscoveredDefaultIP =
        Optional.ofNullable(autoDiscoveredDefaultIP).orElseGet(InetAddress::getLoopbackAddress);

    return autoDiscoveredDefaultIP;
  }

  @CommandLine.Option(
      names = {"--net-restrict"},
      arity = "1..*",
      split = ",",
      converter = SubnetCidrConverter.class,
      description =
          "Comma-separated list of allowed IP subnets (e.g., '192.168.1.0/24,10.0.0.0/8').")
  private List<IPAddress> allowedSubnets;

  @Override
  public P2PDiscoveryConfiguration toDomainObject() {
    applySmartDefaults();

    return new P2PDiscoveryConfiguration(
        p2pEnabled,
        peerDiscoveryEnabled,
        p2pHost,
        p2pInterface,
        p2pPort,
        Optional.ofNullable(p2pHostIpv6),
        Optional.ofNullable(p2pInterfaceIpv6),
        p2pPortIpv6,
        maxPeers,
        isLimitRemoteWireConnectionsEnabled,
        maxRemoteConnectionsPercentage,
        randomPeerPriority,
        bannedNodeIds,
        allowedSubnets,
        poaDiscoveryRetryBootnodes,
        bootNodes,
        discoveryDnsUrl,
        preferIpv6Outbound);
  }

  /**
   * Applies smart defaults for IPv6 dual-stack configuration.
   *
   * <p>If --p2p-host-ipv6 is specified but --p2p-interface-ipv6 is not, automatically sets
   * --p2p-interface-ipv6 to :: (listen on all IPv6 interfaces). This matches the IPv4 behavior
   * where --p2p-interface defaults to 0.0.0.0.
   *
   * <p>Logs a warning if --p2p-interface-ipv6 is specified without --p2p-host-ipv6, as this creates
   * an incomplete dual-stack configuration where the node listens on IPv6 but doesn't advertise an
   * IPv6 address in its ENR.
   */
  private void applySmartDefaults() {
    // Auto-set IPv6 interface to listen on all IPv6 addresses when IPv6 host is specified
    if (p2pHostIpv6 != null && p2pInterfaceIpv6 == null) {
      p2pInterfaceIpv6 = NetworkUtility.INADDR6_ANY;
      LOG.info(
          "Auto-setting --p2p-interface-ipv6={} because --p2p-host-ipv6 was specified. "
              + "To use a different interface, explicitly set --p2p-interface-ipv6.",
          p2pInterfaceIpv6);
    }

    // Warn about incomplete dual-stack configuration
    if (p2pInterfaceIpv6 != null && p2pHostIpv6 == null) {
      LOG.warn(
          "--p2p-interface-ipv6 specified without --p2p-host-ipv6. "
              + "Node will listen on IPv6 but will not advertise IPv6 address in ENR. "
              + "For full dual-stack support, specify --p2p-host-ipv6.");
    }
  }

  /**
   * Validates the provided P2P discovery options.
   *
   * @param commandLine The command line object used for parsing and error reporting.
   * @param networkInterfaceChecker The checker used to validate the network interface.
   */
  public void validate(
      final CommandLine commandLine, final NetworkInterfaceChecker networkInterfaceChecker) {
    validateP2PHost(commandLine);
    validateP2PInterface(commandLine, networkInterfaceChecker);
  }

  private void validateP2PInterface(
      final CommandLine commandLine, final NetworkInterfaceChecker networkInterfaceChecker) {
    validateInterface(commandLine, networkInterfaceChecker, p2pInterface, "--p2p-interface");
    if (p2pInterfaceIpv6 != null) {
      validateInterface(
          commandLine, networkInterfaceChecker, p2pInterfaceIpv6, "--p2p-interface-ipv6");
      validateIpv6Address(commandLine, p2pInterfaceIpv6, "--p2p-interface-ipv6");

      // Enforce primary must be IPv4 when IPv6 is also specified (dual-stack)
      if (!NetworkUtility.isIpV4Address(p2pInterface)
          && !NetworkUtility.INADDR_ANY.equals(p2pInterface)) {
        throw new CommandLine.ParameterException(
            commandLine,
            "When --p2p-interface-ipv6 is specified for dual-stack configuration, "
                + "--p2p-interface must be an IPv4 address or 0.0.0.0, but was: "
                + p2pInterface);
      }
    }
  }

  private void validateInterface(
      final CommandLine commandLine,
      final NetworkInterfaceChecker networkInterfaceChecker,
      final String iface,
      final String optionName) {
    final String failMessage = "The provided " + optionName + " is not available: " + iface;
    try {
      if (!networkInterfaceChecker.isNetworkInterfaceAvailable(iface)) {
        throw new CommandLine.ParameterException(commandLine, failMessage);
      }
    } catch (final UnknownHostException | SocketException e) {
      throw new CommandLine.ParameterException(commandLine, failMessage, e);
    }
  }

  private void validateP2PHost(final CommandLine commandLine) {
    validateHost(commandLine, p2pHost, "--p2p-host");
    if (p2pHostIpv6 != null) {
      validateHost(commandLine, p2pHostIpv6, "--p2p-host-ipv6");
      validateIpv6Address(commandLine, p2pHostIpv6, "--p2p-host-ipv6");

      // Enforce primary must be IPv4 when IPv6 is also specified (dual-stack)
      if (!NetworkUtility.isIpV4Address(p2pHost)) {
        throw new CommandLine.ParameterException(
            commandLine,
            "When --p2p-host-ipv6 is specified for dual-stack configuration, "
                + "--p2p-host must be an IPv4 address, but was: "
                + p2pHost);
      }
    }
  }

  private void validateHost(
      final CommandLine commandLine, final String host, final String optionName) {
    if (!InetAddresses.isInetAddress(host)) {
      final String failMessage = "The provided " + optionName + " is invalid: " + host;
      throw new CommandLine.ParameterException(commandLine, failMessage);
    }
  }

  private void validateIpv6Address(
      final CommandLine commandLine, final String address, final String optionName) {
    if (!NetworkUtility.isIpV6Address(address)) {
      throw new CommandLine.ParameterException(
          commandLine,
          "The provided " + optionName + " must be an IPv6 address, but was: " + address);
    }
  }

  @Override
  public List<String> getCLIOptions() {
    return CommandLineUtils.getCLIOptions(this, new P2PDiscoveryOptions());
  }
}
