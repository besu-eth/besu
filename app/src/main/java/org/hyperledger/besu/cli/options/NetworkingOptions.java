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
package org.hyperledger.besu.cli.options;

import org.hyperledger.besu.cli.converter.DurationMillisConverter;
import org.hyperledger.besu.cli.converter.DurationSecondsConverter;
import org.hyperledger.besu.cli.util.CommandLineUtils;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.ImmutableNetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import picocli.CommandLine;

/** The Networking Cli options. */
public class NetworkingOptions implements CLIOptions<NetworkingConfiguration> {
  private final String INITIATE_CONNECTIONS_FREQUENCY_FLAG =
      "--Xp2p-initiate-connections-frequency";
  private final String CHECK_MAINTAINED_CONNECTIONS_FREQUENCY_FLAG =
      "--Xp2p-check-maintained-connections-frequency";
  private final String P2P_PEER_TASK_TIMEOUT = "--Xp2p-peer-task-timeout";
  private final String DNS_DISCOVERY_SERVER_OVERRIDE_FLAG = "--Xp2p-dns-discovery-server";
  private final String DISCOVERY_PROTOCOL_V5_ENABLED = "--Xv5-discovery-enabled";

  /** The constant FILTER_ON_ENR_FORK_ID. */
  public static final String FILTER_ON_ENR_FORK_ID = "--filter-on-enr-fork-id";

  private static final String DISCV5_DISCOVERY_INTERVAL_SECONDS =
      "--Xv5-discovery-interval-seconds";
  private static final String DISCV5_DISCOVERY_TIMEOUT_SECONDS = "--Xv5-discovery-timeout-seconds";
  private static final String DISCV5_MINIMUM_PEER_RATIO = "--Xv5-minimum-peer-ratio";

  @CommandLine.Option(
      names = INITIATE_CONNECTIONS_FREQUENCY_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description =
          "The frequency (in seconds) at which to initiate new outgoing connections (default: 30)",
      converter = DurationSecondsConverter.class)
  private Duration initiateConnectionsFrequency =
      NetworkingConfiguration.DEFAULT_INITIATE_CONNECTIONS_FREQUENCY;

  @CommandLine.Option(
      names = CHECK_MAINTAINED_CONNECTIONS_FREQUENCY_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description =
          "The frequency (in seconds) at which to check maintained connections (default: 60)",
      converter = DurationSecondsConverter.class)
  private Duration checkMaintainedConnectionsFrequency =
      NetworkingConfiguration.DEFAULT_CHECK_MAINTAINED_CONNECTIONS_FREQUENCY;

  @CommandLine.Option(
      names = P2P_PEER_TASK_TIMEOUT,
      hidden = true,
      paramLabel = "<INTEGER>",
      description =
          "The max amount of time (in millis) to wait for a peer task to complete (default: 5000)",
      converter = DurationMillisConverter.class)
  private Duration p2pPeerTaskTimeout = NetworkingConfiguration.DEFAULT_P2P_PEER_TASK_TIMEOUT;

  @CommandLine.Option(
      names = DNS_DISCOVERY_SERVER_OVERRIDE_FLAG,
      hidden = true,
      description =
          "DNS server host to use for doing DNS Discovery of peers, rather than the machine's configured DNS server")
  private Optional<String> dnsDiscoveryServerOverride = Optional.empty();

  @CommandLine.Option(
      names = DISCOVERY_PROTOCOL_V5_ENABLED,
      hidden = true,
      description = "Whether to enable P2P Discovery Protocol v5 (default: ${DEFAULT-VALUE})")
  private final Boolean isPeerDiscoveryV5Enabled = false;

  @CommandLine.Option(
      names = FILTER_ON_ENR_FORK_ID,
      hidden = true,
      description = "Whether to enable filtering of peers based on the ENR field ForkId)")
  private final Boolean filterOnEnrForkId = NetworkingConfiguration.DEFAULT_FILTER_ON_ENR_FORK_ID;

  @CommandLine.Option(
      names = DISCV5_DISCOVERY_INTERVAL_SECONDS,
      hidden = true,
      paramLabel = "<INTEGER>",
      description = "The interval (in seconds) between DiscV5 peer discovery cycles (default: 1)",
      converter = DurationSecondsConverter.class)
  private Duration discV5DiscoveryIntervalSeconds = Duration.ofSeconds(1);

  @CommandLine.Option(
      names = DISCV5_DISCOVERY_TIMEOUT_SECONDS,
      hidden = true,
      paramLabel = "<INTEGER>",
      description =
          "The timeout (in seconds) for each DiscV5 peer discovery operation (default: 30)",
      converter = DurationSecondsConverter.class)
  private Duration discV5DiscoveryTimeoutSeconds = Duration.ofSeconds(30);

  @CommandLine.Option(
      names = DISCV5_MINIMUM_PEER_RATIO,
      hidden = true,
      paramLabel = "<DOUBLE>",
      description =
          "Minimum ratio of connected peers to max peers required to switch to slow DiscV5 discovery cadence (default: 0.8)")
  private double discV5MinimumPeerRatio = 0.8;

  private NetworkingOptions() {}

  /**
   * Create networking options.
   *
   * @return the networking options
   */
  public static NetworkingOptions create() {
    return new NetworkingOptions();
  }

  /**
   * Create networking options from Networking Configuration.
   *
   * @param networkingConfig the networking config
   * @return the networking options
   */
  public static NetworkingOptions fromConfig(final NetworkingConfiguration networkingConfig) {
    final NetworkingOptions cliOptions = new NetworkingOptions();
    cliOptions.checkMaintainedConnectionsFrequency =
        networkingConfig.checkMaintainedConnectionsFrequency();
    cliOptions.initiateConnectionsFrequency = networkingConfig.initiateConnectionsFrequency();
    cliOptions.p2pPeerTaskTimeout = networkingConfig.p2pPeerTaskTimeout();
    cliOptions.dnsDiscoveryServerOverride = networkingConfig.dnsDiscoveryServerOverride();

    return cliOptions;
  }

  /**
   * Validates networking-related CLI options.
   *
   * @param commandLine the parsed command line input
   */
  public void validate(final CommandLine commandLine) {
    if (discV5MinimumPeerRatio <= 0) {
      throw new CommandLine.ParameterException(
          commandLine, DISCV5_MINIMUM_PEER_RATIO + " must be non-negative");
    }
  }

  @Override
  public NetworkingConfiguration toDomainObject() {
    final var discovery = DiscoveryConfiguration.create();
    discovery.setDiscoveryV5Enabled(isPeerDiscoveryV5Enabled);
    discovery.setFilterOnEnrForkId(filterOnEnrForkId);
    discovery.setDiscV5DiscoveryIntervalSeconds((int) discV5DiscoveryIntervalSeconds.toSeconds());
    discovery.setDiscV5DiscoveryTimeoutSeconds((int) discV5DiscoveryTimeoutSeconds.toSeconds());
    discovery.setDiscV5MinimumPeerRatio(discV5MinimumPeerRatio);

    return ImmutableNetworkingConfiguration.builder()
        .checkMaintainedConnectionsFrequency(checkMaintainedConnectionsFrequency)
        .initiateConnectionsFrequency(initiateConnectionsFrequency)
        .p2pPeerTaskTimeout(p2pPeerTaskTimeout)
        .dnsDiscoveryServerOverride(dnsDiscoveryServerOverride)
        .discoveryConfiguration(discovery)
        .build();
  }

  @Override
  public List<String> getCLIOptions() {
    return CommandLineUtils.getCLIOptions(this, new NetworkingOptions());
  }
}
