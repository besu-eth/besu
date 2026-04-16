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
package org.hyperledger.besu.cli;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.hyperledger.besu.cli.options.ChainPruningOptions.CHAIN_DATA_PRUNING_RETAINED_MINIMUM;
import static org.hyperledger.besu.config.NetworkDefinition.DEV;
import static org.hyperledger.besu.config.NetworkDefinition.EPHEMERY;
import static org.hyperledger.besu.config.NetworkDefinition.EXPERIMENTAL_EIPS;
import static org.hyperledger.besu.config.NetworkDefinition.FUTURE_EIPS;
import static org.hyperledger.besu.config.NetworkDefinition.HOODI;
import static org.hyperledger.besu.config.NetworkDefinition.LINEA_SEPOLIA;
import static org.hyperledger.besu.config.NetworkDefinition.LUKSO;
import static org.hyperledger.besu.config.NetworkDefinition.MAINNET;
import static org.hyperledger.besu.config.NetworkDefinition.SEPOLIA;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis.ENGINE;
import static org.hyperledger.besu.ethereum.p2p.config.DefaultDiscoveryConfiguration.HOODI_BOOTSTRAP_NODES;
import static org.hyperledger.besu.ethereum.p2p.config.DefaultDiscoveryConfiguration.HOODI_DISCOVERY_URL;
import static org.hyperledger.besu.ethereum.p2p.config.DefaultDiscoveryConfiguration.MAINNET_BOOTSTRAP_NODES;
import static org.hyperledger.besu.ethereum.p2p.config.DefaultDiscoveryConfiguration.MAINNET_DISCOVERY_URL;
import static org.hyperledger.besu.ethereum.p2p.config.DefaultDiscoveryConfiguration.SEPOLIA_BOOTSTRAP_NODES;
import static org.hyperledger.besu.ethereum.p2p.config.DefaultDiscoveryConfiguration.SEPOLIA_DISCOVERY_URL;
import static org.hyperledger.besu.plugin.services.storage.DataStorageFormat.BONSAI;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.hyperledger.besu.cli.config.EthNetworkConfig;
import org.hyperledger.besu.cli.config.NativeRequirement;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.MergeConfiguration;
import org.hyperledger.besu.config.NetworkDefinition;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration;
import org.hyperledger.besu.ethereum.api.handlers.TimeoutOptions;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.precompile.AbstractAltBnPrecompiledContract;
import org.hyperledger.besu.evm.precompile.KZGPointEvalPrecompiledContract;
import org.hyperledger.besu.metrics.StandardMetricCategory;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.plugin.data.EnodeURL;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.util.BesuVersionUtils;
import org.hyperledger.besu.util.number.Fraction;
import org.hyperledger.besu.util.number.Percentage;
import org.hyperledger.besu.util.number.PositiveNumber;
import org.hyperledger.besu.util.platform.PlatformDetector;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Splitter;
import com.google.common.io.Resources;
import io.vertx.core.json.JsonObject;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.toml.Toml;
import org.apache.tuweni.toml.TomlParseResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import picocli.CommandLine;

@ExtendWith(MockitoExtension.class)
public class BesuCommandTest extends CommandTestAbstract {

  private static final String VALID_NODE_ID =
      "6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0";
  private static final JsonRpcConfiguration DEFAULT_JSON_RPC_CONFIGURATION;
  private static final GraphQLConfiguration DEFAULT_GRAPH_QL_CONFIGURATION;
  private static final WebSocketConfiguration DEFAULT_WEB_SOCKET_CONFIGURATION;
  private static final MetricsConfiguration DEFAULT_METRICS_CONFIGURATION;
  private static final ApiConfiguration DEFAULT_API_CONFIGURATION;

  private static final int GENESIS_CONFIG_TEST_CHAINID = 3141592;
  private static final JsonObject GENESIS_VALID_JSON =
      (new JsonObject())
          .put("config", (new JsonObject()).put("chainId", GENESIS_CONFIG_TEST_CHAINID));
  private static final JsonObject GENESIS_INVALID_DATA =
      (new JsonObject()).put("config", new JsonObject());
  private static final JsonObject INVALID_GENESIS_EC_CURVE =
      (new JsonObject()).put("config", new JsonObject().put("ecCurve", "abcd"));
  private static final JsonObject VALID_GENESIS_EC_CURVE =
      (new JsonObject()).put("config", new JsonObject().put("ecCurve", "secp256k1"));

  private static final String[] VALID_ENODE_STRINGS = {
    "enode://" + VALID_NODE_ID + "@192.168.0.1:4567",
    "enode://" + VALID_NODE_ID + "@192.168.0.2:4567",
    "enode://" + VALID_NODE_ID + "@192.168.0.3:4567"
  };

  public static final List<EnodeURL> EPHEMERY_BOOT_NODES =
      Collections.unmodifiableList(
          Stream.of(
                  "enode://50a54ecbd2175497640bcf46a25bbe9bb4fae51d7cc2a29ef4947a7ee17496cf39a699b7fe6b703ed0feb9dbaae7e44fc3827fcb7435ca9ac6de4daa4d983b3d@137.74.203.240:30303",
                  "enode://0f2c301a9a3f9fa2ccfa362b79552c052905d8c2982f707f46cd29ece5a9e1c14ecd06f4ac951b228f059a43c6284a1a14fce709e8976cac93b50345218bf2e9@135.181.140.168:30343")
              .map(EnodeURLImpl::fromString)
              .collect(toList()));

  private static final String DNS_DISCOVERY_URL =
      "enrtree://AM5FCQLWIZX2QFPNJAP7VUERCCRNGRHWZG3YYHIUV7BVDQ5FDPRT2@nodes.example.org";
  private static final JsonObject VALID_GENESIS_WITH_DISCOVERY_OPTIONS =
      new JsonObject()
          .put(
              "config",
              new JsonObject()
                  .put(
                      "discovery",
                      new JsonObject()
                          .put("bootnodes", List.of(VALID_ENODE_STRINGS))
                          .put("dns", DNS_DISCOVERY_URL)));

  private static final JsonObject GENESIS_WITH_DATA_BLOBS_ENABLED =
      new JsonObject().put("config", new JsonObject().put("cancunTime", 1L));

  private static final long DEFAULT_TARGET_GAS_LIMIT = 60_000_000L;
  private static final long DEFAULT_TARGET_GAS_LIMIT_TESTNET = 60_000_000L;
  private static final long CUSTOM_TARGET_GAS_LIMIT = 50_000_000L;
  private static final String NETWORK_MAINNET_CONFIG_LOG =
      String.format(
          "%s%s",
          MAINNET.name().charAt(0), MAINNET.name().substring(1).toLowerCase(Locale.getDefault()));
  private static final String NETWORK_HOODI_CONFIG_LOG =
      String.format(
          "%s%s",
          HOODI.name().charAt(0), HOODI.name().substring(1).toLowerCase(Locale.getDefault()));
  private static final String NETWORK_DEV_CONFIG_LOG =
      String.format(
          "%s%s", DEV.name().charAt(0), DEV.name().substring(1).toLowerCase(Locale.getDefault()));

  static {
    DEFAULT_JSON_RPC_CONFIGURATION = JsonRpcConfiguration.createDefault();
    DEFAULT_GRAPH_QL_CONFIGURATION = GraphQLConfiguration.createDefault();
    DEFAULT_WEB_SOCKET_CONFIGURATION = WebSocketConfiguration.createDefault();
    DEFAULT_METRICS_CONFIGURATION = MetricsConfiguration.builder().build();
    DEFAULT_API_CONFIGURATION = ImmutableApiConfiguration.builder().build();
  }

  public BesuCommandTest() throws IOException {}

  @BeforeEach
  public void setup() {
    try {
      // optimistically tear down a potential previous loaded trusted setup
      KZGPointEvalPrecompiledContract.tearDown();
    } catch (Throwable ignore) {
      // and ignore errors in case no trusted setup was already loaded
    }

    MergeConfiguration.setMergeEnabled(false);
  }

  @AfterEach
  public void tearDown() {

    MergeConfiguration.setMergeEnabled(false);
  }

  @Test
  public void testGenesisOverrideOptions() throws Exception {
    parseCommand("--override-genesis-config", "shanghaiTime=123");

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    final EthNetworkConfig config = networkArg.getValue();
    // mainnet defaults
    assertThat(config.networkId()).isEqualTo(BigInteger.valueOf(1));

    // assert that shanghaiTime override is applied
    final GenesisConfig actualGenesisConfig = (config.genesisConfig());
    assertThat(actualGenesisConfig).isNotNull();
    assertThat(actualGenesisConfig.getConfigOptions().getShanghaiTime()).isNotEmpty();
    assertThat(actualGenesisConfig.getConfigOptions().getShanghaiTime().getAsLong()).isEqualTo(123);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void testGenesisOverrideOptionsWithCustomGenesis() throws Exception {
    final Path genesisFile = createFakeGenesisFile(GENESIS_VALID_JSON);

    parseCommand(
        "--genesis-file", genesisFile.toString(), "--override-genesis-config", "shanghaiTime=123");

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    final EthNetworkConfig config = networkArg.getValue();
    assertThat(config.enodeBootNodes()).isEmpty();
    assertThat(config.dnsDiscoveryUrl()).isNull();
    assertThat(config.networkId()).isEqualTo(BigInteger.valueOf(3141592));

    // then assert that the shanghaiTime is applied
    final GenesisConfig actualGenesisConfig = (config.genesisConfig());
    assertThat(actualGenesisConfig).isNotNull();
    assertThat(actualGenesisConfig.getConfigOptions().getShanghaiTime()).isNotEmpty();
    assertThat(actualGenesisConfig.getConfigOptions().getShanghaiTime().getAsLong()).isEqualTo(123);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void callingHelpSubCommandMustDisplayUsage() {
    parseCommand("--help");
    final String expectedOutputStart = String.format("Usage:%n%nbesu [OPTIONS] [COMMAND]");
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).startsWith(expectedOutputStart);
  }

  @Test
  public void callingHelpDisplaysDefaultRpcApisCorrectly() {
    parseCommand("--help");
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).contains("default: [ETH, NET, WEB3]");
  }

  @Test
  public void callingVersionDisplayBesuInfoVersion() {
    parseCommand("--version");
    assertThat(commandOutput.toString(UTF_8))
        .isEqualToIgnoringWhitespace(BesuVersionUtils.version());
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void callingUnstableSubCommandMustNotError() {
    parseCommand("-X");
    final String expectedOutputStart = "Unstable options";
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).startsWith(expectedOutputStart);
  }

  // Testing default values
  @Test
  public void callingBesuCommandWithoutOptionsMustSyncWithDefaultValues() {
    parseCommand();

    final int maxPeers = 25;

    final ArgumentCaptor<EthNetworkConfig> ethNetworkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);
    verify(mockRunnerBuilder).discoveryEnabled(eq(true));
    verify(mockRunnerBuilder).ethNetworkConfig(EthNetworkConfig.getNetworkConfig(MAINNET));
    verify(mockRunnerBuilder).p2pAdvertisedHost(eq("127.0.0.1"));
    verify(mockRunnerBuilder).p2pListenPort(eq(30303));
    verify(mockRunnerBuilder).jsonRpcConfiguration(eq(DEFAULT_JSON_RPC_CONFIGURATION));
    verify(mockRunnerBuilder).graphQLConfiguration(eq(DEFAULT_GRAPH_QL_CONFIGURATION));
    verify(mockRunnerBuilder).webSocketConfiguration(eq(DEFAULT_WEB_SOCKET_CONFIGURATION));
    verify(mockRunnerBuilder).metricsConfiguration(eq(DEFAULT_METRICS_CONFIGURATION));
    verify(mockRunnerBuilder).ethNetworkConfig(ethNetworkArg.capture());
    verify(mockRunnerBuilder).autoLogBloomCaching(eq(true));
    verify(mockRunnerBuilder).apiConfiguration(DEFAULT_API_CONFIGURATION);
    verify(mockRunnerBuilder).build();

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(ethNetworkArg.capture(), any());
    final ArgumentCaptor<MiningConfiguration> miningArg =
        ArgumentCaptor.forClass(MiningConfiguration.class);
    verify(mockControllerBuilder).synchronizerConfiguration(syncConfigurationCaptor.capture());
    verify(mockControllerBuilder).dataDirectory(isNotNull());
    verify(mockControllerBuilder).miningParameters(miningArg.capture());
    verify(mockControllerBuilder).nodeKey(isNotNull());
    verify(mockControllerBuilder).storageProvider(storageProviderArgumentCaptor.capture());
    verify(mockControllerBuilder).maxPeers(eq(maxPeers));
    verify(mockControllerBuilder).maxRemotelyInitiatedPeers(eq((int) Math.floor(0.6 * maxPeers)));
    verify(mockControllerBuilder).build();

    assertThat(storageProviderArgumentCaptor.getValue()).isNotNull();
    assertThat(syncConfigurationCaptor.getValue().getSyncMode()).isEqualTo(SyncMode.SNAP);
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(miningArg.getValue().getCoinbase()).isEqualTo(Optional.empty());
    assertThat(miningArg.getValue().getMinTransactionGasPrice()).isEqualTo(Wei.of(1000));
    assertThat(miningArg.getValue().getExtraData())
        .isEqualTo(BesuVersionUtils.versionForExtraData());
    assertThat(ethNetworkArg.getValue().networkId()).isEqualTo(1);
    assertThat(ethNetworkArg.getValue().enodeBootNodes()).isEqualTo(MAINNET_BOOTSTRAP_NODES);
  }

  // Testing each option
  @Test
  public void callingWithConfigOptionButNoConfigFileShouldDisplayHelp() {
    parseCommand("--config-file");

    final String expectedOutputStart =
        "Missing required parameter for option '--config-file' (<FILE>)";
    assertThat(commandErrorOutput.toString(UTF_8)).startsWith(expectedOutputStart);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void callingWithConfigOptionButNonExistingFileShouldDisplayHelp() throws IOException {
    final Path tempConfigFilePath = createTempFile("an-invalid-file-name-without-extension", "");
    parseCommand("--config-file", tempConfigFilePath.toString());

    final String expectedOutputStart = "Unable to read from empty TOML configuration file.";
    assertThat(commandErrorOutput.toString(UTF_8)).startsWith(expectedOutputStart);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void callingWithConfigOptionButTomlFileNotFoundShouldDisplayHelp() {
    String invalidFile = "./an-invalid-file-name-sdsd87sjhqoi34io23.toml";
    parseCommand("--config-file", invalidFile);

    final String expectedOutputStart =
        String.format("Unable to read TOML configuration, file not found: %s", invalidFile);
    assertThat(commandErrorOutput.toString(UTF_8)).startsWith(expectedOutputStart);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void callingWithConfigOptionButInvalidContentTomlFileShouldDisplayHelp() throws Exception {
    // We write a config file to prevent an invalid file in resource folder to raise errors in
    // code checks (CI + IDE)
    final Path tempConfigFile = createTempFile("invalid_config.toml", ".");

    parseCommand("--config-file", tempConfigFile.toString());

    final String expectedOutputStart =
        "Invalid TOML configuration: org.apache.tuweni.toml.TomlParseError: Unexpected '.', expected a-z, A-Z, 0-9, ', \", a table key, "
            + "a newline, or end-of-input (line 1, column 1)";
    assertThat(commandErrorOutput.toString(UTF_8)).startsWith(expectedOutputStart);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void callingWithNoBootnodesConfig() throws Exception {
    final URL configFile = this.getClass().getResource("/no_bootnodes.toml");
    final Path toml = createTempFile("toml", Resources.toString(configFile, UTF_8));

    parseCommand("--config-file", toml.toAbsolutePath().toString());

    verify(mockRunnerBuilder).ethNetworkConfig(ethNetworkConfigArgumentCaptor.capture());
    assertThat(ethNetworkConfigArgumentCaptor.getValue().enodeBootNodes()).isEmpty();

    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void callingWithConfigOptionButInvalidValueTomlFileShouldDisplayHelp() throws Exception {
    // We write a config file to prevent an invalid file in resource folder to raise errors in
    // code checks (CI + IDE)
    final Path tempConfigFile = createTempFile("invalid_config.toml", "tester===========.......");
    parseCommand("--config-file", tempConfigFile.toString());

    final String expectedOutputStart =
        "Invalid TOML configuration: org.apache.tuweni.toml.TomlParseError: Unexpected '=', expected ', \", ''', \"\"\", a number, "
            + "a boolean, a date/time, an array, or a table (line 1, column 8)";
    assertThat(commandErrorOutput.toString(UTF_8)).startsWith(expectedOutputStart);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void tomlThatConfiguresEverythingExceptPermissioningToml() throws IOException {
    // Load a TOML that configures literally everything (except permissioning TOML config)
    final URL configFile = this.getClass().getResource("/everything_config.toml");
    final Path toml = createTempFile("toml", Resources.toByteArray(configFile));

    // Parse it.
    final CommandLine.Model.CommandSpec spec = parseCommand("--config-file", toml.toString()).spec;
    final TomlParseResult tomlResult = Toml.parse(toml);

    // Verify we configured everything
    final HashSet<CommandLine.Model.OptionSpec> options = new HashSet<>(spec.options());

    // Except for meta-options
    options.remove(spec.optionsMap().get("--config-file"));
    options.remove(spec.optionsMap().get("--help"));
    options.remove(spec.optionsMap().get("--version"));
    options.remove(spec.optionsMap().get("--print-paths-and-exit"));

    for (final String tomlKey : tomlResult.keySet()) {
      final CommandLine.Model.OptionSpec optionSpec = spec.optionsMap().get("--" + tomlKey);
      assertThat(optionSpec)
          .describedAs("Option '%s' should be a configurable option.", tomlKey)
          .isNotNull();
      // Verify TOML stores it by the appropriate type
      if (optionSpec.type().equals(Boolean.class) || optionSpec.type().equals(boolean.class)) {
        tomlResult.getBoolean(tomlKey);
      } else if (optionSpec.isMultiValue() || optionSpec.arity().max() > 1) {
        tomlResult.getArray(tomlKey);
      } else if (optionSpec.type().equals(Double.class)) {
        tomlResult.getDouble(tomlKey);
      } else if (optionSpec.type().equals(Float.class)) {
        tomlResult.getDouble(tomlKey);
      } else if (Number.class.isAssignableFrom(optionSpec.type())) {
        tomlResult.getLong(tomlKey);
      } else if (Wei.class.isAssignableFrom(optionSpec.type())) {
        tomlResult.getLong(tomlKey);
      } else if (Fraction.class.isAssignableFrom(optionSpec.type())) {
        tomlResult.getDouble(tomlKey);
      } else if (Percentage.class.isAssignableFrom(optionSpec.type())) {
        tomlResult.getLong(tomlKey);
      } else if (PositiveNumber.class.isAssignableFrom(optionSpec.type())) {
        tomlResult.getLong(tomlKey);
      } else {
        tomlResult.getString(tomlKey);
      }
      options.remove(optionSpec);
    }
    assertThat(
            options.stream()
                .filter(optionSpec -> !optionSpec.hidden())
                .map(CommandLine.Model.OptionSpec::longestName))
        .isEmpty();
  }

  @Test
  public void nodekeyOptionMustBeUsed() throws Exception {
    final File file = new File("./specific/enclavePrivateKey");

    parseCommand("--node-private-key-file", file.getPath());

    verify(mockControllerBuilder).dataDirectory(isNotNull());
    verify(mockControllerBuilder).nodeKey(isNotNull());
    verify(mockControllerBuilder).build();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void dataDirOptionMustBeUsed(final @TempDir Path path) {

    parseCommand("--data-path", path.toString());

    verify(mockControllerBuilder).dataDirectory(pathArgumentCaptor.capture());
    verify(mockControllerBuilder).nodeKey(isNotNull());
    verify(mockControllerBuilder).build();

    assertThat(pathArgumentCaptor.getValue()).isEqualByComparingTo(path.toAbsolutePath());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void genesisPathOptionMustBeUsed() throws Exception {
    final Path genesisFile = createFakeGenesisFile(GENESIS_VALID_JSON);
    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    parseCommand("--genesis-file", genesisFile.toString());

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    assertThat(networkArg.getValue().genesisConfig())
        .isEqualTo(GenesisConfig.fromConfig(encodeJsonGenesis(GENESIS_VALID_JSON)));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void testGenesisPathEthOptions() throws Exception {
    final Path genesisFile = createFakeGenesisFile(GENESIS_VALID_JSON);

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    parseCommand("--genesis-file", genesisFile.toString());

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    final EthNetworkConfig config = networkArg.getValue();
    assertThat(config.enodeBootNodes()).isEmpty();
    assertThat(config.dnsDiscoveryUrl()).isNull();
    assertThat(config.networkId()).isEqualTo(BigInteger.valueOf(3141592));
  }

  @Test
  public void testGenesisPathMainnetEthConfig() {
    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);
    final ArgumentCaptor<MiningConfiguration> miningArg =
        ArgumentCaptor.forClass(MiningConfiguration.class);

    parseCommand("--network", "mainnet");

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).miningParameters(miningArg.capture());
    verify(mockControllerBuilder).build();

    final EthNetworkConfig config = networkArg.getValue();
    assertThat(config.enodeBootNodes()).isEqualTo(MAINNET_BOOTSTRAP_NODES);
    assertThat(config.dnsDiscoveryUrl()).isEqualTo(MAINNET_DISCOVERY_URL);
    assertThat(config.networkId()).isEqualTo(BigInteger.valueOf(1));
    verify(mockLogger, never()).warn(contains("Mainnet is deprecated and will be shutdown"));
  }

  @Test
  public void testGenesisPathSepoliaEthConfig() {
    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    parseCommand("--network", "sepolia");

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    final EthNetworkConfig config = networkArg.getValue();
    assertThat(config.enodeBootNodes()).isEqualTo(SEPOLIA_BOOTSTRAP_NODES);
    assertThat(config.dnsDiscoveryUrl()).isEqualTo(SEPOLIA_DISCOVERY_URL);
    assertThat(config.networkId()).isEqualTo(BigInteger.valueOf(11155111));
  }

  @Test
  public void testGenesisPathHoodiEthConfig() {
    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    parseCommand("--network", "hoodi");

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    final EthNetworkConfig config = networkArg.getValue();
    assertThat(config.enodeBootNodes()).isEqualTo(HOODI_BOOTSTRAP_NODES);
    assertThat(config.dnsDiscoveryUrl()).isEqualTo(HOODI_DISCOVERY_URL);
    assertThat(config.networkId()).isEqualTo(BigInteger.valueOf(560048));
  }

  @Test
  public void testGenesisPathFutureEipsEthConfig() {
    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    parseCommand("--network", "future_eips");

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    final EthNetworkConfig config = networkArg.getValue();
    assertThat(config.enodeBootNodes()).isEmpty();
    assertThat(config.dnsDiscoveryUrl()).isNull();
    assertThat(config.networkId()).isEqualTo(BigInteger.valueOf(2022));
  }

  @Test
  public void testGenesisPathExperimentalEipsEthConfig() {
    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    parseCommand("--network", "experimental_eips");

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    final EthNetworkConfig config = networkArg.getValue();
    assertThat(config.enodeBootNodes()).isEmpty();
    assertThat(config.dnsDiscoveryUrl()).isNull();
    assertThat(config.networkId()).isEqualTo(BigInteger.valueOf(2023));
  }

  @Test
  public void genesisAndNetworkMustNotBeUsedTogether() throws Exception {
    final Path genesisFile = createFakeGenesisFile(GENESIS_VALID_JSON);

    parseCommand("--genesis-file", genesisFile.toString(), "--network", "mainnet");

    verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .startsWith("--network option and --genesis-file option can't be used at the same time.");
  }

  @Test
  public void nonExistentGenesisGivesError() {
    final String nonExistentGenesis = "non-existent-genesis.json";
    parseCommand("--genesis-file", nonExistentGenesis);

    verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).startsWith("Unable to load genesis file");
    assertThat(commandErrorOutput.toString(UTF_8)).contains(nonExistentGenesis);
  }

  @Test
  public void testDnsDiscoveryUrlEthConfig() {
    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    parseCommand(
        "--discovery-dns-url",
        "enrtree://AM5FCQLWIZX2QFPNJAP7VUERCCRNGRHWZG3YYHIUV7BVDQ5FDPRT2@nodes.example.org");

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    final EthNetworkConfig config = networkArg.getValue();
    assertThat(config.dnsDiscoveryUrl())
        .isEqualTo(
            "enrtree://AM5FCQLWIZX2QFPNJAP7VUERCCRNGRHWZG3YYHIUV7BVDQ5FDPRT2@nodes.example.org");
  }

  @Test
  public void testDnsDiscoveryUrlOverridesNetworkEthConfig() {
    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    parseCommand(
        "--network",
        "dev",
        "--discovery-dns-url",
        "enrtree://AM5FCQLWIZX2QFPNJAP7VUERCCRNGRHWZG3YYHIUV7BVDQ5FDPRT2@nodes.example.org");

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    final EthNetworkConfig config = networkArg.getValue();
    assertThat(config.dnsDiscoveryUrl())
        .isEqualTo(
            "enrtree://AM5FCQLWIZX2QFPNJAP7VUERCCRNGRHWZG3YYHIUV7BVDQ5FDPRT2@nodes.example.org");
  }

  @Test
  public void defaultNetworkIdAndBootnodesForCustomNetworkOptions() throws Exception {
    final Path genesisFile = createFakeGenesisFile(GENESIS_VALID_JSON);

    parseCommand("--genesis-file", genesisFile.toString());

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    assertThat(networkArg.getValue().genesisConfig())
        .isEqualTo(GenesisConfig.fromConfig(encodeJsonGenesis(GENESIS_VALID_JSON)));
    assertThat(networkArg.getValue().enodeBootNodes()).isEmpty();
    assertThat(networkArg.getValue().networkId()).isEqualTo(GENESIS_CONFIG_TEST_CHAINID);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void defaultNetworkIdForInvalidGenesisMustBeMainnetNetworkId() throws Exception {
    final Path genesisFile = createFakeGenesisFile(GENESIS_INVALID_DATA);

    parseCommand("--genesis-file", genesisFile.toString());

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    assertThat(networkArg.getValue().genesisConfig())
        .isEqualTo(GenesisConfig.fromConfig(encodeJsonGenesis(GENESIS_INVALID_DATA)));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void predefinedNetworkIdsMustBeEqualToChainIds() {
    // check the network id against the one in mainnet genesis config
    // it implies that EthNetworkConfig.mainnet().getNetworkId() returns a value equals to the chain
    // id
    // in this network genesis file.

    final var genesisConfig =
        EthNetworkConfig.getNetworkConfig(MAINNET).genesisConfig().getConfigOptions();
    assertThat(genesisConfig.getChainId().isPresent()).isTrue();
    assertThat(genesisConfig.getChainId().get())
        .isEqualTo(EthNetworkConfig.getNetworkConfig(MAINNET).networkId());
  }

  @Test
  public void identityValueTrueMustBeUsed() {
    parseCommand("--identity", "test");

    verify(mockRunnerBuilder).identityString(eq(Optional.of("test")));
    verify(mockRunnerBuilder).build();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void p2pEnabledOptionValueTrueMustBeUsed() {
    parseCommand("--p2p-enabled", "true");

    verify(mockRunnerBuilder).p2pEnabled(eq(true));
    verify(mockRunnerBuilder).build();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void p2pEnabledOptionValueFalseMustBeUsed() {
    parseCommand("--p2p-enabled", "false");

    verify(mockRunnerBuilder).p2pEnabled(eq(false));
    verify(mockRunnerBuilder).build();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void p2pOptionsRequiresServiceToBeEnabled() {
    final String[] nodes = {
      "6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0"
    };

    parseCommand(
        "--p2p-enabled",
        "false",
        "--bootnodes",
        String.join(",", VALID_ENODE_STRINGS),
        "--discovery-enabled",
        "false",
        "--max-peers",
        "42",
        "--remote-connections-max-percentage",
        "50",
        "--banned-node-id",
        String.join(",", nodes),
        "--banned-node-ids",
        String.join(",", nodes));

    verifyOptionsConstraintLoggerCall(
        "--p2p-enabled",
        "--discovery-enabled",
        "--bootnodes",
        "--max-peers",
        "--banned-node-ids",
        "--remote-connections-max-percentage");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void p2pOptionsRequiresServiceToBeEnabledToml() throws IOException {
    final String[] nodes = {
      "6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0"
    };

    final Path toml =
        createTempFile(
            "toml",
            "p2p-enabled=false\n"
                + "bootnodes=[\""
                + String.join("\",\"", VALID_ENODE_STRINGS)
                + "\"]\n"
                + "discovery-enabled=false\n"
                + "max-peers=42\n"
                + "remote-connections-max-percentage=50\n"
                + "banned-node-id=[\""
                + String.join(",", nodes)
                + "\"]\n"
                + "banned-node-ids=[\""
                + String.join(",", nodes)
                + "\"]\n");

    parseCommand("--config-file", toml.toString());

    verifyOptionsConstraintLoggerCall(
        "--p2p-enabled",
        "--discovery-enabled",
        "--bootnodes",
        "--max-peers",
        "--banned-node-ids",
        "--remote-connections-max-percentage");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void discoveryOptionValueTrueMustBeUsed() {
    parseCommand("--discovery-enabled", "true");

    verify(mockRunnerBuilder).discoveryEnabled(eq(true));
    verify(mockRunnerBuilder).build();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void discoveryOptionValueFalseMustBeUsed() {
    parseCommand("--discovery-enabled", "false");

    verify(mockRunnerBuilder).discoveryEnabled(eq(false));
    verify(mockRunnerBuilder).build();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void loadDiscoveryOptionsFromGenesisFile() throws IOException {
    final Path genesisFile = createFakeGenesisFile(VALID_GENESIS_WITH_DISCOVERY_OPTIONS);
    parseCommand("--genesis-file", genesisFile.toString());

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    final EthNetworkConfig config = networkArg.getValue();
    assertThat(config.dnsDiscoveryUrl()).isEqualTo(DNS_DISCOVERY_URL);

    assertThat(config.enodeBootNodes())
        .extracting(bootnode -> bootnode.toURI().toString())
        .containsExactly(VALID_ENODE_STRINGS);
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void discoveryDnsUrlCliArgTakesPrecedenceOverGenesisFile() throws IOException {
    final Path genesisFile = createFakeGenesisFile(VALID_GENESIS_WITH_DISCOVERY_OPTIONS);
    final String discoveryDnsUrlCliArg =
        "enrtree://XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX@nodes.example.org";
    parseCommand(
        "--genesis-file", genesisFile.toString(), "--discovery-dns-url", discoveryDnsUrlCliArg);

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);
    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    final EthNetworkConfig config = networkArg.getValue();
    assertThat(config.dnsDiscoveryUrl()).isEqualTo(discoveryDnsUrlCliArg);
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void bootnodesUrlCliArgTakesPrecedenceOverGenesisFile() throws IOException {
    final Path genesisFile = createFakeGenesisFile(VALID_GENESIS_WITH_DISCOVERY_OPTIONS);
    final URI bootnode =
        URI.create(
            "enode://d2567893371ea5a6fa6371d483891ed0d129e79a8fc74d6df95a00a6545444cd4a6960bbffe0b4e2edcf35135271de57ee559c0909236bbc2074346ef2b5b47c@127.0.0.1:30304");

    parseCommand("--genesis-file", genesisFile.toString(), "--bootnodes", bootnode.toString());

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);
    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    final EthNetworkConfig config = networkArg.getValue();
    assertThat(config.enodeBootNodes()).extracting(EnodeURL::toURI).containsExactly(bootnode);

    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void poaDiscoveryRetryBootnodesValueTrueMustBeUsed() {
    parseCommand("--poa-discovery-retry-bootnodes", "true");

    verify(mockRunnerBuilder).poaDiscoveryRetryBootnodes(eq(true));
    verify(mockRunnerBuilder).build();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void poaDiscoveryRetryBootnodesValueFalseMustBeUsed() {
    parseCommand("--poa-discovery-retry-bootnodes", "false");

    verify(mockRunnerBuilder).poaDiscoveryRetryBootnodes(eq(false));
    verify(mockRunnerBuilder).build();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void callingWithBootnodesOptionButNoValueMustPassEmptyBootnodeList() {
    parseCommand("--bootnodes");

    verify(mockRunnerBuilder).ethNetworkConfig(ethNetworkConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(ethNetworkConfigArgumentCaptor.getValue().enodeBootNodes()).isEmpty();
    assertThat(ethNetworkConfigArgumentCaptor.getValue().enrBootNodes()).isEmpty();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void callingWithValidBootnodeMustSucceed() {
    parseCommand(
        "--bootnodes",
        "enode://d2567893371ea5a6fa6371d483891ed0d129e79a8fc74d6df95a00a6545444cd4a6960bbffe0b4e2edcf35135271de57ee559c0909236bbc2074346ef2b5b47c@127.0.0.1:30304");
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void callingWithValidBootnodeButDiscoveryDisabledMustDisplayWarning() {
    parseCommand(
        "--bootnodes",
        "enode://d2567893371ea5a6fa6371d483891ed0d129e79a8fc74d6df95a00a6545444cd4a6960bbffe0b4e2edcf35135271de57ee559c0909236bbc2074346ef2b5b47c@127.0.0.1:30304",
        "--discovery-enabled",
        "false");
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    verify(mockRunnerBuilder).build();
    verify(mockLogger, atLeast(1)).warn("Discovery disabled: bootnodes will be ignored.");
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void callingWithInvalidBootnodeMustDisplayError() {
    parseCommand("--bootnodes", "invalid_enode_url");
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    final String expectedErrorOutputStart =
        "Invalid enode URL syntax 'invalid_enode_url'. Enode URL should have the following format "
            + "'enode://<node_id>@<ip>:<listening_port>[?discport=<discovery_port>]'.";
    assertThat(commandErrorOutput.toString(UTF_8)).startsWith(expectedErrorOutputStart);
  }

  @Test
  public void callingWithBootnodeThatHasDiscoveryDisabledMustDisplayError() {
    final String validBootnode =
        "enode://d2567893371ea5a6fa6371d483891ed0d129e79a8fc74d6df95a00a6545444cd4a6960bbffe0b4e2edcf35135271de57ee559c0909236bbc2074346ef2b5b47c@127.0.0.1:30304";
    final String invalidBootnode =
        "enode://02567893371ea5a6fa6371d483891ed0d129e79a8fc74d6df95a00a6545444cd4a6960bbffe0b4e2edcf35135271de57ee559c0909236bbc2074346ef2b5b47c@127.0.0.1:30303?discport=0";
    final String bootnodesValue = validBootnode + "," + invalidBootnode;
    parseCommand("--bootnodes", bootnodesValue);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    final String expectedErrorOutputStart =
        "Bootnodes must have discovery enabled. Invalid bootnodes: " + invalidBootnode + ".";
    assertThat(commandErrorOutput.toString(UTF_8)).startsWith(expectedErrorOutputStart);
  }

  // This test ensures non regression on https://pegasys1.atlassian.net/browse/PAN-2387
  @Test
  public void callingWithInvalidBootnodeAndEqualSignMustDisplayError() {
    parseCommand("--bootnodes=invalid_enode_url");
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    final String expectedErrorOutputStart =
        "Invalid enode URL syntax 'invalid_enode_url'. Enode URL should have the following format "
            + "'enode://<node_id>@<ip>:<listening_port>[?discport=<discovery_port>]'.";
    assertThat(commandErrorOutput.toString(UTF_8)).startsWith(expectedErrorOutputStart);
  }

  private static final String VALID_ENR_1 =
      "enr:-Iu4QLm7bZGdAt9NSeJG0cEnJohWcQTQaI9wFLu3Q7eHIDfrI4cwtzvEW3F3VbG9XdFXlrHyFGeXPn9snTCQJ9bnMRABgmlkgnY0gmlwhAOTJQCJc2VjcDI1NmsxoQIZdZD6tDYpkpEfVo5bgiU8MGRjhcOmHGD2nErK0UKRrIN0Y3CCIyiDdWRwgiMo";
  private static final String VALID_ENR_2 =
      "enr:-Iu4QEDJ4Wa_UQNbK8Ay1hFEkXvd8psolVK6OhfTL9irqz3nbXxxWyKwEplPfkju4zduVQj6mMhUCm9R2Lc4YM5jPcIBgmlkgnY0gmlwhANrfESJc2VjcDI1NmsxoQJCYz2-nsqFpeEj6eov9HSi9QssIVIVNr0I89J1vXM9foN0Y3CCIyiDdWRwgiMo";

  @Test
  public void callingWithValidEnrBootnodeAndV5EnabledMustSucceed() {
    parseCommand("--Xv5-discovery-enabled", "--bootnodes", VALID_ENR_1);

    verify(mockRunnerBuilder).ethNetworkConfig(ethNetworkConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(ethNetworkConfigArgumentCaptor.getValue().enrBootNodes()).hasSize(1);
    assertThat(ethNetworkConfigArgumentCaptor.getValue().enodeBootNodes()).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void callingWithMultipleValidEnrBootnodesAndV5EnabledMustSucceed() {
    parseCommand("--Xv5-discovery-enabled", "--bootnodes", VALID_ENR_1 + "," + VALID_ENR_2);

    verify(mockRunnerBuilder).ethNetworkConfig(ethNetworkConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(ethNetworkConfigArgumentCaptor.getValue().enrBootNodes()).hasSize(2);
    assertThat(ethNetworkConfigArgumentCaptor.getValue().enodeBootNodes()).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"enr:-invalidenrdata", "enr:invalidvalue", "invalidvalue"})
  public void callingWithInvalidBootnodeAndV5EnabledMustDisplayError(final String bootnode) {
    parseCommand("--Xv5-discovery-enabled", "--bootnodes", bootnode);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Invalid ENR bootnode: '" + bootnode + "'");
  }

  @Test
  public void bootnodesOptionMustBeUsed() {
    parseCommand("--bootnodes", String.join(",", VALID_ENODE_STRINGS));

    verify(mockRunnerBuilder).ethNetworkConfig(ethNetworkConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(ethNetworkConfigArgumentCaptor.getValue().enodeBootNodes())
        .isEqualTo(
            Stream.of(VALID_ENODE_STRINGS)
                .map(EnodeURLImpl::fromString)
                .collect(Collectors.toList()));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void bannedNodeIdsOptionMustBeUsed() {
    final Bytes[] nodes = {
      Bytes.fromHexString(
          "6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0"),
      Bytes.fromHexString(
          "7f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0"),
      Bytes.fromHexString(
          "0x8f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0")
    };

    final String nodeIdsArg =
        Arrays.stream(nodes).map(Bytes::toShortHexString).collect(Collectors.joining(","));
    parseCommand("--banned-node-ids", nodeIdsArg);

    verify(mockRunnerBuilder).bannedNodeIds(bytesCollectionCollector.capture());
    verify(mockRunnerBuilder).build();

    assertThat(bytesCollectionCollector.getValue().toArray()).isEqualTo(nodes);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void callingWithBannedNodeidsOptionButNoValueMustDisplayError() {
    parseCommand("--banned-node-ids");
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    final String expectedErrorOutputStart =
        "Missing required parameter for option '--banned-node-ids' at index 0 (<NODEID>)";
    assertThat(commandErrorOutput.toString(UTF_8)).startsWith(expectedErrorOutputStart);
  }

  @Test
  public void callingWithBannedNodeidsOptionWithInvalidValuesMustDisplayError() {
    parseCommand("--banned-node-ids", "0x10,20,30");
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    final String expectedErrorOutputStart =
        "Invalid ids supplied to '--banned-node-ids'. Expected 64 bytes in 0x10";
    assertThat(commandErrorOutput.toString(UTF_8)).startsWith(expectedErrorOutputStart);
  }

  @Test
  public void maxpeersOptionMustBeUsed() {

    final int maxPeers = 123;
    parseCommand("--max-peers", String.valueOf(maxPeers));

    verify(mockControllerBuilder).maxPeers(intArgumentCaptor.capture());
    verify(mockControllerBuilder).build();

    assertThat(intArgumentCaptor.getValue()).isEqualTo(maxPeers);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void p2pPeerUpperBound_without_p2pPeerLowerBound_shouldSetMaxPeers() {

    final int maxPeers = 23;
    parseCommand("--p2p-peer-upper-bound", String.valueOf(maxPeers));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();

    verify(mockControllerBuilder).maxPeers(intArgumentCaptor.capture());
    assertThat(intArgumentCaptor.getValue()).isEqualTo(maxPeers);

    verify(mockRunnerBuilder).build();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void remoteConnectionsPercentageOptionMustBeUsed() {

    final int remoteConnectionsPercentage = 12;
    parseCommand(
        "--remote-connections-limit-enabled=true",
        "--remote-connections-max-percentage",
        String.valueOf(remoteConnectionsPercentage));

    verify(mockControllerBuilder).maxRemotelyInitiatedPeers(intArgumentCaptor.capture());
    verify(mockControllerBuilder).build();

    assertThat(intArgumentCaptor.getValue())
        .isEqualTo(
            (int) Math.floor(25 * Fraction.fromPercentage(remoteConnectionsPercentage).getValue()));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void remoteConnectionsPercentageWithInvalidFormatMustFail() {

    parseCommand(
        "--remote-connections-limit-enabled", "--remote-connections-max-percentage", "invalid");
    verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Invalid value for option '--remote-connections-max-percentage'",
            "should be a number between 0 and 100 inclusive");
  }

  @Test
  public void remoteConnectionsPercentageWithOutOfRangeMustFail() {

    parseCommand(
        "--remote-connections-limit-enabled", "--remote-connections-max-percentage", "150");
    verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Invalid value for option '--remote-connections-max-percentage'",
            "should be a number between 0 and 100 inclusive");
  }

  @Test
  public void syncMode_checkpoint() {
    parseCommand("--sync-mode", "CHECKPOINT");
    verify(mockControllerBuilder).synchronizerConfiguration(syncConfigurationCaptor.capture());

    final SynchronizerConfiguration syncConfig = syncConfigurationCaptor.getValue();
    assertThat(syncConfig.getSyncMode()).isEqualTo(SyncMode.SNAP);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void syncMode_full_requires_bonsaiLimitTrieLogsToBeDisabled() {
    parseCommand("--sync-mode", "FULL", "--bonsai-limit-trie-logs-enabled=false");
    verify(mockControllerBuilder).synchronizerConfiguration(syncConfigurationCaptor.capture());

    final SynchronizerConfiguration syncConfig = syncConfigurationCaptor.getValue();
    assertThat(syncConfig.getSyncMode()).isEqualTo(SyncMode.FULL);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void syncMode_invalid() {
    parseCommand("--sync-mode", "bogus");
    verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Invalid value for option '--sync-mode': 'bogus' is not a valid sync mode. Valid values are: FULL, SNAP");
  }

  @Test
  public void syncMode_full_by_default_for_dev() {
    parseCommand("--network", "dev");
    verify(mockControllerBuilder).synchronizerConfiguration(syncConfigurationCaptor.capture());

    final SynchronizerConfiguration syncConfig = syncConfigurationCaptor.getValue();
    assertThat(syncConfig.getSyncMode()).isEqualTo(SyncMode.FULL);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void storage_bonsai_by_default() {
    parseCommand();
    verify(mockControllerBuilder)
        .dataStorageConfiguration(dataStorageConfigurationArgumentCaptor.capture());

    final DataStorageConfiguration dataStorageConfig =
        dataStorageConfigurationArgumentCaptor.getValue();
    assertThat(dataStorageConfig.getDataStorageFormat()).isEqualTo(BONSAI);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void syncMode_snap_by_default() {
    parseCommand();
    verify(mockControllerBuilder).synchronizerConfiguration(syncConfigurationCaptor.capture());

    final SynchronizerConfiguration syncConfig = syncConfigurationCaptor.getValue();
    assertThat(syncConfig.getSyncMode()).isEqualTo(SyncMode.SNAP);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void helpShouldDisplayFastSyncOptions() {
    parseCommand("--help");

    verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).contains("--sync-min-peers");
    // whitelist is now a hidden option
    assertThat(commandOutput.toString(UTF_8)).doesNotContain("whitelist");
  }

  @Test
  public void checkValidDefaultSnapSyncMinPeers() {
    parseCommand("--sync-mode", "SNAP");
    verify(mockControllerBuilder).synchronizerConfiguration(syncConfigurationCaptor.capture());

    final SynchronizerConfiguration syncConfig = syncConfigurationCaptor.getValue();
    assertThat(syncConfig.getSyncMode()).isEqualTo(SyncMode.SNAP);
    assertThat(syncConfig.getSyncMinimumPeerCount()).isEqualTo(5);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void parsesValidSnapSyncMinPeersOption() {
    parseCommand("--sync-mode", "SNAP", "--sync-min-peers", "11");
    verify(mockControllerBuilder).synchronizerConfiguration(syncConfigurationCaptor.capture());

    final SynchronizerConfiguration syncConfig = syncConfigurationCaptor.getValue();
    assertThat(syncConfig.getSyncMode()).isEqualTo(SyncMode.SNAP);
    assertThat(syncConfig.getSyncMinimumPeerCount()).isEqualTo(11);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void parsesValidSyncMinPeersOption() {
    parseCommand("--sync-mode", "SNAP", "--sync-min-peers", "11");
    verify(mockControllerBuilder).synchronizerConfiguration(syncConfigurationCaptor.capture());

    final SynchronizerConfiguration syncConfig = syncConfigurationCaptor.getValue();
    assertThat(syncConfig.getSyncMode()).isEqualTo(SyncMode.SNAP);
    assertThat(syncConfig.getSyncMinimumPeerCount()).isEqualTo(11);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"127.0.0.0/24,10.0.0.0/24", "fd00::/64,fe80::/10", "127.0.0.0/24,fd00::/64"})
  public void netRestrictParsedCorrectly(final String subnets) {
    parseCommand("--net-restrict", subnets);
    verify(mockRunnerBuilder).allowedSubnets(allowedSubnetsArgumentCaptor.capture());
    final List<String> expected = Splitter.on(',').splitToList(subnets);
    assertThat(allowedSubnetsArgumentCaptor.getValue()).hasSize(expected.size());
    for (int i = 0; i < expected.size(); i++) {
      assertThat(allowedSubnetsArgumentCaptor.getValue().get(i).toString())
          .isEqualTo(expected.get(i));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"127.0.0.1/abc", "abc", ""})
  public void netRestrictInvalidShouldFail(final String subnet) {
    parseCommand("--net-restrict", subnet);
    verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Invalid value for option '--net-restrict'");
  }

  @Test
  public void ethStatsOptionIsParsedCorrectly() {
    final String url = "besu-node:secret@host:443";
    parseCommand("--ethstats", url);
    verify(mockRunnerBuilder).ethstatsOptions(ethstatsOptionsArgumentCaptor.capture());
    assertThat(ethstatsOptionsArgumentCaptor.getValue().getEthstatsUrl()).isEqualTo(url);
  }

  @Test
  public void ethStatsContactOptionIsParsedCorrectly() {
    final String contact = "contact@mail.net";
    parseCommand("--ethstats", "besu-node:secret@host:443", "--ethstats-contact", contact);
    verify(mockRunnerBuilder).ethstatsOptions(ethstatsOptionsArgumentCaptor.capture());
    assertThat(ethstatsOptionsArgumentCaptor.getValue().getEthstatsContact()).isEqualTo(contact);
  }

  @Test
  public void ethStatsContactOptionCannotBeUsedWithoutEthStatsServerProvided() {
    parseCommand("--ethstats-contact", "besu-updated");
    verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "The `--ethstats-contact` requires ethstats server URL to be provided. Either remove --ethstats-contact or provide a URL (via --ethstats=nodename:secret@host:port)");
  }

  @Test
  public void pathbasedLimitTrieLogsEnabledByDefault() {
    parseCommand();
    verify(mockControllerBuilder)
        .dataStorageConfiguration(dataStorageConfigurationArgumentCaptor.capture());

    final DataStorageConfiguration dataStorageConfiguration =
        dataStorageConfigurationArgumentCaptor.getValue();
    assertThat(dataStorageConfiguration.getDataStorageFormat()).isEqualTo(BONSAI);
    assertThat(
            dataStorageConfiguration
                .getPathBasedExtraStorageConfiguration()
                .getLimitTrieLogsEnabled())
        .isTrue();
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void bonsaiLimitTrieLogsDisabledWhenFullSyncEnabled() {
    parseCommand("--sync-mode=FULL");

    verify(mockControllerBuilder)
        .dataStorageConfiguration(dataStorageConfigurationArgumentCaptor.capture());

    final DataStorageConfiguration dataStorageConfiguration =
        dataStorageConfigurationArgumentCaptor.getValue();
    assertThat(dataStorageConfiguration.getDataStorageFormat()).isEqualTo(BONSAI);
    assertThat(
            dataStorageConfiguration
                .getPathBasedExtraStorageConfiguration()
                .getLimitTrieLogsEnabled())
        .isFalse();
    verify(mockLogger)
        .warn(
            "Forcing {}, since it cannot be enabled with --sync-mode={} and --data-storage-format={}.",
            "--bonsai-limit-trie-logs-enabled=false",
            SyncMode.FULL,
            DataStorageFormat.BONSAI);
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void parsesInvalidWhenFullSyncAndBonsaiLimitTrieLogsExplicitlyTrue() {
    parseCommand("--sync-mode=FULL", "--bonsai-limit-trie-logs-enabled=true");

    verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Cannot enable --bonsai-limit-trie-logs-enabled with --sync-mode=FULL and --data-storage-format=BONSAI. You must set --bonsai-limit-trie-logs-enabled=false or use a different sync-mode");
  }

  @Test
  public void parsesValidBonsaiHistoricalBlockLimitOption() {
    parseCommand(
        "--bonsai-limit-trie-logs-enabled=false",
        "--data-storage-format",
        "BONSAI",
        "--bonsai-historical-block-limit",
        "11");
    verify(mockControllerBuilder)
        .dataStorageConfiguration(dataStorageConfigurationArgumentCaptor.capture());

    final DataStorageConfiguration dataStorageConfiguration =
        dataStorageConfigurationArgumentCaptor.getValue();
    assertThat(dataStorageConfiguration.getDataStorageFormat()).isEqualTo(BONSAI);
    assertThat(
            dataStorageConfiguration.getPathBasedExtraStorageConfiguration().getMaxLayersToLoad())
        .isEqualTo(11);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void parsesInvalidBonsaiHistoricalBlockLimitOption() {

    parseCommand("--data-storage-format", "BONSAI", "--bonsai-historical-block-limit", "ten");

    verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Invalid value for option '--bonsai-historical-block-limit': 'ten' is not a long");
  }

  @Test
  public void dnsEnabledOptionIsParsedCorrectly() {
    final TestBesuCommand besuCommand = parseCommand("--Xdns-enabled", "true");

    assertThat(besuCommand.getEnodeDnsConfiguration().dnsEnabled()).isTrue();
    assertThat(besuCommand.getEnodeDnsConfiguration().updateEnabled()).isFalse();
  }

  @Test
  public void versionCompatibilityProtectionTrueOptionIsParsedCorrectly() {
    final TestBesuCommand besuCommand = parseCommand("--version-compatibility-protection", "true");

    assertThat(besuCommand.getVersionCompatibilityProtection()).isTrue();
  }

  @Test
  public void dnsUpdateEnabledOptionIsParsedCorrectly() {
    final TestBesuCommand besuCommand =
        parseCommand("--Xdns-enabled", "true", "--Xdns-update-enabled", "true");

    assertThat(besuCommand.getEnodeDnsConfiguration().dnsEnabled()).isTrue();
    assertThat(besuCommand.getEnodeDnsConfiguration().updateEnabled()).isTrue();
  }

  @Test
  public void dnsUpdateEnabledOptionCannotBeUsedWithoutDnsEnabled() {
    parseCommand("--Xdns-update-enabled", "true");
    verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "The `--Xdns-update-enabled` requires dns to be enabled. Either remove --Xdns-update-enabled or specify dns is enabled (--Xdns-enabled)");
  }

  @Test
  public void rpcHttpEnabledPropertyDefaultIsFalse() {
    parseCommand();

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().isEnabled()).isFalse();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void graphQLHttpEnabledPropertyDefaultIsFalse() {
    parseCommand();

    verify(mockRunnerBuilder).graphQLConfiguration(graphQLConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(graphQLConfigArgumentCaptor.getValue().isEnabled()).isFalse();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcApisSupportsEngine() {
    parseCommand("--rpc-http-api", "ENGINE", "--rpc-http-enabled");

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getRpcApis())
        .containsExactlyInAnyOrder(ENGINE.name());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void engineApiAuthOptions() {
    parseCommand("--rpc-http-enabled", "--engine-jwt-secret", "/tmp/fakeKey.hex");
    verify(mockRunnerBuilder).engineJsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    assertThat(jsonRpcConfigArgumentCaptor.getValue().isAuthenticationEnabled()).isTrue();
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void engineApiDisableAuthOptions() {
    parseCommand(
        "--rpc-http-enabled", "--engine-jwt-disabled", "--engine-jwt-secret", "/tmp/fakeKey.hex");
    verify(mockRunnerBuilder).engineJsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    assertThat(jsonRpcConfigArgumentCaptor.getValue().isAuthenticationEnabled()).isFalse();
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void metricsEnabledPropertyDefaultIsFalse() {
    parseCommand();

    verify(mockRunnerBuilder).metricsConfiguration(metricsConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(metricsConfigArgumentCaptor.getValue().isEnabled()).isFalse();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void metricsEnabledPropertyMustBeUsed() {
    parseCommand("--metrics-enabled");

    verify(mockRunnerBuilder).metricsConfiguration(metricsConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(metricsConfigArgumentCaptor.getValue().isEnabled()).isTrue();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void metricsPushOptionsRequiresPushToBeEnabled() {
    parseCommand(
        "--metrics-push-host",
        "0.0.0.0",
        "--metrics-push-port",
        "1234",
        "--metrics-push-interval",
        "2",
        "--metrics-push-prometheus-job",
        "job-name");

    verifyOptionsConstraintLoggerCall(
        "--metrics-push-enabled",
        "--metrics-push-host",
        "--metrics-push-port",
        "--metrics-push-interval",
        "--metrics-push-prometheus-job");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void metricsPushOptionsRequiresPushToBeEnabledToml() throws IOException {
    final Path toml =
        createTempFile(
            "toml",
            "metrics-push-host=\"0.0.0.0\"\n"
                + "metrics-push-port=1234\n"
                + "metrics-push-interval=2\n"
                + "metrics-push-prometheus-job=\"job-name\"\n");

    parseCommand("--config-file", toml.toString());

    verifyOptionsConstraintLoggerCall(
        "--metrics-push-enabled",
        "--metrics-push-host",
        "--metrics-push-port",
        "--metrics-push-interval",
        "--metrics-push-prometheus-job");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void metricsOptionsRequiresPullMetricsToBeEnabled() {
    parseCommand("--metrics-host", "0.0.0.0", "--metrics-port", "1234");

    verifyOptionsConstraintLoggerCall("--metrics-enabled", "--metrics-host", "--metrics-port");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void metricsOptionsRequiresPullMetricsToBeEnabledToml() throws IOException {
    final Path toml = createTempFile("toml", "metrics-host=\"0.0.0.0\"\n" + "metrics-port=1234\n");

    parseCommand("--config-file", toml.toString());

    verifyOptionsConstraintLoggerCall("--metrics-enabled", "--metrics-host", "--metrics-port");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void metricsHostAndPortOptionMustBeUsed() {
    final String host = "1.2.3.4";
    final int port = 1234;
    parseCommand(
        "--metrics-enabled", "--metrics-host", host, "--metrics-port", String.valueOf(port));

    verify(mockRunnerBuilder).metricsConfiguration(metricsConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(metricsConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);
    assertThat(metricsConfigArgumentCaptor.getValue().getPort()).isEqualTo(port);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void metricsHostMayBeLocalhost() {
    final String host = "localhost";
    parseCommand("--metrics-enabled", "--metrics-host", host);

    verify(mockRunnerBuilder).metricsConfiguration(metricsConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(metricsConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void metricsHostMayBeIPv6() {
    final String host = "2600:DB8::8545";
    parseCommand("--metrics-enabled", "--metrics-host", host);

    verify(mockRunnerBuilder).metricsConfiguration(metricsConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(metricsConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void metricsCategoryPropertyMustBeUsed() {
    parseCommand("--metrics-enabled", "--metrics-category", StandardMetricCategory.JVM.toString());

    verify(mockRunnerBuilder).metricsConfiguration(metricsConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(metricsConfigArgumentCaptor.getValue().getMetricCategories())
        .containsExactly(StandardMetricCategory.JVM);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void metricsUnknownCategoryRaiseError() {
    parseCommand("--metrics-enabled", "--metrics-category", "UNKNOWN_CATEGORY");

    verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .startsWith("--metrics-categories contains unknown categories: [UNKNOWN_CATEGORY]");
  }

  @Test
  public void metricsPushEnabledPropertyMustBeUsed() {
    parseCommand("--metrics-push-enabled");

    verify(mockRunnerBuilder).metricsConfiguration(metricsConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(metricsConfigArgumentCaptor.getValue().isPushEnabled()).isTrue();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void metricsPushHostAndPushPortOptionMustBeUsed() {
    final String host = "1.2.3.4";
    final int port = 1234;
    parseCommand(
        "--metrics-push-enabled",
        "--metrics-push-host",
        host,
        "--metrics-push-port",
        String.valueOf(port));

    verify(mockRunnerBuilder).metricsConfiguration(metricsConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(metricsConfigArgumentCaptor.getValue().getPushHost()).isEqualTo(host);
    assertThat(metricsConfigArgumentCaptor.getValue().getPushPort()).isEqualTo(port);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void metricsPushHostMayBeLocalhost() {
    final String host = "localhost";
    parseCommand("--metrics-push-enabled", "--metrics-push-host", host);

    verify(mockRunnerBuilder).metricsConfiguration(metricsConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(metricsConfigArgumentCaptor.getValue().getPushHost()).isEqualTo(host);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void metricsPushIntervalMustBeUsed() {
    parseCommand("--metrics-push-enabled", "--metrics-push-interval", "42");

    verify(mockRunnerBuilder).metricsConfiguration(metricsConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(metricsConfigArgumentCaptor.getValue().getPushInterval()).isEqualTo(42);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void metricsPrometheusJobMustBeUsed() {
    parseCommand("--metrics-push-enabled", "--metrics-push-prometheus-job", "besu-command-test");

    verify(mockRunnerBuilder).metricsConfiguration(metricsConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(metricsConfigArgumentCaptor.getValue().getPrometheusJob())
        .isEqualTo("besu-command-test");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void metricsAndMetricsPushMustNotBeUsedTogether() {
    parseCommand("--metrics-enabled", "--metrics-push-enabled");

    verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .startsWith("--metrics-enabled option and --metrics-push-enabled option can't be used");
  }

  @Test
  public void colorCanBeEnabledOrDisabledExplicitly() {
    Stream.of(true, false)
        .forEach(
            bool -> {
              parseCommand("--color-enabled", bool.toString());
              assertThat(BesuCommand.getColorEnabled()).contains(bool);
            });
  }

  @Test
  public void devModeOptionMustBeUsed() {
    parseCommand("--network", "dev");

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    assertThat(networkArg.getValue()).isEqualTo(EthNetworkConfig.getNetworkConfig(DEV));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void namedNetworkOptionIsUnderscoreHyphenInsensitive() {
    parseCommand("--network", "linea-sepolia");

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    assertThat(networkArg.getValue()).isEqualTo(EthNetworkConfig.getNetworkConfig(LINEA_SEPOLIA));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void nonExistingNetworkThrowsError() {
    parseCommand("--network", "foo");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).contains("Network foo does not exist");
  }

  @Test
  public void futureEipsValuesAreUsed() {
    parseCommand("--network", "future_eips");

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    assertThat(networkArg.getValue()).isEqualTo(EthNetworkConfig.getNetworkConfig(FUTURE_EIPS));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void experimentalEipsValuesAreUsed() {
    parseCommand("--network", "experimental_eips");

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    assertThat(networkArg.getValue())
        .isEqualTo(EthNetworkConfig.getNetworkConfig(EXPERIMENTAL_EIPS));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void sepoliaValuesAreUsed() {
    parseCommand("--network", "sepolia");

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);
    final ArgumentCaptor<MiningConfiguration> miningArg =
        ArgumentCaptor.forClass(MiningConfiguration.class);

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).miningParameters(miningArg.capture());
    verify(mockControllerBuilder).build();

    assertThat(networkArg.getValue()).isEqualTo(EthNetworkConfig.getNetworkConfig(SEPOLIA));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(miningArg.getValue().getTargetGasLimit()).isEmpty();

    verify(mockLogger, never()).warn(contains("Sepolia is deprecated and will be shutdown"));
  }

  @Test
  public void hoodiValuesAreUsed() {
    parseCommand("--network", "hoodi");

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);
    final ArgumentCaptor<MiningConfiguration> miningArg =
        ArgumentCaptor.forClass(MiningConfiguration.class);

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).miningParameters(miningArg.capture());
    verify(mockControllerBuilder).build();

    assertThat(networkArg.getValue()).isEqualTo(EthNetworkConfig.getNetworkConfig(HOODI));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(miningArg.getValue().getTargetGasLimit()).isEmpty();

    verify(mockLogger, never()).warn(contains("Hoodi is deprecated and will be shutdown"));
  }

  @Test
  public void luksoValuesAreUsed() {
    parseCommand("--network", "lukso");

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    assertThat(networkArg.getValue()).isEqualTo(EthNetworkConfig.getNetworkConfig(LUKSO));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void ephemeryValuesAreUsed() {
    parseCommand("--network", "ephemery");

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();
    assertThat(NetworkDefinition.valueOf(String.valueOf(EPHEMERY))).isEqualTo(EPHEMERY);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void sepoliaValuesCanBeOverridden() {
    networkValuesCanBeOverridden("sepolia");
  }

  @Test
  public void hoodiValuesCanBeOverridden() {
    networkValuesCanBeOverridden("hoodi");
  }

  @Test
  public void futureEipsValuesCanBeOverridden() {
    networkValuesCanBeOverridden("future_eips");
  }

  @Test
  public void experimentalEipsValuesCanBeOverridden() {
    networkValuesCanBeOverridden("experimental_eips");
  }

  @Test
  public void devValuesCanBeOverridden() {
    networkValuesCanBeOverridden("dev");
  }

  private void networkValuesCanBeOverridden(final String network) {
    parseCommand(
        "--network",
        network,
        "--network-id",
        "1234567",
        "--bootnodes",
        String.join(",", VALID_ENODE_STRINGS));

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();

    assertThat(networkArg.getValue().enodeBootNodes())
        .isEqualTo(
            Stream.of(VALID_ENODE_STRINGS)
                .map(EnodeURLImpl::fromString)
                .collect(Collectors.toList()));
    assertThat(networkArg.getValue().networkId()).isEqualTo(1234567);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void fullCLIOptionsShown() {
    parseCommand("--help");

    verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).contains("--config-file");
    assertThat(commandOutput.toString(UTF_8)).contains("--data-path");
    assertThat(commandOutput.toString(UTF_8)).contains("--genesis-file");
  }

  @Test
  public void tomlThatHasInvalidOptions() throws IOException {
    final URL configFile = this.getClass().getResource("/complete_config.toml");
    // update genesis file path, "similar" valid option and add invalid options
    final Path genesisFile = createFakeGenesisFile(GENESIS_VALID_JSON);
    final String updatedConfig =
        Resources.toString(configFile, UTF_8)
                .replace("/opt/besu/genesis.json", escapeTomlString(genesisFile.toString()))
                .replace("rpc-http-api", "rpc-http-apis")
            + System.lineSeparator()
            + "invalid_option=true"
            + System.lineSeparator()
            + "invalid_option2=true";

    final Path toml = createTempFile("toml", updatedConfig.getBytes(UTF_8));

    // Parse it.
    parseCommand("--config-file", toml.toString());

    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Unknown options in TOML configuration file: invalid_option, invalid_option2");
  }

  @Test
  public void requiredBlocksSetWhenSpecified() {
    final long blockNumber = 8675309L;
    final String hash = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

    parseCommand("--required-block=" + blockNumber + "=" + hash);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<Map<Long, Hash>> requiredBlocksArg = ArgumentCaptor.forClass(Map.class);

    verify(mockControllerBuilder).requiredBlocks(requiredBlocksArg.capture());
    verify(mockControllerBuilder).build();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();

    assertThat(requiredBlocksArg.getValue()).containsOnlyKeys(blockNumber);
    assertThat(requiredBlocksArg.getValue())
        .containsEntry(blockNumber, Hash.fromHexStringLenient(hash));
  }

  @Test
  public void requiredBlocksEmptyWhenNotSpecified() {
    parseCommand();

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<Map<Long, Hash>> requiredBlocksArg = ArgumentCaptor.forClass(Map.class);

    verify(mockControllerBuilder).requiredBlocks(requiredBlocksArg.capture());
    verify(mockControllerBuilder).build();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();

    assertThat(requiredBlocksArg.getValue()).isEmpty();
  }

  @Test
  public void requiredBlocksMultipleBlocksOneArg() {
    final long block1 = 8675309L;
    final long block2 = 5551212L;
    final String hash1 = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    final String hash2 = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    parseCommand("--required-block=" + block1 + "=" + hash1 + "," + block2 + "=" + hash2);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<Map<Long, Hash>> requiredBlocksArg = ArgumentCaptor.forClass(Map.class);

    verify(mockControllerBuilder).requiredBlocks(requiredBlocksArg.capture());
    verify(mockControllerBuilder).build();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();

    assertThat(requiredBlocksArg.getValue()).containsOnlyKeys(block1, block2);
    assertThat(requiredBlocksArg.getValue())
        .containsEntry(block1, Hash.fromHexStringLenient(hash1));
    assertThat(requiredBlocksArg.getValue())
        .containsEntry(block2, Hash.fromHexStringLenient(hash2));
  }

  @Test
  public void requiredBlocksMultipleBlocksTwoArgs() {
    final long block1 = 8675309L;
    final long block2 = 5551212L;
    final String hash1 = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    final String hash2 = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    parseCommand(
        "--required-block=" + block1 + "=" + hash1, "--required-block=" + block2 + "=" + hash2);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<Map<Long, Hash>> requiredBlocksArg = ArgumentCaptor.forClass(Map.class);

    verify(mockControllerBuilder).requiredBlocks(requiredBlocksArg.capture());
    verify(mockControllerBuilder).build();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();

    assertThat(requiredBlocksArg.getValue()).containsOnlyKeys(block1, block2);
    assertThat(requiredBlocksArg.getValue())
        .containsEntry(block1, Hash.fromHexStringLenient(hash1));
    assertThat(requiredBlocksArg.getValue())
        .containsEntry(block2, Hash.fromHexStringLenient(hash2));
  }

  @Test
  public void logLevelHasNullAsDefaultValue() {
    final TestBesuCommand command = parseCommand();

    assertThat(command.getLogLevel()).isNull();
  }

  @Test
  public void logLevelIsSetByLoggingOption() {
    final TestBesuCommand command = parseCommand("--logging", "WARN");

    assertThat(command.getLogLevel()).isEqualTo("WARN");
  }

  @Test
  public void assertThatDefaultHttpTimeoutSecondsWorks() {
    parseCommand();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();
    assertThat(jsonRpcConfigArgumentCaptor.getValue().getHttpTimeoutSec())
        .isEqualTo(TimeoutOptions.defaultOptions().getTimeoutSeconds());
  }

  @Test
  public void assertThatHttpTimeoutSecondsWorks() {
    parseCommand("--Xhttp-timeout-seconds=513");
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();
    assertThat(jsonRpcConfigArgumentCaptor.getValue().getHttpTimeoutSec()).isEqualTo(513);
  }

  @Test
  public void assertThatInvalidHttpTimeoutSecondsFails() {
    parseCommand("--Xhttp-timeout-seconds=abc");
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Invalid value for option", "--Xhttp-timeout-seconds", "abc", "is not a long");
  }

  @Test
  public void assertThatDefaultWsTimeoutSecondsWorks() {
    parseCommand();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    verify(mockRunnerBuilder).webSocketConfiguration(wsRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();
    assertThat(wsRpcConfigArgumentCaptor.getValue().getTimeoutSec())
        .isEqualTo(TimeoutOptions.defaultOptions().getTimeoutSeconds());
  }

  @Test
  public void assertThatWsTimeoutSecondsWorks() {
    parseCommand("--Xws-timeout-seconds=11112018");
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    verify(mockRunnerBuilder).webSocketConfiguration(wsRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();
    assertThat(wsRpcConfigArgumentCaptor.getValue().getTimeoutSec()).isEqualTo(11112018);
  }

  @Test
  public void assertThatInvalidWsTimeoutSecondsFails() {
    parseCommand("--Xws-timeout-seconds=abc");
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Invalid value for option", "--Xws-timeout-seconds", "abc", "is not a long");
  }

  @Test
  public void assertThatDuplicatePortSpecifiedFails() {
    parseCommand(
        "--p2p-port=9",
        "--rpc-http-enabled",
        "--rpc-http-port=10",
        "--rpc-ws-port=10",
        "--rpc-ws-enabled");
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Port number '10' has been specified multiple times.");
  }

  @Test
  public void assertThatDuplicatePortZeroSucceeds() {
    parseCommand("--p2p-port=0", "--rpc-http-port=0", "--rpc-ws-port=0");
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void assertThatNoPortsSpecifiedSucceeds() {
    parseCommand();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void assertThatAllPortsSpecifiedSucceeds() {
    parseCommand(
        "--p2p-port=9",
        "--graphql-http-port=10",
        "--rpc-http-port=11",
        "--rpc-ws-port=12",
        "--metrics-port=13",
        "--metrics-push-port=14");
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void assertThatCheckPortClashRejectsAsExpectedForEngineApi() throws Exception {
    // use WS port for HTTP
    final int port = 8545;
    parseCommand(
        "--rpc-http-enabled",
        "--rpc-http-port",
        String.valueOf(port),
        "--engine-rpc-port",
        String.valueOf(port));
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Port number '8545' has been specified multiple times. Please review the supplied configuration.");
  }

  @Test
  public void staticNodesFileOptionValueAbsentMessage() {
    parseCommand("--static-nodes-file");
    assertThat(commandErrorOutput.toString(UTF_8))
        .startsWith("Missing required parameter for option");
  }

  @Test
  public void staticNodesFilesOptionInvalidJSONFormatError() throws IOException {
    final Path tempfile =
        createTempFile(
            "static-nodes-badformat.json",
            "\"enode://c0b0e1151971f8a22dc2493c622317c8706c731f6fcf46d93104ef"
                + "3a08f21f7750b5d5e17f311091f732c9f917b02e1ae6d39f076903779fd1e7"
                + "e7e6cd2fcef6@192.168.1.25:30303\"\n]");
    parseCommand("--static-nodes-file", tempfile.toString());
    assertThat(commandErrorOutput.toString(UTF_8))
        .startsWith("Failed to decode:Cannot deserialize ");
  }

  @Test
  public void staticNodesFileOptionFileDoesNotExistMessage() {
    parseCommand("--static-nodes-file", "this-file-does-not-exist-at-all.json");
    assertThat(commandErrorOutput.toString(UTF_8)).contains("Static nodes file", "does not exist");
  }

  @Test
  public void staticNodesFileOptionValidParameter() throws IOException {
    final Path staticNodeTempFile =
        createTempFile(
            "static-nodes-goodformat.json",
            "[\n"
                + "\"enode://c0b0e1151971f8a22dc2493c622317c8706c731f6fcf46d93104ef"
                + "3a08f21f7750b5d5e17f311091f732c9f917b02e1ae6d39f076903779fd1e7"
                + "e7e6cd2fcef6@192.168.1.25:30303\"\n]");
    parseCommand("--static-nodes-file", staticNodeTempFile.toString());
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void invalidEcCurveFailsWithErrorMessage() throws IOException {
    SignatureAlgorithmFactory.resetInstance();
    final Path genesisFile = createFakeGenesisFile(INVALID_GENESIS_EC_CURVE);

    parseCommand("--genesis-file", genesisFile.toString());
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Invalid genesis file configuration for ecCurve. "
                + "abcd is not in the list of valid elliptic curves [secp256k1, secp256r1]");
  }

  @Test
  public void validEcCurveSucceeds() throws IOException {
    SignatureAlgorithmFactory.resetInstance();
    final Path genesisFile = createFakeGenesisFile(VALID_GENESIS_EC_CURVE);

    parseCommand("--genesis-file", genesisFile.toString());
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void nativeSecp256IsDisabled() {
    SignatureAlgorithmFactory.resetInstance();
    parseCommand("--Xsecp256k1-native-enabled", "false");

    verify(mockLogger).info("Using the Java implementation of the signature algorithm");
    assertThat(SignatureAlgorithmFactory.getInstance().isNative()).isFalse();
  }

  @Test
  public void nativeAltBn128IsDisabled() {
    // it is necessary to reset it, because the tested variable
    // is static and it will stay true if it has been set in another test
    // AbstractAltBnPrecompiledContract.resetNative();

    parseCommand("--Xaltbn128-native-enabled", "false");

    verify(mockLogger).info("Using the Java implementation of alt bn128");
    assertThat(AbstractAltBnPrecompiledContract.isNative()).isFalse();
  }

  @Test
  public void nativeLibrariesAreEnabledByDefault() {
    parseCommand();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();

    assertThat(SignatureAlgorithmFactory.getInstance().isNative()).isTrue();
    verify(mockLogger).info("Using the native implementation of the signature algorithm");

    assertThat(AbstractAltBnPrecompiledContract.isNative()).isTrue();
    verify(mockLogger).info("Using the native implementation of alt bn128");
  }

  @Test
  public void logsWarningWhenFailToLoadJemalloc() {
    assumeTrue(PlatformDetector.getOSType().equals("linux"));
    setEnvironmentVariable("BESU_USING_JEMALLOC", "false");
    parseCommand();
    verify(mockLogger)
        .warn(
            argThat(
                arg ->
                    arg.equals(
                            "besu_using_jemalloc is present but is not set to true, jemalloc library not loaded")
                        || arg.equals(
                            "besu_using_jemalloc is present but we failed to load jemalloc library to get the version")));
  }

  @Test
  public void logsSuggestInstallingJemallocWhenEnvVarNotPresent() {
    assumeTrue(PlatformDetector.getOSType().equals("linux"));
    parseCommand();
    verify(mockLogger)
        .info("jemalloc library not found, memory usage may be reduced by installing it");
  }

  @Test
  public void logWarnIfFastSyncMinPeersUsedWithFullSync() {
    parseCommand("--sync-mode", "FULL", "--sync-min-peers", "1");
    verify(mockLogger).warn("--sync-min-peers is ignored in FULL sync-mode");
  }

  @Test
  public void verifyVersionInTheConfigurationOverviewIsCorrect() {
    parseCommand();
    verify(mockLogger)
        .info(argThat(arg -> arg.contains("# Besu version " + BesuVersionUtils.shortVersion())));
  }

  @Test
  public void kzgTrustedSetupFileRequiresDataBlobEnabledNetwork() throws IOException {
    final Path genesisFileWithoutBlobs =
        createFakeGenesisFile(new JsonObject().put("config", new JsonObject()));
    parseCommand(
        "--genesis-file",
        genesisFileWithoutBlobs.toString(),
        "--kzg-trusted-setup",
        "/etc/besu/kzg-trusted-setup.txt");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("--kzg-trusted-setup can only be specified on networks with data blobs enabled");
  }

  @Test
  public void kzgTrustedSetupFileLoadedWithCustomGenesisFile()
      throws IOException, URISyntaxException {
    final Path testSetupAbsolutePath =
        Path.of(BesuCommandTest.class.getResource("/trusted_setup.txt").toURI());
    final Path genesisFileWithBlobs = createFakeGenesisFile(GENESIS_WITH_DATA_BLOBS_ENABLED);
    parseCommand(
        "--genesis-file",
        genesisFileWithBlobs.toString(),
        "--kzg-trusted-setup",
        testSetupAbsolutePath.toString());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void assertUnnamedNetworkNativeRequirements_Met() throws IOException {
    final Path genesisFile =
        createFakeGenesisFile(new JsonObject().put("config", new JsonObject()));
    parseCommand("--genesis-file", genesisFile.toString());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void assertNativeRequirements_UnMet() throws IOException {
    BesuCommand mockCmd = parseCommand("--network=mainnet");
    NetworkDefinition mainnet = NetworkDefinition.MAINNET;
    List<NativeRequirement.NativeRequirementResult> mockNativeRequirements =
        List.of(
            new NativeRequirement.NativeRequirementResult(
                false, "MOCKLIB", Optional.of("Mock error")));
    try (MockedStatic<NativeRequirement> mockStatic = mockStatic(NativeRequirement.class)) {
      mockStatic
          .when(() -> NativeRequirement.getNativeRequirements(mainnet))
          .thenReturn(mockNativeRequirements);
      assertThatExceptionOfType(UnsupportedOperationException.class)
          .isThrownBy(() -> mockCmd.checkRequiredNativeLibraries(mainnet))
          .withMessageContaining("MOCKLIB")
          .withMessageContaining("Mock error")
          .withMessageContaining(System.getProperty("os.arch"))
          .withMessageContaining(System.getProperty("os.name"));
    }
  }

  @Test
  public void assertNativeRequirements_UnMetForUnnamedNetwork() throws IOException {
    final Path fakeGenesisFile = createFakeGenesisFile(GENESIS_VALID_JSON);
    BesuCommand mockCmd = parseCommand("--genesis-file=" + fakeGenesisFile.toString());
    try (var mockStatic = mockStatic(NativeRequirement.class)) {
      // assert no error output
      assertThat(commandOutput.toString(UTF_8)).isEmpty();
      assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();

      // assert no exception
      assertThatNoException()
          .isThrownBy(() -> mockCmd.configureNativeLibs(Optional.of(NetworkDefinition.MAINNET)));
      // assert we didn't check for native requirements for a custom-genesis
      mockStatic.verify(() -> NativeRequirement.getNativeRequirements(any()), times(0));
    }
  }

  @Test
  public void bonsaiFlatDbShouldBeEnabledByDefault() {
    final TestBesuCommand besuCommand = parseCommand();
    assertThat(
            besuCommand
                .getDataStorageOptions()
                .toDomainObject()
                .getPathBasedExtraStorageConfiguration()
                .getUnstable()
                .getFullFlatDbEnabled())
        .isTrue();
  }

  @Test
  public void snapsyncHealingOptionShouldWork() {
    final TestBesuCommand besuCommand = parseCommand("--Xbonsai-full-flat-db-enabled", "false");
    assertThat(
            besuCommand
                .dataStorageOptions
                .toDomainObject()
                .getPathBasedExtraStorageConfiguration()
                .getUnstable()
                .getFullFlatDbEnabled())
        .isFalse();
  }

  @Test
  public void snapsyncForHealingFeaturesShouldFailWhenHealingIsNotEnabled() {
    parseCommand(
        "--Xbonsai-full-flat-db-enabled",
        "false",
        "--Xsnapsync-synchronizer-flat-account-healed-count-per-request",
        "100");
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "--Xsnapsync-synchronizer-flat option can only be used when --Xbonsai-full-flat-db-enabled is true");

    parseCommand(
        "--Xbonsai-full-flat-db-enabled",
        "false",
        "--Xsnapsync-synchronizer-flat-slot-healed-count-per-request",
        "100");
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "--Xsnapsync-synchronizer-flat option can only be used when --Xbonsai-full-flat-db-enabled is true");
  }

  @Test
  public void cacheLastBlocksOptionShouldWork() {
    int numberOfBlocksToCache = 512;
    parseCommand("--cache-last-blocks", String.valueOf(numberOfBlocksToCache));
    verify(mockControllerBuilder).cacheLastBlocks(intArgumentCaptor.capture());
    verify(mockControllerBuilder).build();

    assertThat(intArgumentCaptor.getValue()).isEqualTo(numberOfBlocksToCache);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void cacheLastBlockHeadersOptionShouldWork() {
    int numberOfBlockHeaderToCache = 5000;
    parseCommand("--cache-last-block-headers", String.valueOf(numberOfBlockHeaderToCache));
    verify(mockControllerBuilder).cacheLastBlockHeaders(intArgumentCaptor.capture());
    verify(mockControllerBuilder).build();

    assertThat(intArgumentCaptor.getValue()).isEqualTo(numberOfBlockHeaderToCache);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void isPreloadBlockHeadersCacheEnabledOptionShouldWork() {
    boolean isPreloadBlockHeadersCacheEnabled = true;
    parseCommand("--cache-last-block-headers-preload-enabled=true");
    verify(mockControllerBuilder)
        .isCacheLastBlockHeadersPreloadEnabled(booleanArgumentCaptor.capture());
    verify(mockControllerBuilder).build();

    assertThat(booleanArgumentCaptor.getValue()).isEqualTo(isPreloadBlockHeadersCacheEnabled);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void genesisStateHashCacheEnabledShouldWork() throws IOException {
    final Path genesisFile = createFakeGenesisFile(GENESIS_VALID_JSON);
    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    parseCommand(
        "--genesis-file", genesisFile.toString(), "--genesis-state-hash-cache-enabled=true");

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());
    verify(mockControllerBuilder).build();
    verify(mockControllerBuilder).genesisStateHashCacheEnabled(eq(true));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  void helpOutputShouldDisplayCorrectDefaultValues() {
    parseCommand("--help");

    final String commandOutputString = commandOutput.toString(UTF_8);
    final String errorOutputString = commandErrorOutput.toString(UTF_8);

    assertThat(commandOutputString).doesNotContain("$DEFAULT-VALUE");

    assertThat(errorOutputString).isEmpty();
  }

  @Test
  public void chainPruningNoneModeSkipsValidation() {
    parseCommand(
        "--Xchain-pruning-enabled=NONE",
        "--Xchain-pruning-blocks-retained=-1", // Invalid but should be ignored
        "--Xchain-pruning-bals-retained=-1"); // Invalid but should be ignored

    // Should not throw exception since mode is NONE
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void chainPruningBlocksRetainedMustBeGreaterThanOrEqualToRetainedLimit() {
    parseCommand(
        "--Xchain-pruning-enabled=ALL",
        "--Xchain-pruning-blocks-retained=1000",
        "--Xchain-pruning-retained-minimum=2000");

    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("--Xchain-pruning-blocks-retained must be >= 2000");
  }

  @Test
  public void chainPruningBalsRetainedMustBeGreaterThanOrEqualToRetainedLimit() {
    parseCommand(
        "--Xchain-pruning-enabled=BAL",
        "--Xchain-pruning-bals-retained=1000",
        "--Xchain-pruning-retained-minimum=2000");

    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("--Xchain-pruning-bals-retained must be >= 2000");
  }

  @Test
  public void chainPruningBlocksRetainedCannotBeLessThanBalsRetainedInAllMode() {
    // BALs retained < blocks retained is INVALID
    parseCommand(
        "--Xchain-pruning-enabled=ALL",
        "--Xchain-pruning-blocks-retained=1000",
        "--Xchain-pruning-bals-retained=2000",
        "--Xchain-pruning-retained-minimum=500");

    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "--Xchain-pruning-bals-retained must be <= --Xchain-pruning-blocks-retained when pruning mode is ALL");
  }

  @Test
  public void chainPruningBlocksRetainedCanBeGreaterThanBalsRetainedInAllMode() {
    // BALs retained > blocks retained is VALID
    parseCommand(
        "--Xchain-pruning-enabled=ALL",
        "--Xchain-pruning-blocks-retained=2000",
        "--Xchain-pruning-bals-retained=1000",
        "--Xchain-pruning-retained-minimum=500");

    // Should succeed - we keep more BALs than blocks
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void chainPruningBlocksRetainedCanBeGreaterThanBalsRetainedInBalMode() {
    // In BAL mode, blocks are never pruned (Long.MAX_VALUE)
    // BALs retained can be any value >= retained limit
    parseCommand(
        "--Xchain-pruning-enabled=BAL",
        "--Xchain-pruning-blocks-retained=" + Long.MAX_VALUE,
        "--Xchain-pruning-bals-retained=1000",
        "--Xchain-pruning-retained-minimum=500");

    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void chainPruningBalsRetainedEqualToBlocksRetainedInAllModeSucceeds() {
    // Equal is valid (minimum case)
    parseCommand(
        "--Xchain-pruning-enabled=ALL",
        "--Xchain-pruning-blocks-retained=" + CHAIN_DATA_PRUNING_RETAINED_MINIMUM,
        "--Xchain-pruning-bals-retained=" + CHAIN_DATA_PRUNING_RETAINED_MINIMUM);

    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void chainPruningAllModeWithValidValuesSucceeds() {
    parseCommand(
        "--Xchain-pruning-enabled=ALL",
        "--Xchain-pruning-blocks-retained=" + (CHAIN_DATA_PRUNING_RETAINED_MINIMUM + 100),
        "--Xchain-pruning-bals-retained=" + CHAIN_DATA_PRUNING_RETAINED_MINIMUM,
        "--Xchain-pruning-retained-minimum=" + CHAIN_DATA_PRUNING_RETAINED_MINIMUM);

    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void chainPruningBalModeWithValidValuesSucceeds() {
    parseCommand(
        "--Xchain-pruning-enabled=BAL",
        "--Xchain-pruning-bals-retained=" + CHAIN_DATA_PRUNING_RETAINED_MINIMUM,
        "--Xchain-pruning-retained-minimum=" + CHAIN_DATA_PRUNING_RETAINED_MINIMUM);

    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  void chainPruningEnabledWithPOAShouldFailWhenChainPruningBlocksRetainedValueLessThanEpochLength()
      throws IOException {
    JsonObject genesis = GENESIS_VALID_JSON;

    // for QBFT
    genesis.getJsonObject("config").put("qbft", new JsonObject().put("epochlength", 25000));
    final Path genesisFileQBFT = createFakeGenesisFile(genesis);
    parseCommand(
        "--genesis-file",
        genesisFileQBFT.toString(),
        "--Xchain-pruning-enabled=ALL",
        "--Xchain-pruning-blocks-retained=7200",
        "--version-compatibility-protection=false");
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "--Xchain-pruning-blocks-retained must be >= " + CHAIN_DATA_PRUNING_RETAINED_MINIMUM);
    commandErrorOutput.reset();

    // for IBFT2
    genesis.getJsonObject("config").put("ibft2", new JsonObject().put("epochlength", 20000));
    genesis.getJsonObject("config").remove("qbft");
    final Path genesisFileIBFT = createFakeGenesisFile(genesis);
    parseCommand(
        "--genesis-file",
        genesisFileIBFT.toString(),
        "--Xchain-pruning-enabled=ALL",
        "--Xchain-pruning-blocks-retained=7200",
        "--version-compatibility-protection=false");
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "--Xchain-pruning-blocks-retained must be >= " + CHAIN_DATA_PRUNING_RETAINED_MINIMUM);
  }

  @Test
  void chainPruningEnabledWithPOA() throws IOException {
    JsonObject genesis = GENESIS_VALID_JSON;
    // for QBFT
    genesis
        .getJsonObject("config")
        .put("qbft", new JsonObject().put("epochlength", CHAIN_DATA_PRUNING_RETAINED_MINIMUM));
    final Path genesisFileForQBFT = createFakeGenesisFile(genesis);
    parseCommand(
        "--genesis-file",
        genesisFileForQBFT.toString(),
        "--Xchain-pruning-enabled=ALL",
        "--Xchain-pruning-blocks-retained=" + CHAIN_DATA_PRUNING_RETAINED_MINIMUM,
        "--version-compatibility-protection=false");
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();

    // for IBFT2
    genesis
        .getJsonObject("config")
        .put("ibft2", new JsonObject().put("epochlength", CHAIN_DATA_PRUNING_RETAINED_MINIMUM));
    genesis.getJsonObject("config").remove("qbft");
    final Path genesisFileIBFT = createFakeGenesisFile(genesis);
    parseCommand(
        "--genesis-file",
        genesisFileIBFT.toString(),
        "--Xchain-pruning-enabled=ALL",
        "--Xchain-pruning-blocks-retained=" + CHAIN_DATA_PRUNING_RETAINED_MINIMUM,
        "--version-compatibility-protection=false");

    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  void shouldShowCorrectFormatForMainnetDefaultTargetGasLimit() {
    parseCommand();

    final ArgumentCaptor<MiningConfiguration> miningArg =
        ArgumentCaptor.forClass(MiningConfiguration.class);

    verify(mockControllerBuilder).miningParameters(miningArg.capture());
    verify(mockControllerBuilder).build();
    verify(mockLogger, atLeastOnce()).info(stringArgumentCaptor.capture());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();

    final String startupConfigLog = getStartupConfigLog();
    final String targetGasLimitOutput =
        ConfigurationOverviewBuilder.normalizeGas(DEFAULT_TARGET_GAS_LIMIT);
    assertThat(startupConfigLog)
        .contains(String.format("%s: %s", "Network", NETWORK_MAINNET_CONFIG_LOG));
    assertThat(startupConfigLog)
        .contains(String.format("%s: %s", "Target Gas Limit", targetGasLimitOutput));
  }

  @Test
  void shouldShowCorrectFormatForMainnetCustomTargetGasLimit() {
    parseCommand("--target-gas-limit", String.valueOf(CUSTOM_TARGET_GAS_LIMIT));

    final ArgumentCaptor<MiningConfiguration> miningArg =
        ArgumentCaptor.forClass(MiningConfiguration.class);

    verify(mockControllerBuilder).miningParameters(miningArg.capture());
    verify(mockControllerBuilder).build();
    verify(mockLogger, atLeastOnce()).info(stringArgumentCaptor.capture());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(miningArg.getValue().getTargetGasLimit().getAsLong())
        .isEqualTo(CUSTOM_TARGET_GAS_LIMIT);

    final String startupConfigLog = getStartupConfigLog();
    final String targetGasLimitOutput =
        ConfigurationOverviewBuilder.normalizeGas(CUSTOM_TARGET_GAS_LIMIT);
    assertThat(startupConfigLog)
        .contains(String.format("%s: %s", "Network", NETWORK_MAINNET_CONFIG_LOG));
    assertThat(startupConfigLog)
        .contains(String.format("%s: %s", "Target Gas Limit", targetGasLimitOutput));
  }

  @Test
  void shouldShowCorrectFormatForTestnetDefaultTargetGasLimit() {
    parseCommand("--network", "hoodi");

    final ArgumentCaptor<MiningConfiguration> miningArg =
        ArgumentCaptor.forClass(MiningConfiguration.class);

    verify(mockControllerBuilder).miningParameters(miningArg.capture());
    verify(mockControllerBuilder).build();
    verify(mockLogger, atLeastOnce()).info(stringArgumentCaptor.capture());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();

    final String startupConfigLog = getStartupConfigLog();
    final String targetGasLimitOutput =
        ConfigurationOverviewBuilder.normalizeGas(DEFAULT_TARGET_GAS_LIMIT_TESTNET);
    assertThat(startupConfigLog)
        .contains(String.format("%s: %s", "Network", NETWORK_HOODI_CONFIG_LOG));
    assertThat(startupConfigLog)
        .contains(String.format("%s: %s", "Target Gas Limit", targetGasLimitOutput));
  }

  @Test
  void shouldShowCorrectFormatForTestnetCustomTargetGasLimit() {
    parseCommand(
        "--network", "hoodi", "--target-gas-limit", String.valueOf(CUSTOM_TARGET_GAS_LIMIT));

    final ArgumentCaptor<MiningConfiguration> miningArg =
        ArgumentCaptor.forClass(MiningConfiguration.class);

    verify(mockControllerBuilder).miningParameters(miningArg.capture());
    verify(mockControllerBuilder).build();
    verify(mockLogger, atLeastOnce()).info(stringArgumentCaptor.capture());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(miningArg.getValue().getTargetGasLimit().getAsLong())
        .isEqualTo(CUSTOM_TARGET_GAS_LIMIT);

    final String startupConfigLog = getStartupConfigLog();
    final String targetGasLimitOutput =
        ConfigurationOverviewBuilder.normalizeGas(CUSTOM_TARGET_GAS_LIMIT);
    assertThat(startupConfigLog)
        .contains(String.format("%s: %s", "Network", NETWORK_HOODI_CONFIG_LOG));
    assertThat(startupConfigLog)
        .contains(String.format("%s: %s", "Target Gas Limit", targetGasLimitOutput));
  }

  @Test
  void shouldShowCorrectFormatForCustomNetworkDefaultTargetGasLimit() {
    parseCommand("--network", "dev");

    final ArgumentCaptor<MiningConfiguration> miningArg =
        ArgumentCaptor.forClass(MiningConfiguration.class);

    verify(mockControllerBuilder).miningParameters(miningArg.capture());
    verify(mockControllerBuilder).build();
    verify(mockLogger, atLeastOnce()).info(stringArgumentCaptor.capture());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();

    final String startupConfigLog = getStartupConfigLog();
    final String targetGasLimitOutput =
        ConfigurationOverviewBuilder.normalizeGas(DEFAULT_TARGET_GAS_LIMIT);
    assertThat(startupConfigLog)
        .contains(String.format("%s: %s", "Network", NETWORK_DEV_CONFIG_LOG));
    assertThat(startupConfigLog)
        .contains(String.format("%s: %s", "Target Gas Limit", targetGasLimitOutput));
  }

  @Test
  void shouldShowCorrectFormatForCustomNetworkCustomTargetGasLimit() {
    parseCommand("--network", "dev", "--target-gas-limit", String.valueOf(CUSTOM_TARGET_GAS_LIMIT));

    final ArgumentCaptor<MiningConfiguration> miningArg =
        ArgumentCaptor.forClass(MiningConfiguration.class);

    verify(mockControllerBuilder).miningParameters(miningArg.capture());
    verify(mockControllerBuilder).build();
    verify(mockLogger, atLeastOnce()).info(stringArgumentCaptor.capture());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(miningArg.getValue().getTargetGasLimit().getAsLong())
        .isEqualTo(CUSTOM_TARGET_GAS_LIMIT);

    final String startupConfigLog = getStartupConfigLog();
    final String targetGasLimitOutput =
        ConfigurationOverviewBuilder.normalizeGas(CUSTOM_TARGET_GAS_LIMIT);
    assertThat(startupConfigLog)
        .contains(String.format("%s: %s", "Network", NETWORK_DEV_CONFIG_LOG));
    assertThat(startupConfigLog)
        .contains(String.format("%s: %s", "Target Gas Limit", targetGasLimitOutput));
  }

  private String getStartupConfigLog() {
    // find the startup log config summary
    final Stream<String> stringStream =
        stringArgumentCaptor.getAllValues().stream().filter(p -> p.contains("####"));
    return stringStream.findFirst().orElseThrow();
  }

  @Test
  public void shouldLogErrorIfDuplicateBooleanOptionUsed() {
    parseCommand("--p2p-enabled=true", "--p2p-enabled=false");
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Option '--p2p-enabled' should be specified only once");
  }

  @Test
  public void shouldNotWarnForRepeatableOptionBootnodes() {
    parseCommand(
        "enode://" + VALID_NODE_ID + "@192.168.0.1:4567",
        "enode://" + VALID_NODE_ID + "@192.168.0.2:4567");

    // Verify that the logger does NOT warn about duplication
    verify(mockLogger, never()).warn(contains("bootnodes"));
  }

  @Test
  public void networkEphemeryTest() {
    parseCommand("--network", "ephemery");

    final ArgumentCaptor<EthNetworkConfig> networkArg =
        ArgumentCaptor.forClass(EthNetworkConfig.class);

    verify(mockControllerBuilderFactory).fromEthNetworkConfig(networkArg.capture(), any());

    assertThat(networkArg.getValue().networkId())
        .isEqualTo(networkArg.getValue().genesisConfig().getConfigOptions().getChainId().get());

    Map<String, String> overrides = new HashMap<>();

    overrides.put("chainId", "39438151");
    overrides.put("timestamp", String.valueOf(Instant.now().getEpochSecond()));
    networkArg.getValue().genesisConfig().withOverrides(overrides);

    assertThat(networkArg.getValue().genesisConfig().getConfigOptions().getChainId().get())
        .isEqualTo("39438151");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void checkpointPostMergeWithPostMergeBlockSucceeds() throws IOException {
    final String configText =
        Resources.toString(
            Resources.getResource("valid_post_merge_near_head_checkpoint.json"),
            StandardCharsets.UTF_8);
    final Path genesisFile = createFakeGenesisFile(new JsonObject(configText));

    parseCommand(
        "--genesis-file",
        genesisFile.toString(),
        "--sync-mode",
        "SNAP",
        "--Xcheckpoint-post-merge-enabled");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void checkpointPostMergeWithPostMergeBlockTDEqualsTTDSucceeds() throws IOException {
    final String configText =
        Resources.toString(
            Resources.getResource("valid_pos_checkpoint_pos_TD_equals_TTD.json"),
            StandardCharsets.UTF_8);
    final Path genesisFile = createFakeGenesisFile(new JsonObject(configText));

    parseCommand(
        "--genesis-file",
        genesisFile.toString(),
        "--sync-mode",
        "SNAP",
        "--Xcheckpoint-post-merge-enabled");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }
}
