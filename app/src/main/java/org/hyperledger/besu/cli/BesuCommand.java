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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Long.parseLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.hyperledger.besu.cli.DefaultCommandValues.getDefaultBesuDataPath;
import static org.hyperledger.besu.cli.util.CommandLineUtils.DEPENDENCY_WARNING_MSG;
import static org.hyperledger.besu.cli.util.CommandLineUtils.isOptionSet;
import static org.hyperledger.besu.config.NetworkDefinition.EPHEMERY;
import static org.hyperledger.besu.config.NetworkDefinition.MAINNET;
import static org.hyperledger.besu.controller.BesuController.DATABASE_PATH;
import static org.hyperledger.besu.ethereum.api.jsonrpc.authentication.EngineAuthService.EPHEMERAL_JWT_FILE;

import org.hyperledger.besu.Runner;
import org.hyperledger.besu.RunnerBuilder;
import org.hyperledger.besu.chainexport.Era1BlockExporter;
import org.hyperledger.besu.chainexport.RlpBlockExporter;
import org.hyperledger.besu.chainimport.Era1BlockImporter;
import org.hyperledger.besu.chainimport.JsonBlockImporter;
import org.hyperledger.besu.chainimport.RlpBlockImporter;
import org.hyperledger.besu.cli.config.EthNetworkConfig;
import org.hyperledger.besu.cli.config.NativeRequirement;
import org.hyperledger.besu.cli.config.NativeRequirement.NativeRequirementResult;
import org.hyperledger.besu.cli.config.ProfilesCompletionCandidates;
import org.hyperledger.besu.cli.custom.JsonRPCAllowlistHostsProperty;
import org.hyperledger.besu.cli.error.BesuExecutionExceptionHandler;
import org.hyperledger.besu.cli.error.BesuParameterExceptionHandler;
import org.hyperledger.besu.cli.options.ApiConfigurationOptions;
import org.hyperledger.besu.cli.options.BalConfigurationOptions;
import org.hyperledger.besu.cli.options.ChainPruningOptions;
import org.hyperledger.besu.cli.options.DnsOptions;
import org.hyperledger.besu.cli.options.EngineRPCConfiguration;
import org.hyperledger.besu.cli.options.EngineRPCOptions;
import org.hyperledger.besu.cli.options.EthProtocolOptions;
import org.hyperledger.besu.cli.options.EthstatsOptions;
import org.hyperledger.besu.cli.options.EvmOptions;
import org.hyperledger.besu.cli.options.GraphQlOptions;
import org.hyperledger.besu.cli.options.InProcessRpcOptions;
import org.hyperledger.besu.cli.options.IpcOptions;
import org.hyperledger.besu.cli.options.JsonRpcHttpOptions;
import org.hyperledger.besu.cli.options.LoggingLevelOption;
import org.hyperledger.besu.cli.options.MetricsOptions;
import org.hyperledger.besu.cli.options.MiningOptions;
import org.hyperledger.besu.cli.options.NatOptions;
import org.hyperledger.besu.cli.options.NativeLibraryOptions;
import org.hyperledger.besu.cli.options.NetworkingOptions;
import org.hyperledger.besu.cli.options.NodePrivateKeyFileOption;
import org.hyperledger.besu.cli.options.P2PDiscoveryOptions;
import org.hyperledger.besu.cli.options.PermissionsOptions;
import org.hyperledger.besu.cli.options.PluginsConfigurationOptions;
import org.hyperledger.besu.cli.options.RPCOptions;
import org.hyperledger.besu.cli.options.RpcWebsocketOptions;
import org.hyperledger.besu.cli.options.SynchronizerOptions;
import org.hyperledger.besu.cli.options.TransactionPoolOptions;
import org.hyperledger.besu.cli.options.storage.DataStorageOptions;
import org.hyperledger.besu.cli.options.storage.PathBasedExtraStorageOptions;
import org.hyperledger.besu.cli.options.unstable.QBFTOptions;
import org.hyperledger.besu.cli.presynctasks.PreSynchronizationTaskRunner;
import org.hyperledger.besu.cli.subcommands.PasswordSubCommand;
import org.hyperledger.besu.cli.subcommands.PublicKeySubCommand;
import org.hyperledger.besu.cli.subcommands.TxParseSubCommand;
import org.hyperledger.besu.cli.subcommands.ValidateConfigSubCommand;
import org.hyperledger.besu.cli.subcommands.blocks.BlocksSubCommand;
import org.hyperledger.besu.cli.subcommands.operator.OperatorSubCommand;
import org.hyperledger.besu.cli.subcommands.rlp.RLPSubCommand;
import org.hyperledger.besu.cli.subcommands.storage.StorageSubCommand;
import org.hyperledger.besu.cli.util.BesuCommandCustomFactory;
import org.hyperledger.besu.cli.util.BootnodeResolver;
import org.hyperledger.besu.cli.util.BootnodeResolver.BootnodeResolutionException;
import org.hyperledger.besu.cli.util.CommandLineUtils;
import org.hyperledger.besu.cli.util.ConfigDefaultValueProviderStrategy;
import org.hyperledger.besu.cli.util.VersionProvider;
import org.hyperledger.besu.components.BesuComponent;
import org.hyperledger.besu.config.CheckpointConfigOptions;
import org.hyperledger.besu.config.DiscoveryOptions;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.config.MergeConfiguration;
import org.hyperledger.besu.config.NetworkDefinition;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeCoordinator;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.controller.BesuControllerBuilder;
import org.hyperledger.besu.crypto.Blake2bfMessageDigest;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.KeyPairUtil;
import org.hyperledger.besu.crypto.SECP256R1;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.KeyPairSecurityModule;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.InProcessRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis;
import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.JwtAlgorithm;
import org.hyperledger.besu.ethereum.api.jsonrpc.ipc.JsonRpcIpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.ChainDataPruner.ChainPruningStrategy;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.MiningParametersMetrics;
import org.hyperledger.besu.ethereum.core.VersionMetadata;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.NodeIdentifier;
import org.hyperledger.besu.ethereum.p2p.discovery.P2PDiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.dns.EthereumNodeRecord;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeDnsConfiguration;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.StaticNodesParser;
import org.hyperledger.besu.ethereum.permissioning.LocalPermissioningConfiguration;
import org.hyperledger.besu.ethereum.permissioning.PermissioningConfiguration;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProviderBuilder;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.precompile.AbstractAltBnPrecompiledContract;
import org.hyperledger.besu.evm.precompile.AbstractBLS12PrecompiledContract;
import org.hyperledger.besu.evm.precompile.AbstractPrecompiledContract;
import org.hyperledger.besu.evm.precompile.BigIntegerModularExponentiationPrecompiledContract;
import org.hyperledger.besu.evm.precompile.KZGPointEvalPrecompiledContract;
import org.hyperledger.besu.evm.precompile.P256VerifyPrecompiledContract;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.MetricCategoryRegistryImpl;
import org.hyperledger.besu.metrics.MetricsProtocol;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.metrics.StandardMetricCategory;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.metrics.vertx.VertxMetricsAdapterFactory;
import org.hyperledger.besu.nat.NatMethod;
import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.securitymodule.SecurityModule;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBPlugin;
import org.hyperledger.besu.services.BesuConfigurationImpl;
import org.hyperledger.besu.services.BesuPluginContextImpl;
import org.hyperledger.besu.services.BesuPluginServiceRegistrar;
import org.hyperledger.besu.services.BlockchainServiceImpl;
import org.hyperledger.besu.services.PermissioningServiceImpl;
import org.hyperledger.besu.services.PicoCLIOptionsImpl;
import org.hyperledger.besu.services.RpcEndpointServiceImpl;
import org.hyperledger.besu.services.SecurityModuleServiceImpl;
import org.hyperledger.besu.services.StorageServiceImpl;
import org.hyperledger.besu.services.TransactionPoolValidatorServiceImpl;
import org.hyperledger.besu.services.TransactionSelectionServiceImpl;
import org.hyperledger.besu.services.TransactionSimulationServiceImpl;
import org.hyperledger.besu.services.TransactionValidatorServiceImpl;
import org.hyperledger.besu.services.kvstore.InMemoryStoragePlugin;
import org.hyperledger.besu.util.BesuVersionUtils;
import org.hyperledger.besu.util.EphemeryGenesisUpdater;
import org.hyperledger.besu.util.InvalidConfigurationException;
import org.hyperledger.besu.util.LogConfigurator;
import org.hyperledger.besu.util.NetworkUtility;
import org.hyperledger.besu.util.PermissioningConfigurationValidator;
import org.hyperledger.besu.util.number.Fraction;
import org.hyperledger.besu.util.number.Percentage;
import org.hyperledger.besu.util.number.PositiveNumber;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.jackson.DatabindCodec;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import oshi.PlatformEnum;
import oshi.SystemInfo;
import picocli.AutoComplete;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.Model.ITypeInfo;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;

/** Represents the main Besu CLI command that runs the Besu Ethereum client full node. */
@SuppressWarnings("FieldCanBeLocal") // because Picocli injected fields report false positives
@Command(
    description = "This command runs the Besu Ethereum client full node.",
    abbreviateSynopsis = true,
    name = "besu",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class,
    header = "@|bold,fg(cyan) Usage:|@",
    synopsisHeading = "%n",
    descriptionHeading = "%n@|bold,fg(cyan) Description:|@%n%n",
    optionListHeading = "%n@|bold,fg(cyan) Options:|@%n",
    footerHeading = "%nBesu is licensed under the Apache License 2.0%n",
    footer = {
      "%n%n@|fg(cyan) To get started quickly, just choose a network to sync and a profile to run with suggested defaults:|@",
      "%n@|fg(cyan) for Mainnet|@ --network=mainnet --profile=[minimalist_staker|staker]",
      "%nMore info and other profiles at https://besu.hyperledger.org%n"
    })
public class BesuCommand implements DefaultCommandValues, Runnable {
  @SuppressWarnings("PrivateStaticFinalLoggers")
  // non-static for testing
  private final Logger logger;

  private CommandLine commandLine;

  private final Supplier<RlpBlockImporter> rlpBlockImporter;
  private final Function<BesuController, JsonBlockImporter> jsonBlockImporterFactory;
  private final Supplier<Era1BlockImporter> era1BlockImporter;
  private final Function<Blockchain, RlpBlockExporter> rlpBlockExporterFactory;
  private final BiFunction<Blockchain, NetworkDefinition, Era1BlockExporter>
      era1BlockExporterFactory;

  // Unstable CLI options
  final NetworkingOptions unstableNetworkingOptions = NetworkingOptions.create();
  final SynchronizerOptions unstableSynchronizerOptions = SynchronizerOptions.create();
  final EthProtocolOptions unstableEthProtocolOptions = EthProtocolOptions.create();
  private final DnsOptions unstableDnsOptions = DnsOptions.create();
  private final NatOptions unstableNatOptions = NatOptions.create();
  private final NativeLibraryOptions unstableNativeLibraryOptions = NativeLibraryOptions.create();
  private final RPCOptions unstableRPCOptions = RPCOptions.create();
  private final EvmOptions unstableEvmOptions = EvmOptions.create();
  private final IpcOptions unstableIpcOptions = IpcOptions.create();
  private final ChainPruningOptions unstableChainPruningOptions = ChainPruningOptions.create();
  private final QBFTOptions unstableQbftOptions = QBFTOptions.create();

  // stable CLI options
  final DataStorageOptions dataStorageOptions = DataStorageOptions.create();
  private final EthstatsOptions ethstatsOptions = EthstatsOptions.create();
  private final NodePrivateKeyFileOption nodePrivateKeyFileOption =
      NodePrivateKeyFileOption.create();
  private final LoggingLevelOption loggingLevelOption = LoggingLevelOption.create();

  @CommandLine.ArgGroup(validate = false, heading = "@|bold Tx Pool Common Options|@%n")
  final TransactionPoolOptions transactionPoolOptions = TransactionPoolOptions.create();

  @CommandLine.ArgGroup(validate = false, heading = "@|bold Block Builder Options|@%n")
  final MiningOptions miningOptions = MiningOptions.create();

  private final RunnerBuilder runnerBuilder;
  private final BesuController.Builder controllerBuilder;
  private final BesuPluginContextImpl besuPluginContext;
  private final StorageServiceImpl storageService;
  private final SecurityModuleServiceImpl securityModuleService;
  private final PermissioningServiceImpl permissioningService;
  private final RpcEndpointServiceImpl rpcEndpointServiceImpl;

  private final Map<String, String> environment;
  private final MetricCategoryRegistryImpl metricCategoryRegistry =
      new MetricCategoryRegistryImpl();

  private final PreSynchronizationTaskRunner preSynchronizationTaskRunner =
      new PreSynchronizationTaskRunner();

  private final Set<Integer> allocatedPorts = new HashSet<>();
  private Supplier<GenesisConfig> genesisConfigSupplier =
      Suppliers.memoize(this::readGenesisConfig);

  /** Memoized supplier for genesis configuration options. Protected to allow test access. */
  protected final Supplier<GenesisConfigOptions> genesisConfigOptionsSupplier =
      Suppliers.memoize(this::readGenesisConfigOptions);

  private final Supplier<MiningConfiguration> miningParametersSupplier =
      Suppliers.memoize(this::getMiningParameters);
  private final Supplier<ApiConfiguration> apiConfigurationSupplier =
      Suppliers.memoize(this::getApiConfiguration);

  private RocksDBPlugin rocksDBPlugin;

  private int maxPeers;
  private int maxRemoteInitiatedPeers;

  // CLI options defined by user at runtime.
  // Options parsing is done with CLI library Picocli https://picocli.info/

  // While this variable is never read it is needed for the PicoCLI to create
  // the config file option that is read elsewhere.
  @SuppressWarnings("UnusedVariable")
  @CommandLine.Option(
      names = {CONFIG_FILE_OPTION_NAME},
      paramLabel = MANDATORY_FILE_FORMAT_HELP,
      description = "TOML config file (default: none)")
  private final File configFile = null;

  @CommandLine.Option(
      names = {"--data-path"},
      paramLabel = MANDATORY_PATH_FORMAT_HELP,
      description = "The path to Besu data directory (default: ${DEFAULT-VALUE})")
  Path dataPath = getDefaultBesuDataPath(this);

  // Genesis file path with null default option.
  // This default is handled by Runner
  // to use mainnet json file from resources as indicated in the
  // default network option
  // Then we ignore genesis default value here.
  @CommandLine.Option(
      names = {"--genesis-file"},
      paramLabel = MANDATORY_FILE_FORMAT_HELP,
      description =
          "Genesis file for your custom network. Setting this option requires --network-id to be set. (Cannot be used with --network)")
  private final File genesisFile = null;

  @Option(
      names = {"--genesis-state-hash-cache-enabled"},
      description =
          "Use genesis state hash from data on startup if specified (default: ${DEFAULT-VALUE})")
  private final Boolean genesisStateHashCacheEnabled = false;

  @Option(
      names = "--identity",
      paramLabel = "<String>",
      description = "Identification for this node in the Client ID")
  private final Optional<String> identityString = Optional.empty();

  private Boolean printPathsAndExit = Boolean.FALSE;
  private String besuUserName = "besu";

  @Option(
      names = "--print-paths-and-exit",
      paramLabel = "<username>",
      description = "Print the configured paths and exit without starting the node.",
      arity = "0..1")
  void setUserName(final String userName) {
    PlatformEnum currentPlatform = SystemInfo.getCurrentPlatform();
    // Only allow on Linux and macOS
    if (currentPlatform == PlatformEnum.LINUX || currentPlatform == PlatformEnum.MACOS) {
      if (userName != null) {
        besuUserName = userName;
      }
      printPathsAndExit = Boolean.TRUE;
    } else {
      throw new UnsupportedOperationException(
          "--print-paths-and-exit is only supported on Linux and macOS.");
    }
  }

  // P2P Discovery Option Group
  @CommandLine.ArgGroup(validate = false, heading = "@|bold P2P Discovery Options|@%n")
  P2PDiscoveryOptions p2PDiscoveryOptions = new P2PDiscoveryOptions();

  P2PDiscoveryConfiguration p2PDiscoveryConfig;

  private final TransactionSelectionServiceImpl transactionSelectionServiceImpl;
  private final TransactionPoolValidatorServiceImpl transactionPoolValidatorServiceImpl;
  private final TransactionValidatorServiceImpl transactionValidatorServiceImpl;
  private final TransactionSimulationServiceImpl transactionSimulationServiceImpl;
  private final BlockchainServiceImpl blockchainServiceImpl;
  private BesuComponent besuComponent;

  private SyncMode syncMode = null;

  @Option(
      names = {"--sync-mode"},
      paramLabel = MANDATORY_MODE_FORMAT_HELP,
      description =
          "Synchronization mode, possible values are ${COMPLETION-CANDIDATES} (default: SNAP if a --network is supplied. FULL otherwise.)")
  void setSyncMode(final String value) {
    final String normalized = value.toUpperCase(Locale.ROOT);
    if ("CHECKPOINT".equals(normalized)) {
      logger.warn(
          "CHECKPOINT sync mode is deprecated and has been removed. "
              + "Using SNAP sync mode instead. Your checkpoint configuration will be used automatically.");
      this.syncMode = SyncMode.SNAP;
    } else {
      try {
        this.syncMode = SyncMode.valueOf(normalized);
      } catch (IllegalArgumentException e) {
        throw new ParameterException(
            this.commandLine,
            "Invalid value for option '--sync-mode': '"
                + value
                + "' is not a valid sync mode. "
                + "Valid values are: FULL, SNAP");
      }
    }
  }

  @Option(
      names = "--sync-min-peers",
      paramLabel = MANDATORY_INTEGER_FORMAT_HELP,
      description =
          "Minimum number of peers required before starting sync. Has effect only on non-PoS networks. (default: ${DEFAULT-VALUE})")
  private final Integer syncMinPeerCount = SYNC_MIN_PEER_COUNT;

  private NetworkDefinition network = null;

  @Option(
      names = {"--network"},
      paramLabel = MANDATORY_NETWORK_FORMAT_HELP,
      defaultValue = "MAINNET",
      description =
          "Synchronize against the indicated network, possible values are ${COMPLETION-CANDIDATES}."
              + " (default: ${DEFAULT-VALUE})")
  void setNetwork(final String inputNetwork) {
    // case-insensitive and (_,-)-insensitive
    final var normalizedInputNetwork = inputNetwork.toLowerCase(Locale.ROOT).replace('-', '_');
    this.network =
        Arrays.stream(NetworkDefinition.values())
            .filter(nd -> nd.name().toLowerCase(Locale.ROOT).equals(normalizedInputNetwork))
            .findAny()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Network %s does not exist".formatted(inputNetwork)));
  }

  @Option(
      names = {PROFILE_OPTION_NAME},
      paramLabel = PROFILE_FORMAT_HELP,
      completionCandidates = ProfilesCompletionCandidates.class,
      description =
          "Overwrite default settings. Possible values are ${COMPLETION-CANDIDATES}. (default: none)")
  private String profile = null; // don't set it as final due to picocli completion candidates

  @Option(
      names = {"--nat-method"},
      description =
          "Specify the NAT circumvention method to be used, possible values are ${COMPLETION-CANDIDATES}."
              + " NONE disables NAT functionality. (default: ${DEFAULT-VALUE})")
  private final NatMethod natMethod = DEFAULT_NAT_METHOD;

  @Option(
      names = {"--network-id"},
      paramLabel = "<BIG INTEGER>",
      description =
          "P2P network identifier. (default: the selected network chain ID or custom genesis chain ID)")
  private final BigInteger networkId = null;

  @Option(
      names = {"--kzg-trusted-setup"},
      paramLabel = MANDATORY_FILE_FORMAT_HELP,
      description =
          "Path to file containing the KZG trusted setup, mandatory for custom networks that support data blobs, "
              + "optional for overriding named networks default.")
  private final Path kzgTrustedSetupFile = null;

  @Option(
      names = {"--version-compatibility-protection"},
      description =
          "Perform compatibility checks between the version of Besu being started and the version of Besu that last started with this data directory. (default: ${DEFAULT-VALUE})")
  private Boolean versionCompatibilityProtection = null;

  @CommandLine.ArgGroup(validate = false, heading = "@|bold GraphQL Options|@%n")
  GraphQlOptions graphQlOptions = new GraphQlOptions();

  // Engine JSON-PRC Options
  @CommandLine.ArgGroup(validate = false, heading = "@|bold Engine JSON-RPC Options|@%n")
  EngineRPCOptions engineRPCOptions = new EngineRPCOptions();

  EngineRPCConfiguration engineRPCConfig = engineRPCOptions.toDomainObject();

  // JSON-RPC HTTP Options
  @CommandLine.ArgGroup(validate = false, heading = "@|bold JSON-RPC HTTP Options|@%n")
  JsonRpcHttpOptions jsonRpcHttpOptions = new JsonRpcHttpOptions();

  // JSON-RPC Websocket Options
  @CommandLine.ArgGroup(validate = false, heading = "@|bold JSON-RPC Websocket Options|@%n")
  RpcWebsocketOptions rpcWebsocketOptions = new RpcWebsocketOptions();

  // In-Process RPC Options
  @CommandLine.ArgGroup(validate = false, heading = "@|bold In-Process RPC Options|@%n")
  InProcessRpcOptions inProcessRpcOptions = InProcessRpcOptions.create();

  // Metrics Option Group
  @CommandLine.ArgGroup(validate = false, heading = "@|bold Metrics Options|@%n")
  MetricsOptions metricsOptions = MetricsOptions.create();

  @Option(
      names = {"--host-allowlist"},
      paramLabel = "<hostname>[,<hostname>...]... or * or all",
      description =
          "Comma separated list of hostnames to allow for RPC access, or * to accept any host (default: ${DEFAULT-VALUE})",
      defaultValue = "localhost,127.0.0.1")
  private final JsonRPCAllowlistHostsProperty hostsAllowlist = new JsonRPCAllowlistHostsProperty();

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"})
  @Option(
      names = {"--color-enabled"},
      description =
          "Force color output to be enabled/disabled (default: colorized only if printing to console)")
  private static Boolean colorEnabled = null;

  @Option(
      names = {"--reorg-logging-threshold"},
      description =
          "How deep a chain reorganization must be in order for it to be logged (default: ${DEFAULT-VALUE})")
  private final Long reorgLoggingThreshold = 6L;

  // Permission Option Group
  @CommandLine.ArgGroup(validate = false, heading = "@|bold Permissions Options|@%n")
  PermissionsOptions permissionsOptions = new PermissionsOptions();

  @Option(
      names = {"--revert-reason-enabled"},
      description =
          "Enable passing the revert reason back through TransactionReceipts (default: ${DEFAULT-VALUE})")
  private final Boolean isRevertReasonEnabled = false;

  @Option(
      names = {"--required-blocks", "--required-block"},
      paramLabel = "BLOCK=HASH",
      description = "Block number and hash peers are required to have.",
      arity = "*",
      split = ",")
  private final Map<Long, Hash> requiredBlocks = new HashMap<>();

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @Option(
      names = {"--key-value-storage"},
      description = "Identity for the key-value storage to be used.")
  private String keyValueStorageName = DEFAULT_KEY_VALUE_STORAGE_NAME;

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"})
  @Option(
      names = {"--security-module"},
      paramLabel = "<NAME>",
      description = "Identity for the Security Module to be used.")
  private String securityModuleName = DEFAULT_SECURITY_MODULE;

  @Option(
      names = {"--auto-log-bloom-caching-enabled"},
      description = "Enable automatic log bloom caching (default: ${DEFAULT-VALUE})",
      arity = "1")
  private final Boolean autoLogBloomCachingEnabled = true;

  @Option(
      names = {"--override-genesis-config"},
      paramLabel = "NAME=VALUE",
      description = "Overrides configuration values in the genesis file.  Use with care.",
      arity = "*",
      hidden = true,
      split = ",")
  private final Map<String, String> genesisConfigOverrides =
      new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

  @CommandLine.Option(
      names = {"--pid-path"},
      paramLabel = MANDATORY_PATH_FORMAT_HELP,
      description = "Path to PID file (optional)")
  private final Path pidPath = null;

  // API Configuration Option Group
  @CommandLine.ArgGroup(validate = false, heading = "@|bold API Configuration Options|@%n")
  ApiConfigurationOptions apiConfigurationOptions = new ApiConfigurationOptions();

  @CommandLine.ArgGroup(validate = false, heading = "@|bold Block Access List Options|@%n")
  BalConfigurationOptions balConfigurationOptions = new BalConfigurationOptions();

  @CommandLine.Option(
      names = {"--static-nodes-file"},
      paramLabel = MANDATORY_FILE_FORMAT_HELP,
      description =
          "Specifies the static node file containing the static nodes for this node to connect to")
  private final Path staticNodesFile = null;

  @CommandLine.Option(
      names = {"--cache-last-blocks"},
      description = "Specifies the number of last blocks to cache  (default: ${DEFAULT-VALUE})")
  private final Integer numberOfBlocksToCache = 0;

  @CommandLine.Option(
      names = {"--cache-last-block-headers"},
      description =
          "Specifies the number of last block headers to cache  (default: ${DEFAULT-VALUE})")
  private final Integer numberOfBlockHeadersToCache = 0;

  @CommandLine.Option(
      names = {"--cache-last-block-headers-preload-enabled"},
      description = "Enable preloading of the block header cache (default: ${DEFAULT-VALUE})")
  private final Boolean isCacheLastBlockHeadersPreloadEnabled = false;

  @CommandLine.Option(
      names = {"--cache-precompiles"},
      description = "Specifies whether to cache precompile results (default: ${DEFAULT-VALUE})")
  private final Boolean enablePrecompileCaching = false;

  // Plugins Configuration Option Group
  @CommandLine.ArgGroup(validate = false)
  PluginsConfigurationOptions pluginsConfigurationOptions = new PluginsConfigurationOptions();

  private EthNetworkConfig ethNetworkConfig;
  private JsonRpcConfiguration jsonRpcConfiguration;
  private JsonRpcConfiguration engineJsonRpcConfiguration;
  private GraphQLConfiguration graphQLConfiguration;
  private WebSocketConfiguration webSocketConfiguration;
  private JsonRpcIpcConfiguration jsonRpcIpcConfiguration;
  private InProcessRpcConfiguration inProcessRpcConfiguration;
  private MetricsConfiguration metricsConfiguration;
  private Optional<PermissioningConfiguration> permissioningConfiguration;
  private DataStorageConfiguration dataStorageConfiguration;
  private Collection<EnodeURLImpl> staticNodes;
  private BesuController besuController;
  private BesuConfigurationImpl pluginCommonConfiguration;

  private Vertx vertx;
  private Runner runner;
  private EnodeDnsConfiguration enodeDnsConfiguration;
  private KeyValueStorageProvider keyValueStorageProvider;
  private KeyPair keyPair;
  private BigInteger ephemeryNextCycleId = BigInteger.ZERO;

  /**
   * Besu command constructor.
   *
   * @param rlpBlockImporter RlpBlockImporter supplier
   * @param jsonBlockImporterFactory instance of {@code Function<BesuController, JsonBlockImporter>}
   * @param era1BlockImporter Era1BlockImporter supplier
   * @param rlpBlockExporterFactory instance of {@code Function<Blockchain, RlpBlockExporter>}
   * @param era1BlockExporterFactory instance of {@code Function<Blockchain, Era1BlockExporter>}
   * @param runnerBuilder instance of RunnerBuilder
   * @param controllerBuilder instance of BesuController.Builder
   * @param besuPluginContext instance of BesuPluginContextImpl
   * @param environment Environment variables map
   * @param commandLogger instance of Logger for outputting to the CLI
   */
  public BesuCommand(
      final Supplier<RlpBlockImporter> rlpBlockImporter,
      final Function<BesuController, JsonBlockImporter> jsonBlockImporterFactory,
      final Supplier<Era1BlockImporter> era1BlockImporter,
      final Function<Blockchain, RlpBlockExporter> rlpBlockExporterFactory,
      final BiFunction<Blockchain, NetworkDefinition, Era1BlockExporter> era1BlockExporterFactory,
      final RunnerBuilder runnerBuilder,
      final BesuController.Builder controllerBuilder,
      final BesuPluginContextImpl besuPluginContext,
      final Map<String, String> environment,
      final Logger commandLogger) {
    this(
        rlpBlockImporter,
        jsonBlockImporterFactory,
        era1BlockImporter,
        rlpBlockExporterFactory,
        era1BlockExporterFactory,
        runnerBuilder,
        controllerBuilder,
        besuPluginContext,
        environment,
        new StorageServiceImpl(),
        new SecurityModuleServiceImpl(),
        new PermissioningServiceImpl(),
        new RpcEndpointServiceImpl(),
        new TransactionSelectionServiceImpl(),
        new TransactionPoolValidatorServiceImpl(),
        new TransactionSimulationServiceImpl(),
        new BlockchainServiceImpl(),
        new TransactionValidatorServiceImpl(),
        commandLogger);
  }

  /**
   * Overloaded Besu command constructor visible for testing.
   *
   * @param rlpBlockImporter RlpBlockImporter supplier
   * @param jsonBlockImporterFactory instance of {@code Function<BesuController, JsonBlockImporter>}
   * @param era1BlockImporter Era1BlockImporter supplier
   * @param rlpBlockExporterFactory instance of {@code Function<Blockchain, RlpBlockExporter>}
   * @param era1BlockExporterFactory instance of {@code Function<Blockchain, Era1BlockExporter>}
   * @param runnerBuilder instance of RunnerBuilder
   * @param controllerBuilder instance of BesuController.Builder
   * @param besuPluginContext instance of BesuPluginContextImpl
   * @param environment Environment variables map
   * @param storageService instance of StorageServiceImpl
   * @param securityModuleService instance of SecurityModuleServiceImpl
   * @param permissioningService instance of PermissioningServiceImpl
   * @param rpcEndpointServiceImpl instance of RpcEndpointServiceImpl
   * @param transactionSelectionServiceImpl instance of TransactionSelectionServiceImpl
   * @param transactionPoolValidatorServiceImpl instance of TransactionPoolValidatorServiceImpl
   * @param transactionSimulationServiceImpl instance of TransactionSimulationServiceImpl
   * @param blockchainServiceImpl instance of BlockchainServiceImpl
   * @param transactionValidatorServiceImpl instance of TransactionValidatorServiceImpl
   * @param commandLogger instance of Logger for outputting to the CLI
   */
  @VisibleForTesting
  protected BesuCommand(
      final Supplier<RlpBlockImporter> rlpBlockImporter,
      final Function<BesuController, JsonBlockImporter> jsonBlockImporterFactory,
      final Supplier<Era1BlockImporter> era1BlockImporter,
      final Function<Blockchain, RlpBlockExporter> rlpBlockExporterFactory,
      final BiFunction<Blockchain, NetworkDefinition, Era1BlockExporter> era1BlockExporterFactory,
      final RunnerBuilder runnerBuilder,
      final BesuController.Builder controllerBuilder,
      final BesuPluginContextImpl besuPluginContext,
      final Map<String, String> environment,
      final StorageServiceImpl storageService,
      final SecurityModuleServiceImpl securityModuleService,
      final PermissioningServiceImpl permissioningService,
      final RpcEndpointServiceImpl rpcEndpointServiceImpl,
      final TransactionSelectionServiceImpl transactionSelectionServiceImpl,
      final TransactionPoolValidatorServiceImpl transactionPoolValidatorServiceImpl,
      final TransactionSimulationServiceImpl transactionSimulationServiceImpl,
      final BlockchainServiceImpl blockchainServiceImpl,
      final TransactionValidatorServiceImpl transactionValidatorServiceImpl,
      final Logger commandLogger) {

    this.logger = commandLogger;
    this.rlpBlockImporter = rlpBlockImporter;
    this.jsonBlockImporterFactory = jsonBlockImporterFactory;
    this.era1BlockImporter = era1BlockImporter;
    this.rlpBlockExporterFactory = rlpBlockExporterFactory;
    this.era1BlockExporterFactory = era1BlockExporterFactory;
    this.runnerBuilder = runnerBuilder;
    this.controllerBuilder = controllerBuilder;
    this.besuPluginContext = besuPluginContext;
    this.environment = environment;
    this.storageService = storageService;
    this.securityModuleService = securityModuleService;
    this.permissioningService = permissioningService;
    if (besuPluginContext.getService(BesuConfigurationImpl.class).isPresent()) {
      this.pluginCommonConfiguration =
          besuPluginContext.getService(BesuConfigurationImpl.class).get();
    } else {
      this.pluginCommonConfiguration = new BesuConfigurationImpl();
      besuPluginContext.addService(BesuConfiguration.class, this.pluginCommonConfiguration);
    }
    this.rpcEndpointServiceImpl = rpcEndpointServiceImpl;
    this.transactionSelectionServiceImpl = transactionSelectionServiceImpl;
    this.transactionPoolValidatorServiceImpl = transactionPoolValidatorServiceImpl;
    this.transactionSimulationServiceImpl = transactionSimulationServiceImpl;
    this.blockchainServiceImpl = blockchainServiceImpl;
    this.transactionValidatorServiceImpl = transactionValidatorServiceImpl;
  }

  /**
   * Parses command line arguments and configures the application accordingly.
   *
   * @param resultHandler The strategy to handle the execution result.
   * @param parameterExceptionHandler Handler for exceptions related to command line parameters.
   * @param executionExceptionHandler Handler for exceptions during command execution.
   * @param in The input stream for commands.
   * @param besuComponent The Besu component.
   * @param args The command line arguments.
   * @return The execution result status code.
   */
  @VisibleForTesting
  public int parse(
      final IExecutionStrategy resultHandler,
      final BesuParameterExceptionHandler parameterExceptionHandler,
      final BesuExecutionExceptionHandler executionExceptionHandler,
      final InputStream in,
      final BesuComponent besuComponent,
      final String... args) {
    if (besuComponent == null) {
      throw new IllegalArgumentException("BesuComponent must be provided");
    }
    this.besuComponent = besuComponent;
    initializeCommandLineSettings(in);

    // Create the execution strategy chain.
    final IExecutionStrategy executeTask = createExecuteTask(resultHandler);
    final IExecutionStrategy pluginRegistrationTask = createPluginRegistrationTask(executeTask);
    final IExecutionStrategy setDefaultValueProviderTask =
        createDefaultValueProviderTask(pluginRegistrationTask);

    // 1- Config default value provider
    // 2- Register plugins
    // 3- Execute command
    return executeCommandLine(
        setDefaultValueProviderTask, parameterExceptionHandler, executionExceptionHandler, args);
  }

  private void initializeCommandLineSettings(final InputStream in) {
    toCommandLine();
    // Automatically adjust the width of usage messages to the terminal width.
    commandLine.getCommandSpec().usageMessage().autoWidth(true);

    handleStableOptions();
    addSubCommands(in);
    registerConverters();
    handleUnstableOptions();
    preparePlugins();
  }

  private IExecutionStrategy createExecuteTask(final IExecutionStrategy nextStep) {
    return parseResult -> {
      commandLine.setExecutionStrategy(nextStep);
      // At this point we don't allow unmatched options since plugins were already registered
      commandLine.setUnmatchedArgumentsAllowed(false);
      return commandLine.execute(parseResult.originalArgs().toArray(new String[0]));
    };
  }

  private IExecutionStrategy createPluginRegistrationTask(final IExecutionStrategy nextStep) {
    return parseResult -> {
      if (parseResult.isUsageHelpRequested() || parseResult.isVersionHelpRequested()) {
        // suppressing the info log to avoid that plugin registrations logs are printed
        // before the help or the version information
        suppressInfoLog();
      }
      besuPluginContext.initialize(PluginsConfigurationOptions.fromCommandLine(commandLine));
      besuPluginContext.registerPlugins();
      commandLine.setExecutionStrategy(nextStep);
      return commandLine.execute(parseResult.originalArgs().toArray(new String[0]));
    };
  }

  @SuppressWarnings("BannedMethod")
  private void suppressInfoLog() {
    // this is specific for Log4j2, in case we switch to another logging framework,
    // this need to be adapted for it

    // silence already created loggers
    LoggerContext.getContext(false).getLoggers().forEach(logger -> logger.setLevel(Level.WARN));

    // silence future loggers by configuration
    if (LogManager.getFactory() instanceof Log4jContextFactory log4jContextFactory) {
      final var selector = log4jContextFactory.getSelector();
      selector
          .getLoggerContexts()
          .forEach(
              ctx ->
                  ctx.getConfiguration()
                      .getLoggers()
                      .values()
                      .forEach(loggerConfig -> loggerConfig.setLevel(Level.WARN)));
    }
  }

  private IExecutionStrategy createDefaultValueProviderTask(final IExecutionStrategy nextStep) {
    return new ConfigDefaultValueProviderStrategy(nextStep, environment);
  }

  /**
   * Executes the command line with the provided execution strategy and exception handlers.
   *
   * @param executionStrategy The execution strategy to use.
   * @param args The command line arguments.
   * @return The execution result status code.
   */
  private int executeCommandLine(
      final IExecutionStrategy executionStrategy,
      final BesuParameterExceptionHandler parameterExceptionHandler,
      final BesuExecutionExceptionHandler executionExceptionHandler,
      final String... args) {

    try {
      // Parse and run duplicate-check
      // As this happens before the plugins registration and plugins can add options, we must
      // allow unmatched options
      final ParseResult pr = commandLine.setUnmatchedArgumentsAllowed(true).parseArgs(args);
      rejectDuplicateScalarOptions(pr); // your generic validator
    } catch (ParameterException e) {
      // ← Send it to the standard handler: prints one line & exits status 1
      return parameterExceptionHandler.handleParseException(e, args);
    }
    return commandLine
        .setExecutionStrategy(executionStrategy)
        .setParameterExceptionHandler(parameterExceptionHandler)
        .setExecutionExceptionHandler(executionExceptionHandler)
        // As this happens before the plugins registration and plugins can add options, we must
        // allow unmatched options
        .setUnmatchedArgumentsAllowed(true)
        .execute(args);
  }

  /** Used by Dagger to parse all options into a commandline instance. */
  public void toCommandLine() {
    commandLine =
        new CommandLine(this, new BesuCommandCustomFactory(besuPluginContext))
            .setCaseInsensitiveEnumValuesAllowed(true)
            .setToggleBooleanFlags(false);
  }

  @Override
  public void run() {
    if (network != null && network.isDeprecated()) {
      logger.warn(NetworkDeprecationMessage.generate(network));
    }
    try {
      configureLogging(true);

      if (printPathsAndExit) {
        // Print configured paths requiring read/write permissions to be adjusted
        checkPermissionsAndPrintPaths(besuUserName);
        System.exit(0); // Exit before any services are started
      }

      logger.info("Starting Besu");

      // set merge config on the basis of genesis config
      setMergeConfigOptions();

      instantiateSignatureAlgorithmFactory();

      // Need to create vertx after cmdline has been parsed, such that metricsSystem is configurable
      vertx = createVertx(besuComponent.getMetricsSystem());

      validateOptions();

      initialProcess();

      if (network.equals(EPHEMERY)) {
        long lastGenesisTimestamp = parseLong(genesisConfigOverrides.get("timestamp"));
        runner.scheduleEphemeryRestart(this, lastGenesisTimestamp);
      }
      runner.awaitStop();

    } catch (final Exception e) {
      logger.error("Failed to start Besu: {}", e.getMessage());
      logger.debug("Startup failure cause", e);
      throw new ParameterException(this.commandLine, e.getMessage(), e);
    }
  }

  /**
   * contains the initial setup and configurations required for Ephemery restart cycles.
   *
   * @throws Exception if startup fails
   */
  public void initialProcess() throws Exception {
    if (network.equals(EPHEMERY)) {
      genesisConfigSupplier = Suppliers.memoize(this::readGenesisConfig);
      if (BigInteger.ZERO.equals(ephemeryNextCycleId)) {
        ephemeryNextCycleId = genesisConfigSupplier.get().getConfigOptions().getChainId().get();
      }
      dataPath = dataPath.resolve("Ephemery-data-chain-" + ephemeryNextCycleId);
      ephemeryNextCycleId = ephemeryNextCycleId.add(BigInteger.ONE);
    }

    configure();

    setIgnorableStorageSegments();

    // If we're not running against a named network, or if version compat protection has been
    // explicitly enabled, perform compatibility check
    VersionMetadata.versionCompatibilityChecks(versionCompatibilityProtection, dataDir());

    configureNativeLibs(Optional.ofNullable(network));
    if (enablePrecompileCaching) {
      configurePrecompileCaching();
    }

    besuController = buildController();

    besuPluginContext.beforeExternalServices();

    runner = buildRunner();
    runner.startExternalServices();

    startPlugins(runner);
    setReleaseMetrics();
    preSynchronization();

    runner.startEthereumMainLoop();

    besuPluginContext.afterExternalServicesMainLoop();
  }

  private void configurePrecompileCaching() {
    // enable precompile caching:
    AbstractPrecompiledContract.setPrecompileCaching(enablePrecompileCaching);
    // separately set KZG precompile caching, it does not extend AbstractPrecompiledContract:
    KZGPointEvalPrecompiledContract.setPrecompileCaching(enablePrecompileCaching);
    // separately set BLS precompiles caching, they do not extend AbstractPrecompiledContract:
    AbstractBLS12PrecompiledContract.setPrecompileCaching(enablePrecompileCaching);

    // set a metric logger
    final var precompileCounter =
        getMetricsSystem()
            .createLabelledCounter(
                BesuMetricCategory.BLOCK_PROCESSING,
                "precompile_cache",
                "precompile cache labeled counter",
                "precompile_name",
                "event");

    // set a cache event consumer which logs a metrics event
    AbstractPrecompiledContract.setCacheEventConsumer(
        cacheEvent ->
            precompileCounter
                .labels(cacheEvent.precompile(), cacheEvent.cacheMetric().name())
                .inc());
  }

  /** Reject any option that is not multi-valued but appears more than once. */
  private static void rejectDuplicateScalarOptions(final ParseResult pr) {
    for (OptionSpec spec : pr.matchedOptions()) {

      // skip help/version flags
      if (spec.usageHelp() || spec.versionHelp()) {
        continue;
      }

      // ── determine if this option can repeat
      ITypeInfo type = spec.typeInfo();
      boolean multiValued =
          spec.arity().max() > 1 || type.isMultiValue() || type.isCollection() || type.isArray();

      if (multiValued) {
        continue; // lists are allowed to repeat
      }

      // ── single-valued: abort if it appears more than once ───────────
      if (pr.matchedOption(spec.longestName()).stringValues().size() > 1) {
        throw new ParameterException(
            pr.commandSpec().commandLine(),
            String.format("Option '%s' should be specified only once", spec.longestName()));
      }
    }
  }

  private void checkPermissionsAndPrintPaths(final String userName) {
    // Check permissions for the data path
    checkPermissions(dataDir(), userName, false);

    // Check permissions for genesis file
    try {
      if (genesisFile != null) {
        checkPermissions(genesisFile.toPath(), userName, true);
      }
    } catch (Exception e) {
      commandLine
          .getOut()
          .println("Error: Failed checking genesis file: Reason: " + e.getMessage());
    }
  }

  // Helper method to check permissions on a given path
  private void checkPermissions(final Path path, final String besuUser, final boolean readOnly) {
    try {
      // Get the permissions of the file
      // check if besu user is the owner - get owner permissions if yes
      // else, check if besu user and owner are in the same group - if yes, check the group
      // permission
      // otherwise check permissions for others

      // Get the owner of the file or directory
      UserPrincipal owner = Files.getOwner(path);
      boolean hasReadPermission, hasWritePermission;

      // Get file permissions
      Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);

      // Check if besu is the owner
      if (owner.getName().equals(besuUser)) {
        // Owner permissions
        hasReadPermission = permissions.contains(PosixFilePermission.OWNER_READ);
        hasWritePermission = permissions.contains(PosixFilePermission.OWNER_WRITE);
      } else {
        // Get the group of the file
        // Get POSIX file attributes and then group
        PosixFileAttributes attrs = Files.readAttributes(path, PosixFileAttributes.class);
        GroupPrincipal group = attrs.group();

        // Check if besu user belongs to this group
        boolean isMember = isGroupMember(besuUserName, group);

        if (isMember) {
          // Group's permissions
          hasReadPermission = permissions.contains(PosixFilePermission.GROUP_READ);
          hasWritePermission = permissions.contains(PosixFilePermission.GROUP_WRITE);
        } else {
          // Others' permissions
          hasReadPermission = permissions.contains(PosixFilePermission.OTHERS_READ);
          hasWritePermission = permissions.contains(PosixFilePermission.OTHERS_WRITE);
        }
      }

      if (!hasReadPermission || (!readOnly && !hasWritePermission)) {
        String accessType = readOnly ? "READ" : "READ_WRITE";
        commandLine.getOut().println("PERMISSION_CHECK_PATH:" + path + ":" + accessType);
      }
    } catch (Exception e) {
      // Do nothing upon catching an error
      commandLine
          .getOut()
          .println(
              "Error: Failed to check permissions for path: '"
                  + path
                  + "'. Reason: "
                  + e.getMessage());
    }
  }

  private static boolean isGroupMember(final String userName, final GroupPrincipal group)
      throws IOException {
    // Get the groups of the user by executing 'id -Gn username'
    Process process = Runtime.getRuntime().exec(new String[] {"id", "-Gn", userName});
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8));

    // Read the output of the command
    String line = reader.readLine();
    boolean isMember = false;
    if (line != null) {
      // Split the groups
      Iterable<String> userGroups = Splitter.on(" ").split(line);
      // Check if any of the user's groups match the file's group

      for (String grp : userGroups) {
        if (grp.equals(group.getName())) {
          isMember = true;
          break;
        }
      }
    }
    return isMember;
  }

  @VisibleForTesting
  void setBesuConfiguration(final BesuConfigurationImpl pluginCommonConfiguration) {
    this.pluginCommonConfiguration = pluginCommonConfiguration;
  }

  private void addSubCommands(final InputStream in) {
    commandLine.addSubcommand(
        BlocksSubCommand.COMMAND_NAME,
        new BlocksSubCommand(
            rlpBlockImporter,
            jsonBlockImporterFactory,
            era1BlockImporter,
            rlpBlockExporterFactory,
            era1BlockExporterFactory,
            commandLine.getOut()));
    commandLine.addSubcommand(
        TxParseSubCommand.COMMAND_NAME, new TxParseSubCommand(commandLine.getOut()));
    commandLine.addSubcommand(
        PublicKeySubCommand.COMMAND_NAME, new PublicKeySubCommand(commandLine.getOut()));
    commandLine.addSubcommand(
        PasswordSubCommand.COMMAND_NAME, new PasswordSubCommand(commandLine.getOut()));
    commandLine.addSubcommand(
        RLPSubCommand.COMMAND_NAME, new RLPSubCommand(commandLine.getOut(), in));
    commandLine.addSubcommand(
        OperatorSubCommand.COMMAND_NAME, new OperatorSubCommand(commandLine.getOut()));
    commandLine.addSubcommand(
        ValidateConfigSubCommand.COMMAND_NAME,
        new ValidateConfigSubCommand(commandLine, commandLine.getOut()));
    commandLine.addSubcommand(
        StorageSubCommand.COMMAND_NAME, new StorageSubCommand(commandLine.getOut()));
    final String generateCompletionSubcommandName = "generate-completion";
    commandLine.addSubcommand(
        generateCompletionSubcommandName, AutoComplete.GenerateCompletion.class);
    final CommandLine generateCompletionSubcommand =
        commandLine.getSubcommands().get(generateCompletionSubcommandName);
    generateCompletionSubcommand.getCommandSpec().usageMessage().hidden(true);
  }

  private void registerConverters() {
    commandLine.registerConverter(Address.class, Address::fromHexStringStrict);
    commandLine.registerConverter(Bytes.class, Bytes::fromHexString);
    commandLine.registerConverter(MetricsProtocol.class, MetricsProtocol::fromString);
    commandLine.registerConverter(UInt256.class, (arg) -> UInt256.valueOf(new BigInteger(arg)));
    commandLine.registerConverter(Wei.class, (arg) -> Wei.of(Long.parseUnsignedLong(arg)));
    commandLine.registerConverter(PositiveNumber.class, PositiveNumber::fromString);
    commandLine.registerConverter(Hash.class, Hash::fromHexString);
    commandLine.registerConverter(Optional.class, Optional::of);
    commandLine.registerConverter(Double.class, Double::parseDouble);
  }

  private void handleStableOptions() {
    commandLine.addMixin("Ethstats", ethstatsOptions);
    commandLine.addMixin("Private key file", nodePrivateKeyFileOption);
    commandLine.addMixin("Logging level", loggingLevelOption);
    commandLine.addMixin("Data Storage Options", dataStorageOptions);
  }

  private void handleUnstableOptions() {
    // Add unstable options
    final ImmutableMap.Builder<String, Object> unstableOptionsBuild = ImmutableMap.builder();
    final ImmutableMap<String, Object> unstableOptions =
        unstableOptionsBuild
            .put("Ethereum Wire Protocol", unstableEthProtocolOptions)
            .put("P2P Network", unstableNetworkingOptions)
            .put("RPC", unstableRPCOptions)
            .put("DNS Configuration", unstableDnsOptions)
            .put("NAT Configuration", unstableNatOptions)
            .put("Synchronizer", unstableSynchronizerOptions)
            .put("Native Library", unstableNativeLibraryOptions)
            .put("EVM Options", unstableEvmOptions)
            .put("IPC Options", unstableIpcOptions)
            .put("Chain Data Pruning Options", unstableChainPruningOptions)
            .put("QBFT Options", unstableQbftOptions)
            .build();

    UnstableOptionsSubCommand.createUnstableOptions(commandLine, unstableOptions);
  }

  private void preparePlugins() {
    besuPluginContext.addService(PicoCLIOptions.class, new PicoCLIOptionsImpl(commandLine));

    metricCategoryRegistry.addCategories(BesuMetricCategory.class);
    metricCategoryRegistry.addCategories(StandardMetricCategory.class);

    BesuPluginServiceRegistrar.registerEarlyServices(
        besuPluginContext,
        securityModuleService,
        storageService,
        metricCategoryRegistry,
        permissioningService,
        rpcEndpointServiceImpl,
        transactionSelectionServiceImpl,
        transactionPoolValidatorServiceImpl,
        transactionSimulationServiceImpl,
        blockchainServiceImpl,
        transactionValidatorServiceImpl);

    // register built-in plugins
    rocksDBPlugin = new RocksDBPlugin();
    rocksDBPlugin.register(besuPluginContext);
    new InMemoryStoragePlugin().register(besuPluginContext);

    // register default security module
    securityModuleService.register(
        DEFAULT_SECURITY_MODULE, Suppliers.memoize(this::defaultSecurityModule));
  }

  private SecurityModule defaultSecurityModule() {
    keyPair = loadKeyPair(nodePrivateKeyFileOption.getNodePrivateKeyFile());
    return new KeyPairSecurityModule(keyPair);
  }

  /**
   * Load key pair from private key. Visible to be accessed by subcommands.
   *
   * @param nodePrivateKeyFile File containing private key
   * @return KeyPair loaded from private key file
   */
  public KeyPair loadKeyPair(final File nodePrivateKeyFile) {
    return KeyPairUtil.loadKeyPair(resolveNodePrivateKeyFile(nodePrivateKeyFile));
  }

  private void preSynchronization() {
    preSynchronizationTaskRunner.runTasks(besuController);
  }

  Runner buildRunner() {
    return synchronize(
        besuController,
        p2PDiscoveryConfig.p2pEnabled(),
        p2PDiscoveryConfig.peerDiscoveryEnabled(),
        ethNetworkConfig,
        p2PDiscoveryConfig.p2pHost(),
        p2PDiscoveryConfig.p2pInterface(),
        p2PDiscoveryConfig.p2pPort(),
        graphQLConfiguration,
        jsonRpcConfiguration,
        engineJsonRpcConfiguration,
        webSocketConfiguration,
        jsonRpcIpcConfiguration,
        inProcessRpcConfiguration,
        apiConfigurationSupplier.get(),
        metricsConfiguration,
        permissioningConfiguration,
        staticNodes,
        pidPath);
  }

  private void startPlugins(final Runner runner) {
    blockchainServiceImpl.init(
        besuController.getProtocolContext().getBlockchain(), besuController.getProtocolSchedule());
    transactionSimulationServiceImpl.init(
        besuController.getProtocolContext().getBlockchain(),
        besuController.getTransactionSimulator());
    rpcEndpointServiceImpl.init(runner.getInProcessRpcMethods());

    BesuPluginServiceRegistrar.registerRuntimeServices(
        besuPluginContext,
        besuController,
        runner,
        getMetricsSystem(),
        miningParametersSupplier.get());

    besuPluginContext.startPlugins();
  }

  private void setReleaseMetrics() {
    besuComponent
        .getMetricsSystem()
        .createLabelledSuppliedGauge(
            StandardMetricCategory.PROCESS, "release", "Release information", "version")
        .labels(() -> 1, BesuVersionUtils.version());
  }

  /**
   * Configure logging framework for Besu
   *
   * @param announce sets to true to print the logging level on standard output
   */
  public void configureLogging(final boolean announce) {
    // To change the configuration if color was enabled/disabled
    LogConfigurator.reconfigure();
    // set log level per CLI flags
    final String logLevel = loggingLevelOption.getLogLevel();
    if (logLevel != null) {
      if (announce) {
        System.out.println("Setting logging level to " + logLevel);
      }
      LogConfigurator.setLevel("", logLevel);
    }
  }

  /**
   * Logging in Color enabled or not.
   *
   * @return Optional true or false representing logging color is enabled. Empty if not set.
   */
  public static Optional<Boolean> getColorEnabled() {
    return Optional.ofNullable(colorEnabled);
  }

  @VisibleForTesting
  void configureNativeLibs(final Optional<NetworkDefinition> configuredNetwork) {
    if (unstableNativeLibraryOptions.getNativeAltbn128()
        && AbstractAltBnPrecompiledContract.maybeEnableNative()) {
      logger.info("Using the native implementation of alt bn128");
    } else {
      AbstractAltBnPrecompiledContract.disableNative();
      logger.info("Using the Java implementation of alt bn128");
    }

    if (unstableNativeLibraryOptions.getNativeModExp()
        && BigIntegerModularExponentiationPrecompiledContract.maybeEnableNative()) {
      logger.info("Using the native implementation of modexp");
    } else {
      BigIntegerModularExponentiationPrecompiledContract.disableNative();
      logger.info("Using the Java implementation of modexp");
    }

    if (unstableNativeLibraryOptions.getNativeSecp()
        && SignatureAlgorithmFactory.getInstance().maybeEnableNative()) {
      logger.info("Using the native implementation of the signature algorithm");
    } else {
      SignatureAlgorithmFactory.getInstance().disableNative();
      logger.info("Using the Java implementation of the signature algorithm");
    }

    if (unstableNativeLibraryOptions.getNativeBlake2bf()
        && Blake2bfMessageDigest.Blake2bfDigest.isNative()) {
      logger.info("Using the native implementation of the blake2bf algorithm");
    } else {
      Blake2bfMessageDigest.Blake2bfDigest.disableNative();
      logger.info("Using the Java implementation of the blake2bf algorithm");
    }

    if (unstableNativeLibraryOptions.getNativeP256Verify()
        && P256VerifyPrecompiledContract.maybeEnableNativeBoringSSL()) {
      logger.info("Using the native BoringSSL implementation of p256verify");
    } else {
      P256VerifyPrecompiledContract.disableNativeBoringSSL();
      if (SECP256R1.isNativeAvailable()) {
        logger.info("Using the native secp256r1 signature algorithm implementation of p256verify");
      } else {
        logger.info("Using the Java secp256r1 implementation of p256verify");
      }
    }

    if (hasKzgFork(readGenesisConfigOptions())) {
      if (kzgTrustedSetupFile != null) {
        KZGPointEvalPrecompiledContract.init(kzgTrustedSetupFile);
      } else {
        KZGPointEvalPrecompiledContract.init();
      }
    } else if (kzgTrustedSetupFile != null) {
      throw new ParameterException(
          this.commandLine,
          "--kzg-trusted-setup can only be specified on networks with data blobs enabled");
    }
    // assert required native libraries have been loaded
    if (genesisFile == null && configuredNetwork.isPresent()) {
      checkRequiredNativeLibraries(configuredNetwork.get());
    }
  }

  @VisibleForTesting
  void checkRequiredNativeLibraries(final NetworkDefinition configuredNetwork) {
    if (configuredNetwork == null) {
      return;
    }

    // assert native library requirements for named networks:
    List<NativeRequirementResult> failedNativeReqs =
        NativeRequirement.getNativeRequirements(configuredNetwork).stream()
            .filter(r -> !r.present())
            .toList();

    if (!failedNativeReqs.isEmpty()) {
      String failures =
          failedNativeReqs.stream()
              .map(r -> r.libname() + " " + r.errorMessage())
              .collect(Collectors.joining("\n\t"));
      throw new UnsupportedOperationException(
          String.format(
              "Failed to load required native libraries for network %s. "
                  + "Verify whether your platform %s and arch %s are supported by besu. "
                  + "Failures loading: \n%s",
              configuredNetwork.name(),
              System.getProperty("os.name"),
              System.getProperty("os.arch"),
              failures));
    }
  }

  private void validateOptions() {
    issueOptionWarnings();
    validateP2POptions();
    validateMiningParams();
    validateNatParams();
    validateNetStatsParams();
    validateDnsOptionsParams();
    ensureValidPeerBoundParams();
    validateRpcOptionsParams();
    validateRpcWsOptions();
    validateChainDataPruningParams();
    validatePostMergeCheckpointBlockRequirements();
    validateTransactionPoolOptions();
    validateDataStorageOptions();
    validateGraphQlOptions();
    validatePluginOptions();
    validateUnstableNetworkingOptions();
  }

  private void validatePluginOptions() {
    pluginsConfigurationOptions.validate(commandLine);
  }

  private void validateUnstableNetworkingOptions() {
    unstableNetworkingOptions.validate(commandLine);
  }

  private void validateApiOptions() {
    apiConfigurationOptions.validate(commandLine, logger);
  }

  private void validateTransactionPoolOptions() {
    transactionPoolOptions.validate(commandLine, genesisConfigOptionsSupplier.get());
  }

  private void validateDataStorageOptions() {
    dataStorageOptions.validate(commandLine);
  }

  private void validateMiningParams() {
    miningOptions.validate(
        commandLine, genesisConfigOptionsSupplier.get(), isMergeEnabled(), logger);
  }

  private void validateP2POptions() {
    p2PDiscoveryOptions.validate(commandLine, getNetworkInterfaceChecker());
  }

  /**
   * Returns a network interface checker that can be used to validate P2P options.
   *
   * @return A {@link P2PDiscoveryOptions.NetworkInterfaceChecker} that checks if a network
   *     interface is available.
   */
  protected P2PDiscoveryOptions.NetworkInterfaceChecker getNetworkInterfaceChecker() {
    return NetworkUtility::isNetworkInterfaceAvailable;
  }

  private void validateGraphQlOptions() {
    graphQlOptions.validate(logger, commandLine);
  }

  @SuppressWarnings("ConstantConditions")
  private void validateNatParams() {
    if (natMethod.equals(NatMethod.AUTO) && !unstableNatOptions.getNatMethodFallbackEnabled()) {
      throw new ParameterException(
          this.commandLine,
          "The `--Xnat-method-fallback-enabled` parameter cannot be used in AUTO mode. Either remove --Xnat-method-fallback-enabled"
              + " or select another mode (via --nat--method=XXXX)");
    }
  }

  private void validateNetStatsParams() {
    if (Strings.isNullOrEmpty(ethstatsOptions.getEthstatsUrl())
        && !ethstatsOptions.getEthstatsContact().isEmpty()) {
      throw new ParameterException(
          this.commandLine,
          "The `--ethstats-contact` requires ethstats server URL to be provided. Either remove --ethstats-contact"
              + " or provide a URL (via --ethstats=nodename:secret@host:port)");
    }
  }

  private void validateDnsOptionsParams() {
    if (!unstableDnsOptions.getDnsEnabled() && unstableDnsOptions.getDnsUpdateEnabled()) {
      throw new ParameterException(
          this.commandLine,
          "The `--Xdns-update-enabled` requires dns to be enabled. Either remove --Xdns-update-enabled"
              + " or specify dns is enabled (--Xdns-enabled)");
    }
  }

  private void ensureValidPeerBoundParams() {
    maxPeers = p2PDiscoveryOptions.maxPeers;
    final Boolean isLimitRemoteWireConnectionsEnabled =
        p2PDiscoveryOptions.isLimitRemoteWireConnectionsEnabled;
    if (isLimitRemoteWireConnectionsEnabled) {
      final float fraction =
          Fraction.fromPercentage(p2PDiscoveryOptions.maxRemoteConnectionsPercentage).getValue();
      checkState(
          fraction >= 0.0 && fraction <= 1.0,
          "Fraction of remote connections allowed must be between 0.0 and 1.0 (inclusive).");
      maxRemoteInitiatedPeers = Math.round(fraction * maxPeers);
    } else {
      maxRemoteInitiatedPeers = maxPeers;
    }
  }

  private void validateRpcOptionsParams() {
    final Predicate<String> configuredApis =
        apiName ->
            Arrays.stream(RpcApis.values())
                    .anyMatch(builtInApi -> apiName.equals(builtInApi.name()))
                || rpcEndpointServiceImpl.hasNamespace(apiName);
    jsonRpcHttpOptions.validate(logger, commandLine, configuredApis);
  }

  private void validateRpcWsOptions() {
    final Predicate<String> configuredApis =
        apiName ->
            Arrays.stream(RpcApis.values())
                    .anyMatch(builtInApi -> apiName.equals(builtInApi.name()))
                || rpcEndpointServiceImpl.hasNamespace(apiName);
    rpcWebsocketOptions.validate(logger, commandLine, configuredApis);
  }

  private void validateChainDataPruningParams() {
    final ChainPruningStrategy chainPruningStrategy =
        unstableChainPruningOptions.getChainPruningStrategy();
    final long blocksRetained = unstableChainPruningOptions.getChainDataPruningBlocksRetained();
    final long balsRetained = unstableChainPruningOptions.getChainDataPruningBalsRetained();
    final long retainedMinimum = unstableChainPruningOptions.getChainDataPruningRetainedMinimum();

    // Skip validation if pruning is disabled
    if (chainPruningStrategy == ChainPruningStrategy.NONE) {
      return;
    }

    // Basic validation
    if (blocksRetained < 0) {
      throw new ParameterException(
          this.commandLine, "--Xchain-pruning-blocks-retained must be >= 0");
    }
    if (balsRetained < 0) {
      throw new ParameterException(this.commandLine, "--Xchain-pruning-bals-retained must be >= 0");
    }
    if (retainedMinimum < 0) {
      throw new ParameterException(
          this.commandLine, "--Xchain-pruning-retained-minimum must be >= 0");
    }

    // Validate blocks pruning (when mode is ALL)
    if (chainPruningStrategy == ChainPruningStrategy.ALL) {
      if (blocksRetained < retainedMinimum) {
        throw new ParameterException(
            this.commandLine, "--Xchain-pruning-blocks-retained must be >= " + retainedMinimum);
      }

      final GenesisConfigOptions genesisConfigOptions = readGenesisConfigOptions();
      if (genesisConfigOptions.isPoa()) {
        final var epochLengthOpt = getPoaEpochLength(genesisConfigOptions);
        if (epochLengthOpt.isPresent()) {
          final long epochLength = epochLengthOpt.getAsLong();
          if (blocksRetained < epochLength) {
            throw new ParameterException(
                this.commandLine,
                String.format(
                    "--Xchain-pruning-blocks-retained(%d) must be >= epochlength(%d) for %s",
                    blocksRetained, epochLength, getConsensusMechanism(genesisConfigOptions)));
          }
        }
      }
    }

    // Validate BAL pruning (when mode is BAL or ALL)
    if (chainPruningStrategy == ChainPruningStrategy.BAL
        || chainPruningStrategy == ChainPruningStrategy.ALL) {
      if (balsRetained < retainedMinimum) {
        throw new ParameterException(
            this.commandLine, "--Xchain-pruning-bals-retained must be >= " + retainedMinimum);
      }

      // When both are enabled (ALL mode), BAL retention can't exceed block retention
      if (chainPruningStrategy == ChainPruningStrategy.ALL && balsRetained > blocksRetained) {
        throw new ParameterException(
            this.commandLine,
            "--Xchain-pruning-bals-retained must be <= --Xchain-pruning-blocks-retained when pruning mode is ALL");
      }
    }
  }

  private GenesisConfig readGenesisConfig() {
    GenesisConfig effectiveGenesisFile;
    effectiveGenesisFile =
        network.equals(EPHEMERY)
            ? EphemeryGenesisUpdater.updateGenesis(genesisConfigOverrides)
            : genesisFile != null
                ? GenesisConfig.fromConfig(loadAndTransformGenesisFile(genesisFile))
                : GenesisConfig.fromResource(
                    Optional.ofNullable(network).orElse(MAINNET).getGenesisFile());
    return effectiveGenesisFile.withOverrides(genesisConfigOverrides);
  }

  private GenesisConfigOptions readGenesisConfigOptions() {
    try {
      return genesisConfigSupplier.get().getConfigOptions();
    } catch (final Exception e) {
      throw new ParameterException(
          this.commandLine, "Unable to load genesis file. " + e.getCause());
    }
  }

  /**
   * Loads a genesis file from File and applies Geth-to-Besu transformation if needed.
   *
   * @param genesisFile the genesis file
   * @return the loaded and potentially transformed ObjectNode
   */
  private ObjectNode loadAndTransformGenesisFile(final File genesisFile) {
    try {
      final URL url = genesisFile.toURI().toURL();
      final ObjectNode genesisRoot = JsonUtil.objectNodeFromURL(url, false);

      // Check if this is a Geth format genesis file and transform if needed
      if (isGethFormat(genesisRoot)) {
        transformGethToBesu(genesisRoot);
      }

      return genesisRoot;
    } catch (final Exception e) {
      // Extract the root cause for better error reporting
      Throwable rootCause = e;
      while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
        rootCause = rootCause.getCause();
      }
      throw new RuntimeException("Unable to load genesis file: " + genesisFile, rootCause);
    }
  }

  /**
   * Detects if a genesis file is in Geth format.
   *
   * <p>A genesis file is considered Geth format if:
   *
   * <ul>
   *   <li>It has a "config" section
   *   <li>The config has a "mergeNetsplitBlock" field (Geth-specific)
   *   <li>The config does NOT have an "ethash" field (Besu-specific)
   * </ul>
   *
   * @param genesisRoot the root genesis JSON node
   * @return true if this is a Geth format genesis file
   */
  private boolean isGethFormat(final ObjectNode genesisRoot) {
    final Optional<ObjectNode> configNode = JsonUtil.getObjectNode(genesisRoot, "config");
    if (!configNode.isPresent()) {
      return false;
    }

    final ObjectNode config = configNode.get();
    final boolean hasMergeNetsplitBlock = config.has("mergeNetsplitBlock");
    final boolean hasEthash = config.has("ethash");

    // It's Geth format if it has mergeNetsplitBlock but not ethash
    return hasMergeNetsplitBlock && !hasEthash;
  }

  /**
   * Transforms a Geth-format genesis file to Besu format by applying five transformations.
   *
   * <p>Transformations applied:
   *
   * <ol>
   *   <li><b>Add ethash field:</b> Besu's {@code isEthHash()} method checks for the presence of
   *       this field in the JSON structure. Since this is a structural check, the overrides
   *       mechanism doesn't work - we must add it to the JSON.
   *   <li><b>Map mergeNetsplitBlock to preMergeForkBlock:</b> These fields serve identical purposes
   *       (marking the merge activation block) but use different names in Geth vs Besu.
   *   <li><b>Add baseFeePerGas:</b> When London fork is activated at genesis (block 0), Besu
   *       expects an explicit base fee. Geth may omit this field, so we add the standard default of
   *       1 gwei (0x3B9ACA00).
   *   <li><b>Add withdrawalRequestContractAddress:</b> EIP-7002 withdrawal request contract address
   *       if missing.
   *   <li><b>Add consolidationRequestContractAddress:</b> EIP-7251 consolidation request contract
   *       address if missing.
   * </ol>
   *
   * @param genesisRoot the root genesis JSON node (will be modified in place)
   */
  private void transformGethToBesu(final ObjectNode genesisRoot) {
    final Optional<ObjectNode> configNode = JsonUtil.getObjectNode(genesisRoot, "config");
    if (!configNode.isPresent()) {
      return;
    }

    final ObjectNode config = configNode.get();

    // Add ethash field if not present
    if (!config.has("ethash")) {
      config.set("ethash", JsonUtil.createEmptyObjectNode());
    }

    // Map mergeNetsplitBlock to preMergeForkBlock
    if (config.has("mergeNetsplitBlock") && !config.has("preMergeForkBlock")) {
      final long mergeBlock = config.get("mergeNetsplitBlock").asLong();
      config.put("preMergeForkBlock", mergeBlock);
    }

    // Add baseFeePerGas if London is at genesis
    if (!genesisRoot.has("baseFeePerGas") && config.has("londonBlock")) {
      final long londonBlock = config.get("londonBlock").asLong(Long.MAX_VALUE);
      if (londonBlock == 0) {
        // Add default 1 gwei base fee
        genesisRoot.put("baseFeePerGas", "0x3B9ACA00");
      }
    }

    // Add withdrawalRequestContractAddress if missing (EIP-7002)
    if (!config.has("withdrawalRequestContractAddress")) {
      config.put("withdrawalRequestContractAddress", "0x00000961ef480eb55e80d19ad83579a64c007002");
    }

    // Add consolidationRequestContractAddress if missing (EIP-7251)
    if (!config.has("consolidationRequestContractAddress")) {
      config.put(
          "consolidationRequestContractAddress", "0x0000bbddc7ce488642fb579f8b00f3a590007251");
    }
  }

  /**
   * Checks if the genesis configuration includes any fork times that require KZG initialization.
   * This includes Cancun and all subsequent forks that use KZG commitments for EIP-4844 blob
   * transactions.
   *
   * @param genesisConfigOptions the genesis config options
   * @return true if any KZG-requiring fork time is present
   */
  private boolean hasKzgFork(final GenesisConfigOptions genesisConfigOptions) {
    return genesisConfigOptions.getCancunTime().isPresent()
        || genesisConfigOptions.getPragueTime().isPresent()
        || genesisConfigOptions.getOsakaTime().isPresent()
        || genesisConfigOptions.getBpo1Time().isPresent()
        || genesisConfigOptions.getBpo2Time().isPresent()
        || genesisConfigOptions.getBpo3Time().isPresent()
        || genesisConfigOptions.getBpo4Time().isPresent()
        || genesisConfigOptions.getBpo5Time().isPresent()
        || genesisConfigOptions.getAmsterdamTime().isPresent()
        || genesisConfigOptions.getFutureEipsTime().isPresent();
  }

  /**
   * Gets the block period in seconds based on the consensus mechanism.
   *
   * @param genesisConfigOptions the genesis config options
   * @return the block period in seconds, or empty if not applicable
   */
  private OptionalInt getBlockPeriodSeconds(final GenesisConfigOptions genesisConfigOptions) {
    if (genesisConfigOptions.isIbft2()) {
      return OptionalInt.of(genesisConfigOptions.getBftConfigOptions().getBlockPeriodSeconds());
    }
    if (genesisConfigOptions.isQbft()) {
      return OptionalInt.of(genesisConfigOptions.getQbftConfigOptions().getBlockPeriodSeconds());
    }
    return OptionalInt.empty();
  }

  /**
   * Gets the epoch length for PoA consensus mechanisms.
   *
   * @param genesisConfigOptions the genesis config options
   * @return epoch length if PoA consensus is configured, empty otherwise
   */
  private OptionalLong getPoaEpochLength(final GenesisConfigOptions genesisConfigOptions) {
    if (genesisConfigOptions.isIbft2()) {
      return OptionalLong.of(genesisConfigOptions.getBftConfigOptions().getEpochLength());
    } else if (genesisConfigOptions.isQbft()) {
      return OptionalLong.of(genesisConfigOptions.getQbftConfigOptions().getEpochLength());
    }
    return OptionalLong.empty();
  }

  /**
   * Gets the name of the consensus mechanism configured in the genesis.
   *
   * @param genesisConfigOptions the genesis config options
   * @return the consensus mechanism name (e.g., "IBFT2", "QBFT", "Clique", "Ethash")
   */
  private String getConsensusMechanism(final GenesisConfigOptions genesisConfigOptions) {
    if (genesisConfigOptions.isIbft2()) {
      return "IBFT2";
    } else if (genesisConfigOptions.isQbft()) {
      return "QBFT";
    } else if (genesisConfigOptions.isEthHash()) {
      return "Ethash";
    }
    return "Unknown";
  }

  private void issueOptionWarnings() {

    // Check that P2P options are able to work
    CommandLineUtils.checkOptionDependencies(
        logger,
        commandLine,
        "--p2p-enabled",
        !p2PDiscoveryOptions.p2pEnabled,
        asList(
            "--bootnodes",
            "--discovery-enabled",
            "--max-peers",
            "--banned-node-id",
            "--banned-node-ids",
            "--p2p-host",
            "--p2p-interface",
            "--p2p-port",
            "--remote-connections-max-percentage"));

    if (getDefaultSyncModeIfNotSet() == SyncMode.FULL
        && isOptionSet(commandLine, "--sync-min-peers")) {
      logger.warn("--sync-min-peers is ignored in FULL sync-mode");
    }

    CommandLineUtils.failIfOptionDoesntMeetRequirement(
        commandLine,
        "--Xsnapsync-synchronizer-flat option can only be used when --Xbonsai-full-flat-db-enabled is true",
        dataStorageOptions
            .toDomainObject()
            .getPathBasedExtraStorageConfiguration()
            .getUnstable()
            .getFullFlatDbEnabled(),
        asList(
            "--Xsnapsync-synchronizer-flat-account-healed-count-per-request",
            "--Xsnapsync-synchronizer-flat-slot-healed-count-per-request"));

    if (!securityModuleName.equals(DEFAULT_SECURITY_MODULE)
        && nodePrivateKeyFileOption.getNodePrivateKeyFile() != null) {
      logger.warn(
          DEPENDENCY_WARNING_MSG,
          "--node-private-key-file",
          "--security-module=" + DEFAULT_SECURITY_MODULE);
    }
  }

  private void configure() throws Exception {
    p2PDiscoveryConfig = p2PDiscoveryOptions.toDomainObject();
    engineRPCConfig = engineRPCOptions.toDomainObject();
    checkPortClash();
    checkIfRequiredPortsAreAvailable();
    syncMode = getDefaultSyncModeIfNotSet();
    versionCompatibilityProtection = getDefaultVersionCompatibilityProtectionIfNotSet();

    ethNetworkConfig = updateNetworkConfig(network);

    jsonRpcConfiguration =
        jsonRpcHttpOptions.jsonRpcConfiguration(
            hostsAllowlist,
            p2PDiscoveryConfig.p2pHost(),
            unstableRPCOptions.getHttpTimeoutSec(),
            unstableRPCOptions.getHttpStreamingTimeoutSec());
    logger.info("RPC HTTP JSON-RPC config: {}", jsonRpcConfiguration);
    if (isEngineApiEnabled()) {
      engineJsonRpcConfiguration = createEngineJsonRpcConfiguration();
      logger.info("Engine JSON-RPC config: {}", engineJsonRpcConfiguration);
      // align JSON decoding limit with HTTP body limit
      // character count is close to size in bytes
      final long maxRequestContentLength =
          Math.max(
              jsonRpcConfiguration.getMaxRequestContentLength(),
              engineJsonRpcConfiguration.getMaxRequestContentLength());
      configureVertxJsonDecodingMaxLength((int) maxRequestContentLength);
    }

    graphQLConfiguration =
        graphQlOptions.graphQLConfiguration(
            hostsAllowlist, p2PDiscoveryConfig.p2pHost(), unstableRPCOptions.getHttpTimeoutSec());

    webSocketConfiguration =
        rpcWebsocketOptions.webSocketConfiguration(
            hostsAllowlist, p2PDiscoveryConfig.p2pHost(), unstableRPCOptions.getWsTimeoutSec());
    jsonRpcIpcConfiguration =
        jsonRpcIpcConfiguration(
            unstableIpcOptions.isEnabled(),
            unstableIpcOptions.getIpcPath(),
            unstableIpcOptions.getRpcIpcApis());
    inProcessRpcConfiguration = inProcessRpcOptions.toDomainObject();
    dataStorageConfiguration = getDataStorageConfiguration();

    permissioningConfiguration = permissioningConfiguration();
    staticNodes = loadStaticNodes();

    permissioningConfiguration
        .flatMap(PermissioningConfiguration::getLocalConfig)
        .ifPresent(p -> ensureAllNodesAreInAllowlist(ethNetworkConfig.enodeBootNodes(), p));

    permissioningConfiguration
        .flatMap(PermissioningConfiguration::getLocalConfig)
        .ifPresent(p -> ensureAllNodesAreInAllowlist(ethNetworkConfig.enrBootNodes(), p));

    permissioningConfiguration
        .flatMap(PermissioningConfiguration::getLocalConfig)
        .ifPresent(p -> ensureAllNodesAreInAllowlist(staticNodes, p));
    metricsConfiguration = metricsConfiguration();

    instantiateSignatureAlgorithmFactory();

    logger.info(generateConfigurationOverview());
    logger.info("Security Module: {}", securityModuleName);
  }

  private static void configureVertxJsonDecodingMaxLength(final int maxStringLength) {
    // Supports large sized engine_newPayload decoding

    // This is a global setting for the Vert.x ObjectMapper
    // which is used by both the JsonRpc and EngineJsonRpc services
    // Attempting to limit the scope of the mapper config leads to extra serialisation steps
    ObjectMapper om = DatabindCodec.mapper();
    StreamReadConstraints src =
        StreamReadConstraints.builder().maxStringLength(maxStringLength).build();
    om.getFactory().setStreamReadConstraints(src);
  }

  private Optional<PermissioningConfiguration> permissioningConfiguration() throws Exception {
    return permissionsOptions.permissioningConfiguration(
        jsonRpcHttpOptions,
        rpcWebsocketOptions,
        getEnodeDnsConfiguration(),
        dataDir(),
        logger,
        commandLine);
  }

  private JsonRpcIpcConfiguration jsonRpcIpcConfiguration(
      final Boolean enabled, final Path ipcPath, final List<String> rpcIpcApis) {
    final Path actualPath;
    if (ipcPath == null) {
      actualPath = IpcOptions.getDefaultPath(dataDir());
    } else {
      actualPath = ipcPath;
    }
    return new JsonRpcIpcConfiguration(
        vertx.isNativeTransportEnabled() && enabled, actualPath, rpcIpcApis);
  }

  private void ensureAllNodesAreInAllowlist(
      final Collection<? extends NodeIdentifier> nodeIdentifiers,
      final LocalPermissioningConfiguration permissioningConfiguration) {
    try {
      PermissioningConfigurationValidator.areAllNodesInAllowlist(
          nodeIdentifiers, permissioningConfiguration);
    } catch (final Exception e) {
      throw new ParameterException(this.commandLine, e.getMessage());
    }
  }

  /**
   * Builds BesuController
   *
   * @return instance of BesuController
   */
  public BesuController buildController() {
    try {
      return setupControllerBuilder().build();
    } catch (final Exception e) {
      throw new ExecutionException(this.commandLine, e.getMessage(), e);
    }
  }

  /**
   * Builds BesuControllerBuilder which can be used to build BesuController
   *
   * @return instance of BesuControllerBuilder
   */
  public BesuControllerBuilder setupControllerBuilder() {
    pluginCommonConfiguration
        .init(dataDir(), dataDir().resolve(DATABASE_PATH), getDataStorageConfiguration())
        .withMiningParameters(miningParametersSupplier.get())
        .withJsonRpcHttpOptions(jsonRpcHttpOptions);
    final KeyValueStorageProvider storageProvider = keyValueStorageProvider(keyValueStorageName);
    final ApiConfiguration apiConfiguration = apiConfigurationSupplier.get();
    final BalConfiguration balConfiguration = balConfigurationOptions.toDomainObject();

    BesuControllerBuilder besuControllerBuilder =
        controllerBuilder
            .fromEthNetworkConfig(updateNetworkConfig(network), getDefaultSyncModeIfNotSet())
            .synchronizerConfiguration(buildSyncConfig())
            .ethProtocolConfiguration(unstableEthProtocolOptions.toDomainObject())
            .networkConfiguration(unstableNetworkingOptions.toDomainObject())
            .dataDirectory(dataDir())
            .dataStorageConfiguration(getDataStorageConfiguration())
            .miningParameters(miningParametersSupplier.get())
            .transactionPoolConfiguration(buildTransactionPoolConfiguration())
            .nodeKey(new NodeKey(securityModule()))
            .metricsSystem((ObservableMetricsSystem) besuComponent.getMetricsSystem())
            .messagePermissioningProviders(permissioningService.getMessagePermissioningProviders())
            .clock(Clock.systemUTC())
            .isRevertReasonEnabled(isRevertReasonEnabled)
            .storageProvider(storageProvider)
            .isEarlyRoundChangeEnabled(unstableQbftOptions.isEarlyRoundChangeEnabled())
            .requiredBlocks(requiredBlocks)
            .reorgLoggingThreshold(reorgLoggingThreshold)
            .evmConfiguration(unstableEvmOptions.toDomainObject())
            .maxPeers(p2PDiscoveryOptions.maxPeers)
            .maxRemotelyInitiatedPeers(maxRemoteInitiatedPeers)
            .randomPeerPriority(p2PDiscoveryOptions.randomPeerPriority)
            .chainPruningConfiguration(unstableChainPruningOptions.toDomainObject())
            .cacheLastBlocks(numberOfBlocksToCache)
            .cacheLastBlockHeaders(numberOfBlockHeadersToCache)
            .isCacheLastBlockHeadersPreloadEnabled(isCacheLastBlockHeadersPreloadEnabled)
            .genesisStateHashCacheEnabled(genesisStateHashCacheEnabled)
            .apiConfiguration(apiConfiguration)
            .balConfiguration(balConfiguration)
            .besuComponent(besuComponent);
    if (DataStorageFormat.BONSAI.equals(getDataStorageConfiguration().getDataStorageFormat())) {
      final PathBasedExtraStorageConfiguration subStorageConfiguration =
          getDataStorageConfiguration().getPathBasedExtraStorageConfiguration();
      besuControllerBuilder.isParallelTxProcessingEnabled(
          subStorageConfiguration.getParallelTxProcessingEnabled());
    }
    return besuControllerBuilder;
  }

  private JsonRpcConfiguration createEngineJsonRpcConfiguration() {
    jsonRpcHttpOptions.checkDependencies(logger, commandLine);
    final JsonRpcConfiguration engineConfig =
        jsonRpcHttpOptions.jsonRpcConfiguration(
            engineRPCConfig.engineHostsAllowlist(),
            p2PDiscoveryConfig.p2pHost(),
            unstableRPCOptions.getHttpTimeoutSec(),
            unstableRPCOptions.getHttpStreamingTimeoutSec());
    engineConfig.setPort(engineRPCConfig.engineRpcPort());
    engineConfig.setRpcApis(Arrays.asList("ENGINE", "ETH"));
    engineConfig.setEnabled(isEngineApiEnabled());
    if (!engineRPCConfig.isEngineAuthDisabled()) {
      engineConfig.setAuthenticationEnabled(true);
      engineConfig.setAuthenticationAlgorithm(JwtAlgorithm.HS256);
      if (Objects.nonNull(engineRPCConfig.engineJwtKeyFile())
          && java.nio.file.Files.exists(engineRPCConfig.engineJwtKeyFile())) {
        engineConfig.setAuthenticationPublicKeyFile(engineRPCConfig.engineJwtKeyFile().toFile());
      } else {
        logger.warn(
            "Engine API authentication enabled without key file. Expect ephemeral jwt.hex file in datadir");
      }
    }
    return engineConfig;
  }

  /**
   * Metrics Configuration for Besu
   *
   * @return instance of MetricsConfiguration.
   */
  public MetricsConfiguration metricsConfiguration() {
    if (metricsOptions.getMetricsEnabled() && metricsOptions.getMetricsPushEnabled()) {
      throw new ParameterException(
          this.commandLine,
          "--metrics-enabled option and --metrics-push-enabled option can't be used at the same "
              + "time.  Please refer to CLI reference for more details about this constraint.");
    }

    CommandLineUtils.checkOptionDependencies(
        logger,
        commandLine,
        "--metrics-enabled",
        !metricsOptions.getMetricsEnabled(),
        asList("--metrics-host", "--metrics-port"));

    CommandLineUtils.checkOptionDependencies(
        logger,
        commandLine,
        "--metrics-push-enabled",
        !metricsOptions.getMetricsPushEnabled(),
        asList(
            "--metrics-push-host",
            "--metrics-push-port",
            "--metrics-push-interval",
            "--metrics-push-prometheus-job"));

    metricsOptions.setMetricCategoryRegistry(metricCategoryRegistry);

    metricsOptions.validate(commandLine);

    final MetricsConfiguration.Builder metricsConfigurationBuilder =
        metricsOptions.toDomainObject();
    metricsConfigurationBuilder
        .host(
            Strings.isNullOrEmpty(metricsOptions.getMetricsHost())
                ? p2PDiscoveryConfig.p2pHost()
                : metricsOptions.getMetricsHost())
        .pushHost(
            Strings.isNullOrEmpty(metricsOptions.getMetricsPushHost())
                ? p2PDiscoveryOptions.autoDiscoverDefaultIP().getHostAddress()
                : metricsOptions.getMetricsPushHost())
        .hostsAllowlist(hostsAllowlist);
    final var metricsConfiguration = metricsConfigurationBuilder.build();
    metricCategoryRegistry.setMetricsConfiguration(metricsConfiguration);
    return metricsConfiguration;
  }

  private KeyValueStorageProvider keyValueStorageProvider(final String name) {
    if (this.keyValueStorageProvider == null) {
      this.keyValueStorageProvider =
          new KeyValueStorageProviderBuilder()
              .withStorageFactory(
                  storageService
                      .getByName(name)
                      .orElseThrow(
                          () ->
                              new StorageException(
                                  "No KeyValueStorageFactory found for key: " + name)))
              .withCommonConfiguration(pluginCommonConfiguration)
              .withMetricsSystem(getMetricsSystem())
              .build();
    }
    return this.keyValueStorageProvider;
  }

  /**
   * Get the storage provider
   *
   * @return the storage provider
   */
  public StorageProvider getStorageProvider() {
    return keyValueStorageProvider(keyValueStorageName);
  }

  private SynchronizerConfiguration buildSyncConfig() {
    return unstableSynchronizerOptions
        .toDomainObject()
        .syncMode(getDefaultSyncModeIfNotSet())
        .syncMinimumPeerCount(syncMinPeerCount)
        .build();
  }

  private TransactionPoolConfiguration buildTransactionPoolConfiguration() {
    transactionPoolOptions.setPluginTransactionValidatorService(
        transactionPoolValidatorServiceImpl);
    final var txPoolConf = transactionPoolOptions.toDomainObject();
    final var txPoolConfBuilder =
        ImmutableTransactionPoolConfiguration.builder()
            .from(txPoolConf)
            .saveFile((dataPath.resolve(txPoolConf.getSaveFile().getPath()).toFile()));

    if (genesisConfigOptionsSupplier.get().isZeroBaseFee()) {
      logger.warn(
          "Forcing price bump for transaction replacement to 0, since we are on a zero basefee network");
      txPoolConfBuilder.priceBump(Percentage.ZERO);
    }

    if (miningParametersSupplier.get().getMinTransactionGasPrice().equals(Wei.ZERO)
        && !transactionPoolOptions.isPriceBumpSet(commandLine)) {
      logger.warn(
          "Forcing price bump for transaction replacement to 0, since min-gas-price is set to 0");
      txPoolConfBuilder.priceBump(Percentage.ZERO);
    }

    if (miningParametersSupplier
        .get()
        .getMinTransactionGasPrice()
        .lessThan(txPoolConf.getMinGasPrice())) {
      if (transactionPoolOptions.isMinGasPriceSet(commandLine)) {
        throw new ParameterException(
            commandLine, "tx-pool-min-gas-price cannot be greater than the value of min-gas-price");

      } else {
        // for backward compatibility, if tx-pool-min-gas-price is not set, we adjust its value
        // to be the same as min-gas-price, so the behavior is as before this change, and we notify
        // the user of the change
        logger.warn(
            "Forcing tx-pool-min-gas-price="
                + miningParametersSupplier.get().getMinTransactionGasPrice().toDecimalString()
                + ", since it cannot be greater than the value of min-gas-price");
        txPoolConfBuilder.minGasPrice(miningParametersSupplier.get().getMinTransactionGasPrice());
      }
    }

    return txPoolConfBuilder.build();
  }

  private MiningConfiguration getMiningParameters() {
    miningOptions.setTransactionSelectionService(transactionSelectionServiceImpl);
    final var miningParameters = miningOptions.toDomainObject();
    getBlockPeriodSeconds(readGenesisConfigOptions())
        .ifPresent(miningParameters::setBlockPeriodSeconds);
    initMiningParametersMetrics(miningParameters);

    return miningParameters;
  }

  private ApiConfiguration getApiConfiguration() {
    validateApiOptions();
    return apiConfigurationOptions.apiConfiguration();
  }

  /**
   * Get the data storage configuration
   *
   * @return the data storage configuration
   */
  public DataStorageConfiguration getDataStorageConfiguration() {
    if (dataStorageConfiguration == null) {
      dataStorageConfiguration = dataStorageOptions.toDomainObject();
    }

    if (SyncMode.FULL.equals(getDefaultSyncModeIfNotSet())
        && DataStorageFormat.BONSAI.equals(dataStorageConfiguration.getDataStorageFormat())) {
      final PathBasedExtraStorageConfiguration pathBasedExtraStorageConfiguration =
          dataStorageConfiguration.getPathBasedExtraStorageConfiguration();
      if (pathBasedExtraStorageConfiguration.getLimitTrieLogsEnabled()) {
        if (CommandLineUtils.isOptionSet(
            commandLine, PathBasedExtraStorageOptions.LIMIT_TRIE_LOGS_ENABLED)) {
          throw new ParameterException(
              commandLine,
              String.format(
                  "Cannot enable %s with --sync-mode=%s and --data-storage-format=%s. You must set %s or use a different sync-mode",
                  PathBasedExtraStorageOptions.LIMIT_TRIE_LOGS_ENABLED,
                  SyncMode.FULL,
                  DataStorageFormat.BONSAI,
                  PathBasedExtraStorageOptions.LIMIT_TRIE_LOGS_ENABLED + "=false"));
        }

        dataStorageConfiguration =
            ImmutableDataStorageConfiguration.copyOf(dataStorageConfiguration)
                .withPathBasedExtraStorageConfiguration(
                    ImmutablePathBasedExtraStorageConfiguration.copyOf(
                            dataStorageConfiguration.getPathBasedExtraStorageConfiguration())
                        .withLimitTrieLogsEnabled(false));
        logger.warn(
            "Forcing {}, since it cannot be enabled with --sync-mode={} and --data-storage-format={}.",
            PathBasedExtraStorageOptions.LIMIT_TRIE_LOGS_ENABLED + "=false",
            SyncMode.FULL,
            DataStorageFormat.BONSAI);
      }
    }
    return dataStorageConfiguration;
  }

  /**
   * Gets the network for this BesuCommand
   *
   * @return the network for this BesuCommand
   */
  public NetworkDefinition getNetwork() {
    return network;
  }

  private void initMiningParametersMetrics(final MiningConfiguration miningConfiguration) {
    new MiningParametersMetrics(getMetricsSystem(), miningConfiguration);
  }

  // Blockchain synchronization from peers.
  private Runner synchronize(
      final BesuController controller,
      final boolean p2pEnabled,
      final boolean peerDiscoveryEnabled,
      final EthNetworkConfig ethNetworkConfig,
      final String p2pAdvertisedHost,
      final String p2pListenInterface,
      final int p2pListenPort,
      final GraphQLConfiguration graphQLConfiguration,
      final JsonRpcConfiguration jsonRpcConfiguration,
      final JsonRpcConfiguration engineJsonRpcConfiguration,
      final WebSocketConfiguration webSocketConfiguration,
      final JsonRpcIpcConfiguration jsonRpcIpcConfiguration,
      final InProcessRpcConfiguration inProcessRpcConfiguration,
      final ApiConfiguration apiConfiguration,
      final MetricsConfiguration metricsConfiguration,
      final Optional<PermissioningConfiguration> permissioningConfiguration,
      final Collection<EnodeURLImpl> staticNodes,
      final Path pidPath) {

    checkNotNull(runnerBuilder);

    final Runner runner =
        runnerBuilder
            .vertx(vertx)
            .besuController(controller)
            .p2pEnabled(p2pEnabled)
            .natMethod(natMethod)
            .natMethodFallbackEnabled(unstableNatOptions.getNatMethodFallbackEnabled())
            .discoveryEnabled(peerDiscoveryEnabled)
            .ethNetworkConfig(ethNetworkConfig)
            .permissioningConfiguration(permissioningConfiguration)
            .p2pAdvertisedHost(p2pAdvertisedHost)
            .p2pListenInterface(p2pListenInterface)
            .p2pListenPort(p2pListenPort)
            .p2pAdvertisedHostIpv6(p2PDiscoveryConfig.p2pHostIpv6())
            .p2pListenInterfaceIpv6(p2PDiscoveryConfig.p2pInterfaceIpv6())
            .p2pListenPortIpv6(p2PDiscoveryConfig.p2pPortIpv6())
            .networkingConfiguration(unstableNetworkingOptions.toDomainObject())
            .graphQLConfiguration(graphQLConfiguration)
            .jsonRpcConfiguration(jsonRpcConfiguration)
            .engineJsonRpcConfiguration(engineJsonRpcConfiguration)
            .webSocketConfiguration(webSocketConfiguration)
            .jsonRpcIpcConfiguration(jsonRpcIpcConfiguration)
            .inProcessRpcConfiguration(inProcessRpcConfiguration)
            .apiConfiguration(apiConfiguration)
            .pidPath(pidPath)
            .dataDir(dataDir())
            .bannedNodeIds(p2PDiscoveryConfig.bannedNodeIds())
            .metricsSystem((ObservableMetricsSystem) besuComponent.getMetricsSystem())
            .permissioningService(permissioningService)
            .metricsConfiguration(metricsConfiguration)
            .staticNodes(staticNodes)
            .identityString(identityString)
            .besuPluginContext(besuPluginContext)
            .autoLogBloomCaching(autoLogBloomCachingEnabled)
            .ethstatsOptions(ethstatsOptions)
            .storageProvider(keyValueStorageProvider(keyValueStorageName))
            .rpcEndpointService(rpcEndpointServiceImpl)
            .enodeDnsConfiguration(getEnodeDnsConfiguration())
            .allowedSubnets(p2PDiscoveryConfig.allowedSubnets())
            .poaDiscoveryRetryBootnodes(p2PDiscoveryConfig.poaDiscoveryRetryBootnodes())
            .preferIpv6Outbound(p2PDiscoveryConfig.preferIpv6Outbound())
            .transactionValidatorService(transactionValidatorServiceImpl)
            .build();

    addShutdownHook(runner);

    return runner;
  }

  /**
   * Builds Vertx instance from MetricsSystem. Visible for testing.
   *
   * @param metricsSystem Instance of MetricsSystem
   * @return Instance of Vertx.
   */
  @VisibleForTesting
  protected Vertx createVertx(final MetricsSystem metricsSystem) {
    return Vertx.builder()
        .with(new VertxOptions().setPreferNativeTransport(true))
        .withMetrics(new VertxMetricsAdapterFactory(metricsSystem))
        .build();
  }

  private void addShutdownHook(final Runner runner) {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    besuPluginContext.stopPlugins();
                    runner.close();
                    LogConfigurator.shutdown();
                  } catch (final Exception e) {
                    logger.error("Failed to stop Besu");
                  }
                },
                "BesuCommand-Shutdown-Hook"));
  }

  private EthNetworkConfig updateNetworkConfig(final NetworkDefinition network) {
    final EthNetworkConfig.Builder builder =
        new EthNetworkConfig.Builder(EthNetworkConfig.getNetworkConfig(network));

    if (genesisFile != null) {
      if (commandLine.getParseResult().hasMatchedOption("network")) {
        throw new ParameterException(
            this.commandLine,
            "--network option and --genesis-file option can't be used at the same time.  Please "
                + "refer to CLI reference for more details about this constraint.");
      }

      if (networkId == null) {
        // If no chain id is found in the genesis, use mainnet network id
        try {
          builder.setNetworkId(
              readGenesisConfigOptions()
                  .getChainId()
                  .orElse(EthNetworkConfig.getNetworkConfig(MAINNET).networkId()));
        } catch (final DecodeException e) {
          throw new ParameterException(
              this.commandLine, String.format("Unable to parse genesis file %s.", genesisFile), e);
        } catch (final ArithmeticException e) {
          throw new ParameterException(
              this.commandLine,
              "No networkId specified and chainId in "
                  + "genesis file is too large to be used as a networkId");
        }
      }

      if (p2PDiscoveryOptions.bootNodes == null) {
        builder.setEnodeBootNodes(new ArrayList<>());
        builder.setEnrBootNodes(new ArrayList<>());
      }
      builder.setDnsDiscoveryUrl(null);
    }

    builder.setGenesisConfig(genesisConfigSupplier.get());

    if (networkId != null) {
      builder.setNetworkId(networkId);
    }
    // ChainId update is required for Ephemery network
    if (network.equals(EPHEMERY)) {
      String chainId = genesisConfigOverrides.get("chainId");
      builder.setNetworkId(new BigInteger(chainId));
    }
    if (p2PDiscoveryOptions.discoveryDnsUrl != null) {
      builder.setDnsDiscoveryUrl(p2PDiscoveryOptions.discoveryDnsUrl);
    } else {
      final Optional<String> discoveryDnsUrlFromGenesis =
          genesisConfigOptionsSupplier.get().getDiscoveryOptions().getDiscoveryDnsUrl();
      discoveryDnsUrlFromGenesis.ifPresent(builder::setDnsDiscoveryUrl);
    }

    // Resolve bootnodes: CLI --bootnodes overrides genesis defaults.
    // The discovery protocol version determines the expected format:
    //   V5 → ENR strings ("enr:..."),  V4 → enode URLs ("enode://...")
    final boolean isV5 =
        unstableNetworkingOptions.toDomainObject().discoveryConfiguration().isDiscoveryV5Enabled();
    List<String> rawBootnodes = null;
    final boolean cliBootnodesProvided = p2PDiscoveryOptions.bootNodes != null;
    if (cliBootnodesProvided) {
      try {
        rawBootnodes = BootnodeResolver.resolve(p2PDiscoveryOptions.bootNodes);
      } catch (final BootnodeResolutionException | IllegalArgumentException e) {
        throw new ParameterException(commandLine, e.getMessage(), e);
      }
    } else {
      final DiscoveryOptions discoveryOptions =
          genesisConfigOptionsSupplier.get().getDiscoveryOptions();
      rawBootnodes =
          isV5
              ? discoveryOptions.getV5BootNodes().orElse(null)
              : discoveryOptions.getBootNodes().orElse(null);
    }

    if (rawBootnodes != null && !rawBootnodes.isEmpty()) {
      if (!p2PDiscoveryOptions.peerDiscoveryEnabled) {
        logger.warn("Discovery disabled: bootnodes will be ignored.");
      }
      try {
        if (isV5) {
          builder.setEnrBootNodes(
              rawBootnodes.stream()
                  .map(
                      enr -> {
                        try {
                          return EthereumNodeRecord.fromEnr(enr);
                        } catch (final Exception e) {
                          throw new ParameterException(
                              commandLine,
                              "Invalid ENR bootnode: '"
                                  + enr
                                  + "'. ENR bootnodes must start with 'enr:'. Error: "
                                  + e.getMessage(),
                              e);
                        }
                      })
                  .toList());
        } else {
          final List<EnodeURLImpl> enodes = buildEnodes(rawBootnodes, getEnodeDnsConfiguration());
          DiscoveryConfiguration.assertValidBootnodes(enodes);
          builder.setEnodeBootNodes(enodes);
        }
        // CLI --bootnodes is a full override: clear the unused protocol's list
        if (cliBootnodesProvided) {
          if (isV5) {
            builder.setEnodeBootNodes(Collections.emptyList());
          } else {
            builder.setEnrBootNodes(Collections.emptyList());
          }
        }
      } catch (final ParameterException e) {
        throw e; // re-throw ParameterException from ENR parsing as-is
      } catch (final IllegalArgumentException e) {
        throw new ParameterException(commandLine, e.getMessage());
      } catch (final RuntimeException e) {
        throw new ParameterException(commandLine, "Invalid bootnode format: " + e.getMessage(), e);
      }
    } else if (cliBootnodesProvided) {
      // Explicitly empty --bootnodes clears all default bootnodes
      builder.setEnodeBootNodes(Collections.emptyList());
      builder.setEnrBootNodes(Collections.emptyList());
    }
    return builder.build();
  }

  /**
   * Returns data directory used by Besu. Visible as it is accessed by other subcommands.
   *
   * @return Path representing data directory.
   */
  public Path dataDir() {
    return dataPath.toAbsolutePath();
  }

  private SecurityModule securityModule() {
    return securityModuleService
        .getByName(securityModuleName)
        .orElseThrow(() -> new RuntimeException("Security Module not found: " + securityModuleName))
        .get();
  }

  private File resolveNodePrivateKeyFile(final File nodePrivateKeyFile) {
    return Optional.ofNullable(nodePrivateKeyFile)
        .orElseGet(() -> KeyPairUtil.getDefaultKeyFile(dataDir()));
  }

  /**
   * Metrics System used by Besu
   *
   * @return Instance of MetricsSystem
   */
  public MetricsSystem getMetricsSystem() {
    return besuComponent.getMetricsSystem();
  }

  private Set<EnodeURLImpl> loadStaticNodes() throws IOException {
    final Path staticNodesPath;
    if (staticNodesFile != null) {
      staticNodesPath = staticNodesFile.toAbsolutePath();
      if (!staticNodesPath.toFile().exists()) {
        throw new ParameterException(
            commandLine, String.format("Static nodes file %s does not exist", staticNodesPath));
      }
    } else {
      final String staticNodesFilename = "static-nodes.json";
      staticNodesPath = dataDir().resolve(staticNodesFilename);
    }
    logger.debug("Static Nodes file: {}", staticNodesPath);
    final Set<EnodeURLImpl> staticNodes =
        StaticNodesParser.fromPath(staticNodesPath, getEnodeDnsConfiguration());
    logger.info("Connecting to {} static nodes.", staticNodes.size());
    logger.debug("Static Nodes = {}", staticNodes);
    return staticNodes;
  }

  private List<EnodeURLImpl> buildEnodes(
      final List<String> bootNodes, final EnodeDnsConfiguration enodeDnsConfiguration) {
    return bootNodes.stream()
        .filter(bootNode -> !bootNode.isEmpty())
        .map(bootNode -> EnodeURLImpl.fromString(bootNode, enodeDnsConfiguration))
        .collect(Collectors.toList());
  }

  /**
   * Besu CLI Parameters exception handler used by VertX. Visible for testing.
   *
   * @return instance of BesuParameterExceptionHandler
   */
  public BesuParameterExceptionHandler parameterExceptionHandler() {
    return new BesuParameterExceptionHandler(this::getLogLevel);
  }

  /**
   * Returns BesuExecutionExceptionHandler. Visible as it is used in testing.
   *
   * @return instance of BesuExecutionExceptionHandler used by Vertx.
   */
  public BesuExecutionExceptionHandler executionExceptionHandler() {
    return new BesuExecutionExceptionHandler();
  }

  /**
   * Represents Enode DNS Configuration. Visible for testing.
   *
   * @return instance of EnodeDnsConfiguration
   */
  @VisibleForTesting
  public EnodeDnsConfiguration getEnodeDnsConfiguration() {
    if (enodeDnsConfiguration == null) {
      enodeDnsConfiguration = unstableDnsOptions.toDomainObject();
    }
    return enodeDnsConfiguration;
  }

  private void checkPortClash() {
    getEffectivePorts().stream()
        .filter(Objects::nonNull)
        .filter(port -> port > 0)
        .forEach(
            port -> {
              if (!allocatedPorts.add(port)) {
                throw new ParameterException(
                    commandLine,
                    "Port number '"
                        + port
                        + "' has been specified multiple times. Please review the supplied configuration.");
              }
            });
  }

  /**
   * Check if required ports are available
   *
   * @throws InvalidConfigurationException if ports are not available.
   */
  protected void checkIfRequiredPortsAreAvailable() {
    final List<Integer> unavailablePorts = new ArrayList<>();
    getEffectivePorts().stream()
        .filter(Objects::nonNull)
        .filter(port -> port > 0)
        .forEach(
            port -> {
              if (port.equals(p2PDiscoveryConfig.p2pPort())
                  && (NetworkUtility.isPortUnavailableForTcp(port)
                      || NetworkUtility.isPortUnavailableForUdp(port))) {
                unavailablePorts.add(port);
              }
              if (!port.equals(p2PDiscoveryConfig.p2pPort())
                  && NetworkUtility.isPortUnavailableForTcp(port)) {
                unavailablePorts.add(port);
              }
            });
    if (!unavailablePorts.isEmpty()) {
      throw new InvalidConfigurationException(
          "Port(s) '"
              + unavailablePorts
              + "' already in use. Check for other processes using the port(s).");
    }
  }

  /**
   * * Gets the list of effective ports (ports that are enabled).
   *
   * @return The list of effective ports
   */
  private List<Integer> getEffectivePorts() {
    final List<Integer> effectivePorts = new ArrayList<>();
    addPortIfEnabled(effectivePorts, p2PDiscoveryOptions.p2pPort, p2PDiscoveryOptions.p2pEnabled);
    addPortIfEnabled(
        effectivePorts, graphQlOptions.getGraphQLHttpPort(), graphQlOptions.isGraphQLHttpEnabled());
    addPortIfEnabled(
        effectivePorts, jsonRpcHttpOptions.getRpcHttpPort(), jsonRpcHttpOptions.isRpcHttpEnabled());
    addPortIfEnabled(
        effectivePorts, rpcWebsocketOptions.getRpcWsPort(), rpcWebsocketOptions.isRpcWsEnabled());
    addPortIfEnabled(effectivePorts, engineRPCConfig.engineRpcPort(), isEngineApiEnabled());
    addPortIfEnabled(
        effectivePorts, metricsOptions.getMetricsPort(), metricsOptions.getMetricsEnabled());
    return effectivePorts;
  }

  /**
   * Adds port to the specified list only if enabled.
   *
   * @param ports The list of ports
   * @param port The port value
   * @param enabled true if enabled, false otherwise
   */
  private void addPortIfEnabled(
      final List<Integer> ports, final Integer port, final boolean enabled) {
    if (enabled) {
      ports.add(port);
    }
  }

  @VisibleForTesting
  String getLogLevel() {
    return loggingLevelOption.getLogLevel();
  }

  /**
   * Returns the flag indicating that version compatibility checks will be made.
   *
   * @return true if compatibility checks should be made, otherwise false
   */
  @VisibleForTesting
  public Boolean getVersionCompatibilityProtection() {
    return versionCompatibilityProtection;
  }

  private void instantiateSignatureAlgorithmFactory() {
    getEcCurveFromGenesisFile()
        .ifPresent(
            ecCurve -> {
              try {
                SignatureAlgorithmFactory.switchInstance(ecCurve);
              } catch (final IllegalArgumentException e) {
                throw new CommandLine.InitializationException(
                    "Invalid genesis file configuration for ecCurve. " + e.getMessage());
              }
            });
  }

  private Optional<String> getEcCurveFromGenesisFile() {
    if (genesisFile == null) {
      return Optional.empty();
    }
    return genesisConfigOptionsSupplier.get().getEcCurve();
  }

  /**
   * Return the genesis config options
   *
   * @return the genesis config options
   */
  protected GenesisConfigOptions getGenesisConfigOptions() {
    return genesisConfigOptionsSupplier.get();
  }

  private void setMergeConfigOptions() {
    MergeConfiguration.setMergeEnabled(
        genesisConfigOptionsSupplier.get().getTerminalTotalDifficulty().isPresent());
  }

  /** Set ignorable segments in RocksDB Storage Provider plugin. */
  public void setIgnorableStorageSegments() {
    if (unstableChainPruningOptions.getChainPruningStrategy().equals(ChainPruningStrategy.NONE)
        && !dataStorageConfiguration.getHistoryExpiryPruneEnabled()) {
      rocksDBPlugin.addIgnorableSegmentIdentifier(KeyValueSegmentIdentifier.CHAIN_PRUNER_STATE);
    }
  }

  private void validatePostMergeCheckpointBlockRequirements() {
    final GenesisConfigOptions genesisConfigOptions = readGenesisConfigOptions();
    final CheckpointConfigOptions checkpointConfigOptions =
        genesisConfigOptions.getCheckpointOptions();

    // Only validate if checkpoint config is not the default (empty) one
    if (checkpointConfigOptions != CheckpointConfigOptions.DEFAULT) {
      if (!checkpointConfigOptions.isValid()) {
        throw new InvalidConfigurationException(
            "The checkpoint block configured in the genesis file is not valid.");
      }
    }
  }

  private boolean isMergeEnabled() {
    return MergeConfiguration.isMergeEnabled();
  }

  private boolean isEngineApiEnabled() {
    return engineRPCConfig.overrideEngineRpcEnabled() || isMergeEnabled();
  }

  private SyncMode getDefaultSyncModeIfNotSet() {
    return Optional.ofNullable(syncMode)
        .orElse(
            genesisFile == null
                    && Optional.ofNullable(network)
                        .map(NetworkDefinition::canSnapSync)
                        .orElse(false)
                ? SyncMode.SNAP
                : SyncMode.FULL);
  }

  private Boolean getDefaultVersionCompatibilityProtectionIfNotSet() {
    // Version compatibility protection is enabled by default for non-named networks
    return Optional.ofNullable(versionCompatibilityProtection)
        // if we have a specific genesis file or custom network id, we are not using a named network
        .orElse(genesisFile != null || networkId != null);
  }

  private String generateConfigurationOverview() {
    final ConfigurationOverviewBuilder builder = new ConfigurationOverviewBuilder(logger);

    if (environment != null) {
      builder.setEnvironment(environment);
    }

    if (network != null) {
      builder.setNetwork(network.normalize());
    }

    if (profile != null) {
      builder.setProfile(profile);
    }

    builder.setHasCustomGenesis(genesisFile != null);
    if (genesisFile != null) {
      builder.setCustomGenesis(genesisFile.getAbsolutePath());
    }
    builder.setNetworkId(ethNetworkConfig.networkId());

    builder
        .setDataStorage(dataStorageOptions.normalizeDataStorageFormat())
        .setSyncMode(syncMode.normalize())
        .setSyncMinPeers(syncMinPeerCount);

    builder.setParallelTxProcessingEnabled(
        getDataStorageConfiguration()
            .getPathBasedExtraStorageConfiguration()
            .getParallelTxProcessingEnabled());

    if (jsonRpcConfiguration != null && jsonRpcConfiguration.isEnabled()) {
      builder
          .setRpcPort(jsonRpcConfiguration.getPort())
          .setRpcHttpApis(jsonRpcConfiguration.getRpcApis());
    }

    if (engineJsonRpcConfiguration != null && engineJsonRpcConfiguration.isEnabled()) {
      builder
          .setEnginePort(engineJsonRpcConfiguration.getPort())
          .setEngineApis(engineJsonRpcConfiguration.getRpcApis());
      if (engineJsonRpcConfiguration.isAuthenticationEnabled()) {
        if (engineJsonRpcConfiguration.getAuthenticationPublicKeyFile() != null) {
          builder.setEngineJwtFile(
              engineJsonRpcConfiguration.getAuthenticationPublicKeyFile().getAbsolutePath());
        } else {
          // default ephemeral jwt created later
          builder.setEngineJwtFile(dataDir().toAbsolutePath() + "/" + EPHEMERAL_JWT_FILE);
        }
      }
    }

    if (rocksDBPlugin.isHighSpecEnabled()) {
      builder.setHighSpecEnabled();
    }

    if (DataStorageFormat.BONSAI.equals(getDataStorageConfiguration().getDataStorageFormat())) {
      final PathBasedExtraStorageConfiguration subStorageConfiguration =
          getDataStorageConfiguration().getPathBasedExtraStorageConfiguration();
      if (subStorageConfiguration.getLimitTrieLogsEnabled()) {
        builder
            .setLimitTrieLogsEnabled()
            .setTrieLogRetentionLimit(subStorageConfiguration.getMaxLayersToLoad())
            .setTrieLogsPruningWindowSize(subStorageConfiguration.getTrieLogPruningWindowSize());
      }
    }

    // Add chain pruning configuration
    final ChainPruningStrategy pruningStrategy =
        unstableChainPruningOptions.getChainPruningStrategy();
    if (!pruningStrategy.equals(ChainPruningStrategy.NONE)) {
      builder.setChainPruningEnabled(
          pruningStrategy,
          unstableChainPruningOptions.getChainDataPruningBlocksRetained(),
          unstableChainPruningOptions.getChainDataPruningBalsRetained());
    }

    if (miningParametersSupplier.get().getTargetGasLimit().isPresent()) {
      builder.setTargetGasLimit(miningParametersSupplier.get().getTargetGasLimit().getAsLong());
    } else {
      MergeCoordinator.getDefaultGasLimitByChainId(genesisConfigOptionsSupplier.get().getChainId())
          .ifPresent(builder::setTargetGasLimit);
    }

    miningParametersSupplier
        .get()
        .getMaxBlobsPerTransaction()
        .ifPresent(v -> builder.setMaxBlobsPerTransaction(v));

    miningParametersSupplier
        .get()
        .getMaxBlobsPerBlock()
        .ifPresent(v -> builder.setMaxBlobsPerBlock(v));

    builder
        .setDiscoveryEnabled(p2PDiscoveryOptions.peerDiscoveryEnabled)
        .setSnapServerEnabled(this.unstableSynchronizerOptions.isSnapsyncServerEnabled())
        .setTxPoolImplementation(buildTransactionPoolConfiguration().getTxPoolImplementation())
        .setWorldStateUpdateMode(unstableEvmOptions.toDomainObject().worldUpdaterMode())
        .setEnabledOpcodeOptimizations(unstableEvmOptions.toDomainObject().enableOptimizedOpcodes())
        .setEvmV2(unstableEvmOptions.toDomainObject().enableEvmV2())
        .setPluginContext(this.besuPluginContext)
        .setHistoryExpiryPruneEnabled(getDataStorageConfiguration().getHistoryExpiryPruneEnabled())
        .setBlobDBSettings(rocksDBPlugin.getBlobDBSettings());

    return builder.build();
  }

  /**
   * 2 Returns the plugin context.
   *
   * @return the plugin context.
   */
  public BesuPluginContextImpl getBesuPluginContext() {
    return besuPluginContext;
  }

  /**
   * Returns the metrics options
   *
   * @return the metrics options
   */
  public MetricsOptions getMetricsOptions() {
    return metricsOptions;
  }

  /**
   * returns current runner for Ephemery restart.
   *
   * @return current runner
   */
  public Runner getRunner() {
    return this.runner;
  }

  /**
   * returns rocksDBPlugin for Ephemery restart.
   *
   * @return rocksDBPlugin
   */
  public RocksDBPlugin getRocksDBPlugin() {
    return this.rocksDBPlugin;
  }

  /**
   * returns keyPair for Ephemery restart.
   *
   * @return KeyPair
   */
  public KeyPair getKeyPair() {
    return keyPair;
  }

  /**
   * returns commandLine for Ephemery restart.
   *
   * @return KeyPair
   */
  public CommandLine getCommandLine() {
    return this.commandLine;
  }

  /** Clears all allocated ports for Ephemery restart. */
  public void clearAllocatedPorts() {
    allocatedPorts.clear();
  }

  /** Sets data path to parent for Ephemery restart. */
  public void setDataPathToParent() {
    this.dataPath = dataPath.getParent();
  }

  @VisibleForTesting
  Supplier<GenesisConfig> getGenesisConfigSupplier() {
    return genesisConfigSupplier;
  }
}
