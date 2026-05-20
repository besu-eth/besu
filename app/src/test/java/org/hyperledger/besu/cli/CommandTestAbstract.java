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
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.cli.util.CommandLineUtils.DEPENDENCY_WARNING_MSG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.Runner;
import org.hyperledger.besu.RunnerBuilder;
import org.hyperledger.besu.chainexport.Era1BlockExporter;
import org.hyperledger.besu.chainexport.RlpBlockExporter;
import org.hyperledger.besu.chainimport.Era1BlockImporter;
import org.hyperledger.besu.chainimport.JsonBlockImporter;
import org.hyperledger.besu.chainimport.RlpBlockImporter;
import org.hyperledger.besu.cli.config.EthNetworkConfig;
import org.hyperledger.besu.cli.options.EthProtocolOptions;
import org.hyperledger.besu.cli.options.EthstatsOptions;
import org.hyperledger.besu.cli.options.MiningOptions;
import org.hyperledger.besu.cli.options.NetworkingOptions;
import org.hyperledger.besu.cli.options.P2PDiscoveryOptions;
import org.hyperledger.besu.cli.options.SynchronizerOptions;
import org.hyperledger.besu.cli.options.TransactionPoolOptions;
import org.hyperledger.besu.cli.options.storage.DataStorageOptions;
import org.hyperledger.besu.components.BesuComponent;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.NetworkDefinition;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.controller.BesuControllerBuilder;
import org.hyperledger.besu.controller.NoopPluginServiceFactory;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.KeyPairUtil;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.sync.BlockBroadcaster;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.permissioning.PermissioningConfiguration;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.StorageService;
import org.hyperledger.besu.plugin.services.TransactionSelectionService;
import org.hyperledger.besu.plugin.services.securitymodule.SecurityModule;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageFactory;
import org.hyperledger.besu.services.BesuConfigurationImpl;
import org.hyperledger.besu.services.BesuPluginContextImpl;
import org.hyperledger.besu.services.BlockchainServiceImpl;
import org.hyperledger.besu.services.PermissioningServiceImpl;
import org.hyperledger.besu.services.RpcEndpointServiceImpl;
import org.hyperledger.besu.services.SecurityModuleServiceImpl;
import org.hyperledger.besu.services.StorageServiceImpl;
import org.hyperledger.besu.services.TransactionPoolValidatorServiceImpl;
import org.hyperledger.besu.services.TransactionSelectionServiceImpl;
import org.hyperledger.besu.services.TransactionSimulationServiceImpl;
import org.hyperledger.besu.services.TransactionValidatorServiceImpl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import inet.ipaddr.IPAddress;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.RunLast;

@ExtendWith(MockitoExtension.class)
public abstract class CommandTestAbstract {
  private static final Logger TEST_LOGGER = LoggerFactory.getLogger(CommandTestAbstract.class);

  protected static final int POA_BLOCK_PERIOD_SECONDS = 5;
  protected static final int POA_EMPTY_BLOCK_PERIOD_SECONDS = 50;
  protected static final JsonObject VALID_GENESIS_QBFT_POST_LONDON =
      (new JsonObject())
          .put(
              "config",
              new JsonObject()
                  .put("londonBlock", 0)
                  .put("qbft", new JsonObject().put("blockperiodseconds", POA_BLOCK_PERIOD_SECONDS))
                  .put(
                      "qbft",
                      new JsonObject()
                          .put("emptyblockperiodseconds", POA_EMPTY_BLOCK_PERIOD_SECONDS)));
  protected static final JsonObject VALID_GENESIS_IBFT2_POST_LONDON =
      (new JsonObject())
          .put(
              "config",
              new JsonObject()
                  .put("londonBlock", 0)
                  .put(
                      "ibft2",
                      new JsonObject().put("blockperiodseconds", POA_BLOCK_PERIOD_SECONDS)));

  protected static final JsonObject VALID_GENESIS_CLIQUE_WITH_POS_TRANSITION =
      (new JsonObject())
          .put(
              "config",
              new JsonObject()
                  .put("londonBlock", 0)
                  .put("terminaltotaldifficulty", 10)
                  .put(
                      "clique",
                      new JsonObject().put("blockperiodseconds", POA_BLOCK_PERIOD_SECONDS)));

  protected static final JsonObject GENESIS_WITH_ZERO_BASE_FEE_MARKET =
      new JsonObject().put("config", new JsonObject().put("zeroBaseFee", true));

  protected final PrintStream originalOut = System.out;
  protected final PrintStream originalErr = System.err;
  protected final ByteArrayOutputStream commandOutput = new ByteArrayOutputStream();
  protected final ByteArrayOutputStream commandErrorOutput = new ByteArrayOutputStream();
  private final HashMap<String, String> environment = new HashMap<>();

  private final List<TestBesuCommand> besuCommands = new ArrayList<>();

  private static final KeyPair keyPair;

  static {
    final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    final Bytes32 keyPairPrvKey =
        Bytes32.fromHexString("0xf7a58d5e755d51fa2f6206e91dd574597c73248aaf946ec1964b8c6268d6207b");
    keyPair = signatureAlgorithm.createKeyPair(signatureAlgorithm.createPrivateKey(keyPairPrvKey));
  }

  protected static final RpcEndpointServiceImpl rpcEndpointServiceImpl =
      new RpcEndpointServiceImpl();

  @Mock(lenient = true, answer = Answers.RETURNS_SELF)
  protected RunnerBuilder mockRunnerBuilder;

  @Mock protected Runner mockRunner;

  @Mock(lenient = true)
  protected BesuController.Builder mockControllerBuilderFactory;

  @Mock(lenient = true, answer = Answers.RETURNS_SELF)
  protected BesuControllerBuilder mockControllerBuilder;

  @Mock(lenient = true)
  protected EthProtocolManager mockEthProtocolManager;

  @Mock protected ProtocolSchedule mockProtocolSchedule;

  @Mock(lenient = true)
  protected ProtocolContext mockProtocolContext;

  @Mock protected BlockBroadcaster mockBlockBroadcaster;

  @Mock(lenient = true)
  protected BesuController mockController;

  @Mock(lenient = true)
  protected BesuComponent mockBesuComponent;

  @Mock protected RlpBlockExporter rlpBlockExporter;
  @Mock protected Era1BlockExporter era1BlockExporter;
  @Mock protected JsonBlockImporter jsonBlockImporter;
  @Mock protected RlpBlockImporter rlpBlockImporter;
  @Mock protected Era1BlockImporter era1BlockImporter;
  @Mock protected StorageServiceImpl storageService;
  @Mock protected TransactionSelectionServiceImpl txSelectionService;
  @Mock protected TransactionValidatorServiceImpl txValidatorService;
  @Mock protected SecurityModuleServiceImpl securityModuleService;
  @Mock protected SecurityModule securityModule;
  @Spy protected BesuConfigurationImpl commonPluginConfiguration = new BesuConfigurationImpl();
  @Mock protected KeyValueStorageFactory rocksDBStorageFactory;
  @Mock protected PicoCLIOptions cliOptions;
  @Mock protected NodeKey nodeKey;
  @Mock private BesuPluginContextImpl mockBesuPluginContext;
  @Mock protected MutableBlockchain mockMutableBlockchain;
  @Mock protected WorldStateArchive mockWorldStateArchive;
  @Mock protected TransactionPool mockTransactionPool;
  @Mock protected StorageProvider storageProvider;

  @SuppressWarnings("PrivateStaticFinalLoggers") // @Mocks are inited by JUnit
  @Mock
  protected Logger mockLogger;

  @Captor protected ArgumentCaptor<Collection<Bytes>> bytesCollectionCollector;
  @Captor protected ArgumentCaptor<Path> pathArgumentCaptor;
  @Captor protected ArgumentCaptor<String> stringArgumentCaptor;
  @Captor protected ArgumentCaptor<Integer> intArgumentCaptor;
  @Captor protected ArgumentCaptor<Long> longArgumentCaptor;
  @Captor protected ArgumentCaptor<Boolean> booleanArgumentCaptor;
  @Captor protected ArgumentCaptor<EthNetworkConfig> ethNetworkConfigArgumentCaptor;
  @Captor protected ArgumentCaptor<SynchronizerConfiguration> syncConfigurationCaptor;
  @Captor protected ArgumentCaptor<JsonRpcConfiguration> jsonRpcConfigArgumentCaptor;
  @Captor protected ArgumentCaptor<GraphQLConfiguration> graphQLConfigArgumentCaptor;
  @Captor protected ArgumentCaptor<WebSocketConfiguration> wsRpcConfigArgumentCaptor;
  @Captor protected ArgumentCaptor<MetricsConfiguration> metricsConfigArgumentCaptor;
  @Captor protected ArgumentCaptor<StorageProvider> storageProviderArgumentCaptor;
  @Captor protected ArgumentCaptor<EthProtocolConfiguration> ethProtocolConfigurationArgumentCaptor;
  @Captor protected ArgumentCaptor<DataStorageConfiguration> dataStorageConfigurationArgumentCaptor;

  @Captor
  protected ArgumentCaptor<Optional<PermissioningConfiguration>>
      permissioningConfigurationArgumentCaptor;

  @Captor protected ArgumentCaptor<TransactionPoolConfiguration> transactionPoolConfigCaptor;
  @Captor protected ArgumentCaptor<ApiConfiguration> apiConfigurationCaptor;

  @Captor protected ArgumentCaptor<EthstatsOptions> ethstatsOptionsArgumentCaptor;
  @Captor protected ArgumentCaptor<List<IPAddress>> allowedSubnetsArgumentCaptor;

  @BeforeEach
  public void initMocks() throws Exception {
    when(mockControllerBuilderFactory.fromEthNetworkConfig(any(), any()))
        .thenReturn(mockControllerBuilder);
    when(mockControllerBuilder.build()).thenReturn(mockController);
    lenient().when(mockController.getProtocolManager()).thenReturn(mockEthProtocolManager);
    lenient().when(mockController.getProtocolSchedule()).thenReturn(mockProtocolSchedule);
    lenient().when(mockController.getProtocolContext()).thenReturn(mockProtocolContext);
    lenient()
        .when(mockController.getAdditionalPluginServices())
        .thenReturn(new NoopPluginServiceFactory());
    lenient().when(mockController.getNodeKey()).thenReturn(nodeKey);

    when(mockEthProtocolManager.getBlockBroadcaster()).thenReturn(mockBlockBroadcaster);

    when(mockProtocolContext.getBlockchain()).thenReturn(mockMutableBlockchain);
    lenient().when(mockProtocolContext.getWorldStateArchive()).thenReturn(mockWorldStateArchive);
    when(mockController.getTransactionPool()).thenReturn(mockTransactionPool);
    when(mockController.getStorageProvider()).thenReturn(storageProvider);

    when(mockRunnerBuilder.build()).thenReturn(mockRunner);
    when(mockBesuComponent.getMetricsSystem()).thenReturn(new NoOpMetricsSystem());

    lenient().when(nodeKey.getPublicKey()).thenReturn(keyPair.getPublicKey());

    lenient()
        .when(storageService.getByName(eq("rocksdb")))
        .thenReturn(Optional.of(rocksDBStorageFactory));
    lenient()
        .when(securityModuleService.getByName(eq("localfile")))
        .thenReturn(Optional.of(() -> securityModule));

    lenient()
        .when(getBesuPluginContext().getService(PicoCLIOptions.class))
        .thenReturn(Optional.of(cliOptions));
    lenient()
        .when(getBesuPluginContext().getService(StorageService.class))
        .thenReturn(Optional.of(storageService));
    lenient()
        .when(getBesuPluginContext().getService(TransactionSelectionService.class))
        .thenReturn(Optional.of(txSelectionService));
  }

  @BeforeEach
  public void setUpStreams() {
    // reset the global opentelemetry singleton
    GlobalOpenTelemetry.resetForTest();
    commandOutput.reset();
    commandErrorOutput.reset();
    System.setOut(new PrintStream(commandOutput));
    System.setErr(new PrintStream(commandErrorOutput));
  }

  // Display outputs for debug purpose
  @AfterEach
  public void displayOutput() throws IOException {
    TEST_LOGGER.info("Standard output {}", commandOutput.toString(UTF_8));
    TEST_LOGGER.info("Standard error {}", commandErrorOutput.toString(UTF_8));

    System.setOut(originalOut);
    System.setErr(originalErr);
    besuCommands.forEach(TestBesuCommand::close);
  }

  protected NodeKey getNodeKey() {
    return nodeKey;
  }

  protected void setEnvironmentVariable(final String name, final String value) {
    environment.put(name, value);
  }

  protected TestBesuCommand parseCommand(final String... args) {
    return parseCommand(System.in, args);
  }

  protected TestBesuCommand parseCommand(final InputStream in, final String... args) {
    return parseCommand(TestType.NO_PORT_CHECK, in, args);
  }

  protected TestBesuCommand parseCommandWithRequiredOption(final String... args) {
    return parseCommand(TestType.REQUIRED_OPTION, System.in, args);
  }

  protected TestBesuCommand parseCommandWithPortCheck(final String... args) {
    return parseCommand(TestType.PORT_CHECK, System.in, args);
  }

  private JsonBlockImporter jsonBlockImporterFactory(final BesuController controller) {
    return jsonBlockImporter;
  }

  protected TestBesuCommand parseCommand(
      final TestType testType, final InputStream in, final String... args) {
    // turn off ansi usage globally in picocli
    System.setProperty("picocli.ansi", "false");

    final TestBesuCommand besuCommand = getTestBesuCommand(testType);
    besuCommands.add(besuCommand);

    besuCommand.setBesuConfiguration(commonPluginConfiguration);

    final List<String> argsList = constructArgsWithTmpDataPathIfNotSpecified(args);

    // Determine the data directory that will be used and write the key file there
    final Path dataDir = determineDataDir(argsList);
    final File defaultKeyFile = KeyPairUtil.getDefaultKeyFile(dataDir);
    try {
      // Ensure parent directory exists
      defaultKeyFile.getParentFile().mkdirs();
      Files.writeString(defaultKeyFile.toPath(), keyPair.getPrivateKey().toString());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    // parse using Ansi.OFF to be able to assert on non-formatted output results
    besuCommand.parse(
        new RunLast(),
        besuCommand.parameterExceptionHandler(),
        besuCommand.executionExceptionHandler(),
        in,
        mockBesuComponent,
        argsList.toArray(new String[0]));
    return besuCommand;
  }

  @NotNull
  private static Path determineDataDir(final List<String> argsList) {
    // Look for --data-path in the arguments
    for (int i = 0; i < argsList.size() - 1; i++) {
      if ("--data-path".equals(argsList.get(i))) {
        return Paths.get(argsList.get(i + 1));
      }
    }
    // If no explicit data-path, use default
    return DefaultCommandValues.getDefaultBesuDataPath(null);
  }

  @NotNull
  private static List<String> constructArgsWithTmpDataPathIfNotSpecified(final String[] args) {
    final List<String> argsList = new ArrayList<>(Arrays.asList(args));

    boolean hasDataPath = argsList.stream().anyMatch(arg -> arg.contains("data-path"));
    boolean hasConfigFile = argsList.stream().anyMatch(arg -> arg.contains("config-file"));

    // if data-path is not set, set to a tmp dir
    if (!hasDataPath && !hasConfigFile) {
      try {
        final Path tmpDir = Files.createTempDirectory("besu-test-");
        tmpDir.toFile().deleteOnExit();
        argsList.add(0, "--data-path");
        argsList.add(1, tmpDir.toString());
      } catch (IOException e) {
        throw new RuntimeException("Failed to create temporary directory", e);
      }
    }
    return argsList;
  }

  private TestBesuCommand getTestBesuCommand(final TestType testType) {
    switch (testType) {
      case REQUIRED_OPTION:
        return new TestBesuCommandWithRequiredOption(
            () -> rlpBlockImporter,
            this::jsonBlockImporterFactory,
            () -> era1BlockImporter,
            (blockchain) -> rlpBlockExporter,
            (blockchain, networkName) -> era1BlockExporter,
            mockRunnerBuilder,
            mockControllerBuilderFactory,
            getBesuPluginContext(),
            environment,
            storageService,
            securityModuleService,
            mockLogger);
      case PORT_CHECK:
        return new TestBesuCommand(
            () -> rlpBlockImporter,
            this::jsonBlockImporterFactory,
            () -> era1BlockImporter,
            (blockchain) -> rlpBlockExporter,
            (blockchain, networkName) -> era1BlockExporter,
            mockRunnerBuilder,
            mockControllerBuilderFactory,
            getBesuPluginContext(),
            environment,
            storageService,
            securityModuleService,
            mockLogger);
      default:
        return new TestBesuCommandWithoutPortCheck(
            () -> rlpBlockImporter,
            this::jsonBlockImporterFactory,
            () -> era1BlockImporter,
            (blockchain) -> rlpBlockExporter,
            (blockchain, networkName) -> era1BlockExporter,
            mockRunnerBuilder,
            mockControllerBuilderFactory,
            getBesuPluginContext(),
            environment,
            storageService,
            securityModuleService,
            mockLogger);
    }
  }

  protected Path createTempFile(final String filename, final byte[] contents) throws IOException {
    final Path file = Files.createTempFile(filename, "");
    Files.write(file, contents);
    file.toFile().deleteOnExit();
    return file;
  }

  protected Path createFakeGenesisFile(final JsonObject jsonGenesis) throws IOException {
    return createTempFile("genesisFile", encodeJsonGenesis(jsonGenesis).getBytes(UTF_8));
  }

  protected String encodeJsonGenesis(final JsonObject jsonGenesis) {
    return jsonGenesis.encodePrettily();
  }

  protected Path createTempFile(final String filename, final String contents) throws IOException {
    return createTempFile(filename, contents.getBytes(UTF_8));
  }

  protected BesuPluginContextImpl getBesuPluginContext() {
    return mockBesuPluginContext;
  }

  @CommandLine.Command
  public static class TestBesuCommand extends BesuCommand {

    @CommandLine.Spec CommandLine.Model.CommandSpec spec;
    private Vertx vertx;

    TestBesuCommand(
        final Supplier<RlpBlockImporter> mockBlockImporter,
        final Function<BesuController, JsonBlockImporter> jsonBlockImporterFactory,
        final Supplier<Era1BlockImporter> era1BlockImporter,
        final Function<Blockchain, RlpBlockExporter> rlpBlockExporterFactory,
        final BiFunction<Blockchain, NetworkDefinition, Era1BlockExporter> era1BlockExporterFactory,
        final RunnerBuilder mockRunnerBuilder,
        final BesuController.Builder controllerBuilderFactory,
        final BesuPluginContextImpl besuPluginContext,
        final Map<String, String> environment,
        final StorageServiceImpl storageService,
        final SecurityModuleServiceImpl securityModuleService,
        final Logger commandLogger) {
      super(
          mockBlockImporter,
          jsonBlockImporterFactory,
          era1BlockImporter,
          rlpBlockExporterFactory,
          era1BlockExporterFactory,
          mockRunnerBuilder,
          controllerBuilderFactory,
          besuPluginContext,
          environment,
          storageService,
          securityModuleService,
          new PermissioningServiceImpl(),
          rpcEndpointServiceImpl,
          new TransactionSelectionServiceImpl(),
          new TransactionPoolValidatorServiceImpl(),
          new TransactionSimulationServiceImpl(),
          new BlockchainServiceImpl(),
          new TransactionValidatorServiceImpl(),
          commandLogger);
    }

    @Override
    protected P2PDiscoveryOptions.NetworkInterfaceChecker getNetworkInterfaceChecker() {
      // For testing, don't actually query for networking interfaces to validate this option
      return (networkInterface) -> true;
    }

    @Override
    protected Vertx createVertx(final MetricsSystem metricsSystem) {
      vertx = super.createVertx(metricsSystem);
      return vertx;
    }

    public CommandSpec getSpec() {
      return spec;
    }

    public Supplier<GenesisConfigOptions> getGenesisConfigOptionsSupplier() {
      return genesisConfigOptionsSupplier;
    }

    public NetworkingOptions getNetworkingOptions() {
      return unstableNetworkingOptions;
    }

    public SynchronizerOptions getSynchronizerOptions() {
      return unstableSynchronizerOptions;
    }

    public EthProtocolOptions getEthProtocolOptions() {
      return unstableEthProtocolOptions;
    }

    public MiningOptions getMiningOptions() {
      return miningOptions;
    }

    public TransactionPoolOptions getTransactionPoolOptions() {
      return transactionPoolOptions;
    }

    public DataStorageOptions getDataStorageOptions() {
      return dataStorageOptions;
    }

    public void close() {
      if (vertx != null) {
        final AtomicBoolean closed = new AtomicBoolean(false);
        vertx.close(event -> closed.set(true));
        Awaitility.waitAtMost(30, TimeUnit.SECONDS).until(closed::get);
      }
    }
  }

  @CommandLine.Command
  public static class TestBesuCommandWithRequiredOption extends TestBesuCommand {

    @NotEmpty
    @CommandLine.Option(
        names = {"--accept-terms-and-conditions"},
        description = "You must explicitly accept terms and conditions",
        arity = "1",
        required = true)
    private final Boolean acceptTermsAndConditions = false;

    TestBesuCommandWithRequiredOption(
        final Supplier<RlpBlockImporter> mockBlockImporter,
        final Function<BesuController, JsonBlockImporter> jsonBlockImporterFactory,
        final Supplier<Era1BlockImporter> era1BlockImporter,
        final Function<Blockchain, RlpBlockExporter> rlpBlockExporterFactory,
        final BiFunction<Blockchain, NetworkDefinition, Era1BlockExporter> era1BlockExporterFactory,
        final RunnerBuilder mockRunnerBuilder,
        final BesuController.Builder controllerBuilderFactory,
        final BesuPluginContextImpl besuPluginContext,
        final Map<String, String> environment,
        final StorageServiceImpl storageService,
        final SecurityModuleServiceImpl securityModuleService,
        final Logger commandLogger) {
      super(
          mockBlockImporter,
          jsonBlockImporterFactory,
          era1BlockImporter,
          rlpBlockExporterFactory,
          era1BlockExporterFactory,
          mockRunnerBuilder,
          controllerBuilderFactory,
          besuPluginContext,
          environment,
          storageService,
          securityModuleService,
          commandLogger);
    }

    public Boolean getAcceptTermsAndConditions() {
      return acceptTermsAndConditions;
    }
  }

  @CommandLine.Command
  public static class TestBesuCommandWithoutPortCheck extends TestBesuCommand {

    TestBesuCommandWithoutPortCheck(
        final Supplier<RlpBlockImporter> mockBlockImporter,
        final Function<BesuController, JsonBlockImporter> jsonBlockImporterFactory,
        final Supplier<Era1BlockImporter> era1BlockImporter,
        final Function<Blockchain, RlpBlockExporter> rlpBlockExporterFactory,
        final BiFunction<Blockchain, NetworkDefinition, Era1BlockExporter> era1BlockExporterFactory,
        final RunnerBuilder mockRunnerBuilder,
        final BesuController.Builder controllerBuilderFactory,
        final BesuPluginContextImpl besuPluginContext,
        final Map<String, String> environment,
        final StorageServiceImpl storageService,
        final SecurityModuleServiceImpl securityModuleService,
        final Logger commandLogger) {
      super(
          mockBlockImporter,
          jsonBlockImporterFactory,
          era1BlockImporter,
          rlpBlockExporterFactory,
          era1BlockExporterFactory,
          mockRunnerBuilder,
          controllerBuilderFactory,
          besuPluginContext,
          environment,
          storageService,
          securityModuleService,
          commandLogger);
    }

    @Override
    protected void checkIfRequiredPortsAreAvailable() {
      // For testing, don't check for port conflicts
    }
  }

  private enum TestType {
    REQUIRED_OPTION,
    PORT_CHECK,
    NO_PORT_CHECK
  }

  protected static String escapeTomlString(final String s) {
    return StringEscapeUtils.escapeJava(s);
  }

  /**
   * Check logger calls
   *
   * <p>Here we check the calls to logger and not the result of the log line as we don't test the
   * logger itself but the fact that we call it.
   *
   * @param dependentOptions the string representing the list of dependent options names
   * @param mainOption the main option name
   */
  protected void verifyOptionsConstraintLoggerCall(
      final String mainOption, final String... dependentOptions) {
    verify(mockLogger, atLeast(1))
        .warn(
            stringArgumentCaptor.capture(),
            stringArgumentCaptor.capture(),
            stringArgumentCaptor.capture());
    assertThat(stringArgumentCaptor.getAllValues().get(0)).isEqualTo(DEPENDENCY_WARNING_MSG);

    for (final String option : dependentOptions) {
      assertThat(stringArgumentCaptor.getAllValues().get(1)).contains(option);
    }

    assertThat(stringArgumentCaptor.getAllValues().get(2)).isEqualTo(mainOption);
  }

  /**
   * Check logger calls
   *
   * <p>Here we check the calls to logger and not the result of the log line as we don't test the
   * logger itself but the fact that we call it.
   *
   * @param stringToLog the string that is logged
   */
  void verifyMultiOptionsConstraintLoggerCall(final String stringToLog) {
    verify(mockLogger, atLeast(1)).warn(stringToLog);
  }
}
