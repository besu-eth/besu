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
package org.hyperledger.besu;

import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.config.NetworkDefinition.DEV;
import static org.hyperledger.besu.config.NetworkDefinition.MAINNET;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_BACKGROUND_THREAD_COUNT;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_CACHE_CAPACITY;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_ENABLE_READ_CACHE_FOR_SNAPSHOTS;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_IS_HIGH_SPEC;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_MAX_OPEN_FILES;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.cli.config.EthNetworkConfig;
import org.hyperledger.besu.components.BesuComponent;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.config.MergeConfiguration;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.controller.MainnetBesuControllerBuilder;
import org.hyperledger.besu.crypto.KeyPairUtil;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.ImmutableInProcessRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.ipc.JsonRpcIpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.core.BlockSyncTestUtils;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.NodeIdentifier;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProviderBuilder;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBKeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;
import org.hyperledger.besu.services.BesuConfigurationImpl;
import org.hyperledger.besu.services.BesuPluginContextImpl;
import org.hyperledger.besu.services.PermissioningServiceImpl;
import org.hyperledger.besu.services.RpcEndpointServiceImpl;
import org.hyperledger.besu.services.TransactionValidatorServiceImpl;
import org.hyperledger.besu.testutil.TestClock;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.tuweni.units.bigints.UInt256;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

/** Tests for {@link Runner}. */
@ExtendWith(MockitoExtension.class)
public final class RunnerTest {

  public static final BigInteger NETWORK_ID = BigInteger.valueOf(2929);
  private Vertx vertx;

  @BeforeEach
  public void initVertx() {
    vertx = Vertx.vertx();
  }

  @AfterEach
  public void stopVertx() {
    vertx.close();
  }

  @TempDir private Path temp;

  @Test
  public void getFixedNodes() {
    final EnodeURLImpl staticNode =
        EnodeURLImpl.fromString(
            "enode://8f4b88336cc40ef2516d8b27df812e007fb2384a61e93635f1899051311344f3dcdbb49a4fe49a79f66d2f589a9f282e8cc4f1d7381e8ef7e4fcc6b0db578c77@127.0.0.1:30301");
    final EnodeURLImpl bootnode =
        EnodeURLImpl.fromString(
            "enode://8f4b88336cc40ef2516d8b27df812e007fb2384a61e93635f1899051311344f3dcdbb49a4fe49a79f66d2f589a9f282e8cc4f1d7381e8ef7e4fcc6b0db578c77@127.0.0.1:30302");
    final List<EnodeURLImpl> bootnodes = new ArrayList<>();
    bootnodes.add(bootnode);
    final Collection<EnodeURLImpl> staticNodes = new ArrayList<>();
    staticNodes.add(staticNode);
    final Collection<NodeIdentifier> fixedNodes =
        RunnerBuilder.getFixedNodes(bootnodes, staticNodes);
    assertThat(fixedNodes).containsExactlyInAnyOrder(staticNode, bootnode);
    // bootnodes should be unchanged
    assertThat(bootnodes).containsExactly(bootnode);
  }

  @Test
  public void fullSyncFromGenesis() throws Exception {
    // set merge flag to false, otherwise this test can fail if a merge test runs first
    MergeConfiguration.setMergeEnabled(false);

    syncFromGenesis(SyncMode.FULL, getFastSyncGenesis(), false);
  }

  @Test
  public void fullSyncFromGenesisUsingPeerTaskSystem() throws Exception {
    // set merge flag to false, otherwise this test can fail if a merge test runs first
    MergeConfiguration.setMergeEnabled(false);

    syncFromGenesis(SyncMode.FULL, getFastSyncGenesis(), true);
  }

  private void syncFromGenesis(
      final SyncMode mode, final GenesisConfig genesisConfig, final boolean isPeerTaskSystemEnabled)
      throws Exception {
    final Path dataDirAhead = Files.createTempDirectory(temp, "db-ahead");
    final Path dbAhead = dataDirAhead.resolve("database");
    final int blockCount = 500;
    final NodeKey aheadDbNodeKey = NodeKeyUtils.createFrom(KeyPairUtil.loadKeyPair(dataDirAhead));
    final NodeKey behindDbNodeKey = NodeKeyUtils.generate();
    final SynchronizerConfiguration syncConfigAhead =
        SynchronizerConfiguration.builder()
            .syncMode(SyncMode.FULL)
            .isPeerTaskSystemEnabled(isPeerTaskSystemEnabled)
            .build();
    final ObservableMetricsSystem noOpMetricsSystem = new NoOpMetricsSystem();
    final var miningParameters = MiningConfiguration.newDefault();
    final var dataStorageConfiguration = DataStorageConfiguration.DEFAULT_FOREST_CONFIG;
    // Setup Runner with blocks
    final BesuController controllerAhead =
        getController(
            genesisConfig,
            syncConfigAhead,
            dataDirAhead,
            aheadDbNodeKey,
            createKeyValueStorageProvider(
                dataDirAhead, dbAhead, dataStorageConfiguration, miningParameters),
            noOpMetricsSystem,
            miningParameters);
    setupState(
        blockCount, controllerAhead.getProtocolSchedule(), controllerAhead.getProtocolContext());

    final String listenHost = InetAddress.getLoopbackAddress().getHostAddress();
    final Path pidPath = dataDirAhead.resolve("pid");
    final RunnerBuilder runnerBuilder =
        new RunnerBuilder()
            .vertx(vertx)
            .discoveryEnabled(true)
            .p2pAdvertisedHost(listenHost)
            .p2pListenPort(0)
            .metricsSystem(noOpMetricsSystem)
            .permissioningService(new PermissioningServiceImpl())
            .staticNodes(emptySet())
            .storageProvider(new InMemoryKeyValueStorageProvider())
            .rpcEndpointService(new RpcEndpointServiceImpl())
            .apiConfiguration(ImmutableApiConfiguration.builder().build())
            .transactionValidatorService(new TransactionValidatorServiceImpl());

    Runner runnerBehind = null;
    final Runner runnerAhead =
        runnerBuilder
            .besuController(controllerAhead)
            .ethNetworkConfig(EthNetworkConfig.getNetworkConfig(DEV))
            .jsonRpcConfiguration(jsonRpcConfiguration())
            .graphQLConfiguration(graphQLConfiguration())
            .webSocketConfiguration(wsRpcConfiguration())
            .jsonRpcIpcConfiguration(new JsonRpcIpcConfiguration())
            .inProcessRpcConfiguration(ImmutableInProcessRpcConfiguration.builder().build())
            .metricsConfiguration(metricsConfiguration())
            .dataDir(dbAhead)
            .pidPath(pidPath)
            .besuPluginContext(new BesuPluginContextImpl())
            .rpcEndpointService(new RpcEndpointServiceImpl())
            .build();
    try {
      runnerAhead.startExternalServices();
      runnerAhead.startEthereumMainLoop();
      assertThat(pidPath.toFile()).exists();

      final SynchronizerConfiguration syncConfigBehind =
          SynchronizerConfiguration.builder()
              .syncMode(mode)
              .syncPivotDistance(5)
              .syncMinimumPeerCount(1)
              .build();
      final Path dataDirBehind = Files.createTempDirectory(temp, "db-behind");

      // Setup runner with no block data
      final BesuController controllerBehind =
          getController(
              genesisConfig,
              syncConfigBehind,
              dataDirBehind,
              behindDbNodeKey,
              new InMemoryKeyValueStorageProvider(),
              noOpMetricsSystem,
              miningParameters);

      final EnodeURLImpl aheadEnode = runnerAhead.getLocalEnode().get();
      final EthNetworkConfig behindEthNetworkConfiguration =
          new EthNetworkConfig(
              GenesisConfig.fromResource(DEV.getGenesisFile()),
              DEV.getNetworkId(),
              Collections.singletonList(aheadEnode),
              Collections.emptyList(),
              "");

      runnerBehind =
          runnerBuilder
              .besuController(controllerBehind)
              .ethNetworkConfig(behindEthNetworkConfiguration)
              .jsonRpcConfiguration(jsonRpcConfiguration())
              .graphQLConfiguration(graphQLConfiguration())
              .webSocketConfiguration(wsRpcConfiguration())
              .metricsConfiguration(metricsConfiguration())
              .dataDir(dataDirBehind)
              .metricsSystem(noOpMetricsSystem)
              .build();

      runnerBehind.startExternalServices();
      runnerBehind.startEthereumMainLoop();

      final int behindJsonRpcPort = runnerBehind.getJsonRpcPort().get();
      final OkHttpClient client = new OkHttpClient();
      Awaitility.await()
          .ignoreExceptions()
          .atMost(2L, TimeUnit.MINUTES)
          .untilAsserted(
              () -> {
                final String baseUrl = String.format("http://%s:%s", listenHost, behindJsonRpcPort);
                try (final Response blockNumberResp =
                    client.newCall(getRequest("eth_blockNumber", baseUrl)).execute()) {
                  assertThat(blockNumberResp.code()).isEqualTo(200);
                  final int currentBlock = getNumber(blockNumberResp);
                  blockNumberResp.close();

                  final Response syncingResp =
                      client.newCall(getRequest("eth_syncing", baseUrl)).execute();
                  assertThat(syncingResp.code()).isEqualTo(200);
                  final JsonObject syncingRespJson = new JsonObject(syncingResp.body().string());
                  syncingResp.close();

                  final Response peerCountResp =
                      client.newCall(getRequest("net_peerCount", baseUrl)).execute();
                  assertThat(peerCountResp.code()).isEqualTo(200);
                  final int peerCount = getNumber(peerCountResp);
                  peerCountResp.close();

                  // if the test fails here, it means the node is not peering. check your config.
                  // Expecting value to be true but was false
                  assertThat(peerCount > 0).isTrue();

                  // if not yet at blockCount, we should get a sync result from eth_syncing
                  if (currentBlock < blockCount) {
                    // if the test fails here, it means that the node is not syncing ->
                    //                    Expecting actual:
                    //                    false
                    //                    to be an instance of:
                    //                    io.vertx.core.json.JsonObject
                    //                    but was instance of:
                    //                    java.lang.Boolean
                    assertThat(syncingRespJson.getValue("result")).isInstanceOf(JsonObject.class);
                    final int syncResultCurrentBlock =
                        UInt256.fromHexString(
                                syncingRespJson.getJsonObject("result").getString("currentBlock"))
                            .intValue();
                    assertThat(syncResultCurrentBlock).isLessThan(blockCount);
                  }
                  assertThat(currentBlock).isEqualTo(blockCount);

                  // when we have synced to blockCount, eth_syncing should return false
                  final boolean syncResult = syncingRespJson.getBoolean("result");
                  assertThat(syncResult).isFalse();
                }
              });
      final Promise<String> promise = Promise.promise();
      final HttpClient httpClient = vertx.createHttpClient();
      httpClient.webSocket(
          runnerBehind.getWebSocketPort().get(),
          WebSocketConfiguration.DEFAULT_WEBSOCKET_HOST,
          "/",
          ws -> {
            ws.result()
                .writeTextMessage(
                    "{\"id\": 1, \"method\": \"eth_subscribe\", \"params\": [\"syncing\"]}");
            ws.result()
                .textMessageHandler(
                    payload -> {
                      final boolean matches =
                          payload.equals("{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":\"0x0\"}");
                      if (matches) {
                        promise.complete(payload);
                      } else {
                        promise.fail("Unexpected result: " + payload);
                      }
                    });
          });
      final Future<String> future = promise.future();
      Awaitility.await()
          .catchUncaughtExceptions()
          .atMost(2L, TimeUnit.MINUTES)
          .until(future::isComplete);
    } finally {
      if (runnerBehind != null) {
        runnerBehind.close();
        runnerBehind.awaitStop();
      }
    }
  }

  private int getNumber(final Response response) throws IOException {
    return UInt256.fromHexString(new JsonObject(response.body().string()).getString("result"))
        .intValue();
  }

  private Request getRequest(final String method, final String baseUrl) {
    return new Request.Builder()
        .post(
            RequestBody.create(
                "{\"jsonrpc\":\"2.0\",\"id\":" + Json.encode(7) + ",\"method\":\"" + method + "\"}",
                MediaType.parse("application/json; charset=utf-8")))
        .url(baseUrl)
        .build();
  }

  private GenesisConfig getFastSyncGenesis() throws IOException {
    final ObjectNode jsonNode =
        (ObjectNode)
            new ObjectMapper().readTree(GenesisConfig.class.getResource(MAINNET.getGenesisFile()));
    final Optional<ObjectNode> configNode = JsonUtil.getObjectNode(jsonNode, "config");
    configNode.ifPresent(
        (node) -> {
          // Clear DAO block so that inability to validate DAO block won't interfere with fast sync
          node.remove("daoForkBlock");
          // remove merge terminal difficulty for fast sync in the absence of a CL mock
          node.remove("terminalTotalDifficulty");
        });
    return GenesisConfig.fromConfig(jsonNode);
  }

  private StorageProvider createKeyValueStorageProvider(
      final Path dataDir,
      final Path dbDir,
      final DataStorageConfiguration dataStorageConfiguration,
      final MiningConfiguration miningConfiguration) {
    final var besuConfiguration = new BesuConfigurationImpl();
    besuConfiguration
        .init(dataDir, dbDir, dataStorageConfiguration)
        .withMiningParameters(miningConfiguration);
    return new KeyValueStorageProviderBuilder()
        .withStorageFactory(
            new RocksDBKeyValueStorageFactory(
                () ->
                    new RocksDBFactoryConfiguration(
                        DEFAULT_MAX_OPEN_FILES,
                        DEFAULT_BACKGROUND_THREAD_COUNT,
                        DEFAULT_CACHE_CAPACITY,
                        DEFAULT_IS_HIGH_SPEC,
                        DEFAULT_ENABLE_READ_CACHE_FOR_SNAPSHOTS,
                        false,
                        Optional.empty(),
                        Optional.empty()),
                Arrays.asList(KeyValueSegmentIdentifier.values()),
                RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS))
        .withCommonConfiguration(besuConfiguration)
        .withMetricsSystem(new NoOpMetricsSystem())
        .build();
  }

  private JsonRpcConfiguration jsonRpcConfiguration() {
    final JsonRpcConfiguration configuration = JsonRpcConfiguration.createDefault();
    configuration.setPort(0);
    configuration.setEnabled(true);
    configuration.setHostsAllowlist(Collections.singletonList("*"));
    return configuration;
  }

  private GraphQLConfiguration graphQLConfiguration() {
    final GraphQLConfiguration configuration = GraphQLConfiguration.createDefault();
    configuration.setPort(0);
    configuration.setEnabled(false);
    return configuration;
  }

  private WebSocketConfiguration wsRpcConfiguration() {
    final WebSocketConfiguration configuration = WebSocketConfiguration.createDefault();
    configuration.setPort(0);
    configuration.setEnabled(true);
    configuration.setHostsAllowlist(Collections.singletonList("*"));
    return configuration;
  }

  private MetricsConfiguration metricsConfiguration() {
    return MetricsConfiguration.builder().enabled(false).port(0).build();
  }

  private static void setupState(
      final int count,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext) {
    final List<Block> blocks = BlockSyncTestUtils.firstBlocks(count + 1);

    for (int i = 1; i < count + 1; ++i) {
      final Block block = blocks.get(i);
      final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(block.getHeader());
      final BlockImporter blockImporter = protocolSpec.getBlockImporter();
      final BlockImportResult result =
          blockImporter.importBlock(protocolContext, block, HeaderValidationMode.FULL);
      if (!result.isImported()) {
        throw new IllegalStateException("Unable to import block " + block.getHeader().getNumber());
      }
    }
  }

  private BesuController getController(
      final GenesisConfig genesisConfig,
      final SynchronizerConfiguration syncConfig,
      final Path dataDir,
      final NodeKey nodeKey,
      final StorageProvider storageProvider,
      final ObservableMetricsSystem metricsSystem,
      final MiningConfiguration miningConfiguration) {
    return new MainnetBesuControllerBuilder()
        .genesisConfig(genesisConfig)
        .synchronizerConfiguration(syncConfig)
        .ethProtocolConfiguration(EthProtocolConfiguration.DEFAULT)
        .dataDirectory(dataDir)
        .networkId(NETWORK_ID)
        .miningParameters(miningConfiguration)
        .nodeKey(nodeKey)
        .storageProvider(storageProvider)
        .metricsSystem(metricsSystem)
        .clock(TestClock.fixed())
        .transactionPoolConfiguration(TransactionPoolConfiguration.DEFAULT)
        .dataStorageConfiguration(DataStorageConfiguration.DEFAULT_FOREST_CONFIG)
        .evmConfiguration(EvmConfiguration.DEFAULT)
        .networkConfiguration(NetworkingConfiguration.DEFAULT)
        .randomPeerPriority(Boolean.FALSE)
        .besuComponent(mock(BesuComponent.class))
        .maxPeers(25)
        .maxRemotelyInitiatedPeers(15)
        .apiConfiguration(ImmutableApiConfiguration.builder().build())
        .build();
  }
}
