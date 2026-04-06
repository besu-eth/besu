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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import static com.google.common.base.Preconditions.checkArgument;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.health.HealthService;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterIdGenerator;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterManagerBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterRepository;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.methods.JsonRpcMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.blockcreation.NoopMiningCoordinator;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.testutil.BlockTestUtil.ChainResources;

import java.math.BigInteger;
import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

public abstract class AbstractJsonRpcHttpServiceTest {
  @TempDir private Path folder;

  protected BlockchainSetupUtil blockchainSetupUtil;

  protected static final String CLIENT_NODE_NAME = "TestClientVersion/0.1.0";
  protected static final String CLIENT_VERSION = "0.1.0";
  protected static final String CLIENT_COMMIT = "12345678";
  protected static final BigInteger NETWORK_ID = BigInteger.valueOf(123);
  protected static final Collection<String> JSON_RPC_APIS =
      Arrays.asList(
          RpcApis.ETH.name(),
          RpcApis.NET.name(),
          RpcApis.WEB3.name(),
          RpcApis.DEBUG.name(),
          RpcApis.TRACE.name(),
          RpcApis.TESTING.name());

  protected final Vertx vertx = Vertx.vertx();
  protected final Vertx syncVertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(1));
  protected JsonRpcHttpService service;
  protected OkHttpClient client;
  protected String baseUrl;
  protected final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  protected FilterManager filterManager;

  protected void setupBlockchain() {
    blockchainSetupUtil = getBlockchainSetupUtil(DataStorageFormat.FOREST);
    blockchainSetupUtil.importAllBlocks(HeaderValidationMode.NONE, HeaderValidationMode.NONE);
  }

  protected void setupBonsaiBlockchain() {
    blockchainSetupUtil = getBlockchainSetupUtil(DataStorageFormat.BONSAI);
    blockchainSetupUtil.importAllBlocks(HeaderValidationMode.NONE, HeaderValidationMode.NONE);
  }

  protected BlockchainSetupUtil getBlockchainSetupUtil(final DataStorageFormat storageFormat) {
    return BlockchainSetupUtil.forHiveTesting(storageFormat);
  }

  protected BlockchainSetupUtil createBlockchainSetupUtil(
      final String genesisPath, final String blocksPath, final DataStorageFormat storageFormat) {
    final URL genesisURL = AbstractJsonRpcHttpServiceTest.class.getResource(genesisPath);
    final URL blocksURL = AbstractJsonRpcHttpServiceTest.class.getResource(blocksPath);
    checkArgument(genesisURL != null, "Unable to locate genesis file: " + genesisPath);
    checkArgument(blocksURL != null, "Unable to locate blocks file: " + blocksPath);
    return BlockchainSetupUtil.createForEthashChain(
        new ChainResources(genesisURL, blocksURL), storageFormat);
  }

  @BeforeEach
  public void setup() throws Exception {
    setupBlockchain();
  }

  protected BlockchainSetupUtil startServiceWithEmptyChain(final DataStorageFormat storageFormat) {
    final BlockchainSetupUtil emptySetupUtil = getBlockchainSetupUtil(storageFormat);
    startService(emptySetupUtil);
    return emptySetupUtil;
  }

  protected ApiConfiguration createApiConfiguration() {
    return ImmutableApiConfiguration.builder().gasCap(0L).build();
  }

  protected TransactionPool createTransactionPoolMock() {
    final TransactionPool transactionPoolMock = mock(TransactionPool.class);
    when(transactionPoolMock.addTransactionViaApi(any(Transaction.class)))
        .thenReturn(ValidationResult.valid());
    // nonce too low tests uses a tx with nonce=16
    when(transactionPoolMock.addTransactionViaApi(argThat(tx -> tx.getNonce() == 16)))
        .thenReturn(ValidationResult.invalid(TransactionInvalidReason.NONCE_TOO_LOW));
    return transactionPoolMock;
  }

  protected MiningConfiguration createMiningConfiguration() {
    return ImmutableMiningConfiguration.builder()
        .mutableInitValues(
            MutableInitValues.builder()
                .extraData(Bytes.EMPTY)
                .minTransactionGasPrice(Wei.ONE)
                .coinbase(Address.ZERO)
                .build())
        .build();
  }

  protected Map<String, JsonRpcMethod> getRpcMethods(
      final JsonRpcConfiguration config, final BlockchainSetupUtil blockchainSetupUtil) {
    final ProtocolContext protocolContext = mock(ProtocolContext.class);
    final Synchronizer synchronizerMock = mock(Synchronizer.class);
    final P2PNetwork peerDiscoveryMock = mock(P2PNetwork.class);
    final TransactionPool transactionPoolMock = createTransactionPoolMock();
    final MiningConfiguration miningConfiguration = createMiningConfiguration();
    final ApiConfiguration apiConfiguration = createApiConfiguration();

    when(protocolContext.getBlockchain()).thenReturn(blockchainSetupUtil.getBlockchain());
    when(protocolContext.getWorldStateArchive()).thenReturn(blockchainSetupUtil.getWorldArchive());
    final BlockchainQueries blockchainQueries =
        new BlockchainQueries(
            blockchainSetupUtil.getProtocolSchedule(),
            blockchainSetupUtil.getBlockchain(),
            blockchainSetupUtil.getWorldArchive(),
            miningConfiguration);
    final FilterIdGenerator filterIdGenerator = mock(FilterIdGenerator.class);
    final FilterRepository filterRepository = new FilterRepository();
    when(filterIdGenerator.nextId()).thenReturn("0x1");
    filterManager =
        new FilterManagerBuilder()
            .blockchainQueries(blockchainQueries)
            .transactionPool(transactionPoolMock)
            .filterIdGenerator(filterIdGenerator)
            .filterRepository(filterRepository)
            .build();

    final Set<Capability> supportedCapabilities = new HashSet<>();
    supportedCapabilities.add(EthProtocol.LATEST);

    final NatService natService = new NatService(Optional.empty());

    final var transactionSimulator =
        new TransactionSimulator(
            blockchainSetupUtil.getBlockchain(),
            blockchainSetupUtil.getWorldArchive(),
            blockchainSetupUtil.getProtocolSchedule(),
            miningConfiguration,
            apiConfiguration.getGasCap());

    return new JsonRpcMethodsFactory()
        .methods(
            CLIENT_NODE_NAME,
            CLIENT_VERSION,
            CLIENT_COMMIT,
            NETWORK_ID,
            new StubGenesisConfigOptions(),
            peerDiscoveryMock,
            blockchainQueries,
            synchronizerMock,
            blockchainSetupUtil.getProtocolSchedule(),
            protocolContext,
            filterManager,
            transactionPoolMock,
            miningConfiguration,
            new NoopMiningCoordinator(),
            new NoOpMetricsSystem(),
            supportedCapabilities,
            Optional.empty(),
            Optional.empty(),
            JSON_RPC_APIS,
            config,
            mock(WebSocketConfiguration.class),
            mock(MetricsConfiguration.class),
            mock(GraphQLConfiguration.class),
            natService,
            new HashMap<>(),
            folder,
            mock(EthPeers.class),
            syncVertx,
            ImmutableApiConfiguration.builder().build(),
            Optional.empty(),
            transactionSimulator,
            new EthScheduler(1, 1, 1, new NoOpMetricsSystem()));
  }

  protected void startService() throws Exception {
    startService(blockchainSetupUtil);
  }

  private void startService(final BlockchainSetupUtil blockchainSetupUtil) {

    final JsonRpcConfiguration config = JsonRpcConfiguration.createDefault();
    final Map<String, JsonRpcMethod> methods = getRpcMethods(config, blockchainSetupUtil);
    final NatService natService = new NatService(Optional.empty());

    config.setPort(0);
    config.setMaxBatchSize(10);
    service =
        new JsonRpcHttpService(
            vertx,
            folder,
            config,
            new NoOpMetricsSystem(),
            natService,
            methods,
            HealthService.ALWAYS_HEALTHY,
            HealthService.ALWAYS_HEALTHY);
    service.start().join();

    client = new OkHttpClient();
    baseUrl = service.url();
  }

  @AfterEach
  public void shutdownServer() {
    client.dispatcher().executorService().shutdown();
    client.connectionPool().evictAll();
    service.stop().join();
    vertx.close();
    syncVertx.close();
  }
}
