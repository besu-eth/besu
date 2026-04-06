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

import static java.lang.Thread.sleep;

import org.hyperledger.besu.cli.BesuCommand;
import org.hyperledger.besu.cli.options.PluginsConfigurationOptions;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.crypto.KeyPairUtil;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLHttpService;
import org.hyperledger.besu.ethereum.api.jsonrpc.EngineJsonRpcService;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcHttpService;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.ipc.JsonRpcIpcService;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketService;
import org.hyperledger.besu.ethereum.api.query.cache.AutoTransactionLogBloomCachingService;
import org.hyperledger.besu.ethereum.api.query.cache.TransactionLogBloomCacher;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolEvictionService;
import org.hyperledger.besu.ethereum.p2p.network.NetworkRunner;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethstats.EthStatsService;
import org.hyperledger.besu.metrics.MetricsService;
import org.hyperledger.besu.nat.NatService;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Runner controls various Besu services lifecycle. */
public class Runner implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Runner.class);

  private final Vertx vertx;
  private final CountDownLatch vertxShutdownLatch = new CountDownLatch(1);
  private final CountDownLatch shutdown = new CountDownLatch(1);

  private final NatService natService;
  private final NetworkRunner networkRunner;
  private final Optional<EthStatsService> ethStatsService;
  private final Optional<GraphQLHttpService> graphQLHttp;
  private final Optional<JsonRpcHttpService> jsonRpc;
  private final Optional<EngineJsonRpcService> engineJsonRpc;
  private final Optional<MetricsService> metrics;
  private final Optional<JsonRpcIpcService> ipcJsonRpc;
  private final Map<String, JsonRpcMethod> inProcessRpcMethods;
  private final Optional<Path> pidPath;
  private final Optional<WebSocketService> webSocketRpc;
  private final TransactionPoolEvictionService transactionPoolEvictionService;

  private final BesuController besuController;
  private final Path dataDir;
  private final Optional<AutoTransactionLogBloomCachingService>
      autoTransactionLogBloomCachingService;
  private ScheduledExecutorService ephemeryService;
  private final long ephemeryCycle = TimeUnit.DAYS.toSeconds(28);

  /**
   * Instantiates a new Runner.
   *
   * @param vertx the vertx
   * @param networkRunner the network runner
   * @param natService the nat service
   * @param jsonRpc the json rpc
   * @param engineJsonRpc the engine json rpc
   * @param graphQLHttp the graph ql http
   * @param webSocketRpc the web socket rpc
   * @param ipcJsonRpc the ipc json rpc
   * @param inProcessRpcMethods the in-process rpc methods
   * @param metrics the metrics
   * @param ethStatsService the eth stats service
   * @param besuController the besu controller
   * @param dataDir the data dir
   * @param pidPath the pid path
   * @param transactionLogBloomCacher the transaction log bloom cacher
   * @param blockchain the blockchain
   */
  Runner(
      final Vertx vertx,
      final NetworkRunner networkRunner,
      final NatService natService,
      final Optional<JsonRpcHttpService> jsonRpc,
      final Optional<EngineJsonRpcService> engineJsonRpc,
      final Optional<GraphQLHttpService> graphQLHttp,
      final Optional<WebSocketService> webSocketRpc,
      final Optional<JsonRpcIpcService> ipcJsonRpc,
      final Map<String, JsonRpcMethod> inProcessRpcMethods,
      final Optional<MetricsService> metrics,
      final Optional<EthStatsService> ethStatsService,
      final BesuController besuController,
      final Path dataDir,
      final Optional<Path> pidPath,
      final Optional<TransactionLogBloomCacher> transactionLogBloomCacher,
      final Blockchain blockchain) {
    this.vertx = vertx;
    this.networkRunner = networkRunner;
    this.natService = natService;
    this.graphQLHttp = graphQLHttp;
    this.pidPath = pidPath;
    this.jsonRpc = jsonRpc;
    this.engineJsonRpc = engineJsonRpc;
    this.webSocketRpc = webSocketRpc;
    this.ipcJsonRpc = ipcJsonRpc;
    this.inProcessRpcMethods = inProcessRpcMethods;
    this.metrics = metrics;
    this.ethStatsService = ethStatsService;
    this.besuController = besuController;
    this.dataDir = dataDir;
    this.autoTransactionLogBloomCachingService =
        transactionLogBloomCacher.map(
            cacher -> new AutoTransactionLogBloomCachingService(blockchain, cacher));
    this.transactionPoolEvictionService =
        new TransactionPoolEvictionService(vertx, besuController.getTransactionPool());
  }

  /** Start external services. */
  public void startExternalServices() {
    LOG.info("Starting external services ... ");
    metrics.ifPresent(service -> waitForServiceToStart("metrics", service.start()));

    jsonRpc.ifPresent(service -> waitForServiceToStart("jsonRpc", service.start()));
    engineJsonRpc.ifPresent(service -> waitForServiceToStart("engineJsonRpc", service.start()));
    graphQLHttp.ifPresent(service -> waitForServiceToStart("graphQLHttp", service.start()));
    webSocketRpc.ifPresent(service -> waitForServiceToStart("websocketRpc", service.start()));
    ipcJsonRpc.ifPresent(
        service ->
            waitForServiceToStart(
                "ipcJsonRpc", service.start().toCompletionStage().toCompletableFuture()));
    autoTransactionLogBloomCachingService.ifPresent(AutoTransactionLogBloomCachingService::start);
  }

  private void startExternalServicePostMainLoop() {
    ethStatsService.ifPresent(EthStatsService::start);
  }

  /** Start ethereum main loop. */
  public void startEthereumMainLoop() {
    try {
      LOG.info("Starting Ethereum main loop ... ");
      natService.start();
      networkRunner.start();
      besuController.getMiningCoordinator().subscribe();
      if (networkRunner.getNetwork().isP2pEnabled()) {
        besuController.getSynchronizer().start();
      }
      besuController.getMiningCoordinator().start();
      transactionPoolEvictionService.start();

      LOG.info("Ethereum main loop is up.");
      // we write these values to disk to be able to access them during the acceptance tests
      writeBesuPortsToFile();
      writeBesuNetworksToFile();
      writePidFile();

      // start external service that depends on information from main loop
      startExternalServicePostMainLoop();
    } catch (final Exception ex) {
      LOG.error("unable to start main loop", ex);
      throw new IllegalStateException("Startup failed", ex);
    }
  }

  /**
   * Schedules automatic Ephemery restarts based on the cycle period.
   *
   * @param besuCommand the Besu command instance to restart
   * @param lastGenesisTimestamp the timestamp of the last genesis block in seconds
   */
  public void scheduleEphemeryRestart(
      final BesuCommand besuCommand, final long lastGenesisTimestamp) {
    long currentTimestamp = Instant.now().getEpochSecond();
    // next restart should be triggered in "lastGenesisTime + cycle - now" seconds
    long restartTime = lastGenesisTimestamp + ephemeryCycle - currentTimestamp;

    if (TimeUnit.SECONDS.toDays(restartTime) >= 1) {
      LOG.info(
          "Scheduled Ephemery testnet restarts in {} days", TimeUnit.SECONDS.toDays(restartTime));
    } else {
      if (TimeUnit.SECONDS.toHours(restartTime) >= 1) {
        LOG.info(
            "Scheduled Ephemery testnet restarts in {} hours",
            TimeUnit.SECONDS.toHours(restartTime));
      } else {
        LOG.info(
            "Scheduled Ephemery testnet restarts in {} minutes",
            TimeUnit.SECONDS.toMinutes(restartTime));
      }
    }
    ephemeryService =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread thread = new Thread(r, "ephemery-auto-restart");
              thread.setDaemon(true);
              return thread;
            });

    ephemeryService.scheduleAtFixedRate(
        () -> {
          LOG.info("Scheduled Ephemery testnet restart triggered");
          try {
            besuCommand.getRunner().stopEphemery(besuCommand);
            besuCommand.getRunner().startEphemery(besuCommand);
            LOG.info("Ephemery testnet restarted successfully");
          } catch (Exception e) {
            LOG.error("Failed to restart Ephemery", e);
          }
          LOG.info(
              "Next scheduled Ephemery testnet restart will be in {} days",
              TimeUnit.SECONDS.toDays(ephemeryCycle));
        },
        restartTime,
        ephemeryCycle,
        TimeUnit.SECONDS);
  }

  /**
   * Stops the services, clears data, and resets data path for the next cycle.
   *
   * @param besuCommand the Besu command instance to invoke its methods
   * @throws IOException if file operations fail
   * @throws InterruptedException if the sleep operation is interrupted
   */
  public void stopEphemery(final BesuCommand besuCommand) throws IOException, InterruptedException {
    if (besuController != null) {
      stopServices();
      clearDirectory(dataDir);
      besuCommand.setDataPathToParent();
      besuCommand.clearAllocatedPorts();
      sleep(10000);
    }
  }

  /**
   * Restarts the Ephemery testnet by preparing the environment and initializing services. Stores
   * the key in the new Ephemery directory.
   *
   * @param besuCommand the Besu command instance to prepare and start Ephemery cycle
   * @throws Exception if startup fails
   */
  public void startEphemery(final BesuCommand besuCommand) throws Exception {
    ephemeryRestartPrepare(besuCommand);
    besuCommand.initialProcess();
    var keypair = besuCommand.getKeyPair();
    if (keypair != null) {
      KeyPairUtil.storeKeyFile(keypair, besuCommand.dataDir());
      LOG.info("Stored public key {} to {}", keypair.getPublicKey(), besuCommand.dataDir());
    } else {
      LOG.info("Key is null");
    }
  }

  private void ephemeryRestartPrepare(final BesuCommand besuCommand) {
    besuCommand.getBesuPluginContext().resetState();
    besuCommand.getRocksDBPlugin().reset();
    besuCommand
        .getBesuPluginContext()
        .initialize(PluginsConfigurationOptions.fromCommandLine(besuCommand.getCommandLine()));
    besuCommand.getBesuPluginContext().registerPlugins();
  }

  /** Stop services but not shutdown */
  public void stopServices() {
    transactionPoolEvictionService.stop();
    jsonRpc.ifPresent(service -> waitForServiceToStop("jsonRpc", service.stop()));
    engineJsonRpc.ifPresent(service -> waitForServiceToStop("engineJsonRpc", service.stop()));
    graphQLHttp.ifPresent(service -> waitForServiceToStop("graphQLHttp", service.stop()));
    webSocketRpc.ifPresent(service -> waitForServiceToStop("websocketRpc", service.stop()));
    ipcJsonRpc.ifPresent(
        service ->
            waitForServiceToStop(
                "ipcJsonRpc", service.stop().toCompletionStage().toCompletableFuture()));
    waitForServiceToStop("Transaction Pool", besuController.getTransactionPool().setDisabled());
    metrics.ifPresent(service -> waitForServiceToStop("metrics", service.stop()));
    ethStatsService.ifPresent(EthStatsService::stop);
    besuController.getMiningCoordinator().stop();
    waitForServiceToStop("Mining Coordinator", besuController.getMiningCoordinator()::awaitStop);
    if (networkRunner.getNetwork().isP2pEnabled()) {
      besuController.getSynchronizer().stop();
      waitForServiceToStop("Synchronizer", besuController.getSynchronizer()::awaitStop);
    }

    networkRunner.stop();
    waitForServiceToStop("Network", networkRunner::awaitStop);
    autoTransactionLogBloomCachingService.ifPresent(AutoTransactionLogBloomCachingService::stop);
    natService.stop();
    besuController.close();
  }

  /** Stop services. */
  public void stop() {
    stopServices();
    vertx.close((res) -> vertxShutdownLatch.countDown());
    waitForServiceToStop("Vertx", vertxShutdownLatch::await);
    if (ephemeryService != null) {
      ephemeryService.close();
    }
    shutdown.countDown();
  }

  /** Await stop. */
  public void awaitStop() {
    try {
      shutdown.await();
    } catch (final InterruptedException e) {
      LOG.debug("Interrupted, exiting", e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void close() {
    stop();
    awaitStop();
  }

  private void waitForServiceToStop(
      final String serviceName, final CompletableFuture<?> stopFuture) {
    try {
      stopFuture.get(30, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      LOG.debug("Interrupted while waiting for service to complete", e);
      Thread.currentThread().interrupt();
    } catch (final ExecutionException e) {
      LOG.error("Service " + serviceName + " failed to shutdown", e);
    } catch (final TimeoutException e) {
      LOG.error("Service {} did not shut down cleanly", serviceName);
    }
  }

  private void waitForServiceToStop(final String serviceName, final SynchronousShutdown shutdown) {
    try {
      shutdown.await();
    } catch (final InterruptedException e) {
      LOG.debug("Interrupted while waiting for service {} to stop {}", serviceName, e);
      Thread.currentThread().interrupt();
    }
  }

  private void waitForServiceToStart(
      final String serviceName, final CompletableFuture<?> startFuture) {
    do {
      try {
        startFuture.get(60, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while waiting for service to start", e);
      } catch (final ExecutionException e) {
        throw new IllegalStateException("Service " + serviceName + " failed to start", e);
      } catch (final TimeoutException e) {
        LOG.warn("Service {} is taking an unusually long time to start", serviceName);
      }
    } while (!startFuture.isDone());
  }

  private void writeBesuPortsToFile() {
    final Properties properties = new Properties();
    if (networkRunner.getNetwork().isP2pEnabled()) {
      networkRunner
          .getNetwork()
          .getLocalEnode()
          .ifPresent(
              enode -> {
                enode
                    .getDiscoveryPort()
                    .ifPresent(
                        discoveryPort ->
                            properties.setProperty("discovery", String.valueOf(discoveryPort)));
                enode
                    .getListeningPort()
                    .ifPresent(
                        listeningPort ->
                            properties.setProperty("p2p", String.valueOf(listeningPort)));
              });
    }

    Optional<Integer> port = getJsonRpcPort();
    if (port.isPresent()) {
      properties.setProperty("json-rpc", String.valueOf(port.get()));
    }
    port = getGraphQLHttpPort();
    if (port.isPresent()) {
      properties.setProperty("graphql-http", String.valueOf(port.get()));
    }
    port = getWebSocketPort();
    if (port.isPresent()) {
      properties.setProperty("ws-rpc", String.valueOf(port.get()));
    }
    port = getMetricsPort();
    if (port.isPresent()) {
      properties.setProperty("metrics", String.valueOf(port.get()));
    }
    port = getEngineJsonRpcPort();
    if (port.isPresent()) {
      properties.setProperty("engine-json-rpc", String.valueOf(port.get()));
    }
    // create besu.ports file
    createBesuFile(
        properties, "ports", "This file contains the ports used by the running instance of Besu");
  }

  private void writeBesuNetworksToFile() {
    final Properties properties = new Properties();
    if (networkRunner.getNetwork().isP2pEnabled()) {
      networkRunner
          .getNetwork()
          .getLocalEnode()
          .ifPresent(
              enode -> {
                final String globalIp = natService.queryExternalIPAddress(enode.getIpAsString());
                properties.setProperty("global-ip", globalIp);
                final String localIp = natService.queryLocalIPAddress(enode.getIpAsString());
                properties.setProperty("local-ip", localIp);
              });
    }
    // create besu.networks file
    createBesuFile(
        properties,
        "networks",
        "This file contains the IP Addresses (global and local) used by the running instance of Besu");
  }

  private void writePidFile() {
    pidPath.ifPresent(
        path -> {
          String pid = "";
          try {
            pid = Long.toString(ProcessHandle.current().pid());
          } catch (Throwable t) {
            LOG.error("Error retrieving PID", t);
          }
          try {
            Files.write(
                path,
                pid.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
            path.toFile().deleteOnExit();
          } catch (IOException e) {
            LOG.error("Error writing PID file", e);
          }
        });
  }

  /**
   * Gets json rpc port.
   *
   * @return the json rpc port
   */
  public Optional<Integer> getJsonRpcPort() {
    return jsonRpc.map(service -> service.socketAddress().getPort());
  }

  /**
   * Gets engine json rpc port.
   *
   * @return the engine json rpc port
   */
  public Optional<Integer> getEngineJsonRpcPort() {
    return engineJsonRpc.map(service -> service.socketAddress().getPort());
  }

  /**
   * Gets GraphQl http port.
   *
   * @return the graph ql http port
   */
  public Optional<Integer> getGraphQLHttpPort() {
    return graphQLHttp.map(service -> service.socketAddress().getPort());
  }

  /**
   * Gets web socket port.
   *
   * @return the web socket port
   */
  public Optional<Integer> getWebSocketPort() {
    return webSocketRpc.map(service -> service.socketAddress().getPort());
  }

  /**
   * Gets metrics port.
   *
   * @return the metrics port
   */
  public Optional<Integer> getMetricsPort() {
    if (metrics.isPresent()) {
      return metrics.get().getPort();
    } else {
      return Optional.empty();
    }
  }

  /**
   * Get the RPC methods that can be called in-process
   *
   * @return RPC methods by name
   */
  public Map<String, JsonRpcMethod> getInProcessRpcMethods() {
    return inProcessRpcMethods;
  }

  /**
   * Gets local enode.
   *
   * @return the local enode
   */
  @VisibleForTesting
  Optional<EnodeURLImpl> getLocalEnode() {
    return networkRunner.getNetwork().getLocalEnode();
  }

  /**
   * get P2PNetwork service.
   *
   * @return p2p network service.
   */
  public P2PNetwork getP2PNetwork() {
    return networkRunner.getNetwork();
  }

  @FunctionalInterface
  private interface SynchronousShutdown {
    /**
     * Await for shutdown.
     *
     * @throws InterruptedException the interrupted exception
     */
    void await() throws InterruptedException;
  }

  private void createBesuFile(
      final Properties properties, final String fileName, final String fileHeader) {
    final File file = new File(dataDir.toFile(), String.format("besu.%s", fileName));
    file.deleteOnExit();
    try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
      properties.store(
          fileOutputStream,
          String.format("%s. This file will be deleted after the node is shutdown.", fileHeader));
    } catch (final Exception e) {
      LOG.warn(String.format("Error writing %s file", fileName), e);
    }
  }

  private void clearDirectory(final Path directory) throws IOException {
    if (Files.exists(directory)) {
      try (Stream<Path> pathStream = Files.walk(directory)) {
        pathStream
            .sorted((a, b) -> b.compareTo(a)) // Delete files before directories
            .forEach(
                path -> {
                  try {
                    Files.delete(path);
                    LOG.debug("Deleted: {}", path);
                  } catch (IOException e) {
                    LOG.warn("Could not delete {}: {}", path, e.getMessage());
                  }
                });
      }
    }
  }
}
