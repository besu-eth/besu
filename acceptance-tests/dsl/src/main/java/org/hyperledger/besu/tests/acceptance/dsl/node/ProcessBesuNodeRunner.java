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
package org.hyperledger.besu.tests.acceptance.dsl.node;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

import org.hyperledger.besu.cli.options.NetworkingOptions;
import org.hyperledger.besu.cli.options.TransactionPoolOptions;
import org.hyperledger.besu.cli.options.storage.DataStorageOptions;
import org.hyperledger.besu.ethereum.api.jsonrpc.ipc.JsonRpcIpcConfiguration;
import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.permissioning.PermissioningConfiguration;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.plugin.services.metrics.MetricCategory;
import org.hyperledger.besu.tests.acceptance.dsl.StaticNodesUtils;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.ProcessBuilder.Redirect;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class ProcessBesuNodeRunner implements BesuNodeRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessBesuNodeRunner.class);
  private static final Logger PROCESS_LOG =
      LoggerFactory.getLogger("org.hyperledger.besu.SubProcessLog");

  private final Map<String, Process> besuProcesses = new HashMap<>();
  private final ExecutorService outputProcessorExecutor = Executors.newCachedThreadPool();
  private boolean capturingConsole;
  private final ByteArrayOutputStream consoleContents = new ByteArrayOutputStream();
  private final PrintStream consoleOut = new PrintStream(consoleContents);

  ProcessBesuNodeRunner() {
    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
  }

  @Override
  public void startNode(final BesuNode node) {

    final Path dataDir = node.homeDirectory();

    final List<String> params = commandlineArgs(node, dataDir);

    LOG.info("Creating besu process with params {}", params);
    final ProcessBuilder processBuilder =
        new ProcessBuilder(params)
            .directory(new File(System.getProperty("user.dir")).getParentFile().getParentFile())
            .redirectErrorStream(true)
            .redirectInput(Redirect.INHERIT);
    if (!node.getPlugins().isEmpty()) {
      processBuilder
          .environment()
          .put("BESU_OPTS", "-Dbesu.plugins.dir=" + dataDir.resolve("plugins").toAbsolutePath());
    }
    // Use non-blocking randomness for acceptance tests
    processBuilder
        .environment()
        .put(
            "JAVA_OPTS",
            "-Djava.security.properties="
                + "acceptance-tests/tests/build/resources/test/acceptanceTesting.security");
    // add additional environment variables
    processBuilder.environment().putAll(node.getEnvironment());

    try {
      int debugPort = Integer.parseInt(System.getenv("BESU_DEBUG_CHILD_PROCESS_PORT"));
      LOG.warn("Waiting for debugger to attach to SUSPENDED child process");
      String debugOpts =
          " -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:" + debugPort;
      String prevJavaOpts = processBuilder.environment().get("JAVA_OPTS");
      if (prevJavaOpts == null) {
        processBuilder.environment().put("JAVA_OPTS", debugOpts);
      } else {
        processBuilder.environment().put("JAVA_OPTS", prevJavaOpts + debugOpts);
      }

    } catch (NumberFormatException e) {
      LOG.debug(
          "Child process may be attached to by exporting BESU_DEBUG_CHILD_PROCESS_PORT=<port> to env");
    }

    try {
      checkState(
          isNotAliveOrphan(node.getName()),
          "A live process with name: %s, already exists. Cannot create another with the same name as it would orphan the first",
          node.getName());

      final Process process = processBuilder.start();
      process.onExit().thenRun(() -> node.setExitCode(process.exitValue()));
      outputProcessorExecutor.execute(() -> printOutput(node, process));
      besuProcesses.put(node.getName(), process);
    } catch (final IOException e) {
      LOG.error("Error starting BesuNode process", e);
    }

    if (node.getRunCommand().isEmpty()) {
      waitForFileOrExit(node, "besu.ports");
      waitForFileOrExit(node, "besu.networks");
    }
    MDC.remove("node");
  }

  private List<String> commandlineArgs(final BesuNode node, final Path dataDir) {
    final List<String> params = new ArrayList<>();
    params.add("build/install/besu/bin/besu");

    params.add("--data-path");
    params.add(dataDir.toAbsolutePath().toString());

    if (node.isDevMode()) {
      params.add("--network");
      params.add("DEV");
    } else if (node.getNetwork() != null) {
      params.add("--network");
      params.add(node.getNetwork().name());
    }

    if (node.getSynchronizerConfiguration() != null) {

      if (node.getSynchronizerConfiguration().getSyncMode() != null) {
        params.add("--sync-mode");
        params.add(node.getSynchronizerConfiguration().getSyncMode().toString());
      }
      params.add("--sync-min-peers");
      params.add(Integer.toString(node.getSynchronizerConfiguration().getSyncMinimumPeerCount()));
      params.add("--Xsynchronizer-pivot-distance");
      params.add(Integer.toString(node.getSynchronizerConfiguration().getSyncPivotDistance()));
    } else {
      params.add("--sync-mode");
      params.add("FULL");
    }

    params.add("--snapsync-server-enabled");

    params.add("--discovery-enabled");
    params.add(Boolean.toString(node.isDiscoveryEnabled()));

    params.add("--p2p-host");
    params.add(node.p2pListenHost());

    params.add("--p2p-port");
    params.add(node.getP2pPort());

    params.addAll(
        TransactionPoolOptions.fromConfig(
                ImmutableTransactionPoolConfiguration.builder()
                    .from(node.getTransactionPoolConfiguration())
                    .strictTransactionReplayProtectionEnabled(
                        node.isStrictTxReplayProtectionEnabled())
                    .build())
            .getCLIOptions());

    params.addAll(
        DataStorageOptions.fromConfig(node.getDataStorageConfiguration()).getCLIOptions());

    if (!node.getBootnodes().isEmpty()) {
      params.add("--bootnodes");
      params.add(node.getBootnodes().stream().map(URI::toString).collect(Collectors.joining(",")));
    }

    if (node.hasStaticNodes()) {
      createStaticNodes(node);
    }

    if (node.isDnsEnabled()) {
      params.add("--Xdns-enabled");
      params.add("true");
      params.add("--Xdns-update-enabled");
      params.add("true");
    }

    if (node.isJsonRpcEnabled()) {
      params.add("--rpc-http-enabled");
      params.add("--rpc-http-host");
      params.add(node.jsonRpcListenHost().get());
      params.add("--rpc-http-port");
      params.add(node.jsonRpcListenPort().map(Object::toString).get());
      params.add("--rpc-http-api");
      params.add(apiList(node.jsonRpcConfiguration().getRpcApis()));
      if (!node.jsonRpcConfiguration().getNoAuthRpcApis().isEmpty()) {
        params.add("--rpc-http-api-methods-no-auth");
        params.add(apiList(node.jsonRpcConfiguration().getNoAuthRpcApis()));
      }
      if (node.jsonRpcConfiguration().isAuthenticationEnabled()) {
        params.add("--rpc-http-authentication-enabled");
      }
      if (node.jsonRpcConfiguration().getAuthenticationCredentialsFile() != null) {
        params.add("--rpc-http-authentication-credentials-file");
        params.add(node.jsonRpcConfiguration().getAuthenticationCredentialsFile());
      }
      if (node.jsonRpcConfiguration().getAuthenticationPublicKeyFile() != null) {
        params.add("--rpc-http-authentication-jwt-public-key-file");
        params.add(node.jsonRpcConfiguration().getAuthenticationPublicKeyFile().getAbsolutePath());
      }
      if (node.jsonRpcConfiguration().getAuthenticationAlgorithm() != null) {
        params.add("--rpc-http-authentication-jwt-algorithm");
        params.add(node.jsonRpcConfiguration().getAuthenticationAlgorithm().toString());
      }
    }

    if (node.isEngineRpcEnabled()) {
      params.add("--engine-rpc-port");
      params.add(node.jsonEngineListenPort().get().toString());

      if (node.isEngineAuthDisabled()) {
        params.add("--engine-jwt-disabled");
      }
    }

    if (node.wsRpcEnabled()) {
      params.add("--rpc-ws-enabled");
      params.add("--rpc-ws-host");
      params.add(node.wsRpcListenHost().get());
      params.add("--rpc-ws-port");
      params.add(node.wsRpcListenPort().map(Object::toString).get());
      params.add("--rpc-ws-api");
      params.add(apiList(node.webSocketConfiguration().getRpcApis()));
      if (!node.webSocketConfiguration().getRpcApisNoAuth().isEmpty()) {
        params.add("--rpc-ws-api-methods-no-auth");
        params.add(apiList(node.webSocketConfiguration().getRpcApisNoAuth()));
      }
      if (node.webSocketConfiguration().isAuthenticationEnabled()) {
        params.add("--rpc-ws-authentication-enabled");
      }
      if (node.webSocketConfiguration().getAuthenticationCredentialsFile() != null) {
        params.add("--rpc-ws-authentication-credentials-file");
        params.add(node.webSocketConfiguration().getAuthenticationCredentialsFile());
      }
      if (node.webSocketConfiguration().getAuthenticationPublicKeyFile() != null) {
        params.add("--rpc-ws-authentication-jwt-public-key-file");
        params.add(
            node.webSocketConfiguration().getAuthenticationPublicKeyFile().getAbsolutePath());
      }
      if (node.webSocketConfiguration().getAuthenticationAlgorithm() != null) {
        params.add("--rpc-ws-authentication-jwt-algorithm");
        params.add(node.webSocketConfiguration().getAuthenticationAlgorithm().toString());
      }
    }

    if (node.isJsonRpcIpcEnabled()) {
      final JsonRpcIpcConfiguration ipcConfiguration = node.jsonRpcIpcConfiguration();
      params.add("--Xrpc-ipc-enabled");
      params.add("--Xrpc-ipc-path");
      params.add(ipcConfiguration.getPath().toString());
      params.add("--Xrpc-ipc-apis");
      params.add(String.join(",", ipcConfiguration.getEnabledApis()));
    }

    if (node.isMetricsEnabled()) {
      final MetricsConfiguration metricsConfiguration = node.getMetricsConfiguration();
      params.add("--metrics-enabled");
      params.add("--metrics-host");
      params.add(metricsConfiguration.getHost());
      params.add("--metrics-port");
      params.add(Integer.toString(metricsConfiguration.getPort()));
      for (final MetricCategory category : metricsConfiguration.getMetricCategories()) {
        params.add("--metrics-category");
        params.add(((Enum<?>) category).name());
      }
      if (node.isMetricsEnabled() || metricsConfiguration.isPushEnabled()) {
        params.add("--metrics-protocol");
        params.add(metricsConfiguration.getProtocol().name());
      }
      if (metricsConfiguration.isPushEnabled()) {
        params.add("--metrics-push-enabled");
        params.add("--metrics-push-host");
        params.add(metricsConfiguration.getPushHost());
        params.add("--metrics-push-port");
        params.add(Integer.toString(metricsConfiguration.getPushPort()));
        params.add("--metrics-push-interval");
        params.add(Integer.toString(metricsConfiguration.getPushInterval()));
        params.add("--metrics-push-prometheus-job");
        params.add(metricsConfiguration.getPrometheusJob());
      }
    }

    node.getGenesisConfig()
        .ifPresent(
            genesis -> {
              final Path genesisFile = createGenesisFile(node, genesis);
              params.add("--genesis-file");
              params.add(genesisFile.toAbsolutePath().toString());

              // these fields are only relevant to clique which requires a genesis
              params.add("--miner-extra-data");
              params.add(node.getMiningParameters().getExtraData().toHexString());
              params.add("--min-gas-price");
              params.add(
                  Integer.toString(
                      node.getMiningParameters().getMinTransactionGasPrice().intValue()));
            });

    if (node.getMiningParameters().getTargetGasLimit().isPresent()) {
      params.add("--target-gas-limit");
      params.add(Long.toString(node.getMiningParameters().getTargetGasLimit().getAsLong()));
    }

    if (!node.isP2pEnabled()) {
      params.add("--p2p-enabled");
      params.add("false");
    } else {
      final List<String> networkConfigParams =
          NetworkingOptions.fromConfig(node.getNetworkingConfiguration()).getCLIOptions();
      params.addAll(networkConfigParams);
    }

    if (node.isRevertReasonEnabled()) {
      params.add("--revert-reason-enabled");
    }

    params.add("--Xsecp256k1-native-enabled=" + node.isSecp256k1Native());
    params.add("--Xaltbn128-native-enabled=" + node.isAltbn128Native());

    node.getPermissioningConfiguration()
        .flatMap(PermissioningConfiguration::getLocalConfig)
        .ifPresent(
            permissioningConfiguration -> {
              if (permissioningConfiguration.isNodeAllowlistEnabled()) {
                params.add("--permissions-nodes-config-file-enabled");
              }
              if (permissioningConfiguration.getNodePermissioningConfigFilePath() != null) {
                params.add("--permissions-nodes-config-file");
                params.add(permissioningConfiguration.getNodePermissioningConfigFilePath());
              }
              if (permissioningConfiguration.isAccountAllowlistEnabled()) {
                params.add("--permissions-accounts-config-file-enabled");
              }
              if (permissioningConfiguration.getAccountPermissioningConfigFilePath() != null) {
                params.add("--permissions-accounts-config-file");
                params.add(permissioningConfiguration.getAccountPermissioningConfigFilePath());
              }
            });

    params.addAll(node.getExtraCLIOptions());

    params.add("--key-value-storage");
    params.add("rocksdb");

    params.add("--auto-log-bloom-caching-enabled");
    params.add("false");

    final String level = System.getProperty("root.log.level");
    if (level != null) {
      params.add("--logging=" + level);
    }

    if (!node.getRequestedPlugins().isEmpty()) {
      params.add(
          "--plugins=" + node.getRequestedPlugins().stream().collect(Collectors.joining(",")));
    }

    if (node.getPluginConfiguration() != null) {
      final PluginConfiguration pluginConfig = node.getPluginConfiguration();

      if (!pluginConfig.isExternalPluginsEnabled()) {
        params.add("--plugin-external-enabled=false");
      }

      if (pluginConfig.isContinueOnPluginError()) {
        params.add("--continue-on-plugin-error");
      }

      if (pluginConfig.getPluginsVerificationMode()
          != PluginConfiguration.DEFAULT_PLUGINS_VERIFICATION_MODE) {
        params.add(
            "--plugins-verification-mode=" + pluginConfig.getPluginsVerificationMode().name());
      }
    }

    params.addAll(node.getRunCommand());
    return params;
  }

  private boolean isNotAliveOrphan(final String name) {
    final Process orphan = besuProcesses.get(name);
    return orphan == null || !orphan.isAlive();
  }

  private void printOutput(final BesuNode node, final Process process) {
    try (final BufferedReader in =
        new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8))) {

      MDC.put("node", node.getName());

      String line = in.readLine();
      while (line != null) {
        // would be nice to pass up the log level of the incoming log line
        PROCESS_LOG.info(line);
        if (capturingConsole) {
          consoleOut.println(line);
        }
        line = in.readLine();
      }
    } catch (final IOException e) {
      if (besuProcesses.containsKey(node.getName())) {
        LOG.error("Failed to read output from process for node " + node.getName(), e);
      } else {
        LOG.debug("Stdout from process {} closed", node.getName());
      }
    }
  }

  private Path createGenesisFile(final BesuNode node, final String genesisConfig) {
    try {
      final Path genesisFile = Files.createTempFile(node.homeDirectory(), "genesis", "");
      genesisFile.toFile().deleteOnExit();
      Files.writeString(genesisFile, genesisConfig);
      return genesisFile;
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private void createStaticNodes(final BesuNode node) {
    StaticNodesUtils.createStaticNodesFile(node.homeDirectory(), node.getStaticNodes());
  }

  private String apiList(final Collection<String> rpcApis) {
    return String.join(",", rpcApis);
  }

  private void waitForFileOrExit(final BesuNode node, final String fileName) {
    final File file = new File(node.homeDirectory().toFile(), fileName);
    Awaitility.waitAtMost(60, TimeUnit.SECONDS)
        .until(
            () -> {
              final Process process = besuProcesses.get(node.getName());
              if (!process.isAlive()) {
                LOG.warn(
                    "Besu process for node {} exited with code {} before writing {}",
                    node.getName(),
                    process.exitValue(),
                    fileName);
                return true;
              }

              try (final Stream<String> s = Files.lines(file.toPath())) {
                return s.findAny().isPresent();
              } catch (NoSuchFileException __) {
                return false;
              }
            });
  }

  @Override
  public void stopNode(final BesuNode node) {
    node.stop();
    if (besuProcesses.containsKey(node.getName())) {
      killBesuProcess(node.getName());
    } else {
      LOG.error("There was a request to stop an unknown node: {}", node.getName());
    }
  }

  @Override
  public synchronized void shutdown() {
    final Set<String> localMap = new HashSet<>(besuProcesses.keySet());
    localMap.forEach(this::killBesuProcess);
    outputProcessorExecutor.shutdown();
    try {
      if (!outputProcessorExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.error("Output processor executor did not shutdown cleanly.");
      }
    } catch (final InterruptedException e) {
      LOG.error("Interrupted while already shutting down", e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public boolean isActive(final String nodeName) {
    final Process process = besuProcesses.get(nodeName);
    return process != null && process.isAlive();
  }

  private void killBesuProcess(final String name) {
    final Process process = besuProcesses.remove(name);
    if (process == null) {
      LOG.error("Process {} wasn't in our list", name);
      return;
    }
    if (!process.isAlive()) {
      LOG.info("Process {} already exited, pid {}", name, process.pid());
      return;
    }

    Stream.concat(process.descendants(), Stream.of(process.toHandle()))
        .peek(
            processHandle ->
                LOG.info("Killing {} process, pid {}", processHandle.info(), processHandle.pid()))
        .forEach(ProcessHandle::destroy);
    try {
      process.waitFor(30, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      LOG.warn("Wait for death of process {} was interrupted", name, e);
    }

    if (process.isAlive()) {
      LOG.warn("Process {} still alive, destroying forcibly now, pid {}", name, process.pid());
      try {
        process.destroyForcibly().waitFor(30, TimeUnit.SECONDS);
      } catch (final Exception e) {
        // just die already
      }
      LOG.info("Process exited with code {}", process.exitValue());
    }
  }

  @Override
  public void startConsoleCapture() {
    consoleContents.reset();
    capturingConsole = true;
  }

  @Override
  public String getConsoleContents() {
    capturingConsole = false;
    return consoleContents.toString(UTF_8);
  }
}
