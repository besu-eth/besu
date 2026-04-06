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
package org.hyperledger.besu.cli;

import org.hyperledger.besu.ethereum.chain.ChainDataPruner.ChainPruningStrategy;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions;
import org.hyperledger.besu.services.BesuPluginContextImpl;
import org.hyperledger.besu.util.BesuVersionUtils;
import org.hyperledger.besu.util.log.FramedLogMessage;
import org.hyperledger.besu.util.platform.PlatformDetector;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import oshi.PlatformEnum;
import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;

/** The Configuration overview builder. */
public class ConfigurationOverviewBuilder {
  @SuppressWarnings("PrivateStaticFinalLoggers")
  private final Logger logger;

  private String network;
  private BigInteger networkId;
  private String profile;
  private boolean hasCustomGenesis;
  private String customGenesisFileName;
  private String dataStorage;
  private String syncMode;
  private Integer syncMinPeers;
  private Integer rpcPort;
  private Collection<String> rpcHttpApis;
  private Integer enginePort;
  private Collection<String> engineApis;
  private String engineJwtFilePath;
  private boolean isHighSpec = false;
  private boolean isLimitTrieLogsEnabled = false;
  private long trieLogRetentionLimit = 0;
  private Integer trieLogsPruningWindowSize = null;
  private boolean isDiscoveryEnabled = true;
  private boolean isSnapServerEnabled = false;
  private TransactionPoolConfiguration.Implementation txPoolImplementation;
  private EvmConfiguration.WorldUpdaterMode worldStateUpdateMode;
  private boolean enabledOpcodeOptimizations;
  private boolean evmV2 = false;
  private Map<String, String> environment;
  private BesuPluginContextImpl besuPluginContext;
  private boolean isHistoryExpiryPruneEnabled = false;
  private boolean isParallelTxProcessingEnabled = false;
  private ChainPruningStrategy chainPruningStrategy = ChainPruningStrategy.NONE;
  private Long chainPruningBlocksRetained;
  private Long chainPruningBalsRetained;

  private RocksDBCLIOptions.BlobDBSettings blobDBSettings;
  private Long targetGasLimit;
  private Integer maxBlobsPerTransaction;
  private Integer maxBlobsPerBlock;
  private static final String SNAP_SYNC_MODE = "SNAP";

  /**
   * Create a new ConfigurationOverviewBuilder.
   *
   * @param logger the logger
   */
  public ConfigurationOverviewBuilder(final Logger logger) {
    this.logger = logger;
  }

  /**
   * Sets network.
   *
   * @param network the network
   * @return the network
   */
  public ConfigurationOverviewBuilder setNetwork(final String network) {
    this.network = network;
    return this;
  }

  /**
   * Sets whether a networkId has been specified
   *
   * @param networkId the specified networkId
   * @return the builder
   */
  public ConfigurationOverviewBuilder setNetworkId(final BigInteger networkId) {
    this.networkId = networkId;
    return this;
  }

  /**
   * Sets profile.
   *
   * @param profile the profile
   * @return the profile
   */
  public ConfigurationOverviewBuilder setProfile(final String profile) {
    this.profile = profile;
    return this;
  }

  /**
   * Sets whether a custom genesis has been specified.
   *
   * @param hasCustomGenesis a boolean representing whether a custom genesis file was specified
   * @return the builder
   */
  public ConfigurationOverviewBuilder setHasCustomGenesis(final boolean hasCustomGenesis) {
    this.hasCustomGenesis = hasCustomGenesis;
    return this;
  }

  /**
   * Sets location of custom genesis file specified.
   *
   * @param customGenesisFileName the filename of the custom genesis file, only set if specified
   * @return the builder
   */
  public ConfigurationOverviewBuilder setCustomGenesis(final String customGenesisFileName) {
    this.customGenesisFileName = customGenesisFileName;
    return this;
  }

  /**
   * Sets data storage.
   *
   * @param dataStorage the data storage
   * @return the builder
   */
  public ConfigurationOverviewBuilder setDataStorage(final String dataStorage) {
    this.dataStorage = dataStorage;
    return this;
  }

  /**
   * Sets sync mode.
   *
   * @param syncMode the sync mode
   * @return the builder
   */
  public ConfigurationOverviewBuilder setSyncMode(final String syncMode) {
    this.syncMode = syncMode;
    return this;
  }

  /**
   * Sets sync min peers.
   *
   * @param syncMinPeers number of min peers for sync
   * @return the builder
   */
  public ConfigurationOverviewBuilder setSyncMinPeers(final int syncMinPeers) {
    this.syncMinPeers = syncMinPeers;
    return this;
  }

  /**
   * Sets rpc port.
   *
   * @param rpcPort the rpc port
   * @return the builder
   */
  public ConfigurationOverviewBuilder setRpcPort(final Integer rpcPort) {
    this.rpcPort = rpcPort;
    return this;
  }

  /**
   * Sets rpc http apis.
   *
   * @param rpcHttpApis the rpc http apis
   * @return the builder
   */
  public ConfigurationOverviewBuilder setRpcHttpApis(final Collection<String> rpcHttpApis) {
    this.rpcHttpApis = rpcHttpApis;
    return this;
  }

  /**
   * Sets engine port.
   *
   * @param enginePort the engine port
   * @return the builder
   */
  public ConfigurationOverviewBuilder setEnginePort(final Integer enginePort) {
    this.enginePort = enginePort;
    return this;
  }

  /**
   * Sets engine apis.
   *
   * @param engineApis the engine apis
   * @return the builder
   */
  public ConfigurationOverviewBuilder setEngineApis(final Collection<String> engineApis) {
    this.engineApis = engineApis;
    return this;
  }

  /**
   * Sets high spec enabled.
   *
   * @return the builder
   */
  public ConfigurationOverviewBuilder setHighSpecEnabled() {
    isHighSpec = true;
    return this;
  }

  /**
   * Sets limit trie logs enabled
   *
   * @return the builder
   */
  public ConfigurationOverviewBuilder setLimitTrieLogsEnabled() {
    isLimitTrieLogsEnabled = true;
    return this;
  }

  /**
   * Sets trie log retention limit
   *
   * @param limit the number of blocks to retain trie logs for
   * @return the builder
   */
  public ConfigurationOverviewBuilder setTrieLogRetentionLimit(final long limit) {
    trieLogRetentionLimit = limit;
    return this;
  }

  /**
   * Sets discovery enabled/disabled
   *
   * @param discoveryEnabled bool to indicate if discovery is enabled
   * @return the builder
   */
  public ConfigurationOverviewBuilder setDiscoveryEnabled(final boolean discoveryEnabled) {
    isDiscoveryEnabled = discoveryEnabled;
    return this;
  }

  /**
   * Sets snap server enabled/disabled
   *
   * @param snapServerEnabled bool to indicate if snap server is enabled
   * @return the builder
   */
  public ConfigurationOverviewBuilder setSnapServerEnabled(final boolean snapServerEnabled) {
    isSnapServerEnabled = snapServerEnabled;
    return this;
  }

  /**
   * Sets trie logs pruning window size
   *
   * @param size the max number of blocks to load and prune trie logs for at startup
   * @return the builder
   */
  public ConfigurationOverviewBuilder setTrieLogsPruningWindowSize(final int size) {
    trieLogsPruningWindowSize = size;
    return this;
  }

  /**
   * Sets the txpool implementation in use.
   *
   * @param implementation the txpool implementation
   * @return the builder
   */
  public ConfigurationOverviewBuilder setTxPoolImplementation(
      final TransactionPoolConfiguration.Implementation implementation) {
    txPoolImplementation = implementation;
    return this;
  }

  /**
   * Sets the world state updater mode
   *
   * @param worldStateUpdateMode the world state updater mode
   * @return the builder
   */
  public ConfigurationOverviewBuilder setWorldStateUpdateMode(
      final EvmConfiguration.WorldUpdaterMode worldStateUpdateMode) {
    this.worldStateUpdateMode = worldStateUpdateMode;
    return this;
  }

  /**
   * Whether opcodes optimizations are enabled or not.
   *
   * @param enabledOpcodeOptimizations flag that tells whether optimizations are enabled.
   * @return the builder
   */
  public ConfigurationOverviewBuilder setEnabledOpcodeOptimizations(
      final boolean enabledOpcodeOptimizations) {
    this.enabledOpcodeOptimizations = enabledOpcodeOptimizations;
    return this;
  }

  /**
   * Sets whether the experimental EVM v2 (long[] stack) is enabled.
   *
   * @param evmV2 true if --Xevm-go-fast / --Xevm-v2 is enabled
   * @return the builder
   */
  public ConfigurationOverviewBuilder setEvmV2(final boolean evmV2) {
    this.evmV2 = evmV2;
    return this;
  }

  /**
   * Sets the engine jwt file path.
   *
   * @param engineJwtFilePath the engine apis
   * @return the builder
   */
  public ConfigurationOverviewBuilder setEngineJwtFile(final String engineJwtFilePath) {
    this.engineJwtFilePath = engineJwtFilePath;
    return this;
  }

  /**
   * Sets the environment variables.
   *
   * @param environment the environment variables
   * @return the builder
   */
  public ConfigurationOverviewBuilder setEnvironment(final Map<String, String> environment) {
    this.environment = environment;
    return this;
  }

  /**
   * set the plugin context
   *
   * @param besuPluginContext the plugin context
   * @return the builder
   */
  public ConfigurationOverviewBuilder setPluginContext(
      final BesuPluginContextImpl besuPluginContext) {
    this.besuPluginContext = besuPluginContext;
    return this;
  }

  /**
   * Sets the history expiry prune enabled.
   *
   * @param isHistoryExpiryPruneEnabled the history expiry prune enabled
   * @return the builder
   */
  public ConfigurationOverviewBuilder setHistoryExpiryPruneEnabled(
      final boolean isHistoryExpiryPruneEnabled) {
    this.isHistoryExpiryPruneEnabled = isHistoryExpiryPruneEnabled;
    return this;
  }

  /**
   * Sets the blob db settings.
   *
   * @param blobDBSettings the blob db settings
   * @return the builder
   */
  public ConfigurationOverviewBuilder setBlobDBSettings(
      final RocksDBCLIOptions.BlobDBSettings blobDBSettings) {
    this.blobDBSettings = blobDBSettings;
    return this;
  }

  /**
   * Sets the parallel transaction processing enabled.
   *
   * @param isParallelTxProcessingEnabled parallel transaction processing enabled
   * @return the builder
   */
  public ConfigurationOverviewBuilder setParallelTxProcessingEnabled(
      final boolean isParallelTxProcessingEnabled) {
    this.isParallelTxProcessingEnabled = isParallelTxProcessingEnabled;
    return this;
  }

  /**
   * Sets the target gas limit.
   *
   * @param targetGasLimit the target gas limit
   * @return the builder
   */
  public ConfigurationOverviewBuilder setTargetGasLimit(final Long targetGasLimit) {
    this.targetGasLimit = targetGasLimit;
    return this;
  }

  /**
   * Sets the max blobs per transaction.
   *
   * @param maxBlobsPerTransaction the max blobs per transaction
   * @return the builder
   */
  public ConfigurationOverviewBuilder setMaxBlobsPerTransaction(
      final Integer maxBlobsPerTransaction) {
    this.maxBlobsPerTransaction = maxBlobsPerTransaction;
    return this;
  }

  /**
   * Sets the max blobs per block for block building.
   *
   * @param maxBlobsPerBlock the max blobs per block
   * @return the builder
   */
  public ConfigurationOverviewBuilder setMaxBlobsPerBlock(final Integer maxBlobsPerBlock) {
    this.maxBlobsPerBlock = maxBlobsPerBlock;
    return this;
  }

  /**
   * Sets the chain pruning configuration.
   *
   * @param pruningStrategy the chain pruning strategy
   * @param blocksRetained the number of blocks to retain
   * @param balsRetained the number of BALs to retain
   * @return the builder
   */
  public ConfigurationOverviewBuilder setChainPruningEnabled(
      final ChainPruningStrategy pruningStrategy,
      final Long blocksRetained,
      final Long balsRetained) {
    this.chainPruningStrategy = pruningStrategy;
    this.chainPruningBlocksRetained = blocksRetained;
    this.chainPruningBalsRetained = balsRetained;
    return this;
  }

  /**
   * Build configuration overview.
   *
   * @return the string representing configuration overview
   */
  public String build() {
    final List<String> lines = new ArrayList<>();
    lines.add("Besu version " + BesuVersionUtils.shortVersion());
    lines.add("");
    lines.add("Configuration:");

    // Don't include the default network if a genesis file has been supplied
    if (network != null && !hasCustomGenesis) {
      lines.add("Network: " + network);
    }

    if (hasCustomGenesis) {
      lines.add("Network: Custom genesis file");
      lines.add(
          customGenesisFileName == null ? "Custom genesis file is null" : customGenesisFileName);
    }

    if (networkId != null) {
      lines.add("Network Id: " + networkId);
    }

    if (profile != null) {
      lines.add("Profile: " + profile);
    }

    if (dataStorage != null) {
      lines.add("Data storage: " + dataStorage);
    }

    if (syncMode != null) {
      lines.add("Sync mode: " + syncMode);
      if (syncMode.equalsIgnoreCase(SNAP_SYNC_MODE)) {
        final String snapServerStatus = isSnapServerEnabled ? "enabled" : "disabled";
        lines.add("  SNAP Sync server " + snapServerStatus);
      }
    }

    if (syncMinPeers != null) {
      lines.add("Sync min peers: " + syncMinPeers);
    }

    if (!isDiscoveryEnabled) {
      lines.add("P2P Discovery: disabled");
    }

    if (rpcHttpApis != null) {
      lines.add("RPC HTTP APIs: " + String.join(",", rpcHttpApis));
    }
    if (rpcPort != null) {
      lines.add("RPC HTTP port: " + rpcPort);
    }

    if (engineApis != null) {
      lines.add("Engine APIs: " + String.join(",", engineApis));
    }
    if (enginePort != null) {
      lines.add("Engine port: " + enginePort);
    }
    if (engineJwtFilePath != null) {
      lines.add("Engine JWT: " + engineJwtFilePath);
    }

    lines.add("Using " + txPoolImplementation + " transaction pool implementation");

    lines.add("Using " + worldStateUpdateMode + " worldstate update mode");

    if (evmV2) {
      lines.add("Experimental EVM v2 (long[] stack) enabled");
    } else {
      lines.add("Opcode optimizations " + (enabledOpcodeOptimizations ? "enabled" : "disabled"));
    }

    if (isParallelTxProcessingEnabled) {
      lines.add("Parallel transaction processing enabled");
    } else {
      lines.add("Parallel transaction processing disabled");
    }

    if (isLimitTrieLogsEnabled) {
      final StringBuilder trieLogPruningString = new StringBuilder();
      trieLogPruningString
          .append("Limit trie logs enabled: retention: ")
          .append(trieLogRetentionLimit);
      if (trieLogsPruningWindowSize != null) {
        trieLogPruningString.append("; prune window: ").append(trieLogsPruningWindowSize);
      }
      lines.add(trieLogPruningString.toString());
    }

    if (!chainPruningStrategy.equals(ChainPruningStrategy.NONE)) {
      final StringBuilder chainPruningString = new StringBuilder();

      if (chainPruningStrategy.equals(ChainPruningStrategy.ALL)) {
        chainPruningString
            .append("Chain and BAL pruning enabled (retained ")
            .append("BALs: ")
            .append(chainPruningBalsRetained)
            .append("; Blocks: ")
            .append(chainPruningBlocksRetained);
        ;

      } else if (chainPruningStrategy.equals(ChainPruningStrategy.BAL)) {
        chainPruningString
            .append("BAL pruning enabled (retained BALs: ")
            .append(chainPruningBalsRetained);
      }
      chainPruningString.append(")");
      lines.add(chainPruningString.toString());
    }

    if (isHighSpec) {
      lines.add("Experimental high spec configuration enabled");
    }

    if (isHistoryExpiryPruneEnabled) {
      lines.add("History expiry prune enabled");
    }

    if (blobDBSettings != null && blobDBSettings.isBlockchainGarbageCollectionEnabled()) {
      lines.add("Experimental BlobDB BLOCKCHAIN Garbage Collection enabled");
    }
    if (hasCustomBlobDBSettings()) {
      final StringBuilder blobDBString = new StringBuilder();
      blobDBString.append("Experimental BlobDB GC ");
      if (blobDBSettings.blobGarbageCollectionAgeCutoff().isPresent()) {
        blobDBString
            .append("age cutoff: ")
            .append(blobDBSettings.blobGarbageCollectionAgeCutoff().get())
            .append("; ");
      }
      if (blobDBSettings.blobGarbageCollectionForceThreshold().isPresent()) {
        blobDBString
            .append("force threshold: ")
            .append(blobDBSettings.blobGarbageCollectionForceThreshold().get());
      }
      lines.add(blobDBString.toString());
    }

    if (targetGasLimit != null) {
      lines.add("Target Gas Limit: " + normalizeGas(targetGasLimit));
    }

    if (maxBlobsPerTransaction != null) {
      lines.add("Max Blobs Per Transaction: " + maxBlobsPerTransaction);
    }

    if (maxBlobsPerBlock != null) {
      lines.add("Max Blobs Per Block (builder): " + maxBlobsPerBlock);
    }

    lines.add("");
    lines.add("Host:");

    lines.add("Java: " + PlatformDetector.getVM());
    lines.add("Maximum heap size: " + normalizeSize(Runtime.getRuntime().maxMemory()));
    lines.add("OS: " + PlatformDetector.getOS());

    if (SystemInfo.getCurrentPlatform() == PlatformEnum.LINUX) {
      final String glibcVersion = PlatformDetector.getGlibc();
      if (glibcVersion != null) {
        lines.add("glibc: " + glibcVersion);
      }

      detectJemalloc(lines);
    }

    final HardwareAbstractionLayer hardwareInfo = new SystemInfo().getHardware();

    lines.add("Total memory: " + normalizeSize(hardwareInfo.getMemory().getTotal()));
    lines.add("CPU cores: " + hardwareInfo.getProcessor().getLogicalProcessorCount());

    lines.add("");

    if (besuPluginContext != null) {
      lines.addAll(besuPluginContext.getPluginsSummaryLog());
    }

    return FramedLogMessage.generate(lines);
  }

  private boolean hasCustomBlobDBSettings() {
    return blobDBSettings != null
        && (blobDBSettings.blobGarbageCollectionAgeCutoff().isPresent()
            || blobDBSettings.blobGarbageCollectionForceThreshold().isPresent());
  }

  private void detectJemalloc(final List<String> lines) {
    Optional.ofNullable(Objects.isNull(environment) ? null : environment.get("BESU_USING_JEMALLOC"))
        .ifPresentOrElse(
            jemallocEnabled -> {
              try {
                if (Boolean.parseBoolean(jemallocEnabled)) {
                  final String version = PlatformDetector.getJemalloc();
                  lines.add("jemalloc: " + version);
                } else {
                  logger.warn(
                      "besu_using_jemalloc is present but is not set to true, jemalloc library not loaded");
                }
              } catch (final Throwable throwable) {
                logger.warn(
                    "besu_using_jemalloc is present but we failed to load jemalloc library to get the version");
              }
            },
            () -> {
              // in case the user is using jemalloc without BESU_USING_JEMALLOC env var
              try {
                final String version = PlatformDetector.getJemalloc();
                lines.add("jemalloc: " + version);
              } catch (final Throwable throwable) {
                logger.info(
                    "jemalloc library not found, memory usage may be reduced by installing it");
              }
            });
  }

  /**
   * Normalize gas string.<br>
   * The implemented logic is<br>
   * - if the received gas is greater than 1 million, calculates the precision and returns the
   * number as a floating point number with the calculated precision plus an 'M' at the end (e.g.,
   * 50.55M)<br>
   * - if the received gas is lower than 1 million, returns the number as a decimal integer grouping
   * digits by thousands (e.g., 100,000)
   *
   * @param gas the gas
   * @return the formatted string
   */
  static String normalizeGas(final long gas) {
    final double normalizedGas = gas / 1_000_000D;
    if (normalizedGas < 1) {
      return String.format("%,d", gas);
    } else {
      final int decimals =
          (Iterables.get(Splitter.on('.').split(String.valueOf(normalizedGas)), 1)).length();
      final String format = normalizedGas % 1 == 0 ? "%.0fM" : "%." + decimals + "fM";
      return String.format(format, normalizedGas);
    }
  }

  private String normalizeSize(final long size) {
    return String.format("%.02f", (double) (size) / 1024 / 1024 / 1024) + " GB";
  }
}
