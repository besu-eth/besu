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
package org.hyperledger.besu.controller;

import org.hyperledger.besu.cli.config.EthNetworkConfig;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.PowAlgorithm;
import org.hyperledger.besu.config.QbftConfigOptions;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.methods.JsonRpcMethods;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.config.SubProtocolConfiguration;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Besu controller. */
public class BesuController implements java.io.Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BesuController.class);

  /** The constant DATABASE_PATH. */
  public static final String DATABASE_PATH = "database";

  /** The constant CACHE_PATH. */
  public static final String CACHE_PATH = "caches";

  private final ProtocolSchedule protocolSchedule;
  private final ProtocolContext protocolContext;
  private final EthProtocolManager ethProtocolManager;
  private final GenesisConfigOptions genesisConfigOptions;
  private final SubProtocolConfiguration subProtocolConfiguration;
  private final NodeKey nodeKey;
  private final Synchronizer synchronizer;
  private final JsonRpcMethods additionalJsonRpcMethodsFactory;
  private final TransactionPool transactionPool;
  private final MiningCoordinator miningCoordinator;
  private final List<Closeable> closeables;
  private final MiningConfiguration miningConfiguration;
  private final PluginServiceFactory additionalPluginServices;
  private final SyncState syncState;
  private final EthPeers ethPeers;
  private final StorageProvider storageProvider;
  private final DataStorageConfiguration dataStorageConfiguration;
  private final TransactionSimulator transactionSimulator;

  /**
   * Instantiates a new Besu controller.
   *
   * @param protocolSchedule the protocol schedule
   * @param protocolContext the protocol context
   * @param ethProtocolManager the eth protocol manager
   * @param genesisConfigOptions the genesis config options
   * @param subProtocolConfiguration the sub protocol configuration
   * @param synchronizer the synchronizer
   * @param syncState the sync state
   * @param transactionPool the transaction pool
   * @param miningCoordinator the mining coordinator
   * @param miningConfiguration the mining parameters
   * @param additionalJsonRpcMethodsFactory the additional json rpc methods factory
   * @param nodeKey the node key
   * @param closeables the closeables
   * @param additionalPluginServices the additional plugin services
   * @param ethPeers the eth peers
   * @param storageProvider the storage provider
   * @param dataStorageConfiguration the data storage configuration
   * @param transactionSimulator the transaction simulator
   */
  BesuController(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthProtocolManager ethProtocolManager,
      final GenesisConfigOptions genesisConfigOptions,
      final SubProtocolConfiguration subProtocolConfiguration,
      final Synchronizer synchronizer,
      final SyncState syncState,
      final TransactionPool transactionPool,
      final MiningCoordinator miningCoordinator,
      final MiningConfiguration miningConfiguration,
      final JsonRpcMethods additionalJsonRpcMethodsFactory,
      final NodeKey nodeKey,
      final List<Closeable> closeables,
      final PluginServiceFactory additionalPluginServices,
      final EthPeers ethPeers,
      final StorageProvider storageProvider,
      final DataStorageConfiguration dataStorageConfiguration,
      final TransactionSimulator transactionSimulator) {
    this.protocolSchedule = protocolSchedule;
    this.protocolContext = protocolContext;
    this.ethProtocolManager = ethProtocolManager;
    this.genesisConfigOptions = genesisConfigOptions;
    this.subProtocolConfiguration = subProtocolConfiguration;
    this.synchronizer = synchronizer;
    this.syncState = syncState;
    this.additionalJsonRpcMethodsFactory = additionalJsonRpcMethodsFactory;
    this.nodeKey = nodeKey;
    this.transactionPool = transactionPool;
    this.miningCoordinator = miningCoordinator;
    this.closeables = closeables;
    this.miningConfiguration = miningConfiguration;
    this.additionalPluginServices = additionalPluginServices;
    this.ethPeers = ethPeers;
    this.storageProvider = storageProvider;
    this.dataStorageConfiguration = dataStorageConfiguration;
    this.transactionSimulator = transactionSimulator;
  }

  /**
   * Gets protocol context.
   *
   * @return the protocol context
   */
  public ProtocolContext getProtocolContext() {
    return protocolContext;
  }

  /**
   * Gets protocol schedule.
   *
   * @return the protocol schedule
   */
  public ProtocolSchedule getProtocolSchedule() {
    return protocolSchedule;
  }

  /**
   * Gets protocol manager.
   *
   * @return the protocol manager
   */
  public EthProtocolManager getProtocolManager() {
    return ethProtocolManager;
  }

  /**
   * Gets genesis config options.
   *
   * @return the genesis config options
   */
  public GenesisConfigOptions getGenesisConfigOptions() {
    return genesisConfigOptions;
  }

  /**
   * Gets synchronizer.
   *
   * @return the synchronizer
   */
  public Synchronizer getSynchronizer() {
    return synchronizer;
  }

  /**
   * Gets sub protocol configuration.
   *
   * @return the sub protocol configuration
   */
  public SubProtocolConfiguration getSubProtocolConfiguration() {
    return subProtocolConfiguration;
  }

  /**
   * Gets node key.
   *
   * @return the node key
   */
  public NodeKey getNodeKey() {
    return nodeKey;
  }

  /**
   * Gets transaction pool.
   *
   * @return the transaction pool
   */
  public TransactionPool getTransactionPool() {
    return transactionPool;
  }

  /**
   * Gets mining coordinator.
   *
   * @return the mining coordinator
   */
  public MiningCoordinator getMiningCoordinator() {
    return miningCoordinator;
  }

  /**
   * get the collection of eth peers
   *
   * @return the EthPeers collection
   */
  public EthPeers getEthPeers() {
    return ethPeers;
  }

  /**
   * Get the storage provider
   *
   * @return the storage provider
   */
  public StorageProvider getStorageProvider() {
    return storageProvider;
  }

  @Override
  public void close() {
    closeables.forEach(this::tryClose);
  }

  private void tryClose(final Closeable closeable) {
    try {
      closeable.close();
    } catch (final IOException e) {
      LOG.error("Unable to close resource.", e);
    }
  }

  /**
   * Gets mining parameters.
   *
   * @return the mining parameters
   */
  public MiningConfiguration getMiningParameters() {
    return miningConfiguration;
  }

  /**
   * Gets additional json rpc methods.
   *
   * @param enabledRpcApis the enabled rpc apis
   * @return the additional json rpc methods
   */
  public Map<String, JsonRpcMethod> getAdditionalJsonRpcMethods(
      final Collection<String> enabledRpcApis) {
    return additionalJsonRpcMethodsFactory.create(enabledRpcApis);
  }

  /**
   * Gets sync state.
   *
   * @return the sync state
   */
  public SyncState getSyncState() {
    return syncState;
  }

  /**
   * Gets additional plugin services.
   *
   * @return the additional plugin services
   */
  public PluginServiceFactory getAdditionalPluginServices() {
    return additionalPluginServices;
  }

  /**
   * Gets data storage configuration.
   *
   * @return the data storage configuration
   */
  public DataStorageConfiguration getDataStorageConfiguration() {
    return dataStorageConfiguration;
  }

  /**
   * Gets the transaction simulator
   *
   * @return the transaction simulator
   */
  public TransactionSimulator getTransactionSimulator() {
    return transactionSimulator;
  }

  /**
   * Gets the eth scheduler.
   *
   * @return the eth scheduler
   */
  public EthScheduler getEthScheduler() {
    return ethProtocolManager.ethContext().getScheduler();
  }

  /** The type Builder. */
  public static class Builder {
    /** Instantiates a new Builder. */
    public Builder() {}

    /**
     * From eth network config besu controller builder.
     *
     * @param ethNetworkConfig the eth network config
     * @param syncMode The sync mode
     * @return the besu controller builder
     */
    public BesuControllerBuilder fromEthNetworkConfig(
        final EthNetworkConfig ethNetworkConfig, final SyncMode syncMode) {
      return fromGenesisFile(ethNetworkConfig.genesisConfig(), syncMode)
          .networkId(ethNetworkConfig.networkId());
    }

    /**
     * From genesis config besu controller builder.
     *
     * @param genesisConfig the genesis config file
     * @param syncMode the sync mode
     * @return the besu controller builder
     */
    public BesuControllerBuilder fromGenesisFile(
        final GenesisConfig genesisConfig, final SyncMode syncMode) {
      final BesuControllerBuilder builder;
      final var configOptions = genesisConfig.getConfigOptions();

      if (configOptions.isConsensusMigration()) {
        return createConsensusScheduleBesuControllerBuilder(genesisConfig);
      }

      if (configOptions.getPowAlgorithm() != PowAlgorithm.UNSUPPORTED) {
        builder = new MainnetBesuControllerBuilder();
      } else if (configOptions.isIbft2()) {
        builder = new IbftBesuControllerBuilder();
      } else if (configOptions.isIbftLegacy()) {
        throw new IllegalStateException(
            "IBFT1 (legacy) is no longer supported. Consider using IBFT2 or QBFT.");
      } else if (configOptions.isQbft()) {
        builder = new QbftBesuControllerBuilder();
      } else if (configOptions.isClique()) {
        if (configOptions.getTerminalTotalDifficulty().isEmpty()) {
          throw new IllegalStateException(
              """
                 Clique Block Production (mining) is no longer supported.
                 It is still possible to sync existing Clique networks if they are migrated to PoS.
                 """);
        }
        builder = new CliqueBesuControllerBuilder();
      } else {
        throw new IllegalArgumentException("Unknown consensus mechanism defined");
      }

      // wrap with TransitionBesuControllerBuilder if we have a terminal total difficulty:
      if (configOptions.getTerminalTotalDifficulty().isPresent()) {
        // Enable start with vanilla MergeBesuControllerBuilder for PoS checkpoint block
        if (syncMode == SyncMode.SNAP && isCheckpointPoSBlock(configOptions)) {
          return new MergeBesuControllerBuilder().genesisConfig(genesisConfig);
        } else {
          // TODO this should be changed to vanilla MergeBesuControllerBuilder and the Transition*
          // series of classes removed after we successfully transition to PoS
          // https://github.com/hyperledger/besu/issues/2897
          return new TransitionBesuControllerBuilder(builder, new MergeBesuControllerBuilder())
              .genesisConfig(genesisConfig);
        }

      } else return builder.genesisConfig(genesisConfig);
    }

    private BesuControllerBuilder createConsensusScheduleBesuControllerBuilder(
        final GenesisConfig genesisConfig) {
      final Map<Long, BesuControllerBuilder> besuControllerBuilderSchedule = new HashMap<>();
      final var configOptions = genesisConfig.getConfigOptions();

      final BesuControllerBuilder originalControllerBuilder;
      if (configOptions.isIbft2()) {
        originalControllerBuilder = new IbftBesuControllerBuilder();
      } else if (configOptions.isIbftLegacy()) {
        originalControllerBuilder = new IbftLegacyBesuControllerBuilder();
      } else {
        throw new IllegalStateException(
            "Invalid genesis migration config. Migration is supported from IBFT (legacy) or IBFT2 to QBFT)");
      }
      besuControllerBuilderSchedule.put(0L, originalControllerBuilder);

      final QbftConfigOptions qbftConfigOptions = configOptions.getQbftConfigOptions();
      final Long qbftBlock = readQbftStartBlockConfig(qbftConfigOptions);
      besuControllerBuilderSchedule.put(qbftBlock, new QbftBesuControllerBuilder());

      return new ConsensusScheduleBesuControllerBuilder(besuControllerBuilderSchedule)
          .genesisConfig(genesisConfig);
    }

    private Long readQbftStartBlockConfig(final QbftConfigOptions qbftConfigOptions) {
      final long startBlock =
          qbftConfigOptions
              .getStartBlock()
              .orElseThrow(
                  () ->
                      new IllegalStateException("Missing QBFT startBlock config in genesis file"));

      if (startBlock <= 0) {
        throw new IllegalStateException("Invalid QBFT startBlock config in genesis file");
      }

      return startBlock;
    }

    private boolean isCheckpointPoSBlock(final GenesisConfigOptions configOptions) {
      final UInt256 terminalTotalDifficulty = configOptions.getTerminalTotalDifficulty().get();

      return configOptions.getCheckpointOptions().isValid()
          && (UInt256.fromHexString(configOptions.getCheckpointOptions().getTotalDifficulty().get())
              .greaterThan(terminalTotalDifficulty));
    }
  }
}
