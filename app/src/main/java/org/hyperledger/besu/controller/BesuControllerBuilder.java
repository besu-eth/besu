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

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.chainimport.BlockHeadersCachePreload;
import org.hyperledger.besu.components.BesuComponent;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.UnverifiedForkchoiceSupplier;
import org.hyperledger.besu.consensus.qbft.BFTPivotSelectorFromPeers;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ConsensusContext;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.methods.JsonRpcMethods;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.BlockchainStorage;
import org.hyperledger.besu.ethereum.chain.ChainDataPruner;
import org.hyperledger.besu.ethereum.chain.ChainDataPruner.ChainPruningStrategy;
import org.hyperledger.besu.ethereum.chain.ChainDataPruner.PruningMode;
import org.hyperledger.besu.ethereum.chain.ChainDataPrunerStorage;
import org.hyperledger.besu.ethereum.chain.ChainPrunerConfiguration;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.chain.VariablesStorage;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.MergePeerFilter;
import org.hyperledger.besu.ethereum.eth.manager.MonitoredExecutors;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskRequestSender;
import org.hyperledger.besu.ethereum.eth.manager.snap.SnapProtocolManager;
import org.hyperledger.besu.ethereum.eth.peervalidation.DaoForkPeerValidator;
import org.hyperledger.besu.ethereum.eth.peervalidation.PeerValidator;
import org.hyperledger.besu.ethereum.eth.peervalidation.RequiredBlocksPeerValidator;
import org.hyperledger.besu.ethereum.eth.sync.DefaultSynchronizer;
import org.hyperledger.besu.ethereum.eth.sync.PivotBlockSelector;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSelectorFromPeers;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSelectorFromSafeBlock;
import org.hyperledger.besu.ethereum.eth.sync.common.SingleBlockHeaderDownloader;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.ImmutableCheckpoint;
import org.hyperledger.besu.ethereum.eth.sync.fullsync.SyncTerminationCondition;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolFactory;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.SubProtocolConfiguration;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.trie.forest.ForestWorldStateArchive;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiArchiveWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiArchiver;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive.BonsaiFlatDbToArchiveMigrator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogPruner;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive.WorldStateHealer;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.permissioning.NodeMessagePermissioningProvider;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.WorldStatePreimageStorage;
import org.hyperledger.besu.services.BesuPluginContextImpl;

import java.io.Closeable;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Besu controller builder that builds Besu Controller. */
public abstract class BesuControllerBuilder implements MiningConfigurationOverrides {
  private static final Logger LOG = LoggerFactory.getLogger(BesuControllerBuilder.class);

  /** The genesis file */
  protected GenesisConfig genesisConfig;

  /** The genesis config options; */
  protected GenesisConfigOptions genesisConfigOptions;

  /** The is genesis state hash from data. */
  protected boolean genesisStateHashCacheEnabled;

  /** The Sync config. */
  protected SynchronizerConfiguration syncConfig;

  /** The Ethereum wire protocol configuration. */
  protected EthProtocolConfiguration ethereumWireProtocolConfiguration;

  /** The Transaction pool configuration. */
  protected TransactionPoolConfiguration transactionPoolConfiguration;

  /** The Network id. */
  protected BigInteger networkId;

  /** The Mining parameters. */
  protected MiningConfiguration miningConfiguration;

  /** The Metrics system. */
  protected ObservableMetricsSystem metricsSystem;

  /** The Data directory. */
  protected Path dataDirectory;

  /** The Clock. */
  protected Clock clock;

  /** The Node key. */
  protected NodeKey nodeKey;

  /** The Is revert reason enabled. */
  protected boolean isRevertReasonEnabled;

  /** The Storage provider. */
  protected StorageProvider storageProvider;

  /** The Required blocks. */
  protected Map<Long, Hash> requiredBlocks = Collections.emptyMap();

  /** The Reorg logging threshold. */
  protected long reorgLoggingThreshold;

  /** The Data storage configuration. */
  protected DataStorageConfiguration dataStorageConfiguration =
      DataStorageConfiguration.DEFAULT_CONFIG;

  /** The Message permissioning providers. */
  protected List<NodeMessagePermissioningProvider> messagePermissioningProviders =
      Collections.emptyList();

  /** The Evm configuration. */
  protected EvmConfiguration evmConfiguration;

  /** The Max peers. */
  protected int maxPeers;

  /** Manages a cache of bad blocks globally */
  protected final BadBlockManager badBlockManager = new BadBlockManager();

  private int maxRemotelyInitiatedPeers;

  /** The Chain pruner configuration. */
  protected ChainPrunerConfiguration chainPrunerConfiguration = ChainPrunerConfiguration.DEFAULT;

  private NetworkingConfiguration networkingConfiguration;
  private Boolean randomPeerPriority;

  /** the Dagger configured context that can provide dependencies */
  protected Optional<BesuComponent> besuComponent = Optional.empty();

  private int numberOfBlocksToCache = 0;
  private int numberOfBlockHeadersToCache = 0;
  private boolean isCacheLastBlockHeadersPreloadEnabled;

  /** whether parallel transaction processing is enabled or not */
  protected boolean isParallelTxProcessingEnabled;

  /** Configuration flags related to block access lists. */
  protected BalConfiguration balConfiguration = BalConfiguration.DEFAULT;

  /** The API configuration */
  protected ApiConfiguration apiConfiguration;

  /** The transaction simulator */
  protected TransactionSimulator transactionSimulator;

  /** When enabled, round changes on f+1 RC messages from higher rounds */
  protected boolean isEarlyRoundChangeEnabled = false;

  /** The global code cache */
  protected CodeCache codeCache;

  /** Instantiates a new Besu controller builder. */
  protected BesuControllerBuilder() {}

  /**
   * Provide a BesuComponent which can be used to get other dependencies
   *
   * @param besuComponent application context that can be used to get other dependencies
   * @return the besu controller builder
   */
  public BesuControllerBuilder besuComponent(final BesuComponent besuComponent) {
    this.besuComponent = Optional.ofNullable(besuComponent);
    return this;
  }

  /**
   * Storage provider besu controller builder.
   *
   * @param storageProvider the storage provider
   * @return the besu controller builder
   */
  public BesuControllerBuilder storageProvider(final StorageProvider storageProvider) {
    this.storageProvider = storageProvider;
    return this;
  }

  /**
   * Genesis config file besu controller builder.
   *
   * @param genesisConfig the genesis config
   * @return the besu controller builder
   */
  public BesuControllerBuilder genesisConfig(final GenesisConfig genesisConfig) {
    this.genesisConfig = genesisConfig;
    this.genesisConfigOptions = genesisConfig.getConfigOptions();
    return this;
  }

  /**
   * Genesis state hash from data besu controller builder.
   *
   * @param genesisStateHashCacheEnabled the is genesis state hash from data
   * @return the besu controller builder
   */
  public BesuControllerBuilder genesisStateHashCacheEnabled(
      final Boolean genesisStateHashCacheEnabled) {
    this.genesisStateHashCacheEnabled = genesisStateHashCacheEnabled;
    return this;
  }

  /**
   * Synchronizer configuration besu controller builder.
   *
   * @param synchronizerConfig the synchronizer config
   * @return the besu controller builder
   */
  public BesuControllerBuilder synchronizerConfiguration(
      final SynchronizerConfiguration synchronizerConfig) {
    this.syncConfig = synchronizerConfig;
    return this;
  }

  /**
   * API configuration besu controller builder.
   *
   * @param apiConfiguration the API config
   * @return the besu controller builder
   */
  public BesuControllerBuilder apiConfiguration(final ApiConfiguration apiConfiguration) {
    this.apiConfiguration = apiConfiguration;
    return this;
  }

  /**
   * Eth protocol configuration besu controller builder.
   *
   * @param ethProtocolConfiguration the eth protocol configuration
   * @return the besu controller builder
   */
  public BesuControllerBuilder ethProtocolConfiguration(
      final EthProtocolConfiguration ethProtocolConfiguration) {
    this.ethereumWireProtocolConfiguration = ethProtocolConfiguration;
    return this;
  }

  /**
   * Network id besu controller builder.
   *
   * @param networkId the network id
   * @return the besu controller builder
   */
  public BesuControllerBuilder networkId(final BigInteger networkId) {
    this.networkId = networkId;
    return this;
  }

  /**
   * Mining parameters besu controller builder.
   *
   * @param miningConfiguration the mining parameters
   * @return the besu controller builder
   */
  public BesuControllerBuilder miningParameters(final MiningConfiguration miningConfiguration) {
    this.miningConfiguration = miningConfiguration;
    return this;
  }

  /**
   * Message permissioning providers besu controller builder.
   *
   * @param messagePermissioningProviders the message permissioning providers
   * @return the besu controller builder
   */
  public BesuControllerBuilder messagePermissioningProviders(
      final List<NodeMessagePermissioningProvider> messagePermissioningProviders) {
    this.messagePermissioningProviders = messagePermissioningProviders;
    return this;
  }

  /**
   * Node key besu controller builder.
   *
   * @param nodeKey the node key
   * @return the besu controller builder
   */
  public BesuControllerBuilder nodeKey(final NodeKey nodeKey) {
    this.nodeKey = nodeKey;
    return this;
  }

  /**
   * Metrics system besu controller builder.
   *
   * @param metricsSystem the metrics system
   * @return the besu controller builder
   */
  public BesuControllerBuilder metricsSystem(final ObservableMetricsSystem metricsSystem) {
    this.metricsSystem = metricsSystem;
    return this;
  }

  /**
   * Data directory besu controller builder.
   *
   * @param dataDirectory the data directory
   * @return the besu controller builder
   */
  public BesuControllerBuilder dataDirectory(final Path dataDirectory) {
    this.dataDirectory = dataDirectory;
    return this;
  }

  /**
   * Clock besu controller builder.
   *
   * @param clock the clock
   * @return the besu controller builder
   */
  public BesuControllerBuilder clock(final Clock clock) {
    this.clock = clock;
    return this;
  }

  /**
   * Transaction pool configuration besu controller builder.
   *
   * @param transactionPoolConfiguration the transaction pool configuration
   * @return the besu controller builder
   */
  public BesuControllerBuilder transactionPoolConfiguration(
      final TransactionPoolConfiguration transactionPoolConfiguration) {
    this.transactionPoolConfiguration = transactionPoolConfiguration;
    return this;
  }

  /**
   * Is revert reason enabled besu controller builder.
   *
   * @param isRevertReasonEnabled the is revert reason enabled
   * @return the besu controller builder
   */
  public BesuControllerBuilder isRevertReasonEnabled(final boolean isRevertReasonEnabled) {
    this.isRevertReasonEnabled = isRevertReasonEnabled;
    return this;
  }

  /**
   * Required blocks besu controller builder.
   *
   * @param requiredBlocks the required blocks
   * @return the besu controller builder
   */
  public BesuControllerBuilder requiredBlocks(final Map<Long, Hash> requiredBlocks) {
    this.requiredBlocks = requiredBlocks;
    return this;
  }

  /**
   * Reorg logging threshold besu controller builder.
   *
   * @param reorgLoggingThreshold the reorg logging threshold
   * @return the besu controller builder
   */
  public BesuControllerBuilder reorgLoggingThreshold(final long reorgLoggingThreshold) {
    this.reorgLoggingThreshold = reorgLoggingThreshold;
    return this;
  }

  /**
   * Data storage configuration besu controller builder.
   *
   * @param dataStorageConfiguration the data storage configuration
   * @return the besu controller builder
   */
  public BesuControllerBuilder dataStorageConfiguration(
      final DataStorageConfiguration dataStorageConfiguration) {
    this.dataStorageConfiguration = dataStorageConfiguration;
    return this;
  }

  /**
   * Evm configuration besu controller builder.
   *
   * @param evmConfiguration the evm configuration
   * @return the besu controller builder
   */
  public BesuControllerBuilder evmConfiguration(final EvmConfiguration evmConfiguration) {
    this.evmConfiguration = evmConfiguration;
    return this;
  }

  /**
   * Max peers besu controller builder.
   *
   * @param maxPeers the max peers
   * @return the besu controller builder
   */
  public BesuControllerBuilder maxPeers(final int maxPeers) {
    this.maxPeers = maxPeers;
    return this;
  }

  /**
   * Maximum number of remotely initiated peer connections
   *
   * @param maxRemotelyInitiatedPeers maximum number of remotely initiated peer connections
   * @return the besu controller builder
   */
  public BesuControllerBuilder maxRemotelyInitiatedPeers(final int maxRemotelyInitiatedPeers) {
    this.maxRemotelyInitiatedPeers = maxRemotelyInitiatedPeers;
    return this;
  }

  /**
   * Chain pruning configuration besu controller builder.
   *
   * @param chainPrunerConfiguration the chain pruner configuration
   * @return the besu controller builder
   */
  public BesuControllerBuilder chainPruningConfiguration(
      final ChainPrunerConfiguration chainPrunerConfiguration) {
    this.chainPrunerConfiguration = chainPrunerConfiguration;
    return this;
  }

  /**
   * Sets the number of blocks to cache.
   *
   * @param numberOfBlocksToCache the number of blocks to cache
   * @return the besu controller builder
   */
  public BesuControllerBuilder cacheLastBlocks(final Integer numberOfBlocksToCache) {
    this.numberOfBlocksToCache = numberOfBlocksToCache;
    return this;
  }

  /**
   * Sets the number of block headers to cache.
   *
   * @param numberOfBlockHeadersToCache the number of block headers to cache
   * @return the besu controller builder
   */
  public BesuControllerBuilder cacheLastBlockHeaders(final Integer numberOfBlockHeadersToCache) {
    this.numberOfBlockHeadersToCache = numberOfBlockHeadersToCache;
    return this;
  }

  /**
   * Sets whether the block header cache should be preloaded.
   *
   * @param isCacheLastBlockHeadersPreloadEnabled {@code true} to enable preloading of the block
   *     header cache, {@code false} to disable it
   * @return this builder instance
   */
  public BesuControllerBuilder isCacheLastBlockHeadersPreloadEnabled(
      final Boolean isCacheLastBlockHeadersPreloadEnabled) {
    this.isCacheLastBlockHeadersPreloadEnabled = isCacheLastBlockHeadersPreloadEnabled;
    return this;
  }

  /**
   * sets the networkConfiguration in the builder
   *
   * @param networkingConfiguration the networking config
   * @return the besu controller builder
   */
  public BesuControllerBuilder networkConfiguration(
      final NetworkingConfiguration networkingConfiguration) {
    this.networkingConfiguration = networkingConfiguration;
    return this;
  }

  /**
   * sets the randomPeerPriority flag in the builder
   *
   * @param randomPeerPriority the random peer priority flag
   * @return the besu controller builder
   */
  public BesuControllerBuilder randomPeerPriority(final Boolean randomPeerPriority) {
    this.randomPeerPriority = randomPeerPriority;
    return this;
  }

  /**
   * Sets whether parallel transaction processing is enabled. When parallel transaction processing
   * is enabled, transactions within a block can be processed in parallel and potentially improving
   * performance
   *
   * @param isParallelTxProcessingEnabled true to enable parallel transaction
   * @return the besu controller
   */
  public BesuControllerBuilder isParallelTxProcessingEnabled(
      final boolean isParallelTxProcessingEnabled) {
    this.isParallelTxProcessingEnabled = isParallelTxProcessingEnabled;
    return this;
  }

  /**
   * Sets configuration for functionality related to block-level access lists.
   *
   * @param balConfiguration configuration related to block access lists
   * @return the besu controller
   */
  public BesuControllerBuilder balConfiguration(final BalConfiguration balConfiguration) {
    this.balConfiguration = balConfiguration;
    return this;
  }

  /**
   * check if early round change is enabled when f+1 RC messages from higher rounds are received
   *
   * @param isEarlyRoundChangeEnabled whether to enable early round change
   * @return the besu controller
   */
  public BesuControllerBuilder isEarlyRoundChangeEnabled(final boolean isEarlyRoundChangeEnabled) {
    this.isEarlyRoundChangeEnabled = isEarlyRoundChangeEnabled;
    return this;
  }

  /**
   * Build besu controller.
   *
   * @return the besu controller
   */
  public BesuController build() {
    checkNotNull(genesisConfig, "Missing genesis config file");
    checkNotNull(genesisConfigOptions, "Missing genesis config options");
    checkNotNull(syncConfig, "Missing sync config");
    checkNotNull(ethereumWireProtocolConfiguration, "Missing ethereum protocol configuration");
    checkNotNull(networkId, "Missing network ID");
    checkNotNull(miningConfiguration, "Missing mining parameters");
    checkNotNull(metricsSystem, "Missing metrics system");
    checkNotNull(dataDirectory, "Missing data directory"); // Why do we need this?
    checkNotNull(clock, "Missing clock");
    checkNotNull(transactionPoolConfiguration, "Missing transaction pool configuration");
    checkNotNull(nodeKey, "Missing node key");
    checkNotNull(storageProvider, "Must supply a storage provider");
    checkNotNull(evmConfiguration, "Missing evm config");
    checkNotNull(networkingConfiguration, "Missing network configuration");
    checkNotNull(apiConfiguration, "Missing API configuration");
    checkNotNull(dataStorageConfiguration, "Missing data storage configuration");
    checkNotNull(besuComponent, "Must supply a BesuComponent");

    this.codeCache = besuComponent.map(BesuComponent::getCodeCache).orElse(new CodeCache());
    this.codeCache.setupMetricsSystem(metricsSystem);

    prepForBuild();

    final ProtocolSchedule protocolSchedule = createProtocolSchedule();

    final VariablesStorage variablesStorage = storageProvider.createVariablesStorage();

    final WorldStateStorageCoordinator worldStateStorageCoordinator =
        storageProvider.createWorldStateStorageCoordinator(dataStorageConfiguration);

    final BlockchainStorage blockchainStorage =
        storageProvider.createBlockchainStorage(
            protocolSchedule, variablesStorage, dataStorageConfiguration);

    final var maybeStoredGenesisBlockHash = blockchainStorage.getBlockHash(0L);

    final var genesisState =
        getGenesisState(
            maybeStoredGenesisBlockHash.flatMap(blockchainStorage::getBlockHeader),
            protocolSchedule,
            codeCache);

    final EthScheduler scheduler =
        new EthScheduler(
            syncConfig.getDownloaderParallelism(),
            syncConfig.getTransactionsParallelism(),
            syncConfig.getComputationParallelism(),
            metricsSystem);

    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisState.getBlock(),
            blockchainStorage,
            metricsSystem,
            reorgLoggingThreshold,
            dataDirectory.toString(),
            numberOfBlocksToCache,
            numberOfBlockHeadersToCache);

    if (isCacheLastBlockHeadersPreloadEnabled && numberOfBlockHeadersToCache > 0) {
      LOG.info(
          "--cache-last-block-headers and --cache-last-block-headers-preload-enabled are enabled, start preloading block headers cache");
      preloadBlockHeaderCache(blockchain, scheduler);
    }

    final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader =
        besuComponent
            .map(BesuComponent::getCachedMerkleTrieLoader)
            .orElseGet(() -> new BonsaiCachedMerkleTrieLoader(metricsSystem));

    final var worldStateHealerSupplier = new AtomicReference<WorldStateHealer>();

    final WorldStateArchive worldStateArchive =
        createWorldStateArchive(
            worldStateStorageCoordinator,
            blockchain,
            bonsaiCachedMerkleTrieLoader,
            worldStateHealerSupplier::get);

    if (maybeStoredGenesisBlockHash.isEmpty()) {
      genesisState.writeStateTo(worldStateArchive.getWorldState());
    }

    transactionSimulator =
        new TransactionSimulator(
            blockchain,
            worldStateArchive,
            protocolSchedule,
            miningConfiguration,
            apiConfiguration.getGasCap());

    final var consensusContext =
        createConsensusContext(blockchain, worldStateArchive, protocolSchedule);

    final ProtocolContext protocolContext =
        createProtocolContext(
            blockchain,
            worldStateArchive,
            consensusContext,
            besuComponent
                .map(BesuComponent::getBesuPluginContext)
                .orElse(new BesuPluginContextImpl()));
    validateContext(protocolContext);

    final int maxMessageSize = ethereumWireProtocolConfiguration.getMaxMessageSize();
    final Supplier<ProtocolSpec> currentProtocolSpecSupplier =
        () -> protocolSchedule.getByBlockHeader(blockchain.getChainHeadHeader());
    final ForkIdManager forkIdManager =
        new ForkIdManager(
            blockchain,
            genesisConfigOptions.getForkBlockNumbers(),
            genesisConfigOptions.getForkBlockTimestamps());
    final EthPeers ethPeers =
        new EthPeers(
            currentProtocolSpecSupplier,
            clock,
            metricsSystem,
            maxMessageSize,
            messagePermissioningProviders,
            nodeKey.getPublicKey().getEncodedBytes(),
            maxPeers,
            maxRemotelyInitiatedPeers,
            randomPeerPriority,
            syncConfig.getSyncMode(),
            forkIdManager);

    final EthMessages ethMessages = new EthMessages();
    final EthMessages snapMessages = new EthMessages();

    Optional<Checkpoint> checkpoint = Optional.empty();
    if (genesisConfigOptions.getCheckpointOptions().isValid()) {
      checkpoint =
          Optional.of(
              ImmutableCheckpoint.builder()
                  .blockHash(
                      Hash.fromHexString(
                          genesisConfigOptions.getCheckpointOptions().getHash().get()))
                  .blockNumber(genesisConfigOptions.getCheckpointOptions().getNumber().getAsLong())
                  .totalDifficulty(
                      Difficulty.fromHexString(
                          genesisConfigOptions.getCheckpointOptions().getTotalDifficulty().get()))
                  .build());
    }

    final PeerTaskExecutor peerTaskExecutor =
        new PeerTaskExecutor(
            ethPeers,
            new PeerTaskRequestSender(networkingConfiguration.p2pPeerTaskTimeout()),
            metricsSystem);
    final EthContext ethContext =
        new EthContext(ethPeers, ethMessages, snapMessages, scheduler, peerTaskExecutor);
    final boolean fullSyncDisabled = syncConfig.getSyncMode() != SyncMode.FULL;
    final SyncState syncState = new SyncState(blockchain, ethPeers, fullSyncDisabled, checkpoint);

    final ChainPruningStrategy pruningMode = chainPrunerConfiguration.pruningMode();
    final boolean preMergeEnabled = dataStorageConfiguration.getHistoryExpiryPruneEnabled();

    if (pruningMode != ChainPruningStrategy.NONE || preMergeEnabled) {
      LOG.info("Adding ChainDataPruner to observe block added events");
      final AtomicLong chainDataPrunerObserverId = new AtomicLong();
      final ChainDataPruner chainDataPruner =
          createChainPruner(
              blockchainStorage,
              () -> blockchain.removeObserver(chainDataPrunerObserverId.get()),
              syncState);
      chainDataPrunerObserverId.set(blockchain.observeBlockAdded(chainDataPruner));

      if (pruningMode == ChainPruningStrategy.ALL) {
        LOG.info(
            "Chain and BAL pruning enabled | Blocks retained: {} | BALs retained: {} | Frequency: {}{}",
            chainPrunerConfiguration.chainPruningBlocksRetained(),
            chainPrunerConfiguration.chainPruningBalsRetained(),
            chainPrunerConfiguration.chainPruningFrequency(),
            preMergeEnabled
                ? " | Pre-merge pruning: enabled (quantity="
                    + chainPrunerConfiguration.preMergePruningBlocksQuantity()
                    + ")"
                : "");
      } else if (pruningMode == ChainPruningStrategy.BAL) {
        LOG.info(
            "BAL pruning enabled | BALs retained: {} | Frequency: {}{}",
            chainPrunerConfiguration.chainPruningBalsRetained(),
            chainPrunerConfiguration.chainPruningFrequency(),
            preMergeEnabled
                ? " | Pre-merge pruning: enabled (quantity="
                    + chainPrunerConfiguration.preMergePruningBlocksQuantity()
                    + ")"
                : "");
      } else if (preMergeEnabled) {
        LOG.info(
            "Chain pruning enabled - Mode: PRE_MERGE_ONLY | Frequency: {} | Quantity: {}",
            chainPrunerConfiguration.chainPruningFrequency(),
            chainPrunerConfiguration.preMergePruningBlocksQuantity());
      }
    }

    final TransactionPool transactionPool =
        TransactionPoolFactory.createTransactionPool(
            protocolSchedule,
            protocolContext,
            ethContext,
            clock,
            metricsSystem,
            syncState,
            transactionPoolConfiguration,
            ethereumWireProtocolConfiguration,
            besuComponent.map(BesuComponent::getBlobCache).orElse(new BlobCache()),
            miningConfiguration);

    final List<PeerValidator> peerValidators =
        createPeerValidators(protocolSchedule, peerTaskExecutor);

    final EthProtocolManager ethProtocolManager =
        createEthProtocolManager(
            protocolContext,
            syncConfig,
            transactionPool,
            ethereumWireProtocolConfiguration,
            ethPeers,
            ethContext,
            ethMessages,
            scheduler,
            peerValidators,
            Optional.empty(),
            forkIdManager);

    final PivotBlockSelector pivotBlockSelector =
        createPivotSelector(protocolSchedule, protocolContext, ethContext, syncState, blockchain);

    final DefaultSynchronizer synchronizer =
        createSynchronizer(
            protocolSchedule,
            worldStateStorageCoordinator,
            protocolContext,
            ethContext,
            peerTaskExecutor,
            syncState,
            ethProtocolManager,
            pivotBlockSelector);

    worldStateHealerSupplier.set(synchronizer::healWorldState);

    ethPeers.setTrailingPeerRequirementsSupplier(synchronizer::calculateTrailingPeerRequirements);

    if (syncConfig.getSyncMode() == SyncMode.SNAP) {
      synchronizer.subscribeInSync((b) -> ethPeers.snapServerPeersNeeded(!b));
      ethPeers.snapServerPeersNeeded(true);
    } else {
      ethPeers.snapServerPeersNeeded(false);
    }

    final Optional<SnapProtocolManager> maybeSnapProtocolManager =
        createSnapProtocolManager(
            protocolSchedule,
            protocolContext,
            worldStateStorageCoordinator,
            ethPeers,
            snapMessages,
            scheduler,
            synchronizer);

    final MiningCoordinator miningCoordinator =
        createMiningCoordinator(
            protocolSchedule,
            protocolContext,
            transactionPool,
            miningConfiguration,
            syncState,
            ethProtocolManager);

    final PluginServiceFactory additionalPluginServices =
        createAdditionalPluginServices(blockchain, protocolContext);

    final SubProtocolConfiguration subProtocolConfiguration =
        createSubProtocolConfiguration(ethProtocolManager, maybeSnapProtocolManager);

    final JsonRpcMethods additionalJsonRpcMethodFactory =
        createAdditionalJsonRpcMethodFactory(
            protocolContext, protocolSchedule, miningConfiguration);

    if (DataStorageFormat.BONSAI.equals(dataStorageConfiguration.getDataStorageFormat())) {
      final PathBasedExtraStorageConfiguration subStorageConfiguration =
          dataStorageConfiguration.getPathBasedExtraStorageConfiguration();
      if (subStorageConfiguration.getLimitTrieLogsEnabled()) {
        final TrieLogManager trieLogManager =
            ((BonsaiWorldStateProvider) worldStateArchive).getTrieLogManager();
        final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage =
            worldStateStorageCoordinator.getStrategy(BonsaiWorldStateKeyValueStorage.class);
        final TrieLogPruner trieLogPruner =
            createTrieLogPruner(worldStateKeyValueStorage, blockchain, scheduler);
        trieLogManager.subscribe(trieLogPruner);
      }
    }

    final List<Closeable> closeables = new ArrayList<>();
    closeables.add(protocolContext.getWorldStateArchive());
    closeables.add(storageProvider);

    if (DataStorageFormat.X_BONSAI_ARCHIVE.equals(
        dataStorageConfiguration.getDataStorageFormat())) {
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage =
          worldStateStorageCoordinator.getStrategy(BonsaiWorldStateKeyValueStorage.class);
      final BonsaiArchiver archiver =
          createBonsaiArchiver(
              worldStateKeyValueStorage,
              blockchain,
              scheduler,
              ((BonsaiWorldStateProvider) worldStateArchive).getTrieLogManager());

      if (worldStateStorageCoordinator.isMatchingFlatMode(FlatDbMode.FULL)
          || worldStateStorageCoordinator.isMatchingFlatMode(FlatDbMode.PARTIAL)) {
        final BonsaiFlatDbToArchiveMigrator archiveMigrator =
            createArchiveMigrator(worldStateStorageCoordinator, worldStateArchive, blockchain);
        closeables.add(archiveMigrator);

        final AtomicBoolean migrationStarted = new AtomicBoolean(false);
        final AtomicLong syncSubscriptionId = new AtomicLong();
        syncSubscriptionId.set(
            synchronizer.subscribeInSync(
                (inSync) -> {
                  if (inSync && migrationStarted.compareAndSet(false, true)) {
                    synchronizer.unsubscribeInSync(syncSubscriptionId.get());
                    LOG.info("Node is in sync, starting Bonsai archive migration");
                    archiveMigrator
                        .migrate()
                        .thenRun(() -> blockchain.observeBlockAdded(archiver))
                        .exceptionally(
                            error -> {
                              LOG.error(
                                  "Archive migration failed, archiver will remain disabled until restart",
                                  error);
                              return null;
                            });
                  }
                },
                0));
      } else {
        blockchain.observeBlockAdded(archiver);
      }
    }

    return new BesuController(
        protocolSchedule,
        protocolContext,
        ethProtocolManager,
        genesisConfigOptions,
        subProtocolConfiguration,
        synchronizer,
        syncState,
        transactionPool,
        miningCoordinator,
        miningConfiguration,
        additionalJsonRpcMethodFactory,
        nodeKey,
        closeables,
        additionalPluginServices,
        ethPeers,
        storageProvider,
        dataStorageConfiguration,
        transactionSimulator);
  }

  private void preloadBlockHeaderCache(
      final MutableBlockchain blockchain, final EthScheduler scheduler) {
    final BlockHeadersCachePreload blockHeaderCachePreload =
        new BlockHeadersCachePreload(blockchain, scheduler, numberOfBlockHeadersToCache);
    long startTime = System.nanoTime();
    blockHeaderCachePreload
        .preloadCache()
        .thenRun(
            () -> {
              long duration = System.nanoTime() - startTime;
              LOG.info(
                  "Preloading {} block headers to the cache finished in {} ms",
                  numberOfBlockHeadersToCache,
                  duration / 1_000_000);
            })
        .exceptionally(
            throwable -> {
              long duration = System.nanoTime() - startTime;
              LOG.error(
                  "Preloading {} block headers to the cache failed after {} ms",
                  numberOfBlockHeadersToCache,
                  duration / 1_000_000,
                  throwable);
              return null;
            });
  }

  private GenesisState getGenesisState(
      final Optional<BlockHeader> maybeGenesisBlockHeader,
      final ProtocolSchedule protocolSchedule,
      final CodeCache codeCache) {
    final Optional<Hash> maybeGenesisStateRoot =
        genesisStateHashCacheEnabled
            ? maybeGenesisBlockHeader.map(BlockHeader::getStateRoot)
            : Optional.empty();

    return maybeGenesisStateRoot
        .map(
            genesisStateRoot ->
                GenesisState.fromStorage(genesisStateRoot, genesisConfig, protocolSchedule))
        .orElseGet(
            () ->
                GenesisState.fromConfig(
                    dataStorageConfiguration, genesisConfig, protocolSchedule, codeCache));
  }

  private TrieLogPruner createTrieLogPruner(
      final WorldStateKeyValueStorage worldStateStorage,
      final Blockchain blockchain,
      final EthScheduler scheduler) {
    final boolean isProofOfStake = genesisConfigOptions.getTerminalTotalDifficulty().isPresent();
    final PathBasedExtraStorageConfiguration subStorageConfiguration =
        dataStorageConfiguration.getPathBasedExtraStorageConfiguration();
    final TrieLogPruner trieLogPruner =
        new TrieLogPruner(
            (BonsaiWorldStateKeyValueStorage) worldStateStorage,
            blockchain,
            scheduler::executeServiceTask,
            subStorageConfiguration.getMaxLayersToLoad(),
            subStorageConfiguration.getTrieLogPruningWindowSize(),
            isProofOfStake,
            metricsSystem);
    trieLogPruner.initialize();

    return trieLogPruner;
  }

  private BonsaiArchiver createBonsaiArchiver(
      final WorldStateKeyValueStorage worldStateStorage,
      final Blockchain blockchain,
      final EthScheduler scheduler,
      final TrieLogManager trieLogManager) {
    final BonsaiArchiver archiver =
        new BonsaiArchiver(
            (PathBasedWorldStateKeyValueStorage) worldStateStorage,
            blockchain,
            scheduler::executeServiceTask,
            trieLogManager,
            metricsSystem);

    archiver.initialize();
    LOG.info("Bonsai archiver initialised");
    return archiver;
  }

  private BonsaiFlatDbToArchiveMigrator createArchiveMigrator(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final WorldStateArchive worldStateArchive,
      final Blockchain blockchain) {
    final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage =
        worldStateStorageCoordinator.getStrategy(BonsaiWorldStateKeyValueStorage.class);
    final TrieLogManager trieLogManager =
        ((BonsaiWorldStateProvider) worldStateArchive).getTrieLogManager();
    final ScheduledExecutorService migrationExecutor =
        MonitoredExecutors.newScheduledThreadPool("archive-migrator", 1, metricsSystem);
    final BonsaiArchiveFlatDbStrategy archiveStrategy =
        new BonsaiArchiveFlatDbStrategy(metricsSystem, new CodeHashCodeStorageStrategy());
    return new BonsaiFlatDbToArchiveMigrator(
        worldStateKeyValueStorage,
        trieLogManager,
        blockchain,
        migrationExecutor,
        metricsSystem,
        archiveStrategy);
  }

  /**
   * Create synchronizer synchronizer.
   *
   * @param protocolSchedule the protocol schedule
   * @param worldStateStorageCoordinator the world state storage
   * @param protocolContext the protocol context
   * @param ethContext the eth context
   * @param peerTaskExecutor the PeerTaskExecutor
   * @param syncState the sync state
   * @param ethProtocolManager the eth protocol manager
   * @param pivotBlockSelector the pivot block selector
   * @return the synchronizer
   */
  protected DefaultSynchronizer createSynchronizer(
      final ProtocolSchedule protocolSchedule,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final PeerTaskExecutor peerTaskExecutor,
      final SyncState syncState,
      final EthProtocolManager ethProtocolManager,
      final PivotBlockSelector pivotBlockSelector) {

    return new DefaultSynchronizer(
        syncConfig,
        protocolSchedule,
        protocolContext,
        worldStateStorageCoordinator,
        ethProtocolManager.getBlockBroadcaster(),
        ethContext,
        peerTaskExecutor,
        syncState,
        dataDirectory,
        storageProvider,
        clock,
        metricsSystem,
        getFullSyncTerminationCondition(protocolContext.getBlockchain()),
        pivotBlockSelector);
  }

  private PivotBlockSelector createPivotSelector(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final SyncState syncState,
      final Blockchain blockchain) {

    if (genesisConfigOptions.isQbft() || genesisConfigOptions.isIbft2()) {
      LOG.info(
          "{} is configured, creating initial sync for BFT",
          genesisConfigOptions.getConsensusEngine().toUpperCase(Locale.ROOT));
      return new BFTPivotSelectorFromPeers(
          ethContext,
          syncConfig,
          syncState,
          protocolContext,
          nodeKey,
          blockchain.getChainHeadHeader());
    } else if (genesisConfigOptions.getTerminalTotalDifficulty().isPresent()) {
      LOG.info("TTD difficulty is present, creating initial sync for PoS");

      final MergeContext mergeContext = protocolContext.getConsensusContext(MergeContext.class);
      final UnverifiedForkchoiceSupplier unverifiedForkchoiceSupplier =
          new UnverifiedForkchoiceSupplier();
      final long subscriptionId =
          mergeContext.addNewUnverifiedForkchoiceListener(unverifiedForkchoiceSupplier);

      final Runnable unsubscribeForkchoiceListener =
          () -> {
            mergeContext.removeNewUnverifiedForkchoiceListener(subscriptionId);
            LOG.info("Initial sync done, unsubscribe forkchoice supplier");
          };

      final SingleBlockHeaderDownloader headerDownloader =
          new SingleBlockHeaderDownloader(ethContext, protocolSchedule);

      return new PivotSelectorFromSafeBlock(
          protocolContext,
          protocolSchedule,
          ethContext,
          genesisConfigOptions,
          unverifiedForkchoiceSupplier,
          unsubscribeForkchoiceListener,
          headerDownloader);
    } else {
      LOG.info("TTD difficulty is not present, creating initial sync phase for PoW");
      return new PivotSelectorFromPeers(ethContext, syncConfig, syncState);
    }
  }

  /**
   * Gets full sync termination condition.
   *
   * @param blockchain the blockchain
   * @return the full sync termination condition
   */
  protected SyncTerminationCondition getFullSyncTerminationCondition(final Blockchain blockchain) {
    return genesisConfigOptions
        .getTerminalTotalDifficulty()
        .map(difficulty -> SyncTerminationCondition.difficulty(difficulty, blockchain))
        .orElse(SyncTerminationCondition.never());
  }

  /** Prep for build. */
  protected void prepForBuild() {}

  /**
   * Create additional json rpc method factory json rpc methods.
   *
   * @param protocolContext the protocol context
   * @param protocolSchedule the protocol schedule
   * @param miningConfiguration the mining parameters
   * @return the json rpc methods
   */
  protected JsonRpcMethods createAdditionalJsonRpcMethodFactory(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final MiningConfiguration miningConfiguration) {
    return apis -> Collections.emptyMap();
  }

  /**
   * Create sub protocol configuration sub protocol configuration.
   *
   * @param ethProtocolManager the eth protocol manager
   * @param maybeSnapProtocolManager the maybe snap protocol manager
   * @return the sub protocol configuration
   */
  protected SubProtocolConfiguration createSubProtocolConfiguration(
      final EthProtocolManager ethProtocolManager,
      final Optional<SnapProtocolManager> maybeSnapProtocolManager) {
    final SubProtocolConfiguration subProtocolConfiguration =
        new SubProtocolConfiguration().withSubProtocol(EthProtocol.get(), ethProtocolManager);
    maybeSnapProtocolManager.ifPresent(
        snapProtocolManager ->
            subProtocolConfiguration.withSubProtocol(SnapProtocol.get(), snapProtocolManager));
    return subProtocolConfiguration;
  }

  /**
   * Create mining coordinator.
   *
   * @param protocolSchedule the protocol schedule
   * @param protocolContext the protocol context
   * @param transactionPool the transaction pool
   * @param miningConfiguration the mining parameters
   * @param syncState the sync state
   * @param ethProtocolManager the eth protocol manager
   * @return the mining coordinator
   */
  protected abstract MiningCoordinator createMiningCoordinator(
      ProtocolSchedule protocolSchedule,
      ProtocolContext protocolContext,
      TransactionPool transactionPool,
      MiningConfiguration miningConfiguration,
      SyncState syncState,
      EthProtocolManager ethProtocolManager);

  /**
   * Create protocol schedule protocol schedule.
   *
   * @return the protocol schedule
   */
  protected abstract ProtocolSchedule createProtocolSchedule();

  /**
   * Validate context.
   *
   * @param context the context
   */
  protected void validateContext(final ProtocolContext context) {}

  /**
   * Create consensus context consensus context.
   *
   * @param blockchain the blockchain
   * @param worldStateArchive the world state archive
   * @param protocolSchedule the protocol schedule
   * @return the consensus context
   */
  protected abstract ConsensusContext createConsensusContext(
      final Blockchain blockchain,
      final WorldStateArchive worldStateArchive,
      final ProtocolSchedule protocolSchedule);

  /**
   * Create eth protocol manager eth protocol manager.
   *
   * @param protocolContext the protocol context
   * @param synchronizerConfiguration the synchronizer configuration
   * @param transactionPool the transaction pool
   * @param ethereumWireProtocolConfiguration the ethereum wire protocol configuration
   * @param ethPeers the eth peers
   * @param ethContext the eth context
   * @param ethMessages the eth messages
   * @param scheduler the scheduler
   * @param peerValidators the peer validators
   * @param mergePeerFilter the merge peer filter
   * @param forkIdManager the fork id manager
   * @return the eth protocol manager
   */
  protected EthProtocolManager createEthProtocolManager(
      final ProtocolContext protocolContext,
      final SynchronizerConfiguration synchronizerConfiguration,
      final TransactionPool transactionPool,
      final EthProtocolConfiguration ethereumWireProtocolConfiguration,
      final EthPeers ethPeers,
      final EthContext ethContext,
      final EthMessages ethMessages,
      final EthScheduler scheduler,
      final List<PeerValidator> peerValidators,
      final Optional<MergePeerFilter> mergePeerFilter,
      final ForkIdManager forkIdManager) {
    return new EthProtocolManager(
        protocolContext.getBlockchain(),
        networkId,
        protocolContext.getWorldStateArchive(),
        transactionPool,
        ethereumWireProtocolConfiguration,
        ethPeers,
        ethMessages,
        ethContext,
        peerValidators,
        mergePeerFilter,
        synchronizerConfiguration,
        scheduler,
        forkIdManager);
  }

  /**
   * Create protocol context protocol context.
   *
   * @param blockchain the blockchain
   * @param worldStateArchive the world state archive
   * @param consensusContext the consensus context
   * @param serviceManager plugin service manager
   * @return the protocol context
   */
  protected ProtocolContext createProtocolContext(
      final MutableBlockchain blockchain,
      final WorldStateArchive worldStateArchive,
      final ConsensusContext consensusContext,
      final ServiceManager serviceManager) {
    return new ProtocolContext.Builder()
        .withBlockchain(blockchain)
        .withWorldStateArchive(worldStateArchive)
        .withConsensusContext(consensusContext)
        .withBadBlockManager(badBlockManager)
        .withServiceManager(serviceManager)
        .build();
  }

  private Optional<SnapProtocolManager> createSnapProtocolManager(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final EthPeers ethPeers,
      final EthMessages snapMessages,
      final EthScheduler ethScheduler,
      final Synchronizer synchronizer) {
    return Optional.of(
        new SnapProtocolManager(
            worldStateStorageCoordinator,
            syncConfig.getSnapSyncConfiguration(),
            ethPeers,
            snapMessages,
            ethScheduler,
            protocolSchedule,
            protocolContext,
            synchronizer));
  }

  WorldStateArchive createWorldStateArchive(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final Blockchain blockchain,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final Supplier<WorldStateHealer> worldStateHealerSupplier) {
    return switch (dataStorageConfiguration.getDataStorageFormat()) {
      case BONSAI -> {
        final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage =
            worldStateStorageCoordinator.getStrategy(BonsaiWorldStateKeyValueStorage.class);

        yield new BonsaiWorldStateProvider(
            worldStateKeyValueStorage,
            blockchain,
            dataStorageConfiguration.getPathBasedExtraStorageConfiguration(),
            bonsaiCachedMerkleTrieLoader,
            besuComponent.map(BesuComponent::getBesuPluginContext).orElse(null),
            evmConfiguration,
            worldStateHealerSupplier,
            codeCache);
      }
      case X_BONSAI_ARCHIVE -> {
        final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage =
            worldStateStorageCoordinator.getStrategy(BonsaiWorldStateKeyValueStorage.class);

        yield new BonsaiArchiveWorldStateProvider(
            worldStateKeyValueStorage,
            blockchain,
            dataStorageConfiguration.getPathBasedExtraStorageConfiguration(),
            bonsaiCachedMerkleTrieLoader,
            besuComponent.map(BesuComponent::getBesuPluginContext).orElse(null),
            evmConfiguration,
            worldStateHealerSupplier,
            codeCache);
      }
      case FOREST -> {
        final WorldStatePreimageStorage preimageStorage =
            storageProvider.createWorldStatePreimageStorage();
        yield new ForestWorldStateArchive(
            worldStateStorageCoordinator, preimageStorage, evmConfiguration);
      }
      default ->
          throw new IllegalStateException(
              "Unexpected value: " + dataStorageConfiguration.getDataStorageFormat());
    };
  }

  private ChainDataPruner createChainPruner(
      final BlockchainStorage blockchainStorage,
      final Runnable unsubscribeRunnable,
      final SyncState syncState) {

    final PruningMode mode = determineChainPrunerMode();

    return new ChainDataPruner(
        blockchainStorage,
        unsubscribeRunnable,
        new ChainDataPrunerStorage(
            storageProvider.getStorageBySegmentIdentifier(
                KeyValueSegmentIdentifier.CHAIN_PRUNER_STATE)),
        syncState.getCheckpoint().map(Checkpoint::blockNumber).orElse(0L),
        mode,
        chainPrunerConfiguration,
        MonitoredExecutors.newBoundedThreadPool(
            EthScheduler.class.getSimpleName() + "-ChainDataPruner",
            1,
            ChainDataPruner.MAX_PRUNING_THREAD_QUEUE_SIZE,
            metricsSystem));
  }

  private PruningMode determineChainPrunerMode() {
    if (chainPrunerConfiguration.isBlockPruningEnabled()
        || chainPrunerConfiguration.isBalPruningEnabled()) {
      return PruningMode.CHAIN_PRUNING;
    }
    if (dataStorageConfiguration.getHistoryExpiryPruneEnabled()) {
      return PruningMode.PRE_MERGE_PRUNING;
    }
    throw new IllegalStateException("Chain pruner created without any pruning mode enabled");
  }

  /**
   * Create peer validators list.
   *
   * @param protocolSchedule the protocol schedule
   * @param peerTaskExecutor the peer task executor
   * @return the list
   */
  protected List<PeerValidator> createPeerValidators(
      final ProtocolSchedule protocolSchedule, final PeerTaskExecutor peerTaskExecutor) {
    final List<PeerValidator> validators = new ArrayList<>();

    final OptionalLong daoBlock = genesisConfigOptions.getDaoForkBlock();
    if (daoBlock.isPresent()) {
      // Setup dao validator
      validators.add(
          new DaoForkPeerValidator(protocolSchedule, peerTaskExecutor, daoBlock.getAsLong()));
    }

    for (final Map.Entry<Long, Hash> requiredBlock : requiredBlocks.entrySet()) {
      validators.add(
          new RequiredBlocksPeerValidator(
              protocolSchedule,
              peerTaskExecutor,
              requiredBlock.getKey(),
              requiredBlock.getValue()));
    }

    return validators;
  }

  /**
   * Create additional plugin services plugin service factory.
   *
   * @param blockchain the blockchain
   * @param protocolContext the protocol context
   * @return the plugin service factory
   */
  protected abstract PluginServiceFactory createAdditionalPluginServices(
      final Blockchain blockchain, final ProtocolContext protocolContext);
}
