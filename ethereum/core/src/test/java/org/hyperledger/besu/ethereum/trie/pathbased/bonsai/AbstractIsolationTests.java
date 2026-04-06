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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.hyperledger.besu.ethereum.core.WorldStateHealerHelper.throwingWorldStateHealerSupplier;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisAccount;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.blockcreation.AbstractBlockCreator;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.SealableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionBroadcaster;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolReplacementHandler;
import org.hyperledger.besu.ethereum.eth.transactions.layered.EndLayer;
import org.hyperledger.besu.ethereum.eth.transactions.layered.GasPricePrioritizedTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredPendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.SenderBalanceChecker;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProviderBuilder;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBKeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

public abstract class AbstractIsolationTests {
  protected BonsaiWorldStateProvider archive;
  protected WorldStateKeyValueStorage worldStateKeyValueStorage;
  protected ProtocolContext protocolContext;
  protected EthContext ethContext;
  protected EthScheduler ethScheduler = new DeterministicEthScheduler();
  final Function<Bytes32, KeyPair> asKeyPair =
      key ->
          SignatureAlgorithmFactory.getInstance()
              .createKeyPair(SECPPrivateKey.create(key, "ECDSA"));
  protected final ProtocolSchedule protocolSchedule =
      MainnetProtocolSchedule.fromConfig(
          GenesisConfig.fromResource("/dev.json").getConfigOptions(),
          MiningConfiguration.MINING_DISABLED,
          new BadBlockManager(),
          false,
          BalConfiguration.DEFAULT,
          new NoOpMetricsSystem());
  protected final GenesisState genesisState =
      GenesisState.fromConfig(
          GenesisConfig.fromResource("/dev.json"), protocolSchedule, new CodeCache());
  protected final MutableBlockchain blockchain = createInMemoryBlockchain(genesisState.getBlock());

  protected final TransactionPoolConfiguration poolConfiguration =
      ImmutableTransactionPoolConfiguration.builder().txPoolMaxSize(100).build();

  protected final TransactionPoolReplacementHandler transactionReplacementHandler =
      new TransactionPoolReplacementHandler(
          poolConfiguration.getPriceBump(), poolConfiguration.getBlobPriceBump());

  protected final BiFunction<PendingTransaction, PendingTransaction, Boolean>
      transactionReplacementTester =
          (t1, t2) ->
              transactionReplacementHandler.shouldReplace(
                  t1, t2, protocolContext.getBlockchain().getChainHeadHeader());

  protected TransactionPoolMetrics txPoolMetrics =
      new TransactionPoolMetrics(new NoOpMetricsSystem());

  protected SenderBalanceChecker senderBalanceChecker = new SenderBalanceChecker.NoOpChecker();

  protected final PendingTransactions sorter =
      new LayeredPendingTransactions(
          poolConfiguration,
          new GasPricePrioritizedTransactions(
              poolConfiguration,
              ethScheduler,
              new EndLayer(txPoolMetrics),
              txPoolMetrics,
              transactionReplacementTester,
              new BlobCache(),
              MiningConfiguration.newDefault(),
              senderBalanceChecker),
          ethScheduler);

  protected final List<GenesisAccount> accounts =
      GenesisConfig.fromResource("/dev.json")
          .streamAllocations()
          .filter(ga -> ga.privateKey() != null)
          .toList();

  KeyPair sender1 = Optional.ofNullable(accounts.get(0).privateKey()).map(asKeyPair).orElseThrow();
  TransactionPool transactionPool;

  @TempDir private Path tempData;

  @BeforeEach
  public void createStorage() {
    worldStateKeyValueStorage =
        // FYI: BonsaiSnapshoIsolationTests  work with frozen/cached worldstates, using PARTIAL
        // flat db strategy allows the tests to make account assertions based on trie
        // (whereas a full db strategy will not, since the worldstates are frozen/cached)
        createKeyValueStorageProvider()
            .createWorldStateStorage(DataStorageConfiguration.DEFAULT_BONSAI_PARTIAL_DB_CONFIG);
    archive =
        new BonsaiWorldStateProvider(
            (BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage,
            blockchain,
            ImmutablePathBasedExtraStorageConfiguration.builder().maxLayersToLoad(16L).build(),
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            throwingWorldStateHealerSupplier(),
            new CodeCache());
    var ws = archive.getWorldState();
    genesisState.writeStateTo(ws);
    protocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(archive)
            .build();
    ethContext = mock(EthContext.class, RETURNS_DEEP_STUBS);
    when(ethContext.getEthPeers().subscribeConnect(any())).thenReturn(1L);
    transactionPool =
        new TransactionPool(
            () -> sorter,
            protocolSchedule,
            protocolContext,
            mock(TransactionBroadcaster.class),
            ethContext,
            txPoolMetrics,
            poolConfiguration,
            new BlobCache());
    transactionPool.setEnabled();
  }

  // storage provider which uses a temporary directory based rocksdb
  protected StorageProvider createKeyValueStorageProvider() {
    return new KeyValueStorageProviderBuilder()
        .withStorageFactory(
            new RocksDBKeyValueStorageFactory(
                () ->
                    new RocksDBFactoryConfiguration(
                        1024 /* MAX_OPEN_FILES*/,
                        4 /*BACKGROUND_THREAD_COUNT*/,
                        8388608 /*CACHE_CAPACITY*/,
                        false,
                        false,
                        false,
                        Optional.empty(),
                        Optional.empty()),
                Arrays.asList(KeyValueSegmentIdentifier.values()),
                RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS))
        .withCommonConfiguration(
            new BesuConfiguration() {

              @Override
              public Optional<String> getRpcHttpHost() {
                return Optional.empty();
              }

              @Override
              public Optional<Integer> getRpcHttpPort() {
                return Optional.empty();
              }

              @Override
              public String getConfiguredRpcHttpHost() {
                return "";
              }

              @Override
              public Integer getConfiguredRpcHttpPort() {
                return 0;
              }

              @Override
              public long getConfiguredRpcHttpTimeoutSec() {
                return 0;
              }

              @Override
              public Path getStoragePath() {
                return tempData.resolve("database");
              }

              @Override
              public Path getDataPath() {
                return tempData;
              }

              @Override
              public DataStorageFormat getDatabaseFormat() {
                return DataStorageFormat.BONSAI;
              }

              @Override
              public Wei getMinGasPrice() {
                return MiningConfiguration.newDefault().getMinTransactionGasPrice();
              }

              @Override
              public org.hyperledger.besu.plugin.services.storage.DataStorageConfiguration
                  getDataStorageConfiguration() {
                return new org.hyperledger.besu.plugin.services.storage.DataStorageConfiguration() {
                  @Override
                  public DataStorageFormat getDatabaseFormat() {
                    return DataStorageFormat.BONSAI;
                  }

                  @Override
                  public boolean getReceiptCompactionEnabled() {
                    return false;
                  }
                };
              }
            })
        .withMetricsSystem(new NoOpMetricsSystem())
        .build();
  }

  static class TestBlockCreator extends AbstractBlockCreator {
    private TestBlockCreator(
        final MiningConfiguration miningConfiguration,
        final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
        final ExtraDataCalculator extraDataCalculator,
        final TransactionPool transactionPool,
        final ProtocolContext protocolContext,
        final ProtocolSchedule protocolSchedule,
        final EthScheduler ethScheduler) {
      super(
          miningConfiguration,
          miningBeneficiaryCalculator,
          extraDataCalculator,
          transactionPool,
          protocolContext,
          protocolSchedule,
          ethScheduler);
    }

    static TestBlockCreator forHeader(
        final ProtocolContext protocolContext,
        final ProtocolSchedule protocolSchedule,
        final TransactionPool transactionPool,
        final EthScheduler ethScheduler) {

      final MiningConfiguration miningConfiguration =
          ImmutableMiningConfiguration.builder()
              .mutableInitValues(
                  MutableInitValues.builder()
                      .extraData(Bytes.fromHexString("deadbeef"))
                      .targetGasLimit(30_000_000L)
                      .minTransactionGasPrice(Wei.ONE)
                      .coinbase(Address.ZERO)
                      .build())
              .build();

      return new TestBlockCreator(
          miningConfiguration,
          (__, ___) -> Address.ZERO,
          __ -> Bytes.fromHexString("deadbeef"),
          transactionPool,
          protocolContext,
          protocolSchedule,
          ethScheduler);
    }

    @Override
    protected BlockHeader createFinalBlockHeader(final SealableBlockHeader sealableBlockHeader) {
      return BlockHeaderBuilder.create()
          .difficulty(Difficulty.ZERO)
          .mixHash(Hash.ZERO)
          .populateFrom(sealableBlockHeader)
          .nonce(0L)
          .blockHeaderFunctions(blockHeaderFunctions)
          .buildBlockHeader();
    }
  }

  protected Transaction burnTransaction(final KeyPair sender, final Long nonce, final Address to) {
    return new TransactionTestFixture()
        .sender(
            Address.extract(
                Bytes32.wrap(Hash.hash(sender.getPublicKey().getEncodedBytes()).getBytes())))
        .to(Optional.of(to))
        .value(Wei.of(1_000_000_000_000_000_000L))
        .gasLimit(21_000L)
        .nonce(nonce)
        .createTransaction(sender);
  }

  protected Block forTransactions(final List<Transaction> transactions) {
    return forTransactions(transactions, blockchain.getChainHeadHeader());
  }

  protected Block forTransactions(
      final List<Transaction> transactions, final BlockHeader forHeader) {
    return TestBlockCreator.forHeader(
            protocolContext, protocolSchedule, transactionPool, ethScheduler)
        .createBlock(transactions, Collections.emptyList(), System.currentTimeMillis(), forHeader)
        .getBlock();
  }

  protected BlockProcessingResult executeBlock(final MutableWorldState ws, final Block block) {
    var res =
        protocolSchedule
            .getByBlockHeader(blockHeader(0))
            .getBlockProcessor()
            .processBlock(protocolContext, blockchain, ws, block);
    blockchain.appendBlock(block, res.getReceipts());
    return res;
  }

  private BlockHeader blockHeader(final long number) {
    return new BlockHeaderTestFixture().number(number).buildHeader();
  }
}
