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
package org.hyperledger.besu.consensus.merge.blockcreation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MAX_SCORE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.MergeConfiguration;
import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.PayloadWrapper;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.Unstable;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardSyncContext;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionBroadcaster;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.BaseFeePendingTransactionsSorter;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.StubMetricsSystem;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.testutil.TestClock;
import org.hyperledger.besu.util.number.Fraction;

import java.time.ZoneId;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class MergeCoordinatorCacheReorgTest implements MergeGenesisConfigHelper {

  private static final com.google.common.base.Supplier<SignatureAlgorithm> SIGNATURE_ALGORITHM =
      Suppliers.memoize(SignatureAlgorithmFactory::getInstance);

  private static final SECPPrivateKey PRIVATE_KEY1 =
      SIGNATURE_ALGORITHM
          .get()
          .createPrivateKey(
              Bytes32.fromHexString(
                  "ae6ae8e5ccbfb04590405997ee2d52d2b330726137b875053c36d94e974d162f"));
  private static final KeyPair KEYS1 =
      new KeyPair(PRIVATE_KEY1, SIGNATURE_ALGORITHM.get().createPublicKey(PRIVATE_KEY1));

  @Mock MergeContext mergeContext;
  @Mock BackwardSyncContext backwardSyncContext;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  EthContext ethContext;

  @Mock EthScheduler ethScheduler;

  private MergeCoordinator coordinator;
  private ProtocolContext protocolContext;
  private BonsaiWorldStateProvider worldStateArchive;

  private final ProtocolSchedule protocolSchedule = spy(getMergeProtocolSchedule());
  private final GenesisState genesisState =
      GenesisState.fromConfig(getPosGenesisConfig(), protocolSchedule, new CodeCache());

  private final Address coinbase = genesisAllocations(getPosGenesisConfig()).findFirst().get();
  private final MutableBlockchain blockchain =
      spy(createInMemoryBlockchain(genesisState.getBlock()));

  private final StubMetricsSystem metricsSystem = new StubMetricsSystem();

  private final TransactionPoolConfiguration poolConf =
      ImmutableTransactionPoolConfiguration.builder()
          .txPoolMaxSize(10)
          .txPoolLimitByAccountPercentage(Fraction.fromPercentage(100))
          .build();
  private final BaseFeePendingTransactionsSorter transactions =
      new BaseFeePendingTransactionsSorter(
          poolConf,
          TestClock.system(ZoneId.systemDefault()),
          metricsSystem,
          MergeCoordinatorCacheReorgTest::mockBlockHeader);

  private CompletableFuture<Void> blockCreationTask = CompletableFuture.completedFuture(null);

  @BeforeEach
  public void setUp() {
    when(mergeContext.as(MergeContext.class)).thenReturn(mergeContext);
    when(mergeContext.getTerminalTotalDifficulty())
        .thenReturn(genesisState.getBlock().getHeader().getDifficulty().plus(1L));

    final StorageProvider storageProvider = new InMemoryKeyValueStorageProvider();
    final NoOpMetricsSystem noOpMetrics = new NoOpMetricsSystem();
    final DataStorageConfiguration dataStorageConfig =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .pathBasedExtraStorageConfiguration(PathBasedExtraStorageConfiguration.DEFAULT)
            .bonsaiCacheEnabled(true)
            .build();

    final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage =
        new BonsaiWorldStateKeyValueStorage(storageProvider, noOpMetrics, dataStorageConfig);

    final BonsaiCachedMerkleTrieLoader cachedMerkleTrieLoader =
        new BonsaiCachedMerkleTrieLoader(noOpMetrics);

    final ServiceManager pluginContext = mock(ServiceManager.class);

    worldStateArchive =
        new BonsaiWorldStateProvider(
            worldStateKeyValueStorage,
            blockchain,
            PathBasedExtraStorageConfiguration.DEFAULT,
            cachedMerkleTrieLoader,
            pluginContext,
            EvmConfiguration.DEFAULT,
            () -> null,
            new CodeCache());

    protocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(worldStateArchive)
            .withConsensusContext(mergeContext)
            .withBadBlockManager(new BadBlockManager())
            .build();

    genesisState.writeStateTo(worldStateArchive.getWorldState());
    worldStateArchive.getWorldState().persist(genesisState.getBlock().getHeader());

    when(ethScheduler.scheduleBlockCreationTask(anyLong(), any()))
        .thenAnswer(
            invocation -> {
              final Runnable runnable = invocation.getArgument(1);
              try {
                runnable.run();
              } catch (final Exception e) {
                // swallow
              }
              blockCreationTask = CompletableFuture.completedFuture(null);
              return blockCreationTask;
            });

    MergeConfiguration.setMergeEnabled(true);

    when(ethContext.getEthPeers().subscribeConnect(any())).thenReturn(1L);

    TransactionPool transactionPool =
        new TransactionPool(
            () -> transactions,
            protocolSchedule,
            protocolContext,
            mock(TransactionBroadcaster.class),
            ethContext,
            new TransactionPoolMetrics(metricsSystem),
            poolConf,
            new BlobCache());
    transactionPool.setEnabled();

    MiningConfiguration miningConfiguration =
        ImmutableMiningConfiguration.builder()
            .mutableInitValues(MutableInitValues.builder().coinbase(coinbase).build())
            .unstable(
                Unstable.builder()
                    .posBlockCreationMaxTime(1)
                    .posBlockCreationRepetitionMinDuration(1)
                    .build())
            .build();

    coordinator =
        new MergeCoordinator(
            protocolContext,
            protocolSchedule,
            ethScheduler,
            transactionPool,
            miningConfiguration,
            backwardSyncContext);
  }

  /**
   * Build both competing blocks BEFORE importing either, then:
   *
   * <pre>
   *   genesis ── A1 (tx: nonce 0, value=1)   ← built first, imported, made canonical
   *      │
   *      └────── B1 (tx: nonce 0, value=2)   ← built second (clean state), rememberBlock later
   * </pre>
   *
   * <ol>
   *   <li>preparePayload with tx(nonce=0, value=1) → build A1 (correct state root)
   *   <li>preparePayload with tx(nonce=0, value=2) → build B1 (correct state root, cache still
   *       clean)
   *   <li>rememberBlock(A1) + FCU(A1) → chain A canonical, cache gets nonce=1
   *   <li>rememberBlock(B1) → BUG: cache returns stale nonce=1, tx rejected
   * </ol>
   */
  @Test
  public void rememberBlockShouldNotFailWithStaleNonceAfterReorg() throws Exception {

    // === Step 1: Build block A1 with tx (nonce 0, value 1) ===
    Block blockA1 = buildBlockWithTransaction(Wei.of(1), Bytes32.ZERO);
    assertThat(blockA1.getBody().getTransactions()).hasSize(1);

    // === Step 2: Build block B1 with tx (nonce 0, value 2) ===
    // Built now while cache is still clean — gets correct state root from genesis
    Block blockB1 =
        buildBlockWithTransaction(
            Wei.of(2),
            Bytes32.fromHexString(
                "0x0000000000000000000000000000000000000000000000000000000000000001"));
    assertThat(blockB1.getBody().getTransactions()).hasSize(1);

    // Verify they are different blocks with different hashes
    assertThat(blockA1.getHash()).isNotEqualTo(blockB1.getHash());
    // Both built on genesis
    assertThat(blockA1.getHeader().getParentHash()).isEqualTo(genesisState.getBlock().getHash());
    assertThat(blockB1.getHeader().getParentHash()).isEqualTo(genesisState.getBlock().getHash());

    // === Step 3: Import A1 — rememberBlock(A1) + FCU(A1) ===
    BlockProcessingResult resultA1 = coordinator.rememberBlock(blockA1);
    assertThat(resultA1.getYield()).describedAs("Block A1 should process successfully").isPresent();
    resultA1.getYield().get().getWorldState().persist(blockA1.getHeader());

    coordinator.updateForkChoice(
        blockA1.getHeader(), genesisState.getBlock().getHash(), genesisState.getBlock().getHash());
    assertThat(blockchain.getChainHeadHash()).isEqualTo(blockA1.getHash());

    BlockProcessingResult resultB1 = coordinator.rememberBlock(blockB1);

    assertThat(resultB1.getYield())
        .describedAs(
            "Block B1 (competing block at height 1, same sender, nonce=0) should process "
                + "successfully against genesis state where sender nonce is 0. "
                + "If this fails with 'transaction nonce below sender account nonce', "
                + "the Bonsai VersionedCacheManager returned stale nonce=1 from block A1 "
                + "instead of correct nonce=0 from genesis. "
                + "This is the exact production bug.")
        .isPresent();
  }

  private Block buildBlockWithTransaction(final Wei value, final Bytes32 random) throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Block> result = new AtomicReference<>();

    doAnswer(
            invocation -> {
              PayloadWrapper pw = invocation.getArgument(0, PayloadWrapper.class);
              Block block = pw.blockWithReceipts().getBlock();
              if (!block.getBody().getTransactions().isEmpty()) {
                result.set(block);
                coordinator.finalizeProposalById(pw.payloadIdentifier());
                latch.countDown();
              }
              return null;
            })
        .when(mergeContext)
        .putPayloadById(any());

    transactions.addTransaction(createLocalTransaction(0, value), Optional.empty());

    coordinator.preparePayload(
        genesisState.getBlock().getHeader(),
        System.currentTimeMillis() / 1000,
        random,
        Address.ZERO,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());

    boolean built = latch.await(30, TimeUnit.SECONDS);
    assertThat(built)
        .describedAs("Block with value=%s should have been built within 30 seconds", value)
        .isTrue();

    assertThat(result.get()).isNotNull();
    return result.get();
  }

  private PendingTransaction createLocalTransaction(final long nonce, final Wei value) {
    return PendingTransaction.newPendingTransaction(
        new TransactionTestFixture()
            .value(value)
            .to(Optional.of(Address.ZERO))
            .gasLimit(53000L)
            .gasPrice(
                Wei.fromHexString(
                    "0x00000000000000000000000000000000000000000000000000000013b9aca00"))
            .maxFeePerGas(
                Optional.of(
                    Wei.fromHexString(
                        "0x00000000000000000000000000000000000000000000000000000013b9aca00")))
            .maxPriorityFeePerGas(Optional.of(Wei.of(100_000)))
            .nonce(nonce)
            .createTransaction(KEYS1),
        true,
        true,
        MAX_SCORE);
  }

  private static BlockHeader mockBlockHeader() {
    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(Optional.of(Wei.ONE));
    return blockHeader;
  }
}
