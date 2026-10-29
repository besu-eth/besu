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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.blockcreation.AbstractBlockCreator;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.SealableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveCoverageTracker;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveCoverageTracker.Coverage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Proof history built from real blocks in memory, by inline capture and the archiver. */
class BonsaiArchiveProofCoverageTest {

  private static final Address RECIPIENT =
      Address.fromHexString("0x00000000000000000000000000000000000dead1");

  private final ProtocolSchedule protocolSchedule =
      MainnetProtocolSchedule.fromConfig(
          GenesisConfig.fromResource("/dev.json").getConfigOptions(),
          MiningConfiguration.MINING_DISABLED,
          new BadBlockManager(),
          false,
          BalConfiguration.DEFAULT,
          new NoOpMetricsSystem());
  private final GenesisState genesisState =
      GenesisState.fromConfig(
          GenesisConfig.fromResource("/dev.json"), protocolSchedule, new BonsaiCodeCache());
  private final MutableBlockchain blockchain = createInMemoryBlockchain(genesisState.getBlock());
  private final EthScheduler ethScheduler = new DeterministicEthScheduler();
  private final KeyPair sender =
      SignatureAlgorithmFactory.getInstance()
          .createKeyPair(
              SECPPrivateKey.create(
                  GenesisConfig.fromResource("/dev.json")
                      .streamAllocations()
                      .filter(ga -> ga.privateKey() != null)
                      .findFirst()
                      .orElseThrow()
                      .privateKey(),
                  "ECDSA"));
  private final Map<Long, Wei> recipientBalanceAt = new HashMap<>();

  private final DataStorageConfiguration config =
      ImmutableDataStorageConfiguration.builder()
          .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
          .extraStorageConfiguration(
              ImmutableExtraStorageConfiguration.builder()
                  .maxLayersToLoad(4L)
                  .unstable(
                      ImmutableExtraStorageConfiguration.Unstable.builder()
                          .bonsaiArchiveStateProofsEnabled(true)
                          .build())
                  .build())
          .build();

  private BonsaiWorldStateKeyValueStorage worldStateStorage;
  private ArchiveCoverageTracker coverageTracker;
  private BonsaiArchiveWorldStateProvider provider;
  private ProtocolContext protocolContext;
  private long nonce;

  @BeforeEach
  void setUp() {
    openProvider(
        (BonsaiWorldStateKeyValueStorage)
            new InMemoryKeyValueStorageProvider().createWorldStateStorage(config));
    genesisState.writeStateTo(provider.getWorldState());
  }

  @AfterEach
  void tearDown() {
    provider.close();
  }

  @Test
  void noFinalityCapturesEveryBlockInlineWithCorrectProofs() {
    for (int i = 0; i < 6; i++) {
      importTransferBlock();
    }

    assertCoverage(0, 6);
    assertProofsCorrect(1, 6);
  }

  @Test
  void emptyBlockExtendsCoverageInline() {
    importTransferBlock();
    importBlock(List.of());
    importTransferBlock();

    assertCoverage(0, 3);
    assertProofsCorrect(1, 3);
  }

  @Test
  void blocksAboveFinalizedAreArchivedByTrailingArchiver() {
    for (int i = 0; i < 3; i++) {
      importTransferBlock();
    }
    blockchain.setFinalized(blockchain.getBlockHeader(3).orElseThrow().getHash());
    assertCoverage(0, 3);

    // no pass is due yet, and inline capture stops at the finalized block
    for (int i = 0; i < 3; i++) {
      importTransferBlock();
      assertCoverage(0, 3);
    }
    for (int i = 0; i < 20; i++) {
      importTransferBlock();
      // a pass is due once coverage reaches head - 4, the oldest block the live path serves
      final long oldestLive = blockchain.getChainHeadBlockNumber() - 4;
      await().atMost(10, SECONDS).until(() -> lastCovered() >= oldestLive);
    }

    // archive and live window overlap: every block has a proof
    assertProofsCorrect(1, blockchain.getChainHeadBlockNumber());
  }

  @Test
  void archiverHealsInlineMissAndKeepsCoverageWithinOneBlockOfHead() {
    for (int i = 0; i < 4; i++) {
      importTransferBlock();
    }
    assertCoverage(0, 4);
    rewindCoverageTo(3);

    // block 5 misses inline capture; the archiver heals up to head - 1, or head if inline resumes
    for (int i = 0; i < 4; i++) {
      importTransferBlock();
      final long head = blockchain.getChainHeadBlockNumber();
      await().atMost(10, SECONDS).until(() -> lastCovered() >= head - 1 && lastCovered() <= head);
    }
    final long head = blockchain.getChainHeadBlockNumber();
    assertThat(lastCovered()).isBetween(head - 1, head);
    assertProofsCorrect(1, head);
  }

  @Test
  void restartContinuesCoverage() {
    for (int i = 0; i < 3; i++) {
      importTransferBlock();
    }
    provider.close();

    // in-memory close() keeps the data, so reopen over the same storages
    final BonsaiFlatDbStrategyProvider flatDbStrategyProvider =
        new BonsaiFlatDbStrategyProvider(new NoOpMetricsSystem(), config);
    flatDbStrategyProvider.loadFlatDbStrategy(worldStateStorage.getComposedWorldStateStorage());
    openProvider(
        new BonsaiWorldStateKeyValueStorage(
            flatDbStrategyProvider,
            worldStateStorage.getComposedWorldStateStorage(),
            worldStateStorage.getTrieLogStorage(),
            worldStateStorage.getCacheManager(),
            worldStateStorage.getCurrentVersion()));
    importTransferBlock();
    importTransferBlock();

    assertCoverage(0, 5);
    assertProofsCorrect(1, 5);
  }

  private void openProvider(final BonsaiWorldStateKeyValueStorage storage) {
    worldStateStorage = storage;
    coverageTracker = new ArchiveCoverageTracker(storage.getComposedWorldStateStorage());
    provider =
        new BonsaiArchiveWorldStateProvider(
            worldStateStorage,
            blockchain,
            config,
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            new BonsaiCodeCache(),
            new NoOpMetricsSystem(),
            Optional.empty(),
            2);
    protocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(provider)
            .build();
  }

  private void importTransferBlock() {
    importBlock(List.of(transfer()));
  }

  private void importBlock(final List<Transaction> transactions) {
    final Block block =
        TestBlockCreator.create(protocolContext, protocolSchedule, ethScheduler)
            .createBlock(
                transactions,
                Collections.emptyList(),
                System.currentTimeMillis(),
                blockchain.getChainHeadHeader())
            .getBlock();
    // production order: process on the parent state, append (fires block-added), roll the head
    final BlockImportResult result =
        protocolSchedule
            .getByBlockHeader(block.getHeader())
            .getBlockImporter()
            .importBlock(
                protocolContext, block, HeaderValidationMode.NONE, HeaderValidationMode.NONE);
    assertThat(result.isImported()).isTrue();
    final MutableWorldState head = provider.getWorldState();
    assertThat(head.rootHash()).isEqualTo(block.getHeader().getStateRoot());
    recipientBalanceAt.put(
        block.getHeader().getNumber(),
        Optional.ofNullable(head.get(RECIPIENT)).map(Account::getBalance).orElse(Wei.ZERO));
  }

  private Transaction transfer() {
    return new TransactionTestFixture()
        .sender(
            Address.extract(
                Bytes32.wrap(Hash.hash(sender.getPublicKey().getEncodedBytes()).getBytes())))
        .to(Optional.of(RECIPIENT))
        .value(Wei.of(1_000_000_000_000_000_000L))
        .gasLimit(21_000L)
        .nonce(nonce++)
        .createTransaction(sender);
  }

  private long lastCovered() {
    return coverageTracker.getCoverage().map(Coverage::lastBlock).orElse(-1L);
  }

  private void assertCoverage(final long start, final long last) {
    final Coverage coverage = coverageTracker.getCoverage().orElseThrow();
    assertThat(coverage.startBlock()).isEqualTo(start);
    assertThat(coverage.lastBlock()).isEqualTo(last);
  }

  // Simulates a lost inline capture: coverage ends at {@code last}, hashed as the canonical block.
  private void rewindCoverageTo(final long last) {
    final SegmentedKeyValueStorageTransaction tx =
        worldStateStorage.getComposedWorldStateStorage().startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE_ARCHIVE,
        ArchiveCoverageTracker.COVERAGE_KEY,
        Bytes.concatenate(
                Bytes.ofUnsignedLong(0),
                Bytes.ofUnsignedLong(last),
                blockchain.getBlockHeader(last).orElseThrow().getHash().getBytes())
            .toArrayUnsafe());
    tx.commit();
  }

  private void assertProofsCorrect(final long from, final long to) {
    for (long block = from; block <= to; block++) {
      final BlockHeader header = blockchain.getBlockHeader(block).orElseThrow();
      final Optional<WorldStateProof> proof =
          provider.getAccountProof(header, RECIPIENT, List.of(), Function.identity());
      assertThat(
              proof
                  .flatMap(WorldStateProof::getStateTrieAccountValue)
                  .map(account -> account.getBalance()))
          .as("proof of block %d", block)
          .contains(recipientBalanceAt.get(block));
    }
  }

  private static final class TestBlockCreator extends AbstractBlockCreator {
    private TestBlockCreator(
        final MiningConfiguration miningConfiguration,
        final ProtocolContext protocolContext,
        final ProtocolSchedule protocolSchedule,
        final EthScheduler ethScheduler) {
      super(
          miningConfiguration,
          (__, ___) -> Address.ZERO,
          __ -> Bytes.fromHexString("deadbeef"),
          mock(TransactionPool.class),
          protocolContext,
          protocolSchedule,
          ethScheduler);
    }

    static TestBlockCreator create(
        final ProtocolContext protocolContext,
        final ProtocolSchedule protocolSchedule,
        final EthScheduler ethScheduler) {
      return new TestBlockCreator(
          ImmutableMiningConfiguration.builder()
              .mutableInitValues(
                  MutableInitValues.builder()
                      .extraData(Bytes.fromHexString("deadbeef"))
                      .targetGasLimit(30_000_000L)
                      .minTransactionGasPrice(Wei.ONE)
                      .coinbase(Address.ZERO)
                      .build())
              .build(),
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
}
