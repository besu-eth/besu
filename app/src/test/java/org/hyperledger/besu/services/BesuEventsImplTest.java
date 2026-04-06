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
package org.hyperledger.besu.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockCause;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.BlockBroadcaster;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolFactory;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidatorFactory;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.data.AddedBlockContext;
import org.hyperledger.besu.plugin.data.LogWithMetadata;
import org.hyperledger.besu.plugin.data.PropagatedBlockContext;
import org.hyperledger.besu.plugin.data.SyncStatus;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;
import org.hyperledger.besu.testutil.TestClock;
import org.hyperledger.besu.util.number.ByteUnits;

import java.math.BigInteger;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
public class BesuEventsImplTest {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  private static final KeyPair KEY_PAIR1 = SIGNATURE_ALGORITHM.generateKeyPair();
  private static final org.hyperledger.besu.ethereum.core.Transaction TX1 = createTransaction(0);
  private static final org.hyperledger.besu.ethereum.core.Transaction TX2 = createTransaction(1);

  @Mock private ProtocolSchedule mockProtocolSchedule;
  @Mock private ProtocolContext mockProtocolContext;
  private SyncState syncState;
  @Mock private EthPeers mockEthPeers;
  @Mock private EthContext mockEthContext;
  @Mock private EthMessages mockEthMessages;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private TransactionValidatorFactory mockTransactionValidatorFactory;

  @Mock private ProtocolSpec mockProtocolSpec;
  @Mock private WorldStateArchive mockWorldStateArchive;
  @Mock private MutableWorldState mockWorldState;
  private TransactionPool transactionPool;
  private BlockBroadcaster blockBroadcaster;
  private BesuEventsImpl serviceImpl;
  private MutableBlockchain blockchain;
  private final BlockDataGenerator gen = new BlockDataGenerator();
  private final BadBlockManager badBlockManager = new BadBlockManager();

  @BeforeEach
  public void setUp() {
    blockchain =
        DefaultBlockchain.createMutable(
            gen.genesisBlock(),
            new KeyValueStoragePrefixedKeyBlockchainStorage(
                new InMemoryKeyValueStorage(),
                new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
                new MainnetBlockHeaderFunctions(),
                false),
            new NoOpMetricsSystem(),
            0);

    when(mockEthContext.getEthMessages()).thenReturn(mockEthMessages);
    when(mockEthContext.getEthPeers()).thenReturn(mockEthPeers);
    when(mockEthContext.getScheduler()).thenReturn(new DeterministicEthScheduler());
    lenient().when(mockEthPeers.streamAvailablePeers()).thenAnswer(z -> Stream.empty());
    when(mockProtocolContext.getBlockchain()).thenReturn(blockchain);
    lenient().when(mockProtocolContext.getWorldStateArchive()).thenReturn(mockWorldStateArchive);
    lenient().when(mockProtocolSchedule.getByBlockHeader(any())).thenReturn(mockProtocolSpec);
    lenient()
        .when(mockProtocolSpec.getTransactionValidatorFactory())
        .thenReturn(mockTransactionValidatorFactory);
    lenient().when(mockProtocolSpec.getFeeMarket()).thenReturn(FeeMarket.london(0L));
    lenient()
        .when(
            mockTransactionValidatorFactory
                .get()
                .validate(any(), any(Optional.class), any(Optional.class), any()))
        .thenReturn(ValidationResult.valid());
    lenient()
        .when(mockTransactionValidatorFactory.get().validateForSender(any(), any(), any()))
        .thenReturn(ValidationResult.valid());
    lenient()
        .when(mockWorldStateArchive.getWorldState(any(WorldStateQueryParams.class)))
        .thenReturn(Optional.of(mockWorldState));

    blockBroadcaster = new BlockBroadcaster(mockEthContext, 10 * ByteUnits.MEGABYTE);
    syncState = new SyncState(blockchain, mockEthPeers);
    TransactionPoolConfiguration txPoolConfig =
        ImmutableTransactionPoolConfiguration.builder()
            .txPoolMaxSize(1)
            .minGasPrice(Wei.ZERO)
            .build();

    transactionPool =
        TransactionPoolFactory.createTransactionPool(
            mockProtocolSchedule,
            mockProtocolContext,
            mockEthContext,
            TestClock.system(ZoneId.systemDefault()),
            new NoOpMetricsSystem(),
            syncState,
            txPoolConfig,
            EthProtocolConfiguration.DEFAULT,
            new BlobCache(),
            MiningConfiguration.newDefault());

    serviceImpl =
        new BesuEventsImpl(
            blockchain, blockBroadcaster, transactionPool, syncState, badBlockManager);
  }

  @Test
  public void syncStatusEventFiresAfterSubscribe() {
    final AtomicReference<Optional<SyncStatus>> result = new AtomicReference<>();
    serviceImpl.addSyncStatusListener(result::set);

    assertThat(result.get()).isNull();
    setSyncTarget();
    assertThat(result.get()).isNotNull();

    // Reset result for next event
    result.set(null);

    clearSyncTarget();
    assertThat(result.get()).isNotNull();
  }

  @Test
  public void syncStatusEventDoesNotFireAfterUnsubscribe() {
    final AtomicReference<Optional<SyncStatus>> result = new AtomicReference<>();
    final long id = serviceImpl.addSyncStatusListener(result::set);

    assertThat(result.get()).isNull();
    setSyncTarget();
    assertThat(result.get()).isNotNull();

    // Reset result for next event
    result.set(null);
    // And remove listener
    serviceImpl.removeSyncStatusListener(id);

    clearSyncTarget();
    assertThat(result.get()).isNull();
  }

  private void setSyncTarget() {
    syncState.setSyncTarget(
        mock(EthPeer.class),
        new org.hyperledger.besu.ethereum.core.BlockHeader(
            null, null, null, null, null, null, null, null, 1, 1, 1, 1, null, null, null, 1, null,
            null, null, null, null, null, null, null));
  }

  private void clearSyncTarget() {
    syncState.clearSyncTarget();
  }

  @Test
  public void newBlockEventFiresAfterSubscribe() {
    final AtomicReference<PropagatedBlockContext> result = new AtomicReference<>();
    serviceImpl.addBlockPropagatedListener(result::set);
    final Block block = generateBlock();
    assertThat(result.get()).isNull();
    blockBroadcaster.propagate(block, Difficulty.of(1));

    assertThat(result.get()).isNotNull();
    assertThat(result.get().getBlockHeader()).isEqualTo(block.getHeader());
    assertThat(result.get().getTotalDifficulty().toBigInteger()).isEqualTo(BigInteger.ONE);
  }

  @Test
  public void newBlockEventDoesNotFireAfterUnsubscribe() {
    final AtomicReference<PropagatedBlockContext> result = new AtomicReference<>();
    final long id = serviceImpl.addBlockPropagatedListener(result::set);

    assertThat(result.get()).isNull();
    final Block block = generateBlock();
    blockBroadcaster.propagate(block, Difficulty.of(2));

    assertThat(result.get()).isNotNull();
    assertThat(result.get().getBlockHeader()).isEqualTo(block.getHeader());
    assertThat(result.get().getTotalDifficulty().toBigInteger()).isEqualTo(BigInteger.valueOf(2L));
    serviceImpl.removeBlockPropagatedListener(id);
    result.set(null);

    blockBroadcaster.propagate(generateBlock(), Difficulty.of(1));
    assertThat(result.get()).isNull();
  }

  @Test
  public void propagationWithoutSubscriptionsCompletes() {
    blockBroadcaster.propagate(generateBlock(), Difficulty.of(1));
  }

  @Test
  public void newBlockEventUselessUnsubscribesCompletes() {
    serviceImpl.removeBlockPropagatedListener(5);
    serviceImpl.removeBlockPropagatedListener(5L);
  }

  @Test
  public void addedBlockEventFiresAfterSubscribe() {
    final AtomicReference<AddedBlockContext> result = new AtomicReference<>();
    serviceImpl.addBlockAddedListener(result::set);
    assertThat(result.get()).isNull();

    final var block =
        gen.block(
            new BlockDataGenerator.BlockOptions()
                .setParentHash(blockchain.getGenesisBlock().getHash()));
    List<TransactionReceipt> transactionReceipts = gen.receipts(block);
    blockchain.appendBlock(block, transactionReceipts);
    assertThat(result.get()).isNotNull();
    assertThat(result.get().getBlockHeader()).isEqualTo(block.getHeader());
    assertThat(result.get().getTransactionReceipts()).isEqualTo(transactionReceipts);
    assertThat(result.get().getEventType()).isEqualTo(AddedBlockContext.EventType.HEAD_ADVANCED);
  }

  @Test
  public void addedBlockEventDoesNotFireAfterUnsubscribe() {
    final AtomicReference<AddedBlockContext> result = new AtomicReference<>();
    final long id = serviceImpl.addBlockAddedListener(result::set);
    assertThat(result.get()).isNull();

    serviceImpl.removeBlockAddedListener(id);
    result.set(null);

    final var block =
        gen.block(
            new BlockDataGenerator.BlockOptions()
                .setParentHash(blockchain.getGenesisBlock().getHash()));
    blockchain.appendBlock(block, gen.receipts(block));
    assertThat(result.get()).isNull();
  }

  @Test
  public void additionWithoutSubscriptionsCompletes() {
    final var block =
        gen.block(
            new BlockDataGenerator.BlockOptions()
                .setParentHash(blockchain.getGenesisBlock().getHash()));
    blockchain.appendBlock(block, gen.receipts(block));
  }

  @Test
  public void addedBlockEventUselessUnsubscribesCompletes() {
    serviceImpl.removeBlockAddedListener(5);
    serviceImpl.removeBlockAddedListener(5L);
  }

  @Test
  public void reorgedBlockEventFiresAfterSubscribe() {
    final AtomicReference<AddedBlockContext> result = new AtomicReference<>();
    serviceImpl.addBlockReorgListener(result::set);
    assertThat(result.get()).isNull();

    final var block =
        gen.block(
            new BlockDataGenerator.BlockOptions()
                .setParentHash(blockchain.getGenesisBlock().getHash())
                .setBlockNumber(blockchain.getGenesisBlock().getHeader().getNumber() + 1));
    blockchain.appendBlock(block, gen.receipts(block));
    assertThat(result.get()).isNull();

    final var forkBlock =
        gen.block(
            new BlockDataGenerator.BlockOptions()
                .setParentHash(blockchain.getGenesisBlock().getHash())
                .setDifficulty(block.getHeader().getDifficulty().subtract(1))
                .setBlockNumber(blockchain.getGenesisBlock().getHeader().getNumber() + 1));
    blockchain.appendBlock(forkBlock, gen.receipts(forkBlock));
    assertThat(result.get()).isNull();

    final var reorgBlock =
        gen.block(
            new BlockDataGenerator.BlockOptions()
                .setParentHash(forkBlock.getHash())
                .setDifficulty(Difficulty.of(10000000))
                .setBlockNumber(forkBlock.getHeader().getNumber() + 1));

    List<TransactionReceipt> transactionReceipts = gen.receipts(reorgBlock);
    blockchain.appendBlock(reorgBlock, transactionReceipts);
    assertThat(result.get()).isNotNull();
    assertThat(result.get().getBlockHeader()).isEqualTo(reorgBlock.getHeader());
    assertThat(result.get().getTransactionReceipts()).isEqualTo(transactionReceipts);
  }

  @Test
  public void reorgedBlockEventDoesNotFireAfterUnsubscribe() {
    final AtomicReference<AddedBlockContext> result = new AtomicReference<>();
    final long id = serviceImpl.addBlockReorgListener(result::set);
    assertThat(result.get()).isNull();

    serviceImpl.removeBlockReorgListener(id);
    result.set(null);

    final var block =
        gen.block(
            new BlockDataGenerator.BlockOptions()
                .setParentHash(blockchain.getGenesisBlock().getHash())
                .setBlockNumber(blockchain.getGenesisBlock().getHeader().getNumber() + 1));
    blockchain.appendBlock(block, gen.receipts(block));
    assertThat(result.get()).isNull();

    final var reorgBlock =
        gen.block(
            new BlockDataGenerator.BlockOptions()
                .setParentHash(blockchain.getGenesisBlock().getHash())
                .setBlockNumber(blockchain.getGenesisBlock().getHeader().getNumber() + 1));
    blockchain.appendBlock(reorgBlock, gen.receipts(reorgBlock));
    assertThat(result.get()).isNull();
  }

  @Test
  public void reorgWithoutSubscriptionsCompletes() {
    final var block =
        gen.block(
            new BlockDataGenerator.BlockOptions()
                .setParentHash(blockchain.getGenesisBlock().getHash())
                .setBlockNumber(blockchain.getGenesisBlock().getHeader().getNumber() + 1));
    blockchain.appendBlock(block, gen.receipts(block));

    final var reorgBlock =
        gen.block(
            new BlockDataGenerator.BlockOptions()
                .setParentHash(blockchain.getGenesisBlock().getHash())
                .setBlockNumber(blockchain.getGenesisBlock().getHeader().getNumber() + 1));
    List<TransactionReceipt> transactionReceipts = gen.receipts(reorgBlock);
    blockchain.appendBlock(reorgBlock, transactionReceipts);
  }

  @Test
  public void reorgedBlockEventUselessUnsubscribesCompletes() {
    serviceImpl.removeBlockReorgListener(5);
    serviceImpl.removeBlockReorgListener(5L);
  }

  @Test
  public void transactionAddedEventFiresAfterSubscribe() {
    final AtomicReference<Transaction> result = new AtomicReference<>();
    serviceImpl.addTransactionAddedListener(result::set);

    assertThat(result.get()).isNull();
    transactionPool.addTransactionViaApi(TX1);

    assertThat(result.get()).isNotNull();
  }

  @Test
  public void transactionAddedEventDoesNotFireAfterUnsubscribe() {
    final AtomicReference<Transaction> result = new AtomicReference<>();
    final long id = serviceImpl.addTransactionAddedListener(result::set);

    assertThat(result.get()).isNull();
    transactionPool.addTransactionViaApi(TX1);
    assertThat(result.get()).isNotNull();

    serviceImpl.removeTransactionAddedListener(id);
    result.set(null);

    transactionPool.addTransactionViaApi(TX2);
    assertThat(result.get()).isNull();
  }

  @Test
  public void transactionAddedEventUselessUnsubscribesCompletes() {
    serviceImpl.removeTransactionAddedListener(5);
    serviceImpl.removeTransactionAddedListener(5L);
  }

  @Test
  public void transactionDroppedEventFiresAfterSubscribe() {
    final AtomicReference<Transaction> result = new AtomicReference<>();
    serviceImpl.addTransactionDroppedListener((tx, reason) -> result.set(tx));

    assertThat(result.get()).isNull();
    // sending a replacement with higher gas should drop the previous one
    transactionPool.addTransactionViaApi(TX1);
    transactionPool.addTransactionViaApi(
        bumpTransactionGasPrice(TX1, TX1.getGasPrice().get().multiply(2)));

    assertThat(result.get()).isNotNull();
  }

  @Test
  public void transactionDroppedEventDoesNotFireAfterUnsubscribe() {
    final AtomicReference<Transaction> result = new AtomicReference<>();
    final long id = serviceImpl.addTransactionDroppedListener((tx, reason) -> result.set(tx));

    assertThat(result.get()).isNull();
    transactionPool.addTransactionViaApi(TX1);
    // first replacement with higher gas should drop the previous one
    transactionPool.addTransactionViaApi(
        bumpTransactionGasPrice(TX1, TX1.getGasPrice().get().multiply(2)));

    assertThat(result.get()).isNotNull();
    serviceImpl.removeTransactionDroppedListener(id);
    result.set(null);

    // second replacement with higher gas should drop the previous one
    transactionPool.addTransactionViaApi(
        bumpTransactionGasPrice(TX1, TX1.getGasPrice().get().multiply(4)));
    assertThat(result.get()).isNull();
  }

  @Test
  public void logEventFiresAfterSubscribe() {
    final List<LogWithMetadata> result = new ArrayList<>();
    blockchain.observeLogs(result::add);

    assertThat(result).isEmpty();
    final var block =
        gen.block(
            new BlockDataGenerator.BlockOptions()
                .setParentHash(blockchain.getGenesisBlock().getHash()));
    blockchain.appendBlock(block, gen.receipts(block));
    assertThat(result).hasSize(4);
  }

  @Test
  public void logEventDoesNotFireAfterUnsubscribe() {
    final List<LogWithMetadata> result = new ArrayList<>();
    final long id = blockchain.observeLogs(result::add);

    assertThat(result).isEmpty();
    final var block =
        gen.block(
            new BlockDataGenerator.BlockOptions()
                .setParentHash(blockchain.getGenesisBlock().getHash()));
    blockchain.appendBlock(block, gen.receipts(block));
    assertThat(result).hasSize(4);

    result.clear();

    serviceImpl.removeLogListener(id);
    final var block2 =
        gen.block(new BlockDataGenerator.BlockOptions().setParentHash(block.getHash()));
    blockchain.appendBlock(block2, gen.receipts(block2));

    assertThat(result).isEmpty();
  }

  @Test
  public void badBlockEventFiresAfterSubscribe_badBlockAdded() {
    // Track bad block events
    final AtomicReference<org.hyperledger.besu.plugin.data.BlockHeader> badBlockResult =
        new AtomicReference<>();
    final AtomicReference<org.hyperledger.besu.plugin.data.BadBlockCause> badBlockCauseResult =
        new AtomicReference<>();

    serviceImpl.addBadBlockListener(
        (badBlock, cause) -> {
          badBlockResult.set(badBlock);
          badBlockCauseResult.set(cause);
        });

    // Add bad block
    final BadBlockCause blockCause = BadBlockCause.fromValidationFailure("failed");
    final Block block = gen.block(new BlockDataGenerator.BlockOptions());
    badBlockManager.addBadBlock(block, blockCause);

    // Check we caught the bad block
    assertThat(badBlockResult.get()).isEqualTo(block.getHeader());
    assertThat(badBlockCauseResult.get()).isEqualTo(blockCause);
  }

  @Test
  public void badBlockEventFiresAfterSubscribe_badBlockHeaderAdded() {
    // Track bad block events
    final AtomicReference<org.hyperledger.besu.plugin.data.BlockHeader> badBlockResult =
        new AtomicReference<>();
    final AtomicReference<org.hyperledger.besu.plugin.data.BadBlockCause> badBlockCauseResult =
        new AtomicReference<>();

    serviceImpl.addBadBlockListener(
        (badBlock, cause) -> {
          badBlockResult.set(badBlock);
          badBlockCauseResult.set(cause);
        });

    // Add bad block header
    final BadBlockCause cause = BadBlockCause.fromValidationFailure("oops");
    final Block badBlock = gen.block(new BlockDataGenerator.BlockOptions());
    badBlockManager.addBadHeader(badBlock.getHeader(), cause);

    // Check we caught the bad block
    assertThat(badBlockResult.get()).isEqualTo(badBlock.getHeader());
    assertThat(badBlockCauseResult.get()).isEqualTo(cause);
  }

  @Test
  public void badBlockEventDoesNotFireAfterUnsubscribe() {
    // Track bad block events
    final AtomicReference<org.hyperledger.besu.plugin.data.BlockHeader> badBlockResult =
        new AtomicReference<>();
    final AtomicReference<org.hyperledger.besu.plugin.data.BadBlockCause> badBlockCauseResult =
        new AtomicReference<>();

    final long listenerId =
        serviceImpl.addBadBlockListener(
            (badBlock, cause) -> {
              badBlockResult.set(badBlock);
              badBlockCauseResult.set(cause);
            });
    // Unsubscribe
    serviceImpl.removeBadBlockListener(listenerId);

    // Add bad block
    final BadBlockCause blockCause = BadBlockCause.fromValidationFailure("failed");
    final Block block = gen.block(new BlockDataGenerator.BlockOptions());
    badBlockManager.addBadBlock(block, blockCause);
    // Add bad block header
    final BadBlockCause headerCause = BadBlockCause.fromValidationFailure("oops");
    final Block headerBlock = gen.block(new BlockDataGenerator.BlockOptions());
    badBlockManager.addBadHeader(headerBlock.getHeader(), headerCause);

    // Check we did not process any events
    assertThat(badBlockResult.get()).isNull();
    assertThat(badBlockCauseResult.get()).isNull();
  }

  private Block generateBlock() {
    final BlockBody body = new BlockBody(Collections.emptyList(), Collections.emptyList());
    return new Block(new BlockHeaderTestFixture().buildHeader(), body);
  }

  private static org.hyperledger.besu.ethereum.core.Transaction createTransaction(
      final int transactionNumber) {
    return new TransactionTestFixture()
        .nonce(transactionNumber)
        .gasLimit(0)
        .createTransaction(KEY_PAIR1);
  }

  private static org.hyperledger.besu.ethereum.core.Transaction bumpTransactionGasPrice(
      final Transaction originalTx, final Wei gasPriceBump) {
    return new TransactionTestFixture()
        .nonce(originalTx.getNonce())
        .gasPrice(Wei.fromQuantity(originalTx.getGasPrice().get()).add(gasPriceBump))
        .gasLimit(originalTx.getGasLimit())
        .createTransaction(KEY_PAIR1);
  }
}
