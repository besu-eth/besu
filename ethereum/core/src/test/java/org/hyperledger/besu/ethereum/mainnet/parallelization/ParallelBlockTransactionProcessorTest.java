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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.NoOpBonsaiCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.NoopBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParallelBlockTransactionProcessorTest {

  private enum ProcessorVariant {
    PARALLELIZED,
    BAL
  }

  private final Executor sameThreadExecutor = Runnable::run;
  private static final Address MINING_BENEFICIARY = Address.fromHexString("0x1");
  private static final Wei BLOB_GAS_PRICE = Wei.ZERO;

  private Stream<ProcessorVariant> processorVariants() {
    return Stream.of(ProcessorVariant.PARALLELIZED, ProcessorVariant.BAL);
  }

  private BonsaiWorldState createEmptyWorldState() {
    final BonsaiWorldStateKeyValueStorage bonsaiWorldStateKeyValueStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);

    return new BonsaiWorldState(
        bonsaiWorldStateKeyValueStorage,
        new NoopBonsaiCachedMerkleTrieLoader(),
        new NoOpBonsaiCachedWorldStorageManager(
            bonsaiWorldStateKeyValueStorage, EvmConfiguration.DEFAULT, new CodeCache()),
        new NoOpTrieLogManager(),
        EvmConfiguration.DEFAULT,
        createStatefulConfigWithTrie(),
        new CodeCache());
  }

  private ParallelBlockTransactionProcessor createProcessor(
      final ProcessorVariant variant,
      final MainnetTransactionProcessor transactionProcessor,
      final TransactionCollisionDetector collisionDetector,
      final BlockAccessList blockAccessList) {

    return switch (variant) {
      case PARALLELIZED ->
          new ParallelizedConcurrentTransactionProcessor(transactionProcessor, collisionDetector);
      case BAL -> {
        final BlockProcessingContext bpc = mock(BlockProcessingContext.class);
        when(bpc.getOperationTracer()).thenReturn(OperationTracer.NO_TRACING);
        yield new BalConcurrentTransactionProcessor(
            transactionProcessor, blockAccessList, BalConfiguration.DEFAULT, bpc);
      }
    };
  }

  private record TestEnvironment(
      ProtocolContext protocolContext, BlockHeader blockHeader, BonsaiWorldState worldState) {}

  private TestEnvironment createTestEnvironment() {
    final ProtocolContext protocolContext = mock(ProtocolContext.class);
    final MutableBlockchain blockchain = mock(MutableBlockchain.class);
    final BlockHeader chainHeadBlockHeader = mock(BlockHeader.class);
    final BlockHeader blockHeader = mock(BlockHeader.class);
    final WorldStateArchive worldStateArchive = mock(WorldStateArchive.class);
    final BonsaiWorldState worldState = createEmptyWorldState();

    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHeadHeader()).thenReturn(chainHeadBlockHeader);
    when(chainHeadBlockHeader.getHash()).thenReturn(Hash.ZERO);
    when(chainHeadBlockHeader.getStateRoot()).thenReturn(Hash.EMPTY_TRIE_HASH);

    when(blockHeader.getParentHash()).thenReturn(Hash.ZERO);
    when(blockHeader.getStateRoot()).thenReturn(Hash.EMPTY_TRIE_HASH);
    when(blockHeader.getBlockHash()).thenReturn(Hash.ZERO);

    when(protocolContext.getWorldStateArchive()).thenReturn(worldStateArchive);
    when(worldStateArchive.getWorldState(any())).thenReturn(Optional.of(worldState));

    return new TestEnvironment(protocolContext, blockHeader, worldState);
  }

  private BlockAccessList mockEmptyBlockAccessList() {
    final BlockAccessList blockAccessList = mock(BlockAccessList.class);
    when(blockAccessList.accountChanges()).thenReturn(Collections.emptyList());
    return blockAccessList;
  }

  private Transaction mockTransaction() {
    final Transaction transaction = mock(Transaction.class);
    when(transaction.detachedCopy()).thenReturn(transaction);
    return transaction;
  }

  private record ProcessorTestFixture(
      ProcessorVariant variant,
      MainnetTransactionProcessor transactionProcessor,
      TransactionCollisionDetector collisionDetector,
      BlockAccessList blockAccessList,
      Transaction transaction,
      TestEnvironment env,
      ParallelBlockTransactionProcessor processor) {}

  private ProcessorTestFixture createFixture(final ProcessorVariant variant) {
    final MainnetTransactionProcessor transactionProcessor =
        mock(MainnetTransactionProcessor.class);
    final TransactionCollisionDetector collisionDetector = mock(TransactionCollisionDetector.class);
    final BlockAccessList blockAccessList = mockEmptyBlockAccessList();
    final Transaction transaction = mockTransaction();
    final TestEnvironment env = createTestEnvironment();
    final ParallelBlockTransactionProcessor processor =
        createProcessor(variant, transactionProcessor, collisionDetector, blockAccessList);

    if (variant == ProcessorVariant.PARALLELIZED) {
      when(collisionDetector.hasCollision(any(), any(), any(), any())).thenReturn(false);
    }

    return new ProcessorTestFixture(
        variant,
        transactionProcessor,
        collisionDetector,
        blockAccessList,
        transaction,
        env,
        processor);
  }

  private void stubSuccessfulTransaction(
      final MainnetTransactionProcessor transactionProcessor,
      final Optional<PartialBlockAccessView> partialView) {

    when(transactionProcessor.processTransaction(
            any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            TransactionProcessingResult.successful(
                Collections.emptyList(), 0, 0, Bytes.EMPTY, partialView, ValidationResult.valid()));
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("processorVariants")
  void testRunTransaction(final ProcessorVariant variant) {
    final ProcessorTestFixture f = createFixture(variant);
    stubSuccessfulTransaction(f.transactionProcessor(), Optional.empty());

    f.processor()
        .runAsyncBlock(
            f.env().protocolContext(),
            f.env().blockHeader(),
            Collections.singletonList(f.transaction()),
            MINING_BENEFICIARY,
            (__, ___) -> Hash.EMPTY,
            BLOB_GAS_PRICE,
            sameThreadExecutor,
            Optional.empty(),
            Optional.of(f.env().blockHeader()));

    verify(f.transactionProcessor(), times(1))
        .processTransaction(
            any(WorldUpdater.class),
            eq(f.env().blockHeader()),
            eq(f.transaction()),
            eq(MINING_BENEFICIARY),
            any(OperationTracer.class),
            any(BlockHashLookup.class),
            eq(TransactionValidationParams.processingBlock()),
            eq(BLOB_GAS_PRICE),
            eq(Optional.empty()));

    final Optional<TransactionProcessingResult> maybeResult =
        f.processor()
            .getProcessingResult(
                f.env().worldState(),
                MINING_BENEFICIARY,
                f.transaction(),
                0,
                Optional.empty(),
                Optional.empty());

    assertTrue(maybeResult.isPresent(), "Expected the transaction result to be present");
    assertTrue(maybeResult.get().isSuccessful(), "Expected the processing to be successful");
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("processorVariants")
  void testRunTransactionWithFailure(final ProcessorVariant variant) {
    final ProcessorTestFixture f = createFixture(variant);
    when(f.transactionProcessor()
            .processTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            TransactionProcessingResult.failed(
                0,
                0,
                ValidationResult.invalid(
                    TransactionInvalidReason.BLOB_GAS_PRICE_BELOW_CURRENT_BLOB_BASE_FEE),
                Optional.of(Bytes.EMPTY),
                Optional.empty(),
                Optional.empty()));

    f.processor()
        .runAsyncBlock(
            f.env().protocolContext(),
            f.env().blockHeader(),
            Collections.singletonList(f.transaction()),
            MINING_BENEFICIARY,
            (__, ___) -> Hash.EMPTY,
            BLOB_GAS_PRICE,
            sameThreadExecutor,
            Optional.empty(),
            Optional.of(f.env().blockHeader()));

    final Optional<TransactionProcessingResult> maybeResult =
        f.processor()
            .getProcessingResult(
                f.env().worldState(),
                MINING_BENEFICIARY,
                f.transaction(),
                0,
                Optional.empty(),
                Optional.empty());

    if (variant == ProcessorVariant.BAL) {
      assertFalse(maybeResult.isEmpty(), "Expected non-empty result for the BAL variant");
      assertTrue(maybeResult.get().getStatus() == TransactionProcessingResult.Status.FAILED);
    } else {
      assertTrue(
          maybeResult.isEmpty(),
          "Expected empty result so the block processor re-executes the transaction");
    }
  }

  @Test
  void testRunTransactionWithConflict() {
    final ProcessorTestFixture f = createFixture(ProcessorVariant.PARALLELIZED);
    stubSuccessfulTransaction(f.transactionProcessor(), Optional.empty());

    f.processor()
        .runAsyncBlock(
            f.env().protocolContext(),
            f.env().blockHeader(),
            Collections.singletonList(f.transaction()),
            MINING_BENEFICIARY,
            (__, ___) -> Hash.EMPTY,
            BLOB_GAS_PRICE,
            sameThreadExecutor,
            Optional.empty(),
            Optional.of(f.env().blockHeader()));

    verify(f.transactionProcessor(), times(1))
        .processTransaction(
            any(WorldUpdater.class),
            eq(f.env().blockHeader()),
            eq(f.transaction()),
            eq(MINING_BENEFICIARY),
            any(OperationTracer.class),
            any(BlockHashLookup.class),
            eq(TransactionValidationParams.processingBlock()),
            eq(BLOB_GAS_PRICE),
            eq(Optional.empty()));

    // simulate a conflict
    when(f.collisionDetector().hasCollision(any(), any(), any(), any())).thenReturn(true);

    final Optional<TransactionProcessingResult> maybeResult =
        f.processor()
            .getProcessingResult(
                f.env().worldState(),
                MINING_BENEFICIARY,
                f.transaction(),
                0,
                Optional.empty(),
                Optional.empty());

    assertTrue(
        maybeResult.isEmpty(), "Expected no transaction result to be applied due to conflict");
  }

  @Test
  void testApplyResultUsesAccessLocationTrackerAndUpdatesPartialBlockAccessView() {
    final ProcessorTestFixture f = createFixture(ProcessorVariant.PARALLELIZED);

    final PartialBlockAccessView partialView = mock(PartialBlockAccessView.class);
    final PartialBlockAccessView.AccountChanges beneficiaryChanges =
        mock(PartialBlockAccessView.AccountChanges.class);
    when(beneficiaryChanges.getAddress()).thenReturn(MINING_BENEFICIARY);
    when(partialView.accountChanges()).thenReturn(Collections.singletonList(beneficiaryChanges));

    // Stub processTransaction to also simulate traceBeforeRewardTransaction with a non-zero reward,
    // so that getProcessingResult updates the post-balance in the partial block access view.
    final Wei simulatedReward = Wei.of(1_000_000L);
    when(f.transactionProcessor()
            .processTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              final OperationTracer tracer = invocation.getArgument(4, OperationTracer.class);
              tracer.traceBeforeRewardTransaction(null, null, simulatedReward);
              return TransactionProcessingResult.successful(
                  Collections.emptyList(),
                  0,
                  0,
                  Bytes.EMPTY,
                  Optional.of(partialView),
                  ValidationResult.valid());
            });

    final BlockAccessListBuilder balBuilder = mock(BlockAccessListBuilder.class);

    f.processor()
        .runAsyncBlock(
            f.env().protocolContext(),
            f.env().blockHeader(),
            Collections.singletonList(f.transaction()),
            MINING_BENEFICIARY,
            (__, ___) -> Hash.EMPTY,
            BLOB_GAS_PRICE,
            sameThreadExecutor,
            Optional.of(balBuilder),
            Optional.of(f.env().blockHeader()));

    verify(f.transactionProcessor())
        .processTransaction(
            any(WorldUpdater.class),
            eq(f.env().blockHeader()),
            eq(f.transaction()),
            eq(MINING_BENEFICIARY),
            any(OperationTracer.class),
            any(BlockHashLookup.class),
            eq(TransactionValidationParams.processingBlock()),
            eq(BLOB_GAS_PRICE),
            argThat(Optional::isPresent));

    final Optional<TransactionProcessingResult> maybeResult =
        f.processor()
            .getProcessingResult(
                f.env().worldState(),
                MINING_BENEFICIARY,
                f.transaction(),
                0,
                Optional.empty(),
                Optional.empty());

    assertTrue(
        maybeResult.isPresent(), "Expected the parallelized transaction result to be applied");
    final TransactionProcessingResult result = maybeResult.get();
    assertTrue(result.getPartialBlockAccessView().isPresent(), "Expected BAL view to be present");
    verify(beneficiaryChanges).setPostBalance(any(Wei.class));
  }

  @Test
  void testPreStateSetup() {
    final TestEnvironment env = createTestEnvironment();

    final Address accountAddress =
        Address.fromHexString("0x1000000000000000000000000000000000000001");

    final UInt256 slot1Key = UInt256.ONE;
    final UInt256 slot2Key = UInt256.valueOf(2);
    final StorageSlotKey slot1 = new StorageSlotKey(slot1Key);
    final StorageSlotKey slot2 = new StorageSlotKey(slot2Key);

    final Wei tx0Balance = Wei.of(100);
    final long tx0Nonce = 1L;
    final Bytes tx0Code = Bytes.fromHexString("0xAA");
    final UInt256 tx0Slot1Value = UInt256.valueOf(1);
    final UInt256 tx0Slot2Value = UInt256.valueOf(3);

    final Wei tx1Balance = Wei.of(200);
    final long tx1Nonce = 2L;
    final Bytes tx1Code = Bytes.fromHexString("0xBB");
    final UInt256 tx1Slot1Value = UInt256.valueOf(5);
    final UInt256 tx1Slot2Value = UInt256.ZERO;

    final Wei tx2Balance = Wei.of(300);
    final long tx2Nonce = 3L;
    final Bytes tx2Code = Bytes.fromHexString("0xCC");
    final UInt256 tx2Slot1Value = UInt256.valueOf(7);

    final BlockAccessList.BlockAccessListBuilder balBuilder = BlockAccessList.builder();

    final PartialBlockAccessView.PartialBlockAccessViewBuilder p0 =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(0);
    final PartialBlockAccessView.AccountChangesBuilder a0 =
        p0.getOrCreateAccountBuilder(accountAddress);
    a0.withPostBalance(tx0Balance);
    a0.withNonceChange(tx0Nonce);
    a0.withNewCode(tx0Code);
    a0.addStorageChange(slot1, tx0Slot1Value);
    a0.addStorageChange(slot2, tx0Slot2Value);
    balBuilder.apply(p0.build());

    final PartialBlockAccessView.PartialBlockAccessViewBuilder p1 =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(1);
    final PartialBlockAccessView.AccountChangesBuilder a1 =
        p1.getOrCreateAccountBuilder(accountAddress);
    a1.withPostBalance(tx1Balance);
    a1.withNonceChange(tx1Nonce);
    a1.withNewCode(tx1Code);
    a1.addStorageChange(slot1, tx1Slot1Value);
    a1.addStorageChange(slot2, null);
    balBuilder.apply(p1.build());

    final PartialBlockAccessView.PartialBlockAccessViewBuilder p2 =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(2);
    final PartialBlockAccessView.AccountChangesBuilder a2 =
        p2.getOrCreateAccountBuilder(accountAddress);
    a2.withPostBalance(tx2Balance);
    a2.withNonceChange(tx2Nonce);
    a2.withNewCode(tx2Code);
    a2.addStorageChange(slot1, tx2Slot1Value);
    balBuilder.apply(p2.build());

    final BlockAccessList blockAccessList = balBuilder.build();

    final MainnetTransactionProcessor transactionProcessor =
        mock(MainnetTransactionProcessor.class);

    final AtomicInteger locationCounter = new AtomicInteger(0);
    when(transactionProcessor.processTransaction(
            any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              final int transactionLocation = locationCounter.getAndIncrement();
              final WorldUpdater worldUpdater = invocation.getArgument(0, WorldUpdater.class);
              final Account account = worldUpdater.get(accountAddress);

              assertTrue(account != null, "Expected account to exist in world updater");

              switch (transactionLocation) {
                case 0 -> {
                  // transactionLocation = 0 -> balIndex = 1 -> latest < 1 is tx0
                  assertEquals(tx0Balance, account.getBalance());
                  assertEquals(tx0Nonce, account.getNonce());
                  assertEquals(tx0Code, account.getCode());
                  assertEquals(tx0Slot1Value, account.getStorageValue(slot1Key));
                  assertEquals(tx0Slot2Value, account.getStorageValue(slot2Key));
                }
                case 1 -> {
                  // transactionLocation = 1 -> balIndex = 2 -> latest < 2 is tx1
                  assertEquals(tx1Balance, account.getBalance());
                  assertEquals(tx1Nonce, account.getNonce());
                  assertEquals(tx1Code, account.getCode());
                  assertEquals(tx1Slot1Value, account.getStorageValue(slot1Key));
                  assertEquals(tx1Slot2Value, account.getStorageValue(slot2Key));
                }
                case 2 -> {
                  // transactionLocation = 2 -> balIndex = 3 -> latest < 3 is tx2
                  assertEquals(tx2Balance, account.getBalance());
                  assertEquals(tx2Nonce, account.getNonce());
                  assertEquals(tx2Code, account.getCode());
                  assertEquals(tx2Slot1Value, account.getStorageValue(slot1Key));
                }
                default ->
                    throw new IllegalStateException(
                        "Unexpected transactionLocation " + transactionLocation);
              }

              return TransactionProcessingResult.successful(
                  Collections.emptyList(),
                  0,
                  0,
                  Bytes.EMPTY,
                  Optional.empty(),
                  ValidationResult.valid());
            });

    final BlockProcessingContext bpc = mock(BlockProcessingContext.class);
    when(bpc.getOperationTracer()).thenReturn(OperationTracer.NO_TRACING);
    final BalConcurrentTransactionProcessor processor =
        new BalConcurrentTransactionProcessor(
            transactionProcessor, blockAccessList, BalConfiguration.DEFAULT, bpc);

    final Transaction tx0 = mockTransaction();
    final Transaction tx1 = mockTransaction();
    final Transaction tx2 = mockTransaction();

    processor.runAsyncBlock(
        env.protocolContext(),
        env.blockHeader(),
        java.util.List.of(tx0, tx1, tx2),
        MINING_BENEFICIARY,
        (__, ___) -> Hash.EMPTY,
        BLOB_GAS_PRICE,
        sameThreadExecutor,
        Optional.empty(),
        Optional.of(env.blockHeader()));

    final Transaction[] txs = new Transaction[] {tx0, tx1, tx2};
    for (int i = 0; i < txs.length; i++) {
      final Optional<TransactionProcessingResult> maybeResult =
          processor.getProcessingResult(
              env.worldState(), MINING_BENEFICIARY, txs[i], i, Optional.empty(), Optional.empty());

      assertTrue(
          maybeResult.isPresent(),
          "Expected processing result for transactionLocation " + i + " to be present");
      assertTrue(
          maybeResult.get().isSuccessful(),
          "Expected processing result for transactionLocation " + i + " to be successful");
    }
  }
}
