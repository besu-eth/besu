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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.NoOpBonsaiCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.NoopBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Unit tests for ParallelizedConcurrentTransactionProcessor (Optimistic strategy). Tests verify: -
 * Collision detector is called with correct arguments - Fallback to sequential execution when
 * collision is detected - Access location tracker integration - Partial block access view updates
 */
@ExtendWith(MockitoExtension.class)
class OptimisticTransactionProcessorUnitTest {

  private static final Address MINING_BENEFICIARY = Address.fromHexString("0x1");
  private static final Wei BLOB_GAS_PRICE = Wei.ZERO;
  private final Executor sameThreadExecutor = Runnable::run;

  @Mock private MainnetTransactionProcessor transactionProcessor;
  @Mock private TransactionCollisionDetector collisionDetector;

  private ParallelizedConcurrentTransactionProcessor processor;
  private TestEnvironment env;

  @BeforeEach
  void setUp() {
    processor =
        new ParallelizedConcurrentTransactionProcessor(transactionProcessor, collisionDetector);
    env = createTestEnvironment();
  }

  private record TestEnvironment(
      ProtocolContext protocolContext,
      BlockHeader blockHeader,
      Optional<BlockHeader> maybeParentHeader,
      WorldStateArchive worldStateArchive,
      BonsaiWorldState worldState) {}

  private BonsaiWorldState createEmptyWorldState() {
    final BonsaiWorldStateKeyValueStorage storage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);

    return new BonsaiWorldState(
        storage,
        new NoopBonsaiCachedMerkleTrieLoader(),
        new NoOpBonsaiCachedWorldStorageManager(storage, EvmConfiguration.DEFAULT, new CodeCache()),
        new NoOpTrieLogManager(),
        EvmConfiguration.DEFAULT,
        createStatefulConfigWithTrie(),
        new CodeCache());
  }

  private TestEnvironment createTestEnvironment() {
    final ProtocolContext protocolContext = mock(ProtocolContext.class);
    final BlockHeader parentHeader = mock(BlockHeader.class);
    final BlockHeader blockHeader = mock(BlockHeader.class);
    final WorldStateArchive worldStateArchive = mock(WorldStateArchive.class);
    final BonsaiWorldState worldState = createEmptyWorldState();

    when(protocolContext.getWorldStateArchive()).thenReturn(worldStateArchive);
    when(worldStateArchive.getWorldState(any())).thenReturn(Optional.of(worldState));
    when(parentHeader.getBlockHash()).thenReturn(Hash.ZERO);
    when(parentHeader.getStateRoot()).thenReturn(Hash.EMPTY_TRIE_HASH);

    return new TestEnvironment(
        protocolContext, blockHeader, Optional.of(parentHeader), worldStateArchive, worldState);
  }

  private Transaction mockTransaction() {
    final Transaction transaction = mock(Transaction.class);
    when(transaction.detachedCopy()).thenReturn(transaction);
    return transaction;
  }

  private void stubSuccessfulTransaction(final Optional<PartialBlockAccessView> partialView) {
    when(transactionProcessor.processTransaction(
            any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            TransactionProcessingResult.successful(
                Collections.emptyList(), 0, 0, Bytes.EMPTY, partialView, ValidationResult.valid()));
  }

  @Nested
  @DisplayName("Transaction Processing")
  class TransactionProcessingTests {

    @Test
    @DisplayName("Transaction processor is called with correct parameters")
    void transactionProcessorCalledWithCorrectParams() {
      final Transaction transaction = mockTransaction();
      stubSuccessfulTransaction(Optional.empty());

      processor.runAsyncBlock(
          env.protocolContext(),
          env.blockHeader(),
          Collections.singletonList(transaction),
          MINING_BENEFICIARY,
          (__, ___) -> Hash.EMPTY,
          BLOB_GAS_PRICE,
          sameThreadExecutor,
          Optional.empty(),
          env.maybeParentHeader());

      verify(transactionProcessor, times(1))
          .processTransaction(
              any(WorldUpdater.class),
              eq(env.blockHeader()),
              eq(transaction),
              eq(MINING_BENEFICIARY),
              any(OperationTracer.class),
              any(BlockHashLookup.class),
              eq(TransactionValidationParams.processingBlock()),
              eq(BLOB_GAS_PRICE),
              eq(Optional.empty()));
    }

    @Test
    @DisplayName("Transaction processor is called once per transaction")
    void transactionProcessorCalledOncePerTransaction() {
      final Transaction tx1 = mockTransaction();
      final Transaction tx2 = mockTransaction();
      final Transaction tx3 = mockTransaction();
      stubSuccessfulTransaction(Optional.empty());

      processor.runAsyncBlock(
          env.protocolContext(),
          env.blockHeader(),
          List.of(tx1, tx2, tx3),
          MINING_BENEFICIARY,
          (__, ___) -> Hash.EMPTY,
          BLOB_GAS_PRICE,
          sameThreadExecutor,
          Optional.empty(),
          env.maybeParentHeader());

      verify(transactionProcessor, times(3))
          .processTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }
  }

  @Nested
  @DisplayName("Parent header and world state loading")
  @MockitoSettings(
      strictness = Strictness.LENIENT) // outer @BeforeEach builds env unused by parent-absent test
  class ParentHeaderAndWorldStateTests {

    @Test
    @DisplayName("Does not query archive or process transaction when parent header is absent")
    void skipsProcessingWhenParentHeaderAbsent() {
      final ProtocolContext protocolContext = mock(ProtocolContext.class);
      final BlockHeader blockHeader = mock(BlockHeader.class);
      final Transaction transaction = mock(Transaction.class);
      final BonsaiWorldState worldStateForResult = createEmptyWorldState();

      processor.runAsyncBlock(
          protocolContext,
          blockHeader,
          Collections.singletonList(transaction),
          MINING_BENEFICIARY,
          (__, ___) -> Hash.EMPTY,
          BLOB_GAS_PRICE,
          sameThreadExecutor,
          Optional.empty(),
          Optional.empty());

      verify(protocolContext, never()).getWorldStateArchive();
      verify(transactionProcessor, never())
          .processTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any());
      assertTrue(
          processor
              .getProcessingResult(
                  worldStateForResult,
                  MINING_BENEFICIARY,
                  transaction,
                  0,
                  Optional.empty(),
                  Optional.empty())
              .isEmpty());
    }

    @Test
    @DisplayName("Does not process transaction when world state archive returns empty")
    void skipsProcessingWhenArchiveHasNoWorldState() {
      when(env.worldStateArchive().getWorldState(any())).thenReturn(Optional.empty());
      final Transaction transaction = mock(Transaction.class);

      processor.runAsyncBlock(
          env.protocolContext(),
          env.blockHeader(),
          Collections.singletonList(transaction),
          MINING_BENEFICIARY,
          (__, ___) -> Hash.EMPTY,
          BLOB_GAS_PRICE,
          sameThreadExecutor,
          Optional.empty(),
          env.maybeParentHeader());

      verify(env.worldStateArchive(), times(1)).getWorldState(any());
      verify(transactionProcessor, never())
          .processTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any());
      assertTrue(
          processor
              .getProcessingResult(
                  env.worldState(),
                  MINING_BENEFICIARY,
                  transaction,
                  0,
                  Optional.empty(),
                  Optional.empty())
              .isEmpty());
    }

    @Test
    @DisplayName("World state query uses the parent block header")
    void loadWorldStateUsesParentBlockHeader() {
      final Transaction transaction = mockTransaction();
      stubSuccessfulTransaction(Optional.empty());
      final BlockHeader parent = env.maybeParentHeader().orElseThrow();

      processor.runAsyncBlock(
          env.protocolContext(),
          env.blockHeader(),
          Collections.singletonList(transaction),
          MINING_BENEFICIARY,
          (__, ___) -> Hash.EMPTY,
          BLOB_GAS_PRICE,
          sameThreadExecutor,
          Optional.empty(),
          env.maybeParentHeader());

      verify(env.worldStateArchive())
          .getWorldState(argThat((WorldStateQueryParams p) -> p.getBlockHeader() == parent));
      verify(transactionProcessor, times(1))
          .processTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }
  }

  @Nested
  @DisplayName("Collision Detection")
  class CollisionDetectionTests {

    @Test
    @DisplayName("Collision detector is called with correct transaction")
    void collisionDetectorCalledWithCorrectTransaction() {
      final Transaction transaction = mockTransaction();
      stubSuccessfulTransaction(Optional.empty());
      when(collisionDetector.hasCollision(any(), any(), any(), any())).thenReturn(false);

      processor.runAsyncBlock(
          env.protocolContext(),
          env.blockHeader(),
          Collections.singletonList(transaction),
          MINING_BENEFICIARY,
          (__, ___) -> Hash.EMPTY,
          BLOB_GAS_PRICE,
          sameThreadExecutor,
          Optional.empty(),
          env.maybeParentHeader());

      processor.getProcessingResult(
          env.worldState(), MINING_BENEFICIARY, transaction, 0, Optional.empty(), Optional.empty());

      verify(collisionDetector, times(1))
          .hasCollision(
              eq(transaction),
              eq(MINING_BENEFICIARY),
              any(ParallelizedTransactionContext.class),
              any());
    }

    @Test
    @DisplayName("Collision detector is called with correct mining beneficiary")
    void collisionDetectorCalledWithCorrectMiningBeneficiary() {
      final Transaction transaction = mockTransaction();
      final Address customBeneficiary = Address.fromHexString("0xABCDEF");
      stubSuccessfulTransaction(Optional.empty());
      when(collisionDetector.hasCollision(any(), any(), any(), any())).thenReturn(false);

      processor.runAsyncBlock(
          env.protocolContext(),
          env.blockHeader(),
          Collections.singletonList(transaction),
          customBeneficiary,
          (__, ___) -> Hash.EMPTY,
          BLOB_GAS_PRICE,
          sameThreadExecutor,
          Optional.empty(),
          env.maybeParentHeader());

      processor.getProcessingResult(
          env.worldState(), customBeneficiary, transaction, 0, Optional.empty(), Optional.empty());

      verify(collisionDetector, times(1))
          .hasCollision(eq(transaction), eq(customBeneficiary), any(), any());
    }

    @Test
    @DisplayName("Collision detector is called for each transaction")
    void collisionDetectorCalledForEachTransaction() {
      final Transaction tx1 = mockTransaction();
      final Transaction tx2 = mockTransaction();
      final Transaction tx3 = mockTransaction();
      stubSuccessfulTransaction(Optional.empty());
      when(collisionDetector.hasCollision(any(), any(), any(), any())).thenReturn(false);

      processor.runAsyncBlock(
          env.protocolContext(),
          env.blockHeader(),
          List.of(tx1, tx2, tx3),
          MINING_BENEFICIARY,
          (__, ___) -> Hash.EMPTY,
          BLOB_GAS_PRICE,
          sameThreadExecutor,
          Optional.empty(),
          env.maybeParentHeader());

      processor.getProcessingResult(
          env.worldState(), MINING_BENEFICIARY, tx1, 0, Optional.empty(), Optional.empty());
      processor.getProcessingResult(
          env.worldState(), MINING_BENEFICIARY, tx2, 1, Optional.empty(), Optional.empty());
      processor.getProcessingResult(
          env.worldState(), MINING_BENEFICIARY, tx3, 2, Optional.empty(), Optional.empty());

      verify(collisionDetector, times(1))
          .hasCollision(eq(tx1), eq(MINING_BENEFICIARY), any(), any());
      verify(collisionDetector, times(1))
          .hasCollision(eq(tx2), eq(MINING_BENEFICIARY), any(), any());
      verify(collisionDetector, times(1))
          .hasCollision(eq(tx3), eq(MINING_BENEFICIARY), any(), any());
    }
  }

  @Nested
  @DisplayName("Fallback Behavior")
  class FallbackBehaviorTests {

    @Test
    @DisplayName("Returns empty when collision detected - triggers sequential fallback")
    void returnsEmptyWhenCollisionDetected() {
      final Transaction transaction = mockTransaction();
      stubSuccessfulTransaction(Optional.empty());

      processor.runAsyncBlock(
          env.protocolContext(),
          env.blockHeader(),
          Collections.singletonList(transaction),
          MINING_BENEFICIARY,
          (__, ___) -> Hash.EMPTY,
          BLOB_GAS_PRICE,
          sameThreadExecutor,
          Optional.empty(),
          env.maybeParentHeader());

      when(collisionDetector.hasCollision(any(), any(), any(), any())).thenReturn(true);

      final Optional<TransactionProcessingResult> result =
          processor.getProcessingResult(
              env.worldState(),
              MINING_BENEFICIARY,
              transaction,
              0,
              Optional.empty(),
              Optional.empty());

      assertTrue(result.isEmpty(), "Expected empty result due to collision - triggers fallback");
    }

    @Test
    @DisplayName("Returns empty when parallel context is null")
    void returnsEmptyWhenParallelContextIsNull() {
      final Transaction transaction = mockTransaction();

      when(transactionProcessor.processTransaction(
              any(), any(), any(), any(), any(), any(), any(), any(), any()))
          .thenThrow(new RuntimeException("Simulated failure"));

      processor.runAsyncBlock(
          env.protocolContext(),
          env.blockHeader(),
          Collections.singletonList(transaction),
          MINING_BENEFICIARY,
          (__, ___) -> Hash.EMPTY,
          BLOB_GAS_PRICE,
          sameThreadExecutor,
          Optional.empty(),
          env.maybeParentHeader());

      final Optional<TransactionProcessingResult> result =
          processor.getProcessingResult(
              env.worldState(),
              MINING_BENEFICIARY,
              transaction,
              0,
              Optional.empty(),
              Optional.empty());

      assertTrue(
          result.isEmpty(), "Expected empty result when context is null - triggers fallback");
    }

    @Test
    @DisplayName("Returns result when no collision detected")
    void returnsResultWhenNoCollision() {
      final Transaction transaction = mockTransaction();
      stubSuccessfulTransaction(Optional.empty());
      when(collisionDetector.hasCollision(any(), any(), any(), any())).thenReturn(false);

      processor.runAsyncBlock(
          env.protocolContext(),
          env.blockHeader(),
          Collections.singletonList(transaction),
          MINING_BENEFICIARY,
          (__, ___) -> Hash.EMPTY,
          BLOB_GAS_PRICE,
          sameThreadExecutor,
          Optional.empty(),
          env.maybeParentHeader());

      final Optional<TransactionProcessingResult> result =
          processor.getProcessingResult(
              env.worldState(),
              MINING_BENEFICIARY,
              transaction,
              0,
              Optional.empty(),
              Optional.empty());

      assertTrue(result.isPresent(), "Expected result when no collision");
      assertTrue(result.get().isSuccessful(), "Expected successful result");
    }

    @Test
    @DisplayName("First transaction succeeds, second triggers fallback")
    void partialFallbackOnSecondTransaction() {
      final Transaction tx1 = mockTransaction();
      final Transaction tx2 = mockTransaction();
      stubSuccessfulTransaction(Optional.empty());

      processor.runAsyncBlock(
          env.protocolContext(),
          env.blockHeader(),
          List.of(tx1, tx2),
          MINING_BENEFICIARY,
          (__, ___) -> Hash.EMPTY,
          BLOB_GAS_PRICE,
          sameThreadExecutor,
          Optional.empty(),
          env.maybeParentHeader());

      when(collisionDetector.hasCollision(eq(tx1), any(), any(), any())).thenReturn(false);
      when(collisionDetector.hasCollision(eq(tx2), any(), any(), any())).thenReturn(true);

      final Optional<TransactionProcessingResult> result1 =
          processor.getProcessingResult(
              env.worldState(), MINING_BENEFICIARY, tx1, 0, Optional.empty(), Optional.empty());
      final Optional<TransactionProcessingResult> result2 =
          processor.getProcessingResult(
              env.worldState(), MINING_BENEFICIARY, tx2, 1, Optional.empty(), Optional.empty());

      assertTrue(result1.isPresent(), "First transaction should succeed");
      assertTrue(result2.isEmpty(), "Second transaction should trigger fallback");
    }
  }

  @Nested
  @DisplayName("Access Location Tracker Integration")
  class AccessLocationTrackerTests {

    @Test
    @DisplayName("Access location tracker is passed when BAL builder is provided")
    void accessLocationTrackerPassedWithBalBuilder() {
      final Transaction transaction = mockTransaction();
      stubSuccessfulTransaction(Optional.empty());

      final BlockAccessListBuilder balBuilder = mock(BlockAccessListBuilder.class);

      processor.runAsyncBlock(
          env.protocolContext(),
          env.blockHeader(),
          Collections.singletonList(transaction),
          MINING_BENEFICIARY,
          (__, ___) -> Hash.EMPTY,
          BLOB_GAS_PRICE,
          sameThreadExecutor,
          Optional.of(balBuilder),
          env.maybeParentHeader());

      verify(transactionProcessor)
          .processTransaction(
              any(WorldUpdater.class),
              eq(env.blockHeader()),
              eq(transaction),
              eq(MINING_BENEFICIARY),
              any(OperationTracer.class),
              any(BlockHashLookup.class),
              eq(TransactionValidationParams.processingBlock()),
              eq(BLOB_GAS_PRICE),
              argThat(Optional::isPresent));
    }

    @Test
    @DisplayName("Partial block access view is preserved in result")
    void partialBlockAccessViewPreservedInResult() {
      final Transaction transaction = mockTransaction();

      final PartialBlockAccessView partialView = mock(PartialBlockAccessView.class);

      stubSuccessfulTransaction(Optional.of(partialView));
      when(collisionDetector.hasCollision(any(), any(), any(), any())).thenReturn(false);

      final BlockAccessListBuilder balBuilder = mock(BlockAccessListBuilder.class);

      processor.runAsyncBlock(
          env.protocolContext(),
          env.blockHeader(),
          Collections.singletonList(transaction),
          MINING_BENEFICIARY,
          (__, ___) -> Hash.EMPTY,
          BLOB_GAS_PRICE,
          sameThreadExecutor,
          Optional.of(balBuilder),
          env.maybeParentHeader());

      final Optional<TransactionProcessingResult> maybeResult =
          processor.getProcessingResult(
              env.worldState(),
              MINING_BENEFICIARY,
              transaction,
              0,
              Optional.empty(),
              Optional.empty());

      assertTrue(maybeResult.isPresent(), "Expected result to be applied");
      assertTrue(
          maybeResult.get().getPartialBlockAccessView().isPresent(),
          "Expected BAL view to be present");
    }
  }
}
