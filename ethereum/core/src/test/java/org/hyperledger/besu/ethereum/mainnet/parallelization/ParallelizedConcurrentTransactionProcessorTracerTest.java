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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.SlowBlockTracer;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.NoOpBonsaiCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.NoopBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.tracing.EVMExecutionMetricsTracer;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.tracer.BlockAwareOperationTracer;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for tracer handling in parallel transaction processing, specifically verifying that
 * SlowBlockTracer metrics are correctly captured and merged during parallel transaction execution.
 * Tests cover both the BackgroundTracerFactory utility methods and the end-to-end integration
 * through ParallelizedConcurrentTransactionProcessor.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
class ParallelizedConcurrentTransactionProcessorTracerTest {

  private static final Address MINING_BENEFICIARY = Address.fromHexString("0x1");
  private static final Wei BLOB_GAS_PRICE = Wei.ZERO;

  private final Executor sameThreadExecutor = Runnable::run;

  private MainnetTransactionProcessor transactionProcessor;
  private TransactionCollisionDetector collisionDetector;
  private ProtocolContext protocolContext;
  private BlockHeader blockHeader;
  private BonsaiWorldState worldState;

  @BeforeEach
  void setUp() {
    transactionProcessor = mock(MainnetTransactionProcessor.class);
    collisionDetector = mock(TransactionCollisionDetector.class);

    // No collisions by default
    when(collisionDetector.hasCollision(any(), any(), any(), any())).thenReturn(false);

    // Stub successful transaction processing
    when(transactionProcessor.processTransaction(
            any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            TransactionProcessingResult.successful(
                Collections.emptyList(),
                0,
                0,
                Bytes.EMPTY,
                Optional.empty(),
                ValidationResult.valid()));

    protocolContext = mock(ProtocolContext.class);
    final MutableBlockchain blockchain = mock(MutableBlockchain.class);
    final BlockHeader chainHeadBlockHeader = mock(BlockHeader.class);
    blockHeader = mock(BlockHeader.class);
    final WorldStateArchive worldStateArchive = mock(WorldStateArchive.class);
    worldState = createEmptyWorldState();

    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHeadHeader()).thenReturn(chainHeadBlockHeader);
    when(chainHeadBlockHeader.getHash()).thenReturn(Hash.ZERO);
    when(chainHeadBlockHeader.getStateRoot()).thenReturn(Hash.EMPTY_TRIE_HASH);
    when(blockHeader.getParentHash()).thenReturn(Hash.ZERO);
    when(blockHeader.getStateRoot()).thenReturn(Hash.EMPTY_TRIE_HASH);
    when(blockHeader.getBlockHash()).thenReturn(Hash.ZERO);
    when(protocolContext.getWorldStateArchive()).thenReturn(worldStateArchive);
    when(worldStateArchive.getWorldState(any())).thenReturn(Optional.of(worldState));
  }

  private static BonsaiWorldState createEmptyWorldState() {
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
        WorldStateConfig.createStatefulConfigWithTrie(),
        new CodeCache());
  }

  private static Transaction mockTransaction() {
    final Transaction transaction = mock(Transaction.class);
    when(transaction.detachedCopy()).thenReturn(transaction);
    return transaction;
  }

  private static SlowBlockTracer newSlowBlockTracer() {
    return new SlowBlockTracer(0, mock(BlockAwareOperationTracer.class));
  }

  private ParallelizedConcurrentTransactionProcessor createProcessorWithTracer(
      final BlockProcessingContext bpc) {
    return new ParallelizedConcurrentTransactionProcessor(
        transactionProcessor, collisionDetector, bpc);
  }

  // ---- BackgroundTracerFactory unit tests ----

  @Test
  void createBackgroundTracer_withSlowBlockTracer_createsMetricsTracer() {
    // When blockProcessingContext has a SlowBlockTracer, the background tracer should be
    // an EVMExecutionMetricsTracer (not NO_TRACING)
    final SlowBlockTracer slowBlockTracer = newSlowBlockTracer();
    final BlockProcessingContext bpc = mock(BlockProcessingContext.class);
    when(bpc.getOperationTracer()).thenReturn(slowBlockTracer);

    // Capture the tracer passed to processTransaction
    when(transactionProcessor.processTransaction(
            any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              final OperationTracer tracer = invocation.getArgument(4, OperationTracer.class);
              // The composed tracer should contain an EVMExecutionMetricsTracer (background)
              // and the miningBeneficiaryTracer, NOT NO_TRACING
              assertThat(
                      CompositeOperationTracer.hasTracer(tracer, EVMExecutionMetricsTracer.class))
                  .as("Background tracer should contain EVMExecutionMetricsTracer")
                  .isTrue();
              return TransactionProcessingResult.successful(
                  Collections.emptyList(),
                  0,
                  0,
                  Bytes.EMPTY,
                  Optional.empty(),
                  ValidationResult.valid());
            });

    final Transaction transaction = mockTransaction();
    final ParallelizedConcurrentTransactionProcessor processor = createProcessorWithTracer(bpc);

    processor.runAsyncBlock(
        protocolContext,
        blockHeader,
        Collections.singletonList(transaction),
        MINING_BENEFICIARY,
        (__, ___) -> Hash.EMPTY,
        BLOB_GAS_PRICE,
        sameThreadExecutor,
        Optional.empty(),
        Optional.of(blockHeader));

    // Verify processTransaction was actually called (and our assertion ran)
    verify(transactionProcessor)
        .processTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any());
  }

  // ---- End-to-end integration tests through getProcessingResult ----

  @Test
  void consolidateTracerResults_mergesMetricsIntoSlowBlockTracer() {
    final SlowBlockTracer slowBlockTracer = newSlowBlockTracer();
    slowBlockTracer.traceStartBlock(null, blockHeader, null, MINING_BENEFICIARY);

    final BlockProcessingContext bpc = mock(BlockProcessingContext.class);
    when(bpc.getOperationTracer()).thenReturn(slowBlockTracer);

    final Transaction transaction = mockTransaction();
    final ParallelizedConcurrentTransactionProcessor processor = createProcessorWithTracer(bpc);

    processor.runAsyncBlock(
        protocolContext,
        blockHeader,
        Collections.singletonList(transaction),
        MINING_BENEFICIARY,
        (__, ___) -> Hash.EMPTY,
        BLOB_GAS_PRICE,
        sameThreadExecutor,
        Optional.empty(),
        Optional.of(blockHeader));

    final Optional<TransactionProcessingResult> result =
        processor.getProcessingResult(
            worldState, MINING_BENEFICIARY, transaction, 0, Optional.empty(), Optional.empty());

    assertThat(result).isPresent();
    assertThat(result.get().isSuccessful()).isTrue();

    assertThat(slowBlockTracer.getExecutionStats().getTransactionCount())
        .as("SlowBlockTracer tx_count should be incremented for confirmed parallel tx")
        .isEqualTo(1);
  }

  @Test
  void consolidateTracerResults_incrementsTxCountForMultipleConfirmedTxs() {
    final SlowBlockTracer slowBlockTracer = newSlowBlockTracer();
    slowBlockTracer.traceStartBlock(null, blockHeader, null, MINING_BENEFICIARY);

    final BlockProcessingContext bpc = mock(BlockProcessingContext.class);
    when(bpc.getOperationTracer()).thenReturn(slowBlockTracer);

    final Transaction tx0 = mockTransaction();
    final Transaction tx1 = mockTransaction();
    final Transaction tx2 = mockTransaction();

    final ParallelizedConcurrentTransactionProcessor processor = createProcessorWithTracer(bpc);

    processor.runAsyncBlock(
        protocolContext,
        blockHeader,
        List.of(tx0, tx1, tx2),
        MINING_BENEFICIARY,
        (__, ___) -> Hash.EMPTY,
        BLOB_GAS_PRICE,
        sameThreadExecutor,
        Optional.empty(),
        Optional.of(blockHeader));

    final List<Transaction> txs = List.of(tx0, tx1, tx2);
    for (int i = 0; i < 3; i++) {
      final Optional<TransactionProcessingResult> result =
          processor.getProcessingResult(
              worldState, MINING_BENEFICIARY, txs.get(i), i, Optional.empty(), Optional.empty());
      assertThat(result).isPresent();
    }

    assertThat(slowBlockTracer.getExecutionStats().getTransactionCount())
        .as("tx_count should equal number of confirmed parallel txs")
        .isEqualTo(3);
  }

  @Test
  void consolidateTracerResults_doesNotIncrementTxCountForFailedTx() {
    final SlowBlockTracer slowBlockTracer = newSlowBlockTracer();
    slowBlockTracer.traceStartBlock(null, blockHeader, null, MINING_BENEFICIARY);

    final BlockProcessingContext bpc = mock(BlockProcessingContext.class);
    when(bpc.getOperationTracer()).thenReturn(slowBlockTracer);

    when(transactionProcessor.processTransaction(
            any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            TransactionProcessingResult.failed(
                0,
                0,
                ValidationResult.invalid(
                    org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason
                        .BLOB_GAS_PRICE_BELOW_CURRENT_BLOB_BASE_FEE),
                Optional.of(Bytes.EMPTY),
                Optional.empty(),
                Optional.empty()));

    final Transaction transaction = mockTransaction();
    final ParallelizedConcurrentTransactionProcessor processor = createProcessorWithTracer(bpc);

    processor.runAsyncBlock(
        protocolContext,
        blockHeader,
        Collections.singletonList(transaction),
        MINING_BENEFICIARY,
        (__, ___) -> Hash.EMPTY,
        BLOB_GAS_PRICE,
        sameThreadExecutor,
        Optional.empty(),
        Optional.of(blockHeader));

    final Optional<TransactionProcessingResult> result =
        processor.getProcessingResult(
            worldState, MINING_BENEFICIARY, transaction, 0, Optional.empty(), Optional.empty());

    assertThat(result).isEmpty();

    assertThat(slowBlockTracer.getExecutionStats().getTransactionCount())
        .as("tx_count should not be incremented for failed tx")
        .isEqualTo(0);
  }

  @Test
  void parallelExecution_mergesEvmMetricsFromMultipleThreads() throws Exception {
    // Verify that EVM opcode metrics captured on separate threads are correctly merged
    // into the main SlowBlockTracer after conflict-free parallel execution
    final SlowBlockTracer slowBlockTracer = newSlowBlockTracer();
    slowBlockTracer.traceStartBlock(null, blockHeader, null, MINING_BENEFICIARY);

    final BlockProcessingContext bpc = mock(BlockProcessingContext.class);
    when(bpc.getOperationTracer()).thenReturn(slowBlockTracer);

    // Each parallel thread needs its own world state — return a fresh one per call
    final WorldStateArchive worldStateArchive = mock(WorldStateArchive.class);
    when(worldStateArchive.getWorldState(any()))
        .thenAnswer(invocation -> Optional.of(createEmptyWorldState()));
    when(protocolContext.getWorldStateArchive()).thenReturn(worldStateArchive);

    // Mock processTransaction to simulate EVM opcode activity on the background tracer.
    // The composed tracer (CompositeOperationTracer of EVMExecutionMetricsTracer +
    // miningBeneficiaryTracer)
    // is passed to processTransaction; simulating opcodes on it forwards to the
    // EVMExecutionMetricsTracer.
    when(transactionProcessor.processTransaction(
            any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              final OperationTracer tracer = invocation.getArgument(4, OperationTracer.class);
              // Simulate 2 SLOAD operations per transaction
              simulateOpcode(tracer, "SLOAD");
              simulateOpcode(tracer, "SLOAD");
              // Simulate 1 CALL per transaction
              simulateOpcode(tracer, "CALL");

              return TransactionProcessingResult.successful(
                  Collections.emptyList(),
                  0,
                  0,
                  Bytes.EMPTY,
                  Optional.empty(),
                  ValidationResult.valid());
            });

    final Transaction tx0 = mockTransaction();
    final Transaction tx1 = mockTransaction();
    final Transaction tx2 = mockTransaction();

    final ParallelizedConcurrentTransactionProcessor processor = createProcessorWithTracer(bpc);

    // Use a real thread pool for actual parallel execution
    final ExecutorService threadPool = Executors.newFixedThreadPool(3);
    try {
      processor.runAsyncBlock(
          protocolContext,
          blockHeader,
          List.of(tx0, tx1, tx2),
          MINING_BENEFICIARY,
          (__, ___) -> Hash.EMPTY,
          BLOB_GAS_PRICE,
          threadPool,
          Optional.empty(),
          Optional.of(blockHeader));

      // Wait for all parallel tasks to complete before confirming
      threadPool.shutdown();
      assertThat(threadPool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS))
          .as("Thread pool should terminate within timeout")
          .isTrue();

      // Confirm all three transactions on the main thread
      final List<Transaction> txs = List.of(tx0, tx1, tx2);
      for (int i = 0; i < 3; i++) {
        final Optional<TransactionProcessingResult> result =
            processor.getProcessingResult(
                worldState, MINING_BENEFICIARY, txs.get(i), i, Optional.empty(), Optional.empty());
        assertThat(result).as("Transaction %d should be confirmed", i).isPresent();
      }
    } finally {
      if (!threadPool.isShutdown()) {
        threadPool.shutdown();
      }
    }

    // Verify merged EVM metrics in executionStats: 3 txs x 2 SLOADs = 6, 3 txs x 1 CALL = 3
    assertThat(slowBlockTracer.getExecutionStats().getSloadCount())
        .as("SLOAD count should be sum across all parallel txs")
        .isEqualTo(6);
    assertThat(slowBlockTracer.getExecutionStats().getCallCount())
        .as("CALL count should be sum across all parallel txs")
        .isEqualTo(3);
    assertThat(slowBlockTracer.getExecutionStats().getSstoreCount())
        .as("SSTORE count should be zero (no SSTORE ops simulated)")
        .isEqualTo(0);

    // Verify tx_count
    assertThat(slowBlockTracer.getExecutionStats().getTransactionCount())
        .as("tx_count should equal number of confirmed parallel txs")
        .isEqualTo(3);
  }

  /** Simulates an EVM opcode by calling tracePostExecution with a mocked MessageFrame. */
  private static void simulateOpcode(final OperationTracer tracer, final String opcodeName) {
    final MessageFrame frame = mock(MessageFrame.class);
    final Operation operation = mock(Operation.class);
    when(frame.getCurrentOperation()).thenReturn(operation);
    when(operation.getName()).thenReturn(opcodeName);
    tracer.tracePostExecution(frame, mock(Operation.OperationResult.class));
  }
}
