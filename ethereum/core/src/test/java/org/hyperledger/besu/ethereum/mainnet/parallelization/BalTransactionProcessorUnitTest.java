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
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
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
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for BalConcurrentTransactionProcessor (BAL strategy). Tests verify: - Pre-state setup
 * from BlockAccessList - Balance, nonce, code, and storage slot values are correctly applied -
 * Transaction processing with BAL-based world state
 */
@ExtendWith(MockitoExtension.class)
class BalTransactionProcessorUnitTest {

  private static final Address MINING_BENEFICIARY = Address.fromHexString("0x1");
  private static final Wei BLOB_GAS_PRICE = Wei.ZERO;
  private final Executor sameThreadExecutor = Runnable::run;

  @Mock private MainnetTransactionProcessor transactionProcessor;

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

  private BlockAccessList mockEmptyBlockAccessList() {
    final BlockAccessList blockAccessList = mock(BlockAccessList.class);
    when(blockAccessList.accountChanges()).thenReturn(Collections.emptyList());
    return blockAccessList;
  }

  private void stubSuccessfulTransaction() {
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
  }

  @Nested
  @DisplayName("Transaction Processing")
  class TransactionProcessingTests {

    @Test
    @DisplayName("Transaction processor is called with correct parameters")
    void transactionProcessorCalledWithCorrectParams() {
      final TestEnvironment env = createTestEnvironment();
      final BlockAccessList blockAccessList = mockEmptyBlockAccessList();
      final Transaction transaction = mockTransaction();
      stubSuccessfulTransaction();

      final BalConcurrentTransactionProcessor processor =
          new BalConcurrentTransactionProcessor(
              transactionProcessor, blockAccessList, BalConfiguration.DEFAULT);

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
              any());
    }

    @Test
    @DisplayName("All transactions are processed")
    void allTransactionsProcessed() {
      final TestEnvironment env = createTestEnvironment();
      final BlockAccessList blockAccessList = mockEmptyBlockAccessList();
      final Transaction tx1 = mockTransaction();
      final Transaction tx2 = mockTransaction();
      final Transaction tx3 = mockTransaction();
      stubSuccessfulTransaction();

      final BalConcurrentTransactionProcessor processor =
          new BalConcurrentTransactionProcessor(
              transactionProcessor, blockAccessList, BalConfiguration.DEFAULT);

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

    @Test
    @DisplayName("Processing result is returned for successful transaction")
    void processingResultReturnedForSuccessfulTransaction() {
      final TestEnvironment env = createTestEnvironment();
      final BlockAccessList blockAccessList = mockEmptyBlockAccessList();
      final Transaction transaction = mockTransaction();
      stubSuccessfulTransaction();

      final BalConcurrentTransactionProcessor processor =
          new BalConcurrentTransactionProcessor(
              transactionProcessor, blockAccessList, BalConfiguration.DEFAULT);

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

      assertTrue(result.isPresent(), "Expected processing result to be present");
      assertTrue(result.get().isSuccessful(), "Expected successful result");
    }
  }

  @Nested
  @DisplayName("Parent header and world state loading")
  class ParentHeaderAndWorldStateTests {

    @Test
    @DisplayName("Does not query archive or process transaction when parent header is absent")
    void skipsProcessingWhenParentHeaderAbsent() {
      final ProtocolContext protocolContext = mock(ProtocolContext.class);
      final BlockHeader blockHeader = mock(BlockHeader.class);
      final BlockAccessList blockAccessList = mock(BlockAccessList.class);
      final Transaction transaction = mock(Transaction.class);
      final BonsaiWorldState worldStateForResult = createEmptyWorldState();
      final BalConcurrentTransactionProcessor processor =
          new BalConcurrentTransactionProcessor(
              transactionProcessor, blockAccessList, BalConfiguration.DEFAULT);

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
      final TestEnvironment env = createTestEnvironment();
      when(env.worldStateArchive().getWorldState(any())).thenReturn(Optional.empty());
      final BlockAccessList blockAccessList = mock(BlockAccessList.class);
      final Transaction transaction = mock(Transaction.class);
      final BalConcurrentTransactionProcessor processor =
          new BalConcurrentTransactionProcessor(
              transactionProcessor, blockAccessList, BalConfiguration.DEFAULT);

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
      final TestEnvironment env = createTestEnvironment();
      stubSuccessfulTransaction();
      final BlockAccessList blockAccessList = mockEmptyBlockAccessList();
      final Transaction transaction = mockTransaction();
      final BlockHeader parent = env.maybeParentHeader().orElseThrow();
      final BalConcurrentTransactionProcessor processor =
          new BalConcurrentTransactionProcessor(
              transactionProcessor, blockAccessList, BalConfiguration.DEFAULT);

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
  @DisplayName("Pre-State Setup")
  class PreStateSetupTests {

    @Test
    @DisplayName("Pre-state is correctly set up from BlockAccessList")
    void preStateSetupFromBlockAccessList() {
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
                    assertEquals(tx0Balance, account.getBalance(), "Balance mismatch for tx0");
                    assertEquals(tx0Nonce, account.getNonce(), "Nonce mismatch for tx0");
                    assertEquals(tx0Code, account.getCode(), "Code mismatch for tx0");
                    assertEquals(
                        tx0Slot1Value, account.getStorageValue(slot1Key), "Slot1 mismatch for tx0");
                    assertEquals(
                        tx0Slot2Value, account.getStorageValue(slot2Key), "Slot2 mismatch for tx0");
                  }
                  case 1 -> {
                    assertEquals(tx1Balance, account.getBalance(), "Balance mismatch for tx1");
                    assertEquals(tx1Nonce, account.getNonce(), "Nonce mismatch for tx1");
                    assertEquals(tx1Code, account.getCode(), "Code mismatch for tx1");
                    assertEquals(
                        tx1Slot1Value, account.getStorageValue(slot1Key), "Slot1 mismatch for tx1");
                    assertEquals(
                        tx1Slot2Value, account.getStorageValue(slot2Key), "Slot2 mismatch for tx1");
                  }
                  case 2 -> {
                    assertEquals(tx2Balance, account.getBalance(), "Balance mismatch for tx2");
                    assertEquals(tx2Nonce, account.getNonce(), "Nonce mismatch for tx2");
                    assertEquals(tx2Code, account.getCode(), "Code mismatch for tx2");
                    assertEquals(
                        tx2Slot1Value, account.getStorageValue(slot1Key), "Slot1 mismatch for tx2");
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

      final BalConcurrentTransactionProcessor processor =
          new BalConcurrentTransactionProcessor(
              transactionProcessor, blockAccessList, BalConfiguration.DEFAULT);

      final Transaction tx0 = mockTransaction();
      final Transaction tx1 = mockTransaction();
      final Transaction tx2 = mockTransaction();

      processor.runAsyncBlock(
          env.protocolContext(),
          env.blockHeader(),
          List.of(tx0, tx1, tx2),
          MINING_BENEFICIARY,
          (__, ___) -> Hash.EMPTY,
          BLOB_GAS_PRICE,
          sameThreadExecutor,
          Optional.empty(),
          env.maybeParentHeader());

      final Transaction[] txs = new Transaction[] {tx0, tx1, tx2};
      for (int i = 0; i < txs.length; i++) {
        final Optional<TransactionProcessingResult> maybeResult =
            processor.getProcessingResult(
                env.worldState(),
                MINING_BENEFICIARY,
                txs[i],
                i,
                Optional.empty(),
                Optional.empty());

        assertTrue(
            maybeResult.isPresent(),
            "Expected processing result for transaction " + i + " to be present");
        assertTrue(
            maybeResult.get().isSuccessful(),
            "Expected processing result for transaction " + i + " to be successful");
      }
    }
  }

  @Nested
  @DisplayName("Fallback Behavior")
  class FallbackBehaviorTests {

    @Test
    @DisplayName("Returns empty when parallel context is null")
    void returnsEmptyWhenParallelContextIsNull() {
      final TestEnvironment env = createTestEnvironment();
      final BlockAccessList blockAccessList = mockEmptyBlockAccessList();
      final Transaction transaction = mockTransaction();

      when(transactionProcessor.processTransaction(
              any(), any(), any(), any(), any(), any(), any(), any(), any()))
          .thenThrow(new RuntimeException("Simulated failure"));

      final BalConcurrentTransactionProcessor processor =
          new BalConcurrentTransactionProcessor(
              transactionProcessor, blockAccessList, BalConfiguration.DEFAULT);

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
  }
}
