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
package org.hyperledger.besu.ethereum.mainnet;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.FrameSignature;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListFactory;
import org.hyperledger.besu.ethereum.mainnet.blockhash.FrontierPreExecutionProcessor;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.StateRootCommitterFactory;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.StateGasCostCalculator;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.BlockImportTracerProvider;
import org.hyperledger.besu.plugin.services.tracer.BlockAwareOperationTracer;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
abstract class AbstractBlockProcessorTest {

  @Mock private ProtocolContext protocolContext;
  @Mock private MutableBlockchain blockchain;
  @Mock private MainnetTransactionProcessor transactionProcessor;
  @Mock private AbstractBlockProcessor.TransactionReceiptFactory transactionReceiptFactory;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolSpec protocolSpec;
  @Mock private WithdrawalsProcessor withdrawalsProcessor;
  @Mock private BlockRewardProcessor blockRewardProcessor;

  final MutableWorldState worldState = ReferenceTestWorldState.create(emptyMap());
  private TestBlockProcessor blockProcessor;

  @BeforeEach
  void baseSetup() {
    lenient().when(protocolContext.getBlockchain()).thenReturn(blockchain);
    lenient().when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    // Reject at the reward step so processBlock stops right after withdrawals; these tests only
    // care about withdrawal handling, not the BAL / state-root steps that follow.
    lenient()
        .when(blockRewardProcessor.rewardBeneficiaries(any(), any(), any(), any()))
        .thenReturn(false);
    lenient().when(protocolSpec.getBlockRewardProcessor()).thenReturn(blockRewardProcessor);
    lenient()
        .when(protocolSpec.getPreExecutionProcessor())
        .thenReturn(new FrontierPreExecutionProcessor());
    lenient()
        .when(protocolSpec.getStateRootCommitterFactory())
        .thenReturn(new StateRootCommitterFactory(BalConfiguration.DISABLED));
    blockProcessor =
        new TestBlockProcessor(
            transactionProcessor,
            transactionReceiptFactory,
            BlockHeader::getCoinbase,
            protocolSchedule,
            BalConfiguration.DEFAULT);
  }

  @Test
  void withProcessorAndEmptyWithdrawals_WithdrawalsAreNotProcessed() {
    when(protocolSpec.getWithdrawalsProcessor()).thenReturn(Optional.empty());
    blockProcessor.processBlock(
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(testBlockBuilder(emptyList()))
            .blockAccessList(Optional.empty())
            .build());
    verify(withdrawalsProcessor, never()).processWithdrawals(any(), any(), any(), any());
  }

  @Test
  void withNoProcessorAndEmptyWithdrawals_WithdrawalsAreNotProcessed() {
    when(protocolSpec.getWithdrawalsProcessor()).thenReturn(Optional.empty());
    blockProcessor.processBlock(
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(testBlockBuilder(emptyList()))
            .blockAccessList(Optional.empty())
            .build());
    verify(withdrawalsProcessor, never()).processWithdrawals(any(), any(), any(), any());
  }

  @Test
  void blockTracerFromContextIsUsedInsteadOfBlockImportTracer() {
    final BlockAwareOperationTracer blockTracer = mock(BlockAwareOperationTracer.class);

    blockProcessor.processBlock(
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(testBlockBuilder(emptyList()))
            .blockAccessList(Optional.empty())
            .sequentialExecutionRequested(true)
            .blockTracer(blockTracer)
            .build());

    verify(blockTracer).traceStartBlock(any(), any(BlockHeader.class), any(Address.class));
    verify(protocolContext, never()).getPluginServiceManager();
  }

  @Test
  void blockImportTracerIsUsedWithoutBlockTracerInContext() {
    final BlockAwareOperationTracer importTracer = mock(BlockAwareOperationTracer.class);
    final ServiceManager serviceManager = mock(ServiceManager.class);
    final BlockImportTracerProvider provider = header -> importTracer;
    when(protocolContext.getPluginServiceManager()).thenReturn(serviceManager);
    when(serviceManager.getService(BlockImportTracerProvider.class))
        .thenReturn(Optional.of(provider));

    blockProcessor.processBlock(
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(testBlockBuilder(emptyList()))
            .blockAccessList(Optional.empty())
            .build());

    verify(importTracer).traceStartBlock(any(), any(BlockHeader.class), any(Address.class));
  }

  @Test
  void blockAccessListFactoryFromContextIsUsedWhenTheForkHasNone() {
    // The fork defines no block access list factory (the mocked spec returns empty).
    final BlockAccessListFactory blockAccessListFactory = spy(new BlockAccessListFactory());

    blockProcessor.processBlock(
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(testBlockBuilder(emptyList()))
            .blockAccessList(Optional.empty())
            .blockAccessListFactoryProvider(spec -> Optional.of(blockAccessListFactory))
            .build());

    verify(blockAccessListFactory).newBlockAccessListBuilder();
  }

  @Test
  void withProcessorAndWithdrawals_WithdrawalsAreProcessed() {
    when(protocolSpec.getWithdrawalsProcessor()).thenReturn(Optional.of(withdrawalsProcessor));
    final List<Withdrawal> withdrawals =
        List.of(new Withdrawal(UInt64.ONE, UInt64.ONE, Address.fromHexString("0x1"), GWei.ONE));
    blockProcessor.processBlock(
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(testBlockBuilder(withdrawals))
            .blockAccessList(Optional.empty())
            .build());
    verify(withdrawalsProcessor).processWithdrawals(eq(withdrawals), any(), any(), any());
  }

  @Test
  void withNoProcessorAndWithdrawals_WithdrawalsAreNotProcessed() {
    when(protocolSpec.getWithdrawalsProcessor()).thenReturn(Optional.empty());

    final List<Withdrawal> withdrawals =
        List.of(new Withdrawal(UInt64.ONE, UInt64.ONE, Address.fromHexString("0x1"), GWei.ONE));
    blockProcessor.processBlock(
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(testBlockBuilder(withdrawals))
            .blockAccessList(Optional.empty())
            .build());
    verify(withdrawalsProcessor, never()).processWithdrawals(any(), any(), any(), any());
  }

  @Test
  void hasAvailableBlockBudget_delegates2DCheckToStrategy() {
    // EIP-8037: hasAvailableBlockBudget must delegate to
    // BlockGasAccountingStrategy.hasBlockCapacity so that block import uses the same 2D headroom
    // logic as block building.
    final long blockGasLimit = 100_000L;
    final BlockHeader header = new BlockHeaderTestFixture().gasLimit(blockGasLimit).buildHeader();
    final Transaction tx = mock(Transaction.class);
    when(tx.getType()).thenReturn(TransactionType.EIP1559);
    when(tx.getGasLimit()).thenReturn(50_000L);
    when(tx.getHash()).thenReturn(Hash.fromHexStringLenient("0x1234"));

    final GasCalculator gasCalculator = mock(GasCalculator.class);
    final StateGasCostCalculator stateGasCalc = mock(StateGasCostCalculator.class);
    when(gasCalculator.stateGasCostCalculator()).thenReturn(stateGasCalc);
    when(stateGasCalc.transactionExecutionGasLimit()).thenReturn(Long.MAX_VALUE);
    when(protocolSpec.getGasCalculator()).thenReturn(gasCalculator);

    // Execution=60k, State=40k. Per-dimension: worstCaseExecution = min(MAX, 50k) = 50k.
    // executionAvailable = 100k - 60k = 40k. 50k > 40k → fails AMSTERDAM per-dimension check.
    when(protocolSpec.getBlockGasAccountingStrategy())
        .thenReturn(BlockGasAccountingStrategy.AMSTERDAM);
    assertThat(blockProcessor.hasAvailableBlockBudget(header, tx, 60_000L, 40_000L, protocolSpec))
        .isFalse();

    // txGasLimit=40k: worstCaseExecution=40k <= 40k, worstCaseState=40k <= 60k → passes AMSTERDAM.
    when(tx.getGasLimit()).thenReturn(40_000L);
    assertThat(blockProcessor.hasAvailableBlockBudget(header, tx, 60_000L, 40_000L, protocolSpec))
        .isTrue();

    // Same scenario with FRONTIER (1D check, only execution): 40k <= 100k-60k=40k → passes
    when(protocolSpec.getBlockGasAccountingStrategy())
        .thenReturn(BlockGasAccountingStrategy.FRONTIER);
    assertThat(blockProcessor.hasAvailableBlockBudget(header, tx, 60_000L, 40_000L, protocolSpec))
        .isTrue();
  }

  @Test
  void staticallyInvalidFrameTransactionFailsWithTheFrameErrorBeforeTheBudgetCheck() {
    // EIP-8141: an undefined signature scheme saturates the frame transaction's gas, so the budget
    // check would report it as a gas failure. The frame error must be reported instead.
    when(protocolSpec.getTransactionValidatorFactory()).thenReturn(frameValidatorFactory());
    final Transaction frameTransaction = frameTransactionWithUndefinedScheme();

    final BlockProcessingResult result =
        blockProcessor.processBlock(
            BlockExecutionContext.builder()
                .protocolContext(protocolContext)
                .worldState(worldState)
                .block(blockWith(frameTransaction, 30_000_000L))
                .blockAccessList(Optional.empty())
                .sequentialExecutionRequested(true)
                .build());

    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.errorMessage)
        .hasValueSatisfying(
            message ->
                assertThat(message)
                    .contains("unknown signature scheme 3")
                    .contains(frameTransaction.getHash().getBytes().toHexString()));
    verify(protocolSpec, never()).getBlockGasAccountingStrategy();
    verifyNoInteractions(transactionProcessor);
  }

  @Test
  void staticallyInvalidFrameTransactionResetsTheBonsaiAccumulator() {
    when(protocolSpec.getTransactionValidatorFactory()).thenReturn(frameValidatorFactory());
    final BonsaiWorldState bonsaiWorldState = mock(BonsaiWorldState.class);
    final BonsaiWorldStateUpdateAccumulator accumulator =
        mock(BonsaiWorldStateUpdateAccumulator.class);
    doReturn(accumulator).when(bonsaiWorldState).updater();

    final BlockProcessingResult result =
        blockProcessor.processBlock(
            BlockExecutionContext.builder()
                .protocolContext(protocolContext)
                .worldState(bonsaiWorldState)
                .block(blockWith(frameTransactionWithUndefinedScheme(), 30_000_000L))
                .blockAccessList(Optional.empty())
                .sequentialExecutionRequested(true)
                .build());

    assertThat(result.isSuccessful()).isFalse();
    verify(accumulator).reset();
    verifyNoInteractions(transactionProcessor);
  }

  @Test
  void staticallyValidFrameTransactionReachesTheFrameAwareBudgetCheck() {
    when(protocolSpec.getTransactionValidatorFactory()).thenReturn(frameValidatorFactory());
    final GasCalculator gasCalculator = mock(GasCalculator.class);
    final StateGasCostCalculator stateGasCalc = mock(StateGasCostCalculator.class);
    when(gasCalculator.stateGasCostCalculator()).thenReturn(stateGasCalc);
    when(stateGasCalc.transactionExecutionGasLimit()).thenReturn(Long.MAX_VALUE);
    when(protocolSpec.getGasCalculator()).thenReturn(gasCalculator);
    when(protocolSpec.getBlockGasAccountingStrategy())
        .thenReturn(BlockGasAccountingStrategy.AMSTERDAM);

    // Valid, but it declares more state gas than the whole block has.
    final Transaction frameTransaction =
        frameTransactionBuilder()
            .frames(
                List.of(
                    new Frame(
                        Frame.MODE_SENDER,
                        Frame.APPROVE_SCOPE_MASK,
                        Optional.empty(),
                        50_000L,
                        200_000L,
                        Wei.ZERO,
                        Bytes.EMPTY)))
            .build();

    final BlockProcessingResult result =
        blockProcessor.processBlock(
            BlockExecutionContext.builder()
                .protocolContext(protocolContext)
                .worldState(worldState)
                .block(blockWith(frameTransaction, 100_000L))
                .blockAccessList(Optional.empty())
                .sequentialExecutionRequested(true)
                .build());

    assertThat(result).isSameAs(BlockProcessingResult.INSUFFICIENT_BLOCK_GAS);
    verifyNoInteractions(transactionProcessor);
  }

  private static TransactionValidatorFactory frameValidatorFactory() {
    return new TransactionValidatorFactory(
        mock(GasCalculator.class),
        GasLimitCalculator.constant(),
        FeeMarket.london(0L),
        false,
        Optional.of(BigInteger.ONE),
        Set.of(TransactionType.EIP1559, TransactionType.FRAME),
        Integer.MAX_VALUE);
  }

  private static Transaction.Builder frameTransactionBuilder() {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(BigInteger.ONE)
        .nonce(0)
        .sender(Address.fromHexString("0x1111111111111111111111111111111111111111"))
        .frameSignatures(List.of())
        .maxPriorityFeePerGas(Wei.ZERO)
        .maxFeePerGas(Wei.ZERO);
  }

  private static Transaction frameTransactionWithUndefinedScheme() {
    return frameTransactionBuilder()
        .frames(
            List.of(
                new Frame(
                    Frame.MODE_VERIFY,
                    Frame.APPROVE_SCOPE_MASK,
                    Optional.empty(),
                    50_000L,
                    0L,
                    Wei.ZERO,
                    Bytes.EMPTY)))
        .frameSignatures(List.of(new FrameSignature(3, Bytes.EMPTY, Bytes.EMPTY, Bytes.EMPTY)))
        .build();
  }

  private static Block blockWith(final Transaction transaction, final long gasLimit) {
    return new Block(
        new BlockHeaderTestFixture()
            .gasLimit(gasLimit)
            .baseFeePerGas(Wei.ZERO)
            .transactionsRoot(Hash.EMPTY_LIST_HASH)
            .ommersHash(Hash.EMPTY_LIST_HASH)
            .buildHeader(),
        new BlockBody(List.of(transaction), emptyList()));
  }

  private static class TestBlockProcessor extends AbstractBlockProcessor {

    protected TestBlockProcessor(
        final MainnetTransactionProcessor transactionProcessor,
        final TransactionReceiptFactory transactionReceiptFactory,
        final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
        final ProtocolSchedule protocolSchedule,
        final BalConfiguration balConfiguration) {
      super(
          transactionProcessor,
          transactionReceiptFactory,
          miningBeneficiaryCalculator,
          protocolSchedule,
          balConfiguration);
    }
  }

  Block testBlockBuilder(final List<Withdrawal> withdrawals) {
    return new Block(
        new BlockHeaderTestFixture()
            .transactionsRoot(Hash.EMPTY_LIST_HASH)
            .ommersHash(Hash.EMPTY_LIST_HASH)
            .buildHeader(),
        new BlockBody(emptyList(), emptyList(), Optional.ofNullable(withdrawals)));
  }
}
