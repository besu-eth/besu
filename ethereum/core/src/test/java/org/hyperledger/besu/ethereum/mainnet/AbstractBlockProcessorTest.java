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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.blockhash.FrontierPreExecutionProcessor;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.DefaultStateRootCommitterFactory;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestBlockchain;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestWorldState;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.StateGasCostCalculator;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
abstract class AbstractBlockProcessorTest {

  @Mock private ProtocolContext protocolContext;
  @Mock private MainnetTransactionProcessor transactionProcessor;
  @Mock private AbstractBlockProcessor.TransactionReceiptFactory transactionReceiptFactory;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolSpec protocolSpec;
  @Mock private WithdrawalsProcessor withdrawalsProcessor;

  final Blockchain blockchain = new ReferenceTestBlockchain();
  final MutableWorldState worldState = ReferenceTestWorldState.create(emptyMap());
  private TestBlockProcessor blockProcessor;

  @BeforeEach
  void baseSetup() {
    lenient().when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    lenient()
        .when(protocolSpec.getPreExecutionProcessor())
        .thenReturn(new FrontierPreExecutionProcessor());
    lenient()
        .when(protocolSpec.getStateRootCommitterFactory())
        .thenReturn(new DefaultStateRootCommitterFactory());
    blockProcessor =
        new TestBlockProcessor(
            transactionProcessor,
            transactionReceiptFactory,
            Wei.ZERO,
            BlockHeader::getCoinbase,
            true,
            protocolSchedule,
            BalConfiguration.DEFAULT);
  }

  @Test
  void withProcessorAndEmptyWithdrawals_WithdrawalsAreNotProcessed() {
    when(protocolSpec.getWithdrawalsProcessor()).thenReturn(Optional.empty());
    blockProcessor.processBlock(
        protocolContext, blockchain, worldState, testBlockBuilder(emptyList()));
    verify(withdrawalsProcessor, never()).processWithdrawals(any(), any(), any(), any());
  }

  @Test
  void withNoProcessorAndEmptyWithdrawals_WithdrawalsAreNotProcessed() {
    when(protocolSpec.getWithdrawalsProcessor()).thenReturn(Optional.empty());
    blockProcessor.processBlock(
        protocolContext, blockchain, worldState, testBlockBuilder(emptyList()));
    verify(withdrawalsProcessor, never()).processWithdrawals(any(), any(), any(), any());
  }

  @Test
  void withProcessorAndWithdrawals_WithdrawalsAreProcessed() {
    when(protocolSpec.getWithdrawalsProcessor()).thenReturn(Optional.of(withdrawalsProcessor));
    final List<Withdrawal> withdrawals =
        List.of(new Withdrawal(UInt64.ONE, UInt64.ONE, Address.fromHexString("0x1"), GWei.ONE));
    blockProcessor.processBlock(
        protocolContext, blockchain, worldState, testBlockBuilder(withdrawals));
    verify(withdrawalsProcessor).processWithdrawals(eq(withdrawals), any(), any(), any());
  }

  @Test
  void withNoProcessorAndWithdrawals_WithdrawalsAreNotProcessed() {
    when(protocolSpec.getWithdrawalsProcessor()).thenReturn(Optional.empty());

    final List<Withdrawal> withdrawals =
        List.of(new Withdrawal(UInt64.ONE, UInt64.ONE, Address.fromHexString("0x1"), GWei.ONE));
    blockProcessor.processBlock(
        protocolContext, blockchain, worldState, testBlockBuilder(withdrawals));
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
    when(tx.getGasLimit()).thenReturn(50_000L);
    when(tx.getHash()).thenReturn(Hash.fromHexStringLenient("0x1234"));
    when(tx.getAccessList()).thenReturn(Optional.empty());
    when(tx.codeDelegationListSize()).thenReturn(0);
    when(tx.isContractCreation()).thenReturn(false);

    final GasCalculator gasCalculator = mock(GasCalculator.class);
    final StateGasCostCalculator stateGasCalc = mock(StateGasCostCalculator.class);
    when(gasCalculator.stateGasCostCalculator()).thenReturn(stateGasCalc);
    when(gasCalculator.accessListGasCost(0, 0)).thenReturn(0L);
    when(gasCalculator.delegateCodeGasCost(0)).thenReturn(0L);
    when(gasCalculator.transactionIntrinsicGasCost(tx, 0L)).thenReturn(0L);
    when(stateGasCalc.transactionIntrinsicStateGas(false, 0L)).thenReturn(0L);
    when(stateGasCalc.transactionRegularGasLimit()).thenReturn(Long.MAX_VALUE);
    when(protocolSpec.getGasCalculator()).thenReturn(gasCalculator);

    // Regular=60k, State=40k. Per-dimension: worstCaseRegular = min(MAX, 50k - 0) = 50k.
    // regularAvailable = 100k - 60k = 40k. 50k > 40k → fails AMSTERDAM per-dimension check.
    when(protocolSpec.getBlockGasAccountingStrategy())
        .thenReturn(BlockGasAccountingStrategy.AMSTERDAM);
    assertThat(blockProcessor.hasAvailableBlockBudget(header, tx, 60_000L, 40_000L, protocolSpec))
        .isFalse();

    // txGasLimit=40k: worstCaseRegular=40k <= 40k, worstCaseState=40k <= 60k → passes AMSTERDAM.
    when(tx.getGasLimit()).thenReturn(40_000L);
    assertThat(blockProcessor.hasAvailableBlockBudget(header, tx, 60_000L, 40_000L, protocolSpec))
        .isTrue();

    // Same scenario with FRONTIER (1D check, only regular): 40k <= 100k-60k=40k → passes
    when(protocolSpec.getBlockGasAccountingStrategy())
        .thenReturn(BlockGasAccountingStrategy.FRONTIER);
    assertThat(blockProcessor.hasAvailableBlockBudget(header, tx, 60_000L, 40_000L, protocolSpec))
        .isTrue();
  }

  private static class TestBlockProcessor extends AbstractBlockProcessor {

    protected TestBlockProcessor(
        final MainnetTransactionProcessor transactionProcessor,
        final TransactionReceiptFactory transactionReceiptFactory,
        final Wei blockReward,
        final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
        final boolean skipZeroBlockRewards,
        final ProtocolSchedule protocolSchedule,
        final BalConfiguration balConfiguration) {
      super(
          transactionProcessor,
          transactionReceiptFactory,
          blockReward,
          miningBeneficiaryCalculator,
          skipZeroBlockRewards,
          protocolSchedule,
          balConfiguration);
    }

    @Override
    boolean rewardCoinbase(
        final MutableWorldState worldState,
        final BlockHeader header,
        final List<BlockHeader> ommers,
        final boolean skipZeroBlockRewards) {
      return false;
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
