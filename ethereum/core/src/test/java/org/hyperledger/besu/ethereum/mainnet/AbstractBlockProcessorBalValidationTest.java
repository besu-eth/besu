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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListFactory;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.mainnet.blockhash.FrontierPreExecutionProcessor;
import org.hyperledger.besu.ethereum.mainnet.parallelization.PreprocessingContext;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.DefaultStateRootCommitterFactory;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestBlockchain;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestWorldState;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Verifies EIP-7928 BAL checks wired in {@link AbstractBlockProcessor}: per-transaction item budget
 * (fail-fast) and post-build hash + size validation.
 */
@ExtendWith(MockitoExtension.class)
class AbstractBlockProcessorBalValidationTest {

  @Mock private ProtocolContext protocolContext;
  @Mock private MainnetTransactionProcessor transactionProcessor;
  @Mock private AbstractBlockProcessor.TransactionReceiptFactory transactionReceiptFactory;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolSpec protocolSpec;
  @Mock private GasCalculator gasCalculator;

  private final Blockchain blockchain = new ReferenceTestBlockchain();
  private final MutableWorldState worldState = ReferenceTestWorldState.create(emptyMap());

  @BeforeEach
  void wireProtocolSpec() {
    lenient().when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    lenient()
        .when(protocolSpec.getPreExecutionProcessor())
        .thenReturn(new FrontierPreExecutionProcessor());
    lenient()
        .when(protocolSpec.getStateRootCommitterFactory())
        .thenReturn(new DefaultStateRootCommitterFactory());
    lenient()
        .when(protocolSpec.getBlockGasAccountingStrategy())
        .thenReturn(BlockGasAccountingStrategy.FRONTIER);
    lenient().when(protocolSpec.getGasCalculator()).thenReturn(gasCalculator);
    lenient().when(gasCalculator.getBlobGasPerBlob()).thenReturn(1L);
    lenient().when(protocolSpec.getWithdrawalsProcessor()).thenReturn(Optional.empty());
    lenient().when(protocolSpec.getRequestProcessorCoordinator()).thenReturn(Optional.empty());
    lenient()
        .when(protocolSpec.getBlockAccessListFactory())
        .thenReturn(Optional.of(new BlockAccessListFactory()));
    lenient()
        .when(protocolSpec.getBlockAccessListValidator())
        .thenAnswer(__ -> MainnetBlockAccessListValidator.create(protocolSchedule));
    lenient()
        .when(transactionReceiptFactory.create(any(), any(), any(), anyLong()))
        .thenAnswer(invocation -> mock(TransactionReceipt.class, Answers.RETURNS_DEEP_STUBS));
  }

  @Test
  void successfulBlockReturnsBuiltBalMatchingHeaderHash() {
    lenient().when(gasCalculator.getBlockAccessListItemCost()).thenReturn(2000L);
    final PartialBlockAccessView partial = partialOneAccountOneSlot(0, testAddress(1), 1L);
    final BlockAccessList.BlockAccessListBuilder builder = BlockAccessList.builder();
    builder.apply(partial);
    final BlockAccessList expectedBal = builder.build();
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .gasLimit(6000L)
            .balHash(BodyValidation.balHash(expectedBal))
            .buildHeader();

    final AtomicInteger txCalls = new AtomicInteger(0);
    final AbstractBlockProcessor processor =
        new BalStubBlockProcessor(
            transactionProcessor,
            transactionReceiptFactory,
            Wei.ZERO,
            BlockHeader::getCoinbase,
            true,
            protocolSchedule,
            BalConfiguration.DEFAULT,
            loc -> {
              txCalls.incrementAndGet();
              assertThat(loc).isZero();
              return TransactionProcessingResult.successful(
                  List.of(),
                  1000L,
                  0L,
                  Bytes.EMPTY,
                  Optional.of(partial),
                  ValidationResult.valid());
            });

    final BlockProcessingResult result =
        processor.processBlock(
            protocolContext,
            blockchain,
            worldState,
            blockWithTxs(header, 1, 5_000L),
            Optional.empty());

    assertThat(result.isSuccessful()).isTrue();
    assertThat(txCalls).hasValue(1);
    assertThat(result.getYield().flatMap(y -> y.getBlockAccessList()))
        .isPresent()
        .get()
        .satisfies(
            bal ->
                assertThat(BodyValidation.balHash(bal))
                    .isEqualTo(header.getBalHash().orElseThrow()));
  }

  @Test
  void perTransactionBalSizeFailFastDoesNotRunFollowingTransactions() {
    lenient().when(gasCalculator.getBlockAccessListItemCost()).thenReturn(2000L);
    final long gasLimit = 16_000L;
    final int maxItems = 8;
    assertThat(gasLimit / 2000L).isEqualTo(maxItems);

    final BlockHeader header = new BlockHeaderTestFixture().gasLimit(gasLimit).buildHeader();

    final AtomicInteger txCalls = new AtomicInteger(0);
    final IntFunction<PartialBlockAccessView> partialForIndex =
        loc -> partialOneAccountOneSlot(loc, testAddress(loc + 1L), loc * 100L + 1L);

    final AbstractBlockProcessor processor =
        new BalStubBlockProcessor(
            transactionProcessor,
            transactionReceiptFactory,
            Wei.ZERO,
            BlockHeader::getCoinbase,
            true,
            protocolSchedule,
            BalConfiguration.DEFAULT,
            loc -> {
              txCalls.incrementAndGet();
              return TransactionProcessingResult.successful(
                  List.of(),
                  1000L,
                  0L,
                  Bytes.EMPTY,
                  Optional.of(partialForIndex.apply(loc)),
                  ValidationResult.valid());
            });

    final BlockProcessingResult result =
        processor.processBlock(
            protocolContext,
            blockchain,
            worldState,
            blockWithTxs(header, 6, 2000L),
            Optional.empty());

    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.errorMessage.orElse("")).contains("Block access list size exceeds maximum");
    assertThat(txCalls).hasValue(5);
  }

  @Test
  void afterBuildFailsWhenHeaderBalHashDoesNotMatchConstructedBal() {
    lenient().when(gasCalculator.getBlockAccessListItemCost()).thenReturn(2000L);
    final PartialBlockAccessView partial = partialOneAccountOneSlot(0, testAddress(1), 1L);
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .gasLimit(30_000_000L)
            .balHash(Hash.fromHexString("cd".repeat(32)))
            .buildHeader();

    final AbstractBlockProcessor processor =
        new BalStubBlockProcessor(
            transactionProcessor,
            transactionReceiptFactory,
            Wei.ZERO,
            BlockHeader::getCoinbase,
            true,
            protocolSchedule,
            BalConfiguration.DEFAULT,
            loc ->
                TransactionProcessingResult.successful(
                    List.of(),
                    1000L,
                    0L,
                    Bytes.EMPTY,
                    Optional.of(partial),
                    ValidationResult.valid()));

    final BlockProcessingResult result =
        processor.processBlock(
            protocolContext,
            blockchain,
            worldState,
            blockWithTxs(header, 1, 500_000L),
            Optional.empty());

    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.errorMessage.orElse("")).contains("hash mismatch");
  }

  private static Address testAddress(final long suffix) {
    return Address.fromHexString(String.format("0x%040x", suffix));
  }

  private static PartialBlockAccessView partialOneAccountOneSlot(
      final int txIndex, final Address addr, final long slotBase) {
    final PartialBlockAccessView.PartialBlockAccessViewBuilder builder =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(txIndex);
    builder
        .getOrCreateAccountBuilder(addr)
        .addStorageChange(new StorageSlotKey(UInt256.valueOf(slotBase)), UInt256.ZERO);
    return builder.build();
  }

  private static Block blockWithTxs(
      final BlockHeader header, final int txCount, final long txGasLimit) {
    final Transaction tx = mock(Transaction.class);
    lenient().when(tx.getGasLimit()).thenReturn(txGasLimit);
    lenient().when(tx.getHash()).thenReturn(Hash.EMPTY);
    lenient().when(tx.getType()).thenReturn(TransactionType.FRONTIER);
    lenient().when(tx.getVersionedHashes()).thenReturn(Optional.empty());
    final List<Transaction> txs = new ArrayList<>();
    for (int i = 0; i < txCount; i++) {
      txs.add(tx);
    }
    return new Block(header, new BlockBody(txs, emptyList(), Optional.empty()));
  }

  private static final class BalStubBlockProcessor extends AbstractBlockProcessor {

    private final IntFunction<TransactionProcessingResult> resultByTxIndex;

    BalStubBlockProcessor(
        final MainnetTransactionProcessor transactionProcessor,
        final TransactionReceiptFactory transactionReceiptFactory,
        final Wei blockReward,
        final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
        final boolean skipZeroBlockRewards,
        final ProtocolSchedule protocolSchedule,
        final BalConfiguration balConfiguration,
        final IntFunction<TransactionProcessingResult> resultByTxIndex) {
      super(
          transactionProcessor,
          transactionReceiptFactory,
          blockReward,
          miningBeneficiaryCalculator,
          skipZeroBlockRewards,
          protocolSchedule,
          balConfiguration);
      this.resultByTxIndex = resultByTxIndex;
    }

    @Override
    protected boolean rewardCoinbase(
        final MutableWorldState worldState,
        final BlockHeader header,
        final List<org.hyperledger.besu.ethereum.core.BlockHeader> ommers,
        final boolean skipZeroBlockRewards) {
      return true;
    }

    @Override
    protected TransactionProcessingResult getTransactionProcessingResult(
        final Optional<PreprocessingContext> preProcessingContext,
        final BlockProcessingContext blockProcessingContext,
        final WorldUpdater transactionUpdater,
        final Wei blobGasPrice,
        final Address miningBeneficiary,
        final Transaction transaction,
        final int location,
        final BlockHashLookup blockHashLookup,
        final Optional<AccessLocationTracker> accessLocationTracker) {
      return resultByTxIndex.apply(location);
    }
  }
}
