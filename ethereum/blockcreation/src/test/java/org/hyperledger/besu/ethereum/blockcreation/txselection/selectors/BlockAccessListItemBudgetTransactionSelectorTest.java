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
package org.hyperledger.besu.ethereum.blockcreation.txselection.selectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.BLOCK_ACCESS_LIST_ITEM_BUDGET_EXCEEDED;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.SELECTED;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.blockcreation.txselection.BlockSelectionContext;
import org.hyperledger.besu.ethereum.blockcreation.txselection.TransactionEvaluationContext;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.mainnet.BlockAccessListItemSizeCheck;
import org.hyperledger.besu.ethereum.mainnet.BlockAccessListValidationError;
import org.hyperledger.besu.ethereum.mainnet.BlockAccessListValidator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;

import java.util.List;
import java.util.Optional;

import com.google.common.base.Stopwatch;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for {@link BlockAccessListItemBudgetTransactionSelector}: when a BAL builder is
 * present, post-processing probes committed state + the current tx partial and delegates the
 * EIP-7928 item budget to {@link BlockAccessListValidator#validateExecutedBlockAccessListItemSize};
 * when no builder is configured, the selector is a no-op.
 */
@ExtendWith(MockitoExtension.class)
class BlockAccessListItemBudgetTransactionSelectorTest {

  @Mock private ProcessableBlockHeader pendingBlockHeader;
  @Mock private ProtocolSpec protocolSpec;
  @Mock private BlockAccessListValidator balValidator;
  @Mock private PendingTransaction pendingTransaction;

  @BeforeEach
  void setUp() {
    lenient().when(protocolSpec.getBlockAccessListValidator()).thenReturn(balValidator);
  }

  /**
   * This selector never applies the EIP-7928 item budget in the pre-processing phase, so a
   * transaction is never dropped at pre-processing for that reason (budget is enforced only in
   * post-processing when a BAL builder is present).
   */
  @Test
  void preProcessingAlwaysSelectsRegardlessOfBalBuilderPresence() {
    final TransactionEvaluationContext ctx = evalContext();

    final BlockAccessListItemBudgetTransactionSelector withoutBuilder =
        new BlockAccessListItemBudgetTransactionSelector(context(), Optional.empty());
    assertThat(withoutBuilder.evaluateTransactionPreProcessing(ctx)).isEqualTo(SELECTED);

    final BlockAccessListItemBudgetTransactionSelector withBuilder =
        new BlockAccessListItemBudgetTransactionSelector(
            context(), Optional.of(BlockAccessList.builder()));
    assertThat(withBuilder.evaluateTransactionPreProcessing(ctx)).isEqualTo(SELECTED);
  }

  /**
   * Empty committed BAL + partial adding two EIP-7928 items: the probe item count passed to the
   * validator is 2; when the validator accepts, the selector returns {@code SELECTED}.
   */
  @Test
  void postProcessingWithinBudgetPassesValidatorAndReturnsSelected() {
    when(balValidator.validateExecutedBlockAccessListItemSize(
            anyLong(), eq(pendingBlockHeader), eq(protocolSpec)))
        .thenReturn(BlockAccessListItemSizeCheck.withinBudget());

    final BlockAccessList.BlockAccessListBuilder balBuilder = BlockAccessList.builder();
    final BlockAccessListItemBudgetTransactionSelector selector =
        new BlockAccessListItemBudgetTransactionSelector(context(), Optional.of(balBuilder));

    final TransactionProcessingResult result =
        TransactionProcessingResult.successful(
            List.of(),
            21_000L,
            0L,
            Bytes.EMPTY,
            Optional.of(twoItemPartial(0)),
            ValidationResult.valid());

    assertThat(selector.evaluateTransactionPostProcessing(evalContext(), result))
        .isEqualTo(SELECTED);

    final ArgumentCaptor<Long> countCaptor = ArgumentCaptor.forClass(Long.class);
    verify(balValidator)
        .validateExecutedBlockAccessListItemSize(
            countCaptor.capture(), eq(pendingBlockHeader), eq(protocolSpec));
    assertThat(countCaptor.getValue()).isEqualTo(2L);
  }

  /**
   * Committed builder already holds 2 items; the candidate partial adds 2 more (probe total 4).
   * Stubbed validator rejects counts {@code > 3}, so the selector returns {@code
   * BLOCK_ACCESS_LIST_ITEM_BUDGET_EXCEEDED} without mutating the main builder’s committed view
   * beyond what the test applied before the selector ran.
   */
  @Test
  void postProcessingWhenValidatorRejectsReturnsBudgetExceeded() {
    when(balValidator.validateExecutedBlockAccessListItemSize(
            anyLong(), eq(pendingBlockHeader), eq(protocolSpec)))
        .thenAnswer(
            inv ->
                ((long) inv.getArgument(0)) > 3L
                    ? BlockAccessListItemSizeCheck.overBudget(
                        new BlockAccessListValidationError("over budget"))
                    : BlockAccessListItemSizeCheck.withinBudget());

    final BlockAccessList.BlockAccessListBuilder balBuilder = BlockAccessList.builder();
    balBuilder.apply(twoItemPartial(0));

    final BlockAccessListItemBudgetTransactionSelector selector =
        new BlockAccessListItemBudgetTransactionSelector(context(), Optional.of(balBuilder));

    final TransactionProcessingResult result =
        TransactionProcessingResult.successful(
            List.of(),
            21_000L,
            0L,
            Bytes.EMPTY,
            Optional.of(twoItemPartial(1)),
            ValidationResult.valid());

    assertThat(selector.evaluateTransactionPostProcessing(evalContext(), result))
        .isEqualTo(BLOCK_ACCESS_LIST_ITEM_BUDGET_EXCEEDED);
  }

  private BlockSelectionContext context() {
    return new BlockSelectionContext(
        MiningConfiguration.newDefault(),
        pendingBlockHeader,
        protocolSpec,
        Wei.ZERO,
        Address.fromHexString("0x0000000000000000000000000000000000000001"),
        null);
  }

  private TransactionEvaluationContext evalContext() {
    return new TransactionEvaluationContext(
        pendingBlockHeader,
        pendingTransaction,
        Stopwatch.createStarted(),
        Wei.ONE,
        Wei.ONE,
        () -> false);
  }

  /** One account + one storage write → 2 EIP-7928 items (same shape as other BAL tests). */
  private static PartialBlockAccessView twoItemPartial(final int txIndex) {
    final Address addr = Address.fromHexString(String.format("0x%040x", txIndex + 100L));
    final PartialBlockAccessView.PartialBlockAccessViewBuilder b =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(txIndex);
    b.getOrCreateAccountBuilder(addr)
        .addStorageChange(new StorageSlotKey(UInt256.ONE), UInt256.ZERO);
    return b.build();
  }
}
