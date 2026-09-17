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
package org.hyperledger.besu.ethereum.blockcreation.txselection.selectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.blockcreation.txselection.selectors.BlockRlpSizeTransactionSelector.INITIAL_RLP_SIZE;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MAX_SCORE;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.SELECTED;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.blockcreation.txselection.BlockSelectionContext;
import org.hyperledger.besu.ethereum.blockcreation.txselection.TransactionEvaluationContext;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;
import org.hyperledger.besu.plugin.services.txselection.SelectorsStateManager;

import java.util.Optional;

import com.google.common.base.Supplier;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BlockRlpSizeTransactionSelectorTest {
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  private static final KeyPair KEYS = SIGNATURE_ALGORITHM.generateKeyPair();

  @SuppressWarnings("UnnecessaryLambda")
  private static final Supplier<Boolean> NEVER_CANCELLED = () -> false;

  @Mock(answer = RETURNS_DEEP_STUBS)
  BlockSelectionContext blockSelectionContext;

  @Mock TransactionProcessingResult transactionProcessingResult;

  SelectorsStateManager selectorsStateManager;
  BlockRlpSizeTransactionSelector selector;

  @BeforeEach
  void setup() {
    selectorsStateManager = new SelectorsStateManager();
    selector = new BlockRlpSizeTransactionSelector(blockSelectionContext, selectorsStateManager);
  }

  @Test
  void transactionSelectedWhenUnderBlockSize() {
    final var tx1 = createEIP1559PendingTransaction(Bytes.random(10));
    final var tx2 = createEIP1559PendingTransaction(Bytes.random(80));
    final int maxRlpBlockSize =
        (int)
                (INITIAL_RLP_SIZE
                    + tx1.getTransaction().getSizeForBlockInclusion()
                    + tx2.getTransaction().getSizeForBlockInclusion())
            + 20; // ensure there's some room left
    when(blockSelectionContext.maxRlpBlockSize()).thenReturn(maxRlpBlockSize);

    // transaction is under the total block size limit, so it should be selected
    var txEvaluationContext1 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx1, null, null, null, NEVER_CANCELLED);
    selectorsStateManager.blockSelectionStarted();
    evaluateAndAssertSelected(txEvaluationContext1);

    // add another transaction still under the total block size limit, so it should also be selected
    var txEvaluationContext =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx2, null, null, null, NEVER_CANCELLED);
    selectorsStateManager.blockSelectionStarted();
    evaluateAndAssertSelected(txEvaluationContext);
  }

  @Test
  void smallTransactionSelectedAfterLargeTransaction() {
    final var tx1 = createEIP1559PendingTransaction(Bytes.random(10));
    final var tx2 = createEIP1559PendingTransaction(Bytes.random(100));
    final var tx3 = createEIP1559PendingTransaction(Bytes.random(20));
    final int maxRlpBlockSize =
        (int)
            (INITIAL_RLP_SIZE
                + tx1.getTransaction().getSizeForBlockInclusion()
                + tx3.getTransaction().getSizeForBlockInclusion());
    when(blockSelectionContext.maxRlpBlockSize()).thenReturn(maxRlpBlockSize);

    // transaction is under the total block size limit, so it should be selected
    var txEvaluationContext1 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx1, null, null, null, NEVER_CANCELLED);
    selectorsStateManager.blockSelectionStarted();
    evaluateAndAssertSelected(txEvaluationContext1);

    // add another transaction which is too large for the block, so it should not be selected
    var txEvaluationContext2 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx2, null, null, null, NEVER_CANCELLED);
    selectorsStateManager.blockSelectionStarted();
    evaluateAndAssertNotSelected(
        txEvaluationContext2, TransactionSelectionResult.TOO_LARGE_FOR_REMAINING_BLOCK_SIZE);

    // transaction is under the total block size limit, so it should be selected
    var txEvaluationContext3 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx3, null, null, null, NEVER_CANCELLED);
    selectorsStateManager.blockSelectionStarted();
    evaluateAndAssertSelected(txEvaluationContext3);
  }

  @Test
  void transactionRejectedWhenEqualToBlockSize() {
    final var tx1 = createEIP1559PendingTransaction(Bytes.random(50));
    final var tx2 = createEIP1559PendingTransaction(Bytes.random(50));
    final int maxRlpBlockSize =
        (int)
            (INITIAL_RLP_SIZE
                + tx1.getTransaction().getSizeForBlockInclusion()
                + tx2.getTransaction().getSizeForBlockInclusion());
    when(blockSelectionContext.maxRlpBlockSize()).thenReturn(maxRlpBlockSize);

    final var txEvaluationContext1 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx1, null, null, null, NEVER_CANCELLED);
    selectorsStateManager.blockSelectionStarted();
    evaluateAndAssertSelected(txEvaluationContext1);

    final var txEvaluationContext2 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx2, null, null, null, NEVER_CANCELLED);
    selectorsStateManager.blockSelectionStarted();
    evaluateAndAssertSelected(txEvaluationContext2);
    evaluateAndAssertNotSelected(
        txEvaluationContext2, TransactionSelectionResult.TOO_LARGE_FOR_REMAINING_BLOCK_SIZE);
  }

  @Test
  void transactionRejectedWhenOverBlockSize() {
    final var tx1 = createEIP1559PendingTransaction(Bytes.random(50));
    final var tx2 = createEIP1559PendingTransaction(Bytes.random(100));
    final int maxRlpBlockSize =
        (int)
            (INITIAL_RLP_SIZE
                + tx1.getTransaction().getSizeForBlockInclusion()
                + tx2.getTransaction().getSizeForBlockInclusion());
    when(blockSelectionContext.maxRlpBlockSize()).thenReturn(maxRlpBlockSize);

    final var txEvaluationContext1 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx1, null, null, null, NEVER_CANCELLED);
    selectorsStateManager.blockSelectionStarted();
    evaluateAndAssertSelected(txEvaluationContext1);

    final var txEvaluationContext2 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx2, null, null, null, NEVER_CANCELLED);
    selectorsStateManager.blockSelectionStarted();
    evaluateAndAssertSelected(txEvaluationContext2);
    evaluateAndAssertNotSelected(
        txEvaluationContext2, TransactionSelectionResult.TOO_LARGE_FOR_REMAINING_BLOCK_SIZE);
  }

  private void evaluateAndAssertSelected(final TransactionEvaluationContext txEvaluationContext) {
    assertThat(selector.evaluateTransactionPreProcessing(txEvaluationContext)).isEqualTo(SELECTED);
    assertThat(
            selector.evaluateTransactionPostProcessing(
                txEvaluationContext, transactionProcessingResult))
        .isEqualTo(SELECTED);
  }

  private void evaluateAndAssertNotSelected(
      final TransactionEvaluationContext txEvaluationContext,
      final TransactionSelectionResult preProcessedResult) {
    assertThat(selector.evaluateTransactionPreProcessing(txEvaluationContext))
        .isEqualTo(preProcessedResult);
  }

  private PendingTransaction createEIP1559PendingTransaction(final Bytes payload) {
    return PendingTransaction.newPendingTransaction(
        createTransaction(TransactionType.EIP1559, payload), false, false, MAX_SCORE);
  }

  private Transaction createTransaction(final TransactionType type, final Bytes payload) {
    var tx =
        new TransactionTestFixture()
            .to(Optional.of(Address.fromHexString("0x634316eA0EE79c701c6F67C53A4C54cBAfd2316d")))
            .nonce(0)
            .type(type)
            .payload(payload);
    return tx.createTransaction(KEYS);
  }
}
