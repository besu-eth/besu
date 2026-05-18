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
package org.hyperledger.besu.ethereum.blockcreation.txselection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.BLOCK_SELECTION_TIMEOUT;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.INVALID_TX_EVALUATION_TOO_LONG;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;

import org.junit.jupiter.api.Test;

public class TransactionSelectionResultsTest {

  private Transaction mockTx() {
    final Transaction tx = mock(Transaction.class);
    when(tx.getType()).thenReturn(TransactionType.FRONTIER);
    return tx;
  }

  @Test
  public void selectedTxsEvaluationTimeStartsAtZero() {
    final TransactionSelectionResults results = new TransactionSelectionResults();
    assertThat(results.getSelectedTxsEvaluationTimeNanos()).isZero();
  }

  @Test
  public void updateSelectedAccumulatesEvaluationTime() {
    final TransactionSelectionResults results = new TransactionSelectionResults();
    final TransactionReceipt receipt = mock(TransactionReceipt.class);

    results.updateSelected(mockTx(), receipt, 21_000L, 21_000L, 0L, 1_000_000L);
    results.updateSelected(mockTx(), receipt, 21_000L, 21_000L, 0L, 2_000_000L);
    results.updateSelected(mockTx(), receipt, 21_000L, 21_000L, 0L, 500_000L);

    assertThat(results.getSelectedTxsEvaluationTimeNanos()).isEqualTo(3_500_000L);
  }

  @Test
  public void updateNotSelectedDoesNotAffectEvaluationTime() {
    final TransactionSelectionResults results = new TransactionSelectionResults();
    final TransactionReceipt receipt = mock(TransactionReceipt.class);

    results.updateSelected(mockTx(), receipt, 21_000L, 21_000L, 0L, 1_000_000L);
    results.updateNotSelected(mockTx(), BLOCK_SELECTION_TIMEOUT);
    results.updateNotSelected(mockTx(), INVALID_TX_EVALUATION_TOO_LONG);

    assertThat(results.getSelectedTxsEvaluationTimeNanos()).isEqualTo(1_000_000L);
  }
}
