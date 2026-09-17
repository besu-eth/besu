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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_2;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_3;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_4;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_1_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.MINING_BENEFICIARY;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Integration tests for mining beneficiary BAL balance tracking. */
public abstract class AbstractMiningBeneficiaryBalTest
    extends AbstractParallelBlockProcessorIntegrationTest {

  @Test
  @DisplayName(
      "Parallel BAL must record correct cumulative mining beneficiary postBalance with priority fees")
  void miningBeneficiaryPostBalanceWithPriorityFees() {
    // Two independent transfers with non-zero priority fees.
    // baseFee=1 so effectivePriorityFee = min(maxPriorityFeePerGas, maxFeePerGas - baseFee) > 0.
    // The mining beneficiary accumulates the priority fee from each transaction.
    final Transaction tx1 =
        createTransferTransaction(
            0, 1_000_000_000_000_000_000L, 300_000L, 2L, 10L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR);
    final Transaction tx2 =
        createTransferTransaction(
            0, 2_000_000_000_000_000_000L, 300_000L, 3L, 10L, ACCOUNT_3, ACCOUNT_GENESIS_2_KEYPAIR);

    final ComparisonResult result = executeAndCompare(Wei.of(1), tx1, tx2);

    final Optional<BlockAccessList> seqBal = getBlockAccessList(result.seqResult());
    final Optional<BlockAccessList> parBal = getBlockAccessList(result.parResult());
    assertThat(seqBal).as("Sequential BAL should be present").isPresent();
    assertThat(parBal).as("Parallel BAL should be present").isPresent();

    final List<BalanceChange> seqBalChanges =
        getBalanceChangesFor(seqBal.get(), MINING_BENEFICIARY);
    final List<BalanceChange> parBalChanges =
        getBalanceChangesFor(parBal.get(), MINING_BENEFICIARY);

    assertThat(seqBalChanges)
        .as("Sequential BAL should have balance changes for mining beneficiary")
        .isNotEmpty();
    assertThat(parBalChanges)
        .as("Parallel BAL balance changes for mining beneficiary must match sequential")
        .isEqualTo(seqBalChanges);
  }

  @Test
  @DisplayName(
      "Multiple transactions with varying priority fees produce correct cumulative beneficiary balances")
  void multipleTxsWithVaryingPriorityFees() {
    // Three transactions: two from sender A (nonces 0,1) and one from sender B (nonce 0),
    // each with a different priority fee. baseFee=1 ensures positive effective priority fees.
    final Transaction tx1 =
        createTransferTransaction(
            0, 100_000_000_000_000L, 300_000L, 1L, 10L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR);
    final Transaction tx2 =
        createTransferTransaction(
            0, 200_000_000_000_000L, 300_000L, 4L, 10L, ACCOUNT_3, ACCOUNT_GENESIS_2_KEYPAIR);
    final Transaction tx3 =
        createTransferTransaction(
            1, 300_000_000_000_000L, 300_000L, 2L, 10L, ACCOUNT_4, ACCOUNT_GENESIS_1_KEYPAIR);

    final ComparisonResult result = executeAndCompare(Wei.of(1), tx1, tx2, tx3);

    final Optional<BlockAccessList> seqBal = getBlockAccessList(result.seqResult());
    final Optional<BlockAccessList> parBal = getBlockAccessList(result.parResult());
    assertThat(seqBal).isPresent();
    assertThat(parBal).isPresent();

    final List<BalanceChange> seqBalChanges =
        getBalanceChangesFor(seqBal.get(), MINING_BENEFICIARY);
    final List<BalanceChange> parBalChanges =
        getBalanceChangesFor(parBal.get(), MINING_BENEFICIARY);

    assertThat(seqBalChanges).isNotEmpty();
    assertThat(parBalChanges)
        .as("Parallel BAL mining beneficiary balance changes must match sequential")
        .isEqualTo(seqBalChanges);

    // Each balance change should be strictly increasing (cumulative rewards)
    for (int i = 1; i < seqBalChanges.size(); i++) {
      assertThat(seqBalChanges.get(i).postBalance())
          .as("Balance change at index %d must be >= previous", i)
          .isGreaterThanOrEqualTo(seqBalChanges.get(i - 1).postBalance());
    }
  }
}
