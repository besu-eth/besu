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
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_5;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_6;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_1;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_1_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.MINING_BENEFICIARY;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.MINING_BENEFICIARY_KEYPAIR;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Integration tests for simple ETH transfer scenarios. */
public abstract class AbstractSimpleTransferTest
    extends AbstractParallelBlockProcessorIntegrationTest {

  @Test
  @DisplayName("Independent transfers from different senders produce matching state")
  void independentTransfers() {
    final Transaction tx1 =
        createTransferTransaction(
            0, 1_000_000_000_000_000_000L, 300_000L, 0L, 5L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR);
    final Transaction tx2 =
        createTransferTransaction(
            0, 2_000_000_000_000_000_000L, 300_000L, 0L, 5L, ACCOUNT_3, ACCOUNT_GENESIS_2_KEYPAIR);

    final ComparisonResult result = executeAndCompare(Wei.of(5), tx1, tx2);

    final Address addr2 = Address.fromHexStringStrict(ACCOUNT_2);
    final Address addr3 = Address.fromHexStringStrict(ACCOUNT_3);
    final Address sender1 = Address.fromHexStringStrict(ACCOUNT_GENESIS_1);
    final Address sender2 = Address.fromHexStringStrict(ACCOUNT_GENESIS_2);

    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), addr2);
    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), addr3);
    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), sender1);
    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), sender2);

    assertThat(((BonsaiAccount) result.seqWorldState().get(addr2)).getBalance())
        .isEqualTo(Wei.of(1_000_000_000_000_000_000L));
    assertThat(((BonsaiAccount) result.seqWorldState().get(addr3)).getBalance())
        .isEqualTo(Wei.of(2_000_000_000_000_000_000L));
  }

  @Test
  @DisplayName(
      "Independent transfers from different senders match when parent is chain head minus one")
  void independentTransfersWithParentAtHeadMinusOne() {
    final Transaction tx1 =
        createTransferTransaction(
            0, 1_000_000_000_000_000_000L, 300_000L, 0L, 5L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR);
    final Transaction tx2 =
        createTransferTransaction(
            0, 2_000_000_000_000_000_000L, 300_000L, 0L, 5L, ACCOUNT_3, ACCOUNT_GENESIS_2_KEYPAIR);

    final ComparisonResult result = executeAndCompareParentOfChainHead(Wei.of(5), tx1, tx2);

    final Address addr2 = Address.fromHexStringStrict(ACCOUNT_2);
    final Address addr3 = Address.fromHexStringStrict(ACCOUNT_3);
    final Address sender1 = Address.fromHexStringStrict(ACCOUNT_GENESIS_1);
    final Address sender2 = Address.fromHexStringStrict(ACCOUNT_GENESIS_2);

    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), addr2);
    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), addr3);
    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), sender1);
    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), sender2);

    assertThat(((BonsaiAccount) result.seqWorldState().get(addr2)).getBalance())
        .isEqualTo(Wei.of(1_000_000_000_000_000_000L));
    assertThat(((BonsaiAccount) result.seqWorldState().get(addr3)).getBalance())
        .isEqualTo(Wei.of(2_000_000_000_000_000_000L));
  }

  @Test
  @DisplayName("Multiple transactions from the same sender produce matching state")
  void sameSenderConflict() {
    final Transaction tx1 =
        createTransferTransaction(
            0, 1_000_000_000_000_000_000L, 300_000L, 0L, 5L, ACCOUNT_4, ACCOUNT_GENESIS_1_KEYPAIR);
    final Transaction tx2 =
        createTransferTransaction(
            1, 2_000_000_000_000_000_000L, 300_000L, 0L, 5L, ACCOUNT_5, ACCOUNT_GENESIS_1_KEYPAIR);
    final Transaction tx3 =
        createTransferTransaction(
            2, 3_000_000_000_000_000_000L, 300_000L, 0L, 5L, ACCOUNT_6, ACCOUNT_GENESIS_1_KEYPAIR);

    final ComparisonResult result = executeAndCompare(Wei.of(5), tx1, tx2, tx3);

    final Address addr4 = Address.fromHexStringStrict(ACCOUNT_4);
    final Address addr5 = Address.fromHexStringStrict(ACCOUNT_5);
    final Address addr6 = Address.fromHexStringStrict(ACCOUNT_6);
    final Address sender = Address.fromHexStringStrict(ACCOUNT_GENESIS_1);

    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), addr4);
    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), addr5);
    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), addr6);
    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), sender);

    assertThat(((BonsaiAccount) result.seqWorldState().get(addr4)).getBalance())
        .isEqualTo(Wei.of(1_000_000_000_000_000_000L));
    assertThat(((BonsaiAccount) result.seqWorldState().get(addr5)).getBalance())
        .isEqualTo(Wei.of(2_000_000_000_000_000_000L));
    assertThat(((BonsaiAccount) result.seqWorldState().get(addr6)).getBalance())
        .isEqualTo(Wei.of(3_000_000_000_000_000_000L));

    assertThat(((BonsaiAccount) result.seqWorldState().get(sender)).getNonce()).isEqualTo(3L);
  }

  @Test
  @DisplayName("Receiver of tx1 is sender of tx2 produces matching state")
  void receiverSenderOverlap() {
    final Transaction tx1 =
        createTransferTransaction(
            0,
            1_000_000_000_000_000_000L,
            300_000L,
            0L,
            5L,
            ACCOUNT_GENESIS_2,
            ACCOUNT_GENESIS_1_KEYPAIR);
    final Transaction tx2 =
        createTransferTransaction(
            0, 2_000_000_000_000_000_000L, 300_000L, 0L, 5L, ACCOUNT_2, ACCOUNT_GENESIS_2_KEYPAIR);

    final ComparisonResult result = executeAndCompare(Wei.of(5), tx1, tx2);

    final Address addr2 = Address.fromHexStringStrict(ACCOUNT_2);
    final Address sender1 = Address.fromHexStringStrict(ACCOUNT_GENESIS_1);
    final Address sender2 = Address.fromHexStringStrict(ACCOUNT_GENESIS_2);

    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), addr2);
    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), sender1);
    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), sender2);

    assertThat(((BonsaiAccount) result.seqWorldState().get(addr2)).getBalance())
        .isEqualTo(Wei.of(2_000_000_000_000_000_000L));
    assertThat(((BonsaiAccount) result.seqWorldState().get(sender1)).getNonce()).isEqualTo(1L);
    assertThat(((BonsaiAccount) result.seqWorldState().get(sender2)).getNonce()).isEqualTo(1L);
  }

  @Test
  @DisplayName("Single transaction produces matching state")
  void singleTransaction() {
    final Transaction tx1 =
        createTransferTransaction(
            0, 5_000_000_000_000_000_000L, 300_000L, 0L, 5L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR);

    final ComparisonResult result = executeAndCompare(Wei.of(5), tx1);

    final Address addr2 = Address.fromHexStringStrict(ACCOUNT_2);
    final Address sender = Address.fromHexStringStrict(ACCOUNT_GENESIS_1);

    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), addr2);
    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), sender);

    assertThat(((BonsaiAccount) result.seqWorldState().get(addr2)).getBalance())
        .isEqualTo(Wei.of(5_000_000_000_000_000_000L));
  }

  @Test
  @DisplayName(
      "Sender of tx2 is the mining beneficiary triggers collision and produces matching state")
  void senderIsMiningBeneficiary() {
    final Transaction tx1 =
        createTransferTransaction(
            0,
            1_000_000_000_000_000_000L,
            300_000L,
            0L,
            5L,
            MINING_BENEFICIARY.toHexString(),
            ACCOUNT_GENESIS_1_KEYPAIR);
    final Transaction tx2 =
        createTransferTransaction(
            0, 2_000_000_000_000_000_000L, 300_000L, 0L, 5L, ACCOUNT_3, MINING_BENEFICIARY_KEYPAIR);

    final ComparisonResult result = executeAndCompare(Wei.of(5), tx1, tx2);

    final Address addr3 = Address.fromHexStringStrict(ACCOUNT_3);
    final Address sender1 = Address.fromHexStringStrict(ACCOUNT_GENESIS_1);

    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), MINING_BENEFICIARY);
    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), addr3);
    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), sender1);
  }

  @Test
  @DisplayName("Both senders send to the same recipient produce matching state")
  void sameRecipientFromDifferentSenders() {
    final Transaction tx1 =
        createTransferTransaction(
            0, 1_000_000_000_000_000_000L, 300_000L, 0L, 5L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR);
    final Transaction tx2 =
        createTransferTransaction(
            0, 3_000_000_000_000_000_000L, 300_000L, 0L, 5L, ACCOUNT_2, ACCOUNT_GENESIS_2_KEYPAIR);

    final ComparisonResult result = executeAndCompare(Wei.of(5), tx1, tx2);

    final Address addr2 = Address.fromHexStringStrict(ACCOUNT_2);
    assertAccountsMatch(result.seqWorldState(), result.parWorldState(), addr2);
    assertThat(((BonsaiAccount) result.seqWorldState().get(addr2)).getBalance())
        .isEqualTo(Wei.of(4_000_000_000_000_000_000L));
  }
}
