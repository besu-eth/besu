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
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_4;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_1_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.CONTRACT_ADDRESS;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.MINING_BENEFICIARY;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;

import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for optimistic (collision-detection based) parallel block processing. Uses
 * ParallelizedConcurrentTransactionProcessor under the hood.
 *
 * <p>Tests are organized into nested classes by category, each extending the appropriate abstract
 * test class to inherit the test methods while providing the optimistic-specific configuration.
 */
class OptimisticParallelBlockProcessorIntegrationTest {

  private static final BalConfiguration OPTIMISTIC_CONFIG =
      ImmutableBalConfiguration.builder().isPerfectParallelizationEnabled(false).build();

  private static String getVariant() {
    return "Optimistic (Collision Detection)";
  }

  @Nested
  @DisplayName("Simple Transfers")
  class SimpleTransfers extends AbstractSimpleTransferTest {
    @Override
    protected String getVariantName() {
      return getVariant();
    }

    @Override
    protected BalConfiguration getBalConfiguration() {
      return OPTIMISTIC_CONFIG;
    }
  }

  @Nested
  @DisplayName("Contract Storage")
  class ContractStorage extends AbstractContractStorageTest {
    @Override
    protected String getVariantName() {
      return getVariant();
    }

    @Override
    protected BalConfiguration getBalConfiguration() {
      return OPTIMISTIC_CONFIG;
    }
  }

  @Nested
  @DisplayName("Storage Dependency (Collision Detection)")
  class StorageDependency extends AbstractStorageDependencyTest {
    @Override
    protected String getVariantName() {
      return getVariant();
    }

    @Override
    protected BalConfiguration getBalConfiguration() {
      return OPTIMISTIC_CONFIG;
    }
  }

  @Nested
  @DisplayName("Mining Beneficiary BAL")
  class MiningBeneficiaryBal extends AbstractMiningBeneficiaryBalTest {
    @Override
    protected String getVariantName() {
      return getVariant();
    }

    @Override
    protected BalConfiguration getBalConfiguration() {
      return OPTIMISTIC_CONFIG;
    }
  }

  @Nested
  @DisplayName("Fee Recipient Delegation")
  class FeeRecipientDelegation extends AbstractFeeRecipientDelegationTest {
    @Override
    protected String getVariantName() {
      return getVariant();
    }

    @Override
    protected BalConfiguration getBalConfiguration() {
      return OPTIMISTIC_CONFIG;
    }
  }

  @Nested
  @DisplayName("Reuse of Speculative Results")
  class SpeculativeResultReuse extends AbstractParallelBlockProcessorIntegrationTest {

    @Override
    protected String getVariantName() {
      return getVariant();
    }

    @Override
    protected BalConfiguration getBalConfiguration() {
      return OPTIMISTIC_CONFIG;
    }

    private Transaction transfer(final long nonce, final String to) {
      return createTransferTransaction(
          nonce, 1_000_000_000_000_000_000L, 300_000L, 0L, 5L, to, ACCOUNT_GENESIS_1_KEYPAIR);
    }

    @Test
    @DisplayName("Credits to an account whose balance changed earlier in the block are reused")
    void creditsToTheSameAccountAreReused() {
      final String recipient = "0xa4664C40AACeBD82A2Db79f0ea36C06Bc6A19Adb";
      final ComparisonResult result =
          executeAndCompare(
              Wei.of(5),
              transfer(0, recipient),
              createTransferTransaction(
                  0,
                  2_000_000_000_000_000_000L,
                  300_000L,
                  0L,
                  5L,
                  recipient,
                  ACCOUNT_GENESIS_2_KEYPAIR));

      assertAccountsMatch(
          result.seqWorldState(), result.parWorldState(), Address.fromHexString(recipient));
      assertThat(result.parResult().getNbParallelizedTransactions()).contains(2);
    }

    @Test
    @DisplayName("A credit to an account whose balance the transaction reads is executed again")
    void creditToAnAccountWhoseBalanceIsReadIsExecutedAgain() {
      // the contract self-destructs to itself, which reads its balance
      final String recipient = "0x0000000000000000000000000000000000007701";
      final ComparisonResult result =
          executeAndCompare(
              Wei.of(5),
              transfer(0, recipient),
              createTransferTransaction(
                  0,
                  2_000_000_000_000_000_000L,
                  300_000L,
                  0L,
                  5L,
                  recipient,
                  ACCOUNT_GENESIS_2_KEYPAIR));

      assertAccountsMatch(
          result.seqWorldState(), result.parWorldState(), Address.fromHexString(recipient));
      assertThat(result.parResult().getNbParallelizedTransactions()).contains(1);
    }

    @Test
    @DisplayName("Spending from an account whose balance changed earlier in the block is reused")
    void spendingFromAnAccountWhoseBalanceChangedIsReused() {
      // the contract pays 1 wei from its own balance on every call, without reading the balance
      final String payer = "0x0000000000000000000000000000000000007720";
      final ComparisonResult result =
          executeAndCompare(
              Wei.of(5),
              transfer(0, payer),
              createTransferTransaction(0, 0L, 300_000L, 0L, 5L, payer, ACCOUNT_GENESIS_2_KEYPAIR));

      assertAccountsMatch(
          result.seqWorldState(), result.parWorldState(), Address.fromHexString(payer));
      assertAccountsMatch(
          result.seqWorldState(),
          result.parWorldState(),
          Address.fromHexString("0xa4664C40AACeBD82A2Db79f0ea36C06Bc6A19Adb"));
      assertThat(result.parResult().getNbParallelizedTransactions()).contains(2);
    }

    @Test
    @DisplayName("A payment to the mining beneficiary after paid priority fees is reused")
    void paymentToTheMiningBeneficiaryIsReused() {
      final ComparisonResult result =
          executeAndCompare(
              Wei.of(1),
              createTransferTransaction(
                  0,
                  1_000_000_000_000_000_000L,
                  300_000L,
                  2L,
                  10L,
                  ACCOUNT_4,
                  ACCOUNT_GENESIS_1_KEYPAIR),
              createTransferTransaction(
                  0,
                  2_000_000_000_000_000_000L,
                  300_000L,
                  3L,
                  10L,
                  MINING_BENEFICIARY.toHexString(),
                  ACCOUNT_GENESIS_2_KEYPAIR));

      assertAccountsMatch(result.seqWorldState(), result.parWorldState(), MINING_BENEFICIARY);
      assertThat(result.parResult().getNbParallelizedTransactions()).contains(2);
    }

    @Test
    @DisplayName("A reverted transaction keeps its speculative result")
    void revertedTransactionIsReused() {
      final Address contract = Address.fromHexStringStrict(CONTRACT_ADDRESS);
      final ComparisonResult result =
          executeAndCompare(
              Wei.of(5),
              createContractCallTransaction(
                  0, contract, "setSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(100)),
              createContractCallTransaction(
                  0, contract, "noSuchMethod", ACCOUNT_GENESIS_2_KEYPAIR, Optional.empty()));

      assertThat(result.seqResult().getYield().orElseThrow().getReceipts().get(1).getStatus())
          .as("the call of a missing method reverts")
          .isZero();
      assertAccountsMatch(
          result.seqWorldState(),
          result.parWorldState(),
          Address.fromHexStringStrict(ACCOUNT_GENESIS_2));
      assertThat(result.parResult().getNbParallelizedTransactions()).contains(2);
    }
  }
}
