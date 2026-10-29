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
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_5;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_6;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_1;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_1_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.CONTRACT_ADDRESS;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.PARALLEL_TEST_CONTRACT;

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
    private final Address sender = Address.fromHexStringStrict(ACCOUNT_GENESIS_1);

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
    @DisplayName("Transfers chained after an earlier one of their sender are reused")
    void chainedTransfersAreReused() {
      final ComparisonResult result =
          executeAndCompare(
              Wei.of(5), transfer(0, ACCOUNT_4), transfer(1, ACCOUNT_5), transfer(2, ACCOUNT_6));

      assertAccountsMatch(result.seqWorldState(), result.parWorldState(), sender);
      assertThat(result.parResult().getNbParallelizedTransactions()).contains(3);
    }

    @Test
    @DisplayName("A chained transfer is executed again when another sender paid its sender")
    void chainedTransferIsExecutedAgainWhenItsSenderWasPaid() {
      final Transaction payment =
          createTransferTransaction(
              0,
              1_000_000_000_000_000_000L,
              300_000L,
              0L,
              5L,
              ACCOUNT_GENESIS_1,
              ACCOUNT_GENESIS_2_KEYPAIR);

      final ComparisonResult result =
          executeAndCompare(Wei.of(5), transfer(0, ACCOUNT_4), payment, transfer(1, ACCOUNT_5));

      assertAccountsMatch(result.seqWorldState(), result.parWorldState(), sender);
      assertThat(result.parResult().getNbParallelizedTransactions()).contains(1);
    }

    @Test
    @DisplayName("Storage writes chained after an earlier one of their sender are reused")
    void chainedStorageWritesAreReused() {
      final Address contract = Address.fromHexStringStrict(CONTRACT_ADDRESS);
      final ComparisonResult result =
          executeAndCompare(
              Wei.of(5),
              createContractCallTransaction(
                  0, contract, "setSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(100)),
              createContractCallTransaction(
                  1, contract, "setSlot2", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(200)),
              createContractCallTransaction(
                  2, contract, "setSlot3", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(300)));

      for (int slot = 0; slot < 3; slot++) {
        assertContractStorageMatches(
            result.seqWorldState(), result.parWorldState(), contract, slot);
      }
      assertThat(result.parResult().getNbParallelizedTransactions()).contains(3);
    }

    @Test
    @DisplayName("A chained increment is executed again when another sender wrote the slot")
    void chainedIncrementIsExecutedAgainWhenTheSlotWasWritten() {
      final Address contract = Address.fromHexStringStrict(PARALLEL_TEST_CONTRACT);
      final ComparisonResult result =
          executeAndCompare(
              Wei.of(5),
              createContractCallTransaction(
                  0, contract, "setSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(100)),
              createContractCallTransaction(
                  0, contract, "setSlot1", ACCOUNT_GENESIS_2_KEYPAIR, Optional.of(999)),
              createContractCallTransaction(
                  1, contract, "incrementSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.empty()));

      assertContractStorage(result.seqWorldState(), contract, 0, 1000);
      assertContractStorageMatches(result.seqWorldState(), result.parWorldState(), contract, 0);
      assertThat(result.parResult().getNbParallelizedTransactions()).contains(1);
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
