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
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_1_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.CONTRACT_ADDRESS;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;

import java.math.BigInteger;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
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

    /** Sets slot 0 when called without call data, else reverts while slot 0 is unset. */
    private static final Address SLOT_GATE =
        Address.fromHexStringStrict("0x0000000000000000000000000000000000001111");

    /** Calls the slot gate with call data and reverts if that call reverts. */
    private static final Address SLOT_GATE_CALLER =
        Address.fromHexStringStrict("0x0000000000000000000000000000000000002222");

    @Override
    protected String getVariantName() {
      return getVariant();
    }

    @Override
    protected BalConfiguration getBalConfiguration() {
      return OPTIMISTIC_CONFIG;
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

    @Test
    @DisplayName("A transaction that reverted on a slot an earlier one set is executed again")
    void revertedTransactionReadingChangedSlotIsExecutedAgain() {
      final ComparisonResult result =
          executeAndCompare(
              Wei.of(5),
              call(SLOT_GATE, Bytes.EMPTY, ACCOUNT_GENESIS_1_KEYPAIR),
              call(SLOT_GATE, Bytes.of(0), ACCOUNT_GENESIS_2_KEYPAIR));

      assertSecondTransactionExecutedAgain(result);
    }

    @Test
    @DisplayName(
        "A transaction whose nested call reverted on a slot an earlier one set is executed again")
    void transactionRevertedInNestedCallIsExecutedAgain() {
      final ComparisonResult result =
          executeAndCompare(
              Wei.of(5),
              call(SLOT_GATE, Bytes.EMPTY, ACCOUNT_GENESIS_1_KEYPAIR),
              call(SLOT_GATE_CALLER, Bytes.EMPTY, ACCOUNT_GENESIS_2_KEYPAIR));

      assertSecondTransactionExecutedAgain(result);
    }

    private void assertSecondTransactionExecutedAgain(final ComparisonResult result) {
      assertThat(result.parResult().getYield().orElseThrow().getReceipts().get(1).getStatus())
          .as("the second transaction passes the gate once the first one set the slot")
          .isEqualTo(1);
      assertThat(result.parResult().getNbParallelizedTransactions())
          .as("only the speculative result of the first transaction is kept")
          .contains(1);
    }

    private Transaction call(final Address to, final Bytes payload, final KeyPair keyPair) {
      return Transaction.builder()
          .type(TransactionType.EIP1559)
          .nonce(0)
          .maxPriorityFeePerGas(Wei.ZERO)
          .maxFeePerGas(Wei.of(5))
          .gasLimit(1_000_000L)
          .to(to)
          .value(Wei.ZERO)
          .payload(payload)
          .chainId(BigInteger.valueOf(42))
          .signAndBuild(keyPair);
    }
  }
}
