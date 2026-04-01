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

import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

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

  private static ParallelTransactionPreprocessing createPreprocessing(
      final MainnetTransactionProcessor transactionProcessor) {
    return new ParallelTransactionPreprocessing(
        transactionProcessor, Runnable::run, OPTIMISTIC_CONFIG);
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

    @Override
    protected ParallelTransactionPreprocessing createParallelPreprocessing(
        final MainnetTransactionProcessor transactionProcessor) {
      return createPreprocessing(transactionProcessor);
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

    @Override
    protected ParallelTransactionPreprocessing createParallelPreprocessing(
        final MainnetTransactionProcessor transactionProcessor) {
      return createPreprocessing(transactionProcessor);
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

    @Override
    protected ParallelTransactionPreprocessing createParallelPreprocessing(
        final MainnetTransactionProcessor transactionProcessor) {
      return createPreprocessing(transactionProcessor);
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

    @Override
    protected ParallelTransactionPreprocessing createParallelPreprocessing(
        final MainnetTransactionProcessor transactionProcessor) {
      return createPreprocessing(transactionProcessor);
    }
  }
}
