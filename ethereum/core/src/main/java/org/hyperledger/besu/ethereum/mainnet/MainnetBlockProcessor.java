/*
 * Copyright ConsenSys AG.
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

import org.hyperledger.besu.plugin.services.MetricsSystem;

public class MainnetBlockProcessor extends AbstractBlockProcessor {

  public MainnetBlockProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final AbstractBlockProcessor.TransactionReceiptFactory transactionReceiptFactory,
      final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
      final ProtocolSchedule protocolSchedule,
      final BalConfiguration balConfiguration) {
    super(
        transactionProcessor,
        transactionReceiptFactory,
        miningBeneficiaryCalculator,
        protocolSchedule,
        balConfiguration);
  }

  public MainnetBlockProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final AbstractBlockProcessor.TransactionReceiptFactory transactionReceiptFactory,
      final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
      final ProtocolSchedule protocolSchedule,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    super(
        transactionProcessor,
        transactionReceiptFactory,
        miningBeneficiaryCalculator,
        protocolSchedule,
        balConfiguration,
        metricsSystem);
  }

  public static final class MainnetBlockProcessorBuilder
      implements ProtocolSpecBuilder.BlockProcessorBuilder {

    private final MetricsSystem metricsSystem;

    public MainnetBlockProcessorBuilder(final MetricsSystem metricsSystem) {
      this.metricsSystem = metricsSystem;
    }

    @Override
    public BlockProcessor apply(
        final MainnetTransactionProcessor transactionProcessor,
        final TransactionReceiptFactory transactionReceiptFactory,
        final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
        final ProtocolSchedule protocolSchedule,
        final BalConfiguration balConfiguration) {

      return new MainnetBlockProcessor(
          transactionProcessor,
          transactionReceiptFactory,
          miningBeneficiaryCalculator,
          protocolSchedule,
          balConfiguration,
          metricsSystem);
    }
  }
}
