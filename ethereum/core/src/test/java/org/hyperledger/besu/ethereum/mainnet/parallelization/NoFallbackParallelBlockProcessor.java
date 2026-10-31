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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockExecutionContext;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.MiningBeneficiaryCalculator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

/**
 * Parallel execution on the calling thread without the sequential fallback: a parallel processing
 * failure is returned as-is rather than masked by a successful rerun.
 */
public class NoFallbackParallelBlockProcessor extends MainnetParallelBlockProcessor {

  public NoFallbackParallelBlockProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final TransactionReceiptFactory transactionReceiptFactory,
      final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
      final ProtocolSchedule protocolSchedule,
      final BalConfiguration balConfiguration) {
    super(
        transactionProcessor,
        transactionReceiptFactory,
        miningBeneficiaryCalculator,
        protocolSchedule,
        balConfiguration,
        new NoOpMetricsSystem(),
        Runnable::run);
  }

  @Override
  protected boolean shouldRerunSequentially(final BlockExecutionContext context) {
    return false;
  }
}
