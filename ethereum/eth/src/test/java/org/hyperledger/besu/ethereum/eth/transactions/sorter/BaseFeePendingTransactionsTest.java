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
package org.hyperledger.besu.ethereum.eth.transactions.sorter;

import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.testutil.TestClock;

import java.time.Clock;
import java.time.ZoneId;
import java.util.Optional;
import java.util.Random;

public class BaseFeePendingTransactionsTest extends AbstractPendingTransactionsTestBase {

  @Override
  AbstractPendingTransactionsSorter getPendingTransactions(
      final TransactionPoolConfiguration poolConfig, final Optional<Clock> clock) {
    return new BaseFeePendingTransactionsSorter(
        poolConfig,
        clock.orElse(TestClock.system(ZoneId.systemDefault())),
        metricsSystem,
        AbstractPendingTransactionsTestBase::mockBlockHeader,
        transactionEventNotifier.newEmitter());
  }

  private static final Random randomizeTxType = new Random();

  @Override
  protected Transaction createTransaction(final long transactionNumber) {
    var tx = new TransactionTestFixture().value(Wei.of(transactionNumber)).nonce(transactionNumber);
    if (randomizeTxType.nextBoolean()) {
      tx.type(TransactionType.EIP1559)
          .maxFeePerGas(Optional.of(Wei.of(5000L)))
          .maxPriorityFeePerGas(Optional.of(Wei.of(50L)));
    }
    return tx.createTransaction(KEYS1);
  }
}
