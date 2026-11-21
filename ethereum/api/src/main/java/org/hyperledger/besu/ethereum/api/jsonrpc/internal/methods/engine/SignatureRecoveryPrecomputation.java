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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recovers the senders and EIP-7702 authorities of a payload's transactions in the background, so
 * that executing the payload finds them already recovered.
 *
 * <p>For a block executed in parallel the recovery starts from the last transaction: the parallel
 * workers recover the signatures of the transactions they reach first themselves, so the two split
 * the block between them. For a block executed sequentially it starts from the first transaction,
 * so it stays ahead of execution while executing a transaction takes longer than recovering it.
 *
 * <p>The payload has not been validated when this starts, so the work must not outlive its
 * execution. A fixed number of tasks share the transactions, recover one signature at a time, and
 * stop as soon as {@link #cancel()} is called. Queueing one task per signature instead would leave
 * work behind an invalid payload that nothing can stop, on the computation pool shared with other
 * consumers.
 */
final class SignatureRecoveryPrecomputation {
  private static final Logger LOG = LoggerFactory.getLogger(SignatureRecoveryPrecomputation.class);

  /** At most this many tasks run the recovery: the default size of the computation pool. */
  static final int MAX_WORKERS = 2;

  private final List<Transaction> transactions;
  private final boolean fromLastTransaction;
  private final AtomicInteger claimedTransactions = new AtomicInteger();
  private volatile boolean cancelled;

  private SignatureRecoveryPrecomputation(
      final List<Transaction> transactions, final boolean fromLastTransaction) {
    this.transactions = transactions;
    this.fromLastTransaction = fromLastTransaction;
  }

  /**
   * Starts recovering the signatures of the given transactions on the scheduler's computation pool.
   *
   * @param scheduler the scheduler whose computation pool runs the recovery
   * @param transactions the transactions to recover the senders and authorities of
   * @param parallelExecution whether the block is executed in parallel, which makes the recovery
   *     start from the last transaction instead of the first
   * @return a handle to stop the recovery
   */
  static SignatureRecoveryPrecomputation start(
      final EthScheduler scheduler,
      final List<Transaction> transactions,
      final boolean parallelExecution) {
    final SignatureRecoveryPrecomputation precomputation =
        new SignatureRecoveryPrecomputation(transactions, parallelExecution);
    final int workers = Math.min(MAX_WORKERS, transactions.size());
    for (int i = 0; i < workers; i++) {
      scheduler.scheduleComputationTask(precomputation::recoverUntilDoneOrCancelled);
    }
    return precomputation;
  }

  /**
   * Stops the recovery. A signature being recovered when this is called is still completed, but no
   * other one is started, and tasks that have not started yet do nothing.
   */
  void cancel() {
    cancelled = true;
  }

  private Void recoverUntilDoneOrCancelled() {
    final int count = transactions.size();
    int claimed;
    while (!cancelled && (claimed = claimedTransactions.getAndIncrement()) < count) {
      recover(transactions.get(fromLastTransaction ? count - 1 - claimed : claimed));
    }
    return null;
  }

  private void recover(final Transaction transaction) {
    try {
      final var sender = transaction.getSender();
      LOG.atTrace()
          .setMessage("The sender for transaction {} is calculated : {}")
          .addArgument(transaction::getHash)
          .addArgument(sender)
          .log();
    } catch (final RuntimeException e) {
      // the transaction is invalid, which block execution reports; carry on with the others
      LOG.atTrace()
          .setMessage("Could not recover the sender of transaction {}")
          .addArgument(transaction::getHash)
          .setCause(e)
          .log();
    }
    if (transaction.getType().supportsDelegateCode()) {
      transaction.getCodeDelegationList().ifPresent(list -> recoverAuthorities(transaction, list));
    }
  }

  private void recoverAuthorities(
      final Transaction transaction, final List<CodeDelegation> codeDelegations) {
    int index = 0;
    for (final CodeDelegation codeDelegation : codeDelegations) {
      // one transaction can carry many authorizations, so check between each of them
      if (cancelled) {
        return;
      }
      final int authorizationIndex = index++;
      try {
        final var authority = codeDelegation.authorizer();
        LOG.atTrace()
            .setMessage(
                "The code delegation authority at index {} for transaction {} is calculated : {}")
            .addArgument(authorizationIndex)
            .addArgument(transaction::getHash)
            .addArgument(authority)
            .log();
      } catch (final RuntimeException e) {
        LOG.atTrace()
            .setMessage(
                "Could not recover the code delegation authority at index {} for transaction {}")
            .addArgument(authorizationIndex)
            .addArgument(transaction::getHash)
            .setCause(e)
            .log();
      }
    }
  }
}
