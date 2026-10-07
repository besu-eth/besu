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
package org.hyperledger.besu.tests.acceptance.dsl.condition.eth;

import org.hyperledger.besu.tests.acceptance.dsl.condition.Condition;
import org.hyperledger.besu.tests.acceptance.dsl.transaction.eth.EthTransactions;

import java.math.BigInteger;
import java.util.List;

public class EthConditions {

  private final EthTransactions transactions;

  public EthConditions(final EthTransactions transactions) {
    this.transactions = transactions;
  }

  public Condition accountsExceptional(final String expectedMessage) {
    return new ExpectEthAccountsException(transactions.accounts(), expectedMessage);
  }

  public Condition expectSuccessfulTransactionReceipt(final String transactionHash) {
    return new ExpectSuccessfulEthGetTransactionReceipt(
        transactions.getTransactionReceipt(transactionHash));
  }

  public Condition expectNoTransactionReceipt(final String transactionHash) {
    return new ExpectEthGetTransactionReceiptIsAbsent(
        transactions.getTransactionReceipt(transactionHash));
  }

  public Condition expectEthSendRawTransactionException(
      final String transactionData, final String expectedMessage) {
    return new ExpectEthSendRawTransactionException(
        transactions.sendRawTransaction(transactionData), expectedMessage);
  }

  public Condition expectSuccessfulEthRawTransaction(final String transactionData) {
    return new ExpectSuccessfulEthSendRawTransaction(
        transactions.sendRawTransaction(transactionData));
  }

  public Condition expectSuccessfulTransactionReceiptWithReason(
      final String transactionHash, final String revertReason) {
    return new ExpectSuccessfulEthGetTransactionReceiptWithReason(
        transactions.getTransactionReceiptWithRevertReason(transactionHash), revertReason);
  }

  public Condition expectSuccessfulTransactionReceiptWithoutReason(final String transactionHash) {
    return new ExpectSuccessfulEthGetTransactionReceiptWithoutReason(
        transactions.getTransactionReceiptWithRevertReason(transactionHash));
  }

  public Condition syncingStatus(final boolean isSyncing) {
    return new SyncingStatusCondition(transactions.syncing(), isSyncing);
  }

  public Condition expectNewPendingTransactions(
      final BigInteger filterId, final List<String> transactionHashes) {
    return new NewPendingTransactionFilterChangesCondition(
        transactions.filterChanges(filterId), transactionHashes);
  }
}
