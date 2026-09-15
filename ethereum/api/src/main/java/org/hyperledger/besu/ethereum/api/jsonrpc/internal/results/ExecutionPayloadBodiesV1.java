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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({
  "transactions",
  "withdrawals",
})
public sealed class ExecutionPayloadBodiesV1 permits ExecutionPayloadBodiesV2 {
  private final List<Transaction> transactions;
  private final List<Withdrawal> withdrawals;

  public ExecutionPayloadBodiesV1(
      final List<Transaction> transactions, final List<Withdrawal> withdrawals) {
    this.transactions = transactions;
    this.withdrawals = withdrawals;
  }

  @JsonGetter(value = "transactions")
  public List<Transaction> getTransactions() {
    return transactions;
  }

  @JsonGetter(value = "withdrawals")
  public List<Withdrawal> getWithdrawals() {
    return withdrawals;
  }
}
