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
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"transactions", "withdrawals", "blockAccessList"})
public final class ExecutionPayloadBodiesV2 extends ExecutionPayloadBodiesV1 {
  private final BlockAccessList blockAccessList;

  public ExecutionPayloadBodiesV2(
      final ExecutionPayloadBodiesV1 executionPayloadBodiesV1,
      final BlockAccessList blockAccessList) {
    this(
        executionPayloadBodiesV1.getTransactions(),
        executionPayloadBodiesV1.getWithdrawals(),
        blockAccessList);
  }

  public ExecutionPayloadBodiesV2(
      final List<Transaction> transactions,
      final List<Withdrawal> withdrawals,
      final BlockAccessList blockAccessList) {
    super(transactions, withdrawals);
    this.blockAccessList = blockAccessList;
  }

  @JsonGetter(value = "blockAccessList")
  public BlockAccessList getBlockAccessList() {
    return blockAccessList;
  }
}
