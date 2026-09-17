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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;

public class TransactionWithMetadataResult extends TransactionBaseResult {

  private final String blockHash;
  private final String blockNumber;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final String blockTimestamp;

  private final String transactionIndex;

  public TransactionWithMetadataResult(final TransactionWithMetadata tx) {
    super(tx.getTransaction(), tx.getBaseFee());
    this.blockHash = tx.getBlockHash().map(Hash::toString).orElse(null);
    this.blockNumber = tx.getBlockNumber().map(Quantity::create).orElse(null);
    this.blockTimestamp = tx.getBlockTimestamp().map(Quantity::create).orElse(null);
    this.transactionIndex = tx.getTransactionIndex().map(Quantity::create).orElse(null);
  }

  @Override
  @JsonGetter(value = "blockHash")
  public String getBlockHash() {
    return blockHash;
  }

  @Override
  @JsonGetter(value = "blockNumber")
  public String getBlockNumber() {
    return blockNumber;
  }

  @Override
  @JsonGetter(value = "blockTimestamp")
  public String getBlockTimestamp() {
    return blockTimestamp;
  }

  @Override
  @JsonGetter(value = "transactionIndex")
  public String getTransactionIndex() {
    return transactionIndex;
  }
}
