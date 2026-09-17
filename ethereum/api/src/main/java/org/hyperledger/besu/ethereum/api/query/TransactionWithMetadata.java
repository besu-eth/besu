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
package org.hyperledger.besu.ethereum.api.query;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.util.Optional;

public class TransactionWithMetadata {

  private final Transaction transaction;
  private final Optional<Long> blockNumber;
  private final Optional<Wei> baseFee;
  private final Optional<Hash> blockHash;
  private final Optional<Integer> transactionIndex;
  private final Optional<Long> blockTimestamp;

  public TransactionWithMetadata(final Transaction transaction) {
    this.transaction = transaction;
    this.blockNumber = Optional.empty();
    this.baseFee = Optional.empty();
    this.blockHash = Optional.empty();
    this.transactionIndex = Optional.empty();
    this.blockTimestamp = Optional.empty();
  }

  public TransactionWithMetadata(
      final Transaction transaction,
      final long blockNumber,
      final Optional<Wei> baseFee,
      final Hash blockHash,
      final int transactionIndex,
      final long blockTimestamp) {
    this.transaction = transaction;
    this.blockNumber = Optional.of(blockNumber);
    this.baseFee = baseFee;
    this.blockHash = Optional.of(blockHash);
    this.transactionIndex = Optional.of(transactionIndex);
    this.blockTimestamp = Optional.of(blockTimestamp);
  }

  public Transaction getTransaction() {
    return transaction;
  }

  public Optional<Long> getBlockNumber() {
    return blockNumber;
  }

  public Optional<Wei> getBaseFee() {
    return baseFee;
  }

  public Optional<Hash> getBlockHash() {
    return blockHash;
  }

  public Optional<Integer> getTransactionIndex() {
    return transactionIndex;
  }

  public Optional<Long> getBlockTimestamp() {
    return blockTimestamp;
  }
}
