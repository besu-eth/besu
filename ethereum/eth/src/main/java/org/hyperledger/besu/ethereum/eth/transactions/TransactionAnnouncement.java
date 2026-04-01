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
package org.hyperledger.besu.ethereum.eth.transactions;

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.util.ArrayList;
import java.util.List;

public record TransactionAnnouncement(Hash hash, TransactionType type, Long size) {
  public TransactionAnnouncement(final Transaction transaction) {
    this(
        checkNotNull(transaction, "Transaction cannot be null").getHash(),
        transaction.getType(),
        (long) transaction.getSizeForAnnouncement());
  }

  public TransactionAnnouncement(final Hash hash, final TransactionType type, final Long size) {
    this.hash = checkNotNull(hash, "Hash cannot be null");
    this.type = checkNotNull(type, "Type cannot be null");
    this.size = checkNotNull(size, "Size cannot be null");
  }

  public static List<TransactionAnnouncement> create(
      final List<TransactionType> types, final List<Long> sizes, final List<Hash> hashes) {
    // Check if lists have the same size
    if (!(types.size() == hashes.size() && hashes.size() == sizes.size())) {
      throw new IllegalArgumentException(
          "Hashes, sizes and types must have the same number of elements");
    }
    final List<TransactionAnnouncement> transactions = new ArrayList<>(hashes.size());
    for (int i = 0; i < hashes.size(); i++) {
      transactions.add(new TransactionAnnouncement(hashes.get(i), types.get(i), sizes.get(i)));
    }
    return transactions;
  }

  public static List<TransactionAnnouncement> create(final List<Transaction> transactions) {
    List<TransactionAnnouncement> list = new ArrayList<>(transactions.size());
    for (Transaction transaction : transactions) {
      TransactionAnnouncement announcement = new TransactionAnnouncement(transaction);
      list.add(announcement);
    }
    return list;
  }
}
