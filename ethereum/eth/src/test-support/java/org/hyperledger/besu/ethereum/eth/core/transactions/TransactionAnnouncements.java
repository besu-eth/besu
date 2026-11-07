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
package org.hyperledger.besu.ethereum.eth.core.transactions;

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionAnnouncement;

import java.util.ArrayList;
import java.util.List;

/**
 * Announcements built from the transactions they describe, which is how a test states what a peer
 * announced. Production code announces from a message it is encoding or decoding, where the sizes
 * and the cell mask come from the version negotiated with the peer rather than from a transaction
 * in hand, so nothing there builds an announcement this way.
 */
public class TransactionAnnouncements {

  /**
   * The announcement a pre-eth/72 peer would make for a transaction, sized by the pooled form that
   * carries the blob payloads.
   *
   * @param transaction the transaction being announced
   * @return the announcement
   */
  public static TransactionAnnouncement create(final Transaction transaction) {
    final Transaction tx = checkNotNull(transaction, "Transaction cannot be null");
    return new TransactionAnnouncement(
        tx.getHash(),
        tx.getType(),
        (long) tx.getSizeForAnnouncement(),
        TransactionAnnouncement.announcedCellMask(tx).orElse(null));
  }

  /**
   * The announcements a pre-eth/72 peer would make for a list of transactions.
   *
   * @param transactions the transactions being announced
   * @return the announcements, in the order given
   */
  public static List<TransactionAnnouncement> create(final List<Transaction> transactions) {
    final List<TransactionAnnouncement> announcements = new ArrayList<>(transactions.size());
    for (final Transaction transaction : transactions) {
      announcements.add(create(transaction));
    }
    return announcements;
  }

  /**
   * An announcement carrying no cell mask. Valid only for non blob types; a blob announcement must
   * carry the announcer's availability, so the record rejects a null mask for those.
   *
   * @param hash the transaction hash
   * @param type the transaction type, which must not support blobs
   * @param size the announced size
   * @return the announcement
   */
  public static TransactionAnnouncement create(
      final Hash hash, final TransactionType type, final Long size) {
    return new TransactionAnnouncement(hash, type, size, null);
  }

  /**
   * Announcements carrying no cell mask, as a pre-eth/72 message decodes to.
   *
   * @param types the announced transaction types
   * @param sizes the announced sizes
   * @param hashes the announced hashes
   * @return the announcements, in the order given
   */
  public static List<TransactionAnnouncement> create(
      final List<TransactionType> types, final List<Long> sizes, final List<Hash> hashes) {
    return TransactionAnnouncement.create(types, sizes, hashes, null);
  }
}
