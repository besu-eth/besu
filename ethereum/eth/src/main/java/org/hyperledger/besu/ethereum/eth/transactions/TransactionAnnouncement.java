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
import org.hyperledger.besu.ethereum.core.kzg.CellMask;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public record TransactionAnnouncement(
    Hash hash, TransactionType type, Long size, CellMask cellMask) {
  public TransactionAnnouncement(final Transaction transaction) {
    final Transaction tx = checkNotNull(transaction, "Transaction cannot be null");
    this(
        tx.getHash(),
        tx.getType(),
        (long) tx.getSizeForAnnouncement(),
        announcedCellMask(tx).orElse(null));
  }

  /**
   * The cell mask a transaction is announced with: for a blob transaction the availability of its
   * blobs, and nothing at all for any other type. Every place that has to work this out goes
   * through here, so that the mask a transaction is grouped by, the one a message is encoded with
   * and the one an announcement records cannot disagree.
   *
   * @param transaction the transaction being announced
   * @return the mask, or empty for a transaction type that carries no blobs
   * @throws IllegalArgumentException if a blob transaction has no sidecar, and so nothing to
   *     announce the availability of
   */
  public static Optional<CellMask> announcedCellMask(final Transaction transaction) {
    if (!transaction.getType().supportsBlob()) {
      return Optional.empty();
    }
    return Optional.of(
        transaction
            .getBlobsWithCommitments()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Blob transaction "
                            + transaction.getHash()
                            + " has no sidecar, so no cell availability to announce"))
            .getCellMask());
  }

  /**
   * Announces a transaction with no cell mask. Valid only for non blob types; a blob announcement
   * must carry the announcer's availability, so the canonical constructor rejects a null mask for
   * those. Mirrors the pre eth/72 {@link #create(List, List, List)} overload.
   *
   * @param hash the transaction hash
   * @param type the transaction type, which must not support blobs
   * @param size the announced size
   */
  public TransactionAnnouncement(final Hash hash, final TransactionType type, final Long size) {
    this(hash, type, size, null);
  }

  public TransactionAnnouncement(
      final Hash hash, final TransactionType type, final Long size, final CellMask cellMask) {
    this.hash = checkNotNull(hash, "Hash cannot be null");
    this.type = checkNotNull(type, "Type cannot be null");
    this.size = checkNotNull(size, "Size cannot be null");
    this.cellMask = type.supportsBlob() ? checkNotNull(cellMask, "Cell mask cannot be null") : null;
  }

  public static List<TransactionAnnouncement> create(
      final List<TransactionType> types, final List<Long> sizes, final List<Hash> hashes) {
    return create(types, sizes, hashes, null);
  }

  public static List<TransactionAnnouncement> create(
      final List<TransactionType> types,
      final List<Long> sizes,
      final List<Hash> hashes,
      final CellMask cellMask) {
    // Check if lists have the same size
    if (!(types.size() == hashes.size() && hashes.size() == sizes.size())) {
      throw new IllegalArgumentException(
          "Hashes, sizes and types must have the same number of elements");
    }
    final List<TransactionAnnouncement> transactions = new ArrayList<>(hashes.size());
    for (int i = 0; i < hashes.size(); i++) {
      transactions.add(
          new TransactionAnnouncement(hashes.get(i), types.get(i), sizes.get(i), cellMask));
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
