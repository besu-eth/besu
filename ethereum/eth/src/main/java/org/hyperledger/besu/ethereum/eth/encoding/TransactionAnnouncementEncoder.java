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
package org.hyperledger.besu.ethereum.eth.encoding;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionAnnouncement;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.util.ArrayList;
import java.util.List;
import java.util.function.ToIntFunction;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;

public class TransactionAnnouncementEncoder {

  /**
   * Upper bound of the bytes one announcement adds to a message: its type byte, its size as an RLP
   * scalar of at most 4 bytes plus a 1-byte prefix, and its 32-byte hash plus a 1-byte prefix.
   */
  public static final int MAX_ENCODED_ANNOUNCEMENT_SIZE = 1 + (1 + 4) + (1 + 32);

  /**
   * Upper bound of what frames a message, in any version: the RLP prefixes of the outer list, the
   * types string and the sizes and hashes lists, each at most 1 + 4 bytes for any length that fits
   * an int, plus the eth/72 cell mask, a 16-byte string with a 1-byte prefix. The mask is counted
   * for every version so that one size limit admits a full message on all of them.
   */
  public static final int MAX_ENCODED_MESSAGE_OVERHEAD = 4 * (1 + 4) + (1 + CellMask.BYTE_LENGTH);

  /**
   * Returns the largest size a message carrying {@code maxAnnouncements} announcements can have,
   * whatever their types and sizes, in any protocol version.
   *
   * @param maxAnnouncements the maximum number of announcements in a message
   * @return the maximum encoded message size in bytes, capped at {@link Integer#MAX_VALUE}
   */
  public static int maxMessageSize(final int maxAnnouncements) {
    final long maxSize =
        MAX_ENCODED_MESSAGE_OVERHEAD + (long) maxAnnouncements * MAX_ENCODED_ANNOUNCEMENT_SIZE;
    return (int) Math.min(maxSize, Integer.MAX_VALUE);
  }

  @FunctionalInterface
  public interface Encoder {
    Bytes encode(List<Transaction> transaction);
  }

  /**
   * Returns the correct encoder given an Eth Capability
   *
   * <p>See <a href="https://eips.ethereum.org/EIPS/eip-5793">EIP-5793</a>
   *
   * @param capability the version of the eth protocol
   * @return the correct encoder
   */
  public static Encoder getEncoder(final Capability capability) {
    if (EthProtocol.isEth72Compatible(capability)) {
      return TransactionAnnouncementEncoder::encodeForEth72;
    }
    return TransactionAnnouncementEncoder::encodeForEth68;
  }

  /**
   * Encode a list of transactions for the NewPooledTransactionHashesMessage using eth/72.
   *
   * <p>format: {@code [txtypes: B, [txsize1: P, ...], [txhash1: B_32, ...], cells: B_16]}
   *
   * <p>Two things differ from eth/68. The trailing {@code cells} bitmap announces which cell
   * indices the sender holds for every blob transaction in the message, and the sizes are those of
   * the blob-elided encoding, because that is what an eth/72 {@code GetPooledTransactions} response
   * returns.
   *
   * @param transactions the list to encode
   * @return the encoded value
   */
  private static Bytes encodeForEth72(final List<Transaction> transactions) {
    return encode(
        transactions, Transaction::getSizeForEth72Announcement, messageCellMask(transactions));
  }

  /**
   * Encode a list of transactions for the NewPooledTransactionHashesMessage using the Eth/68
   *
   * <p>format: [[type_0: B_1, type_1: B_1, ...], [size_0: P, size_1: P, ...], ...]
   *
   * @param transactions the list to encode
   * @return the encoded value. The message data will contain hashes, types and sizes.
   */
  private static Bytes encodeForEth68(final List<Transaction> transactions) {
    return encode(transactions, Transaction::getSizeForAnnouncement, null);
  }

  /**
   * The two versions differ only in how a transaction is sized and in the trailing cell mask, so
   * they share everything else.
   *
   * @param transactions the transactions to announce
   * @param announcedSize the size of the pooled form the receiving peer will be served
   * @param cellMask the message cell mask, or null for a version that carries none
   * @return the encoded message
   */
  private static Bytes encode(
      final List<Transaction> transactions,
      final ToIntFunction<Transaction> announcedSize,
      final CellMask cellMask) {
    final List<Integer> sizes = new ArrayList<>(transactions.size());
    final byte[] types = new byte[transactions.size()];
    final List<Hash> hashes = new ArrayList<>(transactions.size());

    for (int i = 0; i < transactions.size(); i++) {
      final Transaction transaction = transactions.get(i);
      types[i] = transaction.getType().getEthSerializedType();
      sizes.add(announcedSize.applyAsInt(transaction));
      hashes.add(transaction.getHash());
    }

    return encode(types, sizes, hashes, cellMask);
  }

  /**
   * The single cell mask carried by the message, describing availability for every blob transaction
   * announced in it.
   *
   * <p>The wire format has room for only one mask per message, so callers must group announcements
   * such that a message holds transactions of a single availability; {@code
   * NewPooledTransactionHashesMessageSender} does that by keeping one open batch per mask. A
   * violation is a wire-level bug that peers would see as us lying about what we hold, so it fails
   * loudly here rather than silently announcing the wrong availability.
   *
   * @param transactions the transactions being announced
   * @return the shared mask, or an all-zero mask when no blob transaction is announced
   */
  private static CellMask messageCellMask(final List<Transaction> transactions) {
    CellMask messageMask = null;
    for (final Transaction transaction : transactions) {
      final CellMask txMask = TransactionAnnouncement.announcedCellMask(transaction).orElse(null);
      if (txMask == null) {
        continue;
      }
      if (messageMask == null) {
        messageMask = txMask;
      } else {
        checkArgument(
            messageMask.equals(txMask),
            "Blob transactions announced together must share a cell mask, got %s and %s",
            messageMask,
            txMask);
      }
    }
    // The bitmap is ignored by receivers when no blob transaction is announced, but it is a
    // fixed-width field, so an all-zero mask is written rather than omitting it.
    return messageMask == null ? CellMask.EMPTY : messageMask;
  }

  @VisibleForTesting
  public static Bytes encodeForEth72(
      final byte[] types,
      final List<Integer> sizes,
      final List<Hash> hashes,
      final CellMask cellMask) {
    return encode(types, sizes, hashes, checkNotNull(cellMask, "Cell mask cannot be null"));
  }

  @VisibleForTesting
  public static Bytes encodeForEth68(
      final List<TransactionType> types, final List<Integer> sizes, final List<Hash> hashes) {

    final byte[] byteTypes = new byte[types.size()];
    for (int i = 0; i < types.size(); i++) {
      final TransactionType type = types.get(i);
      byteTypes[i] = type.getEthSerializedType();
    }
    return encodeForEth68(byteTypes, sizes, hashes);
  }

  @VisibleForTesting
  public static Bytes encodeForEth68(
      final byte[] types, final List<Integer> sizes, final List<Hash> hashes) {
    return encode(types, sizes, hashes, null);
  }

  private static Bytes encode(
      final byte[] types,
      final List<Integer> sizes,
      final List<Hash> hashes,
      final CellMask cellMask) {
    // Check if lists have the same size
    if (!(types.length == hashes.size() && hashes.size() == sizes.size())) {
      throw new IllegalArgumentException(
          "Hashes, sizes and types must have the same number of elements");
    }
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(Bytes.wrap(types));
    out.writeList(sizes, (h, w) -> w.writeUnsignedInt(h));
    out.writeList(hashes, (h, w) -> w.writeBytes(h.getBytes()));
    if (cellMask != null) {
      out.writeBytes(cellMask.toBytes());
    }
    out.endList();
    return out.encoded();
  }
}
