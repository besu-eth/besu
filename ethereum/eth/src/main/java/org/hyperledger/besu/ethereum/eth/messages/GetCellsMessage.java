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
package org.hyperledger.besu.ethereum.eth.messages;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes;

public final class GetCellsMessage extends AbstractMessageData {

  private static final int MESSAGE_CODE = EthProtocolMessages.GET_CELLS;

  private final Supplier<MessageFields> fieldsSupplier;

  private GetCellsMessage(final Bytes rlp) {
    super(rlp);
    fieldsSupplier = Suppliers.memoize(this::parse);
  }

  private GetCellsMessage(final Bytes rlp, final MessageFields messageFields) {
    super(rlp);
    fieldsSupplier = Suppliers.ofInstance(messageFields);
  }

  @Override
  public int getCode() {
    return MESSAGE_CODE;
  }

  /**
   * Encodes a GetCells request.
   *
   * <p>devp2p schema: {@code [request-id: P, [txhash1: B_32, txhash2: B_32, ...], cells: B_16]}
   *
   * <p>The request id is prepended later by {@link
   * org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData#wrapMessageData}, so the body encoded
   * here is the two remaining elements as separate top level RLP items, not wrapped in a list of
   * their own. {@link BytesValueRLPOutput} only accepts a top level byte string as its very first
   * write, so the two items are encoded separately and concatenated; RLP items are self delimiting,
   * so the result is a valid two item stream.
   *
   * @param pooledTransactions the transactions whose cells are being requested
   * @param cellMask the cell indexes being requested
   * @return the encoded message
   */
  public static GetCellsMessage create(
      final Collection<Transaction> pooledTransactions, final CellMask cellMask) {
    final List<Hash> hashes = Transaction.toHashList(pooledTransactions);

    final BytesValueRLPOutput hashesOut = new BytesValueRLPOutput();
    hashesOut.writeList(hashes, (h, w) -> w.writeBytes(h.getBytes()));

    final BytesValueRLPOutput cellsOut = new BytesValueRLPOutput();
    cellsOut.writeBytes(cellMask.toBytes());

    return new GetCellsMessage(
        Bytes.concatenate(hashesOut.encoded(), cellsOut.encoded()),
        new MessageFields(hashes, cellMask));
  }

  public static GetCellsMessage readFrom(final MessageData message) {
    if (message instanceof GetCellsMessage getCellsMessage) {
      return getCellsMessage;
    }
    final int code = message.getCode();
    if (code != MESSAGE_CODE) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a GetCellsMessage.", code));
    }

    return new GetCellsMessage(message.getData());
  }

  public Iterable<Hash> pooledTransactions() {
    return fieldsSupplier.get().pooledTransactions();
  }

  public CellMask cellMask() {
    return fieldsSupplier.get().cellMask();
  }

  private MessageFields parse() {
    // Two sibling top level items, not a list; see create(). A single BytesValueRLPInput cannot
    // read past the first item when that item is a list, because it clamps its size to that item,
    // so the body is split by item size first, as MessageData#unwrapMessageData does.
    final Bytes data = getData();
    final int hashesSize = RLP.calculateSize(data);

    final Iterable<Hash> pooledTransactions =
        new BytesValueRLPInput(data.slice(0, hashesSize), false)
            .readList(rlp -> Hash.wrap(rlp.readBytes32()));
    final CellMask cellMask =
        CellMask.fromBytes(new BytesValueRLPInput(data.slice(hashesSize), false).readBytes());

    return new MessageFields(pooledTransactions, cellMask);
  }

  private record MessageFields(Iterable<Hash> pooledTransactions, CellMask cellMask) {}
}
