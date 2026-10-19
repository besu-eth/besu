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

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.kzg.Cell;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes;

public final class CellsMessage extends AbstractMessageData {

  private static final int MESSAGE_CODE = EthProtocolMessages.CELLS;
  private final Supplier<MessageFields> fieldsSupplier;

  private CellsMessage(final Bytes rlp) {
    super(rlp);
    fieldsSupplier = Suppliers.memoize(this::parse);
  }

  private CellsMessage(final Bytes rlp, final MessageFields fields) {
    super(rlp);
    fieldsSupplier = Suppliers.ofInstance(fields);
  }

  @Override
  public int getCode() {
    return MESSAGE_CODE;
  }

  public static CellsMessage createUnsafe(final Bytes data) {
    return new CellsMessage(data);
  }

  /**
   * Encodes a Cells response.
   *
   * <p>devp2p schema: {@code [request-id: P, [txhash1: B_32, ...], [[cell1: B_2048, cell2: B_2048,
   * ...], [cell1: B_2048, ...], ...], cells: B_16]}
   *
   * <p>The request id is prepended later by {@link
   * org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData#wrapMessageData}, so the three elements
   * encoded here are separate top level RLP items rather than a list of their own. They are encoded
   * separately and concatenated, because {@link BytesValueRLPOutput} only accepts a top level byte
   * string as its very first write.
   *
   * <p>The cell groups are indexed in parallel with {@code txHashes}. A group holds {@code
   * blobCount * cellMask.cardinality()} cells laid out <b>blob major</b>: for each blob in
   * transaction order, its requested cells by ascending index. The receiver recovers the blob count
   * from the transaction body and the cells per blob from {@code cells}; see {@code
   * GetCellsFromPeerTask}.
   *
   * <p>Note that the devp2p prose currently states the opposite order ("cells are listed by
   * ascending index, and for each index in the order in which the blobs appear"), but go-ethereum,
   * the reference implementation, is blob major on both ends: {@code answerGetCells} flattens
   * {@code for blob { for index }}, {@code sortCells} de-interleaves with {@code
   * d.Cells[b*n:(b+1)*n]}, and its sidecar storage indexes {@code Cells[blobIdx*cellsPerBlob+pos]}.
   * Interoperability follows the implementation, so this does too. Worth raising upstream.
   *
   * @param txHashes the transactions cells are returned for
   * @param cellsPerTx for each transaction, its cells ordered blob major
   * @param cellMask the cell indexes actually served, identical for every returned transaction
   * @return the encoded message
   */
  public static CellsMessage create(
      final List<Hash> txHashes, final List<List<Cell>> cellsPerTx, final CellMask cellMask) {
    checkArgument(
        txHashes.size() == cellsPerTx.size(),
        "tx hashes (%s) and cell lists (%s) must have the same number of elements",
        txHashes.size(),
        cellsPerTx.size());

    final BytesValueRLPOutput hashesOut = new BytesValueRLPOutput();
    hashesOut.writeList(txHashes, (h, w) -> w.writeBytes(h.getBytes()));

    final BytesValueRLPOutput cellsOut = new BytesValueRLPOutput();
    cellsOut.writeList(cellsPerTx, (cells, w) -> w.writeList(cells, Cell::writeTo));

    final BytesValueRLPOutput maskOut = new BytesValueRLPOutput();
    maskOut.writeBytes(cellMask.toBytes());

    return new CellsMessage(
        Bytes.concatenate(hashesOut.encoded(), cellsOut.encoded(), maskOut.encoded()),
        new MessageFields(txHashes, cellsPerTx, cellMask));
  }

  public static CellsMessage readFrom(final MessageData message) {
    if (message instanceof CellsMessage cellsMessage) {
      return cellsMessage;
    }
    final int code = message.getCode();
    if (code != MESSAGE_CODE) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a CellsMessage.", code));
    }

    return new CellsMessage(message.getData());
  }

  public List<Hash> txHashes() {
    return fieldsSupplier.get().txHashes;
  }

  public List<List<Cell>> cellsList() {
    return fieldsSupplier.get().cellsList;
  }

  public CellMask cellMask() {
    return fieldsSupplier.get().cellMask;
  }

  private MessageFields parse() {
    // Three sibling top level items, not a list; see create(). A single BytesValueRLPInput cannot
    // read past the first item when that item is a list, because it clamps its size to that item,
    // so the body is split by item size first, as MessageData#unwrapMessageData does.
    final Bytes data = getData();
    final int hashesSize = RLP.calculateSize(data);
    final Bytes afterHashes = data.slice(hashesSize);
    final int cellsSize = RLP.calculateSize(afterHashes);

    final List<Hash> txHashes =
        new BytesValueRLPInput(data.slice(0, hashesSize), false)
            .readList(rlp -> Hash.wrap(rlp.readBytes32()));
    final List<List<Cell>> cells = new ArrayList<>();
    new BytesValueRLPInput(afterHashes.slice(0, cellsSize), false)
        .readList(rlp -> cells.add(rlp.readList(Cell::readFrom)));
    final CellMask cellMask =
        CellMask.fromBytes(new BytesValueRLPInput(afterHashes.slice(cellsSize), false).readBytes());

    if (txHashes.size() != cells.size()) {
      throw new RLPException(
          "Received %d tx hashes but %d cell lists".formatted(txHashes.size(), cells.size()));
    }
    // Each returned transaction contributes one cell per blob per set mask index, so its flat cell
    // list must divide evenly by the mask cardinality. The blob count is not known here, so this is
    // only a structural check; the caller, which knows the transaction, validates the exact count.
    final int cellsPerBlob = cellMask.cardinality();
    for (final List<Cell> txCells : cells) {
      if (cellsPerBlob == 0 ? !txCells.isEmpty() : txCells.size() % cellsPerBlob != 0) {
        throw new RLPException(
            "Received %d cells for a transaction, not a multiple of the %d cells per blob implied by the cell mask"
                .formatted(txCells.size(), cellsPerBlob));
      }
    }
    return new MessageFields(txHashes, cells, cellMask);
  }

  private record MessageFields(
      List<Hash> txHashes, List<List<Cell>> cellsList, CellMask cellMask) {}
}
