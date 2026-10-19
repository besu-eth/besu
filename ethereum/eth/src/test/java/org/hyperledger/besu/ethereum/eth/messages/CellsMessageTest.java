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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.kzg.Cell;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class CellsMessageTest {

  /** Indexes 0 and 2, so two cells per blob. */
  private static final CellMask MASK =
      CellMask.fromBytes(Bytes.fromHexString("0x05" + "00".repeat(15)));

  private static Cell cell(final int seed) {
    return new Cell(Bytes.repeat((byte) seed, Cell.SIZE));
  }

  private static Hash hash(final int seed) {
    return Hash.wrap(Bytes32.wrap(Bytes.repeat((byte) seed, 32)));
  }

  /**
   * One flat cell group per transaction, laid out blob major as go-ethereum does: for each blob in
   * transaction order, its requested cells by ascending index. So the group holds blobCount *
   * MASK.cardinality() cells.
   */
  private static List<Cell> cellsForTx(final int txSeed, final int blobCount) {
    final List<Cell> cells = new ArrayList<>();
    for (int blob = 0; blob < blobCount; blob++) {
      for (int maskIndex = 0; maskIndex < MASK.cardinality(); maskIndex++) {
        cells.add(cell(txSeed * 10 + blob * 20 + maskIndex));
      }
    }
    return cells;
  }

  @Test
  void roundTripsMultipleTransactionsAndBlobs() {
    final List<Hash> hashes = List.of(hash(1), hash(2));
    final List<List<Cell>> cellsPerTx = List.of(cellsForTx(1, 3), cellsForTx(2, 3));

    final CellsMessage msg = CellsMessage.create(hashes, cellsPerTx, MASK);
    assertThat(msg.getCode()).isEqualTo(EthProtocolMessages.CELLS);

    final CellsMessage reparsed =
        CellsMessage.readFrom(new RawMessage(EthProtocolMessages.CELLS, msg.getData()));

    assertThat(reparsed.txHashes()).isEqualTo(hashes);
    assertThat(reparsed.cellsList()).isEqualTo(cellsPerTx);
    assertThat(reparsed.cellMask()).isEqualTo(MASK);
  }

  @Test
  void roundTripsThroughRequestIdWrapping() {
    final CellsMessage msg = CellsMessage.create(List.of(hash(1)), List.of(cellsForTx(1, 1)), MASK);
    final var unwrapped = msg.wrapMessageData(BigInteger.valueOf(7)).unwrapMessageData();

    assertThat(unwrapped.getKey()).isEqualTo(BigInteger.valueOf(7));
    final CellsMessage reparsed = CellsMessage.readFrom(unwrapped.getValue());
    assertThat(reparsed.txHashes()).containsExactly(hash(1));
    assertThat(reparsed.cellsList().getFirst()).hasSize(MASK.cardinality()); // 1 blob
    assertThat(reparsed.cellMask()).isEqualTo(MASK);
  }

  @Test
  void wrappedMessageHasTheDevp2pElementCount() {
    // devp2p: [request-id: P, [txhash...], [[cell...], ...], cells: B_16] -- four elements once the
    // request id is prepended, so the body must be three sibling items rather than one nested list.
    final CellsMessage msg = CellsMessage.create(List.of(hash(1)), List.of(cellsForTx(1, 1)), MASK);
    final Bytes wrapped = msg.wrapMessageData(BigInteger.valueOf(3)).getData();

    final RLPInput input = new BytesValueRLPInput(wrapped, false);
    assertThat(input.enterList()).isEqualTo(4);
    assertThat(input.readBigIntegerScalar()).isEqualTo(BigInteger.valueOf(3));
    assertThat(input.readList(rlp -> Hash.wrap(rlp.readBytes32()))).containsExactly(hash(1));
    input.skipNext(); // the cell groups
    assertThat(CellMask.fromBytes(input.readBytes())).isEqualTo(MASK);
    input.leaveList();
  }

  @Test
  void roundTripsEmptyResponse() {
    // A responder that holds none of the requested transactions returns no hashes and no cells.
    final CellsMessage msg = CellsMessage.create(List.of(), List.of(), CellMask.EMPTY);
    final CellsMessage reparsed =
        CellsMessage.readFrom(new RawMessage(EthProtocolMessages.CELLS, msg.getData()));

    assertThat(reparsed.txHashes()).isEmpty();
    assertThat(reparsed.cellsList()).isEmpty();
    assertThat(reparsed.cellMask().isEmpty()).isTrue();
  }

  @Test
  void createRejectsMismatchedHashAndCellCounts() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () -> CellsMessage.create(List.of(hash(1), hash(2)), List.of(cellsForTx(1, 1)), MASK));
  }

  @Test
  void parseRejectsCellCountNotMatchingMask() {
    // Three cells cannot be split into blobs of two, so the peer's response is malformed.
    // Hand-encoded rather than built with create(), because create() carries the fields through
    // directly and never goes via parse(); only bytes received from a peer are validated.
    final CellsMessage msg =
        CellsMessage.readFrom(
            new RawMessage(
                EthProtocolMessages.CELLS,
                encodeRaw(List.of(hash(1)), List.of(List.of(cell(1), cell(2), cell(3))), MASK)));
    assertThatExceptionOfType(RLPException.class).isThrownBy(msg::cellsList);
  }

  /** Encodes a Cells body directly, bypassing create()'s consistency checks. */
  private static Bytes encodeRaw(
      final List<Hash> txHashes, final List<List<Cell>> cellsPerTx, final CellMask cellMask) {
    final BytesValueRLPOutput hashesOut = new BytesValueRLPOutput();
    hashesOut.writeList(txHashes, (h, w) -> w.writeBytes(h.getBytes()));
    final BytesValueRLPOutput cellsOut = new BytesValueRLPOutput();
    cellsOut.writeList(cellsPerTx, (cells, w) -> w.writeList(cells, Cell::writeTo));
    final BytesValueRLPOutput maskOut = new BytesValueRLPOutput();
    maskOut.writeBytes(cellMask.toBytes());
    return Bytes.concatenate(hashesOut.encoded(), cellsOut.encoded(), maskOut.encoded());
  }

  @Test
  void parseRejectsCellsWithoutMatchingHashes() {
    // Hand-encode a response whose hash list and cell list disagree, which create() would refuse.
    final BytesValueRLPOutput hashesOut = new BytesValueRLPOutput();
    hashesOut.writeList(List.of(hash(1)), (h, w) -> w.writeBytes(h.getBytes()));
    final BytesValueRLPOutput cellsOut = new BytesValueRLPOutput();
    cellsOut.writeList(
        List.of(cellsForTx(1, 1), cellsForTx(2, 1)),
        (cells, w) -> w.writeList(cells, Cell::writeTo));
    final BytesValueRLPOutput maskOut = new BytesValueRLPOutput();
    maskOut.writeBytes(MASK.toBytes());

    final CellsMessage msg =
        CellsMessage.readFrom(
            new RawMessage(
                EthProtocolMessages.CELLS,
                Bytes.concatenate(hashesOut.encoded(), cellsOut.encoded(), maskOut.encoded())));
    assertThatExceptionOfType(RLPException.class).isThrownBy(msg::txHashes);
  }

  @Test
  void parseRejectsUndersizedCell() {
    final BytesValueRLPOutput hashesOut = new BytesValueRLPOutput();
    hashesOut.writeList(List.of(hash(1)), (h, w) -> w.writeBytes(h.getBytes()));
    final BytesValueRLPOutput cellsOut = new BytesValueRLPOutput();
    cellsOut.startList();
    cellsOut.startList();
    cellsOut.writeBytes(Bytes.repeat((byte) 1, Cell.SIZE - 1));
    cellsOut.endList();
    cellsOut.endList();
    final BytesValueRLPOutput maskOut = new BytesValueRLPOutput();
    maskOut.writeBytes(MASK.toBytes());

    final CellsMessage msg =
        CellsMessage.readFrom(
            new RawMessage(
                EthProtocolMessages.CELLS,
                Bytes.concatenate(hashesOut.encoded(), cellsOut.encoded(), maskOut.encoded())));
    assertThatExceptionOfType(RLPException.class).isThrownBy(msg::cellsList);
  }

  @Test
  void readFromMessageWithWrongCodeThrows() {
    final RawMessage rawMsg = new RawMessage(EthProtocolMessages.BLOCK_HEADERS, Bytes.of(0));
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> CellsMessage.readFrom(rawMsg));
  }
}
