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
package org.hyperledger.besu.ethereum.eth.encoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.CellsOnlyBlobTransactionFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.EthProtocolVersion;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionAnnouncement;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/** Covers the eth/72 {@code NewPooledTransactionHashes} format, which adds the cells bitmap. */
class TransactionAnnouncementEth72Test {

  private static final int MAX_ANNOUNCEMENTS =
      EthProtocolConfiguration.DEFAULT_MAX_ANNOUNCEMENTS_PER_MESSAGE;

  private static final CellMask MASK =
      CellMask.fromBytes(Bytes.fromHexString("0x05" + "00".repeat(15)));

  private static Hash hash(final int seed) {
    return Hash.wrap(Bytes32.wrap(Bytes.repeat((byte) seed, 32)));
  }

  private static Bytes encodeEth72(final CellMask mask) {
    return TransactionAnnouncementEncoder.encodeForEth72(
        new byte[] {
          TransactionType.BLOB.getEthSerializedType(),
          TransactionType.EIP1559.getEthSerializedType()
        },
        List.of(1000, 2000),
        List.of(hash(1), hash(2)),
        mask);
  }

  @Test
  void eth72MessageHasFourElementsEndingInTheCellsBitmap() {
    final RLPInput input = new BytesValueRLPInput(encodeEth72(MASK), false);
    assertThat(input.enterList()).isEqualTo(4);
    input.skipNext(); // txtypes
    input.skipNext(); // sizes
    input.skipNext(); // hashes
    final Bytes cells = input.readBytes();
    input.leaveList();

    // Fixed width B_16, as go-ethereum's CustodyBitmap [16]byte requires.
    assertThat(cells.size()).isEqualTo(CellMask.BYTE_LENGTH);
    assertThat(CellMask.fromBytes(cells)).isEqualTo(MASK);
  }

  @Test
  void eth68MessageStillHasThreeElements() {
    final Bytes encoded =
        TransactionAnnouncementEncoder.encodeForEth68(
            new byte[] {TransactionType.EIP1559.getEthSerializedType()},
            List.of(1000),
            List.of(hash(1)));
    final RLPInput input = new BytesValueRLPInput(encoded, false);
    assertThat(input.enterList()).isEqualTo(3);
  }

  @Test
  void roundTripsCellMaskOnEth72() {
    final List<TransactionAnnouncement> announcements =
        TransactionAnnouncementDecoder.getDecoder(EthProtocolVersion.V72.getCapability())
            .decode(RLP.input(encodeEth72(MASK)), MAX_ANNOUNCEMENTS);

    assertThat(announcements).hasSize(2);
    // The mask applies to blob transactions only; others carry none.
    assertThat(announcements.get(0).type()).isEqualTo(TransactionType.BLOB);
    assertThat(announcements.get(0).cellMask()).isEqualTo(MASK);
    assertThat(announcements.get(1).type()).isEqualTo(TransactionType.EIP1559);
    assertThat(announcements.get(1).cellMask()).isNull();
  }

  @Test
  void roundTripsAnAllZeroMaskWhenNoBlobTransactionIsAnnounced() {
    final Bytes encoded =
        TransactionAnnouncementEncoder.encodeForEth72(
            new byte[] {TransactionType.EIP1559.getEthSerializedType()},
            List.of(1000),
            List.of(hash(1)),
            CellMask.EMPTY);

    final RLPInput input = new BytesValueRLPInput(encoded, false);
    assertThat(input.enterList()).isEqualTo(4);
    input.skipNext();
    input.skipNext();
    input.skipNext();
    // Still written, and still full width, because the field is not optional.
    assertThat(input.readBytes().size()).isEqualTo(CellMask.BYTE_LENGTH);

    assertThat(
            TransactionAnnouncementDecoder.getDecoder(EthProtocolVersion.V72.getCapability())
                .decode(RLP.input(encoded), MAX_ANNOUNCEMENTS))
        .hasSize(1);
  }

  @Test
  void blobAnnouncementFromAPreEth72PeerIsTreatedAsFullyAvailable() {
    // An eth/71 peer serves whole blob payloads on GetPooledTransactions, so announcing a blob tx
    // implies it holds every cell. Decoding must not require a bitmap that the format lacks.
    final Bytes encoded =
        TransactionAnnouncementEncoder.encodeForEth68(
            new byte[] {TransactionType.BLOB.getEthSerializedType()},
            List.of(1000),
            List.of(hash(1)));

    final List<TransactionAnnouncement> announcements =
        TransactionAnnouncementDecoder.getDecoder(EthProtocolVersion.V71.getCapability())
            .decode(RLP.input(encoded), MAX_ANNOUNCEMENTS);

    assertThat(announcements).hasSize(1);
    assertThat(announcements.getFirst().cellMask()).isEqualTo(CellMask.FULL);
  }

  @Test
  void refusesToAnnounceTwoAvailabilitiesInOneMessage() {
    // The format has room for one mask per message, so a message mixing two is us telling a peer
    // we hold cells of a blob transaction that we do not. The sender groups announcements to keep
    // that from happening; this is the backstop, and it fails rather than picking a mask.
    final CellsOnlyBlobTransactionFixture fixture = new CellsOnlyBlobTransactionFixture();
    final List<Transaction> differingAvailability =
        List.of(fixture.create(1, CellMask.FULL), fixture.create(1, MASK));

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                TransactionAnnouncementEncoder.getEncoder(EthProtocolVersion.V72.getCapability())
                    .encode(differingAvailability))
        .withMessageContaining("must share a cell mask");
  }

  @Test
  void aBlobAnnouncementWithoutAMaskIsRejected() {
    // Every blob announcement states an availability, so a null mask is only for the types that
    // carry none.
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> new TransactionAnnouncement(hash(1), TransactionType.BLOB, 1000L, null))
        .withMessageContaining("Cell mask cannot be null");
  }

  @Test
  void aCellsBitmapOfTheWrongWidthIsAnRlpException() {
    // The processor charges a peer for an RLPException and lets anything else through as an
    // internal error, and decoding happens on a worker thread away from the handler, so a
    // malformed bitmap has to arrive as an RLPException or the peer is never disconnected for it.
    for (final int width : new int[] {0, CellMask.BYTE_LENGTH - 1, CellMask.BYTE_LENGTH + 1}) {
      final BytesValueRLPOutput out = new BytesValueRLPOutput();
      out.startList();
      out.writeBytes(Bytes.of(TransactionType.BLOB.getEthSerializedType()));
      out.writeList(List.of(1000), (size, rlp) -> rlp.writeUnsignedInt(size));
      out.writeList(List.of(hash(1)), (h, rlp) -> rlp.writeBytes(h.getBytes()));
      out.writeBytes(Bytes.repeat((byte) 0xff, width));
      out.endList();

      assertThatExceptionOfType(RLPException.class)
          .as("a %d byte cells bitmap", width)
          .isThrownBy(
              () ->
                  TransactionAnnouncementDecoder.getDecoder(EthProtocolVersion.V72.getCapability())
                      .decode(RLP.input(out.encoded()), MAX_ANNOUNCEMENTS));
    }
  }

  @Test
  void eth72DecoderIsSelectedOnlyForEth72AndLater() {
    final Bytes eth68 =
        TransactionAnnouncementEncoder.encodeForEth68(
            new byte[] {TransactionType.EIP1559.getEthSerializedType()},
            List.of(1000),
            List.of(hash(1)));

    // The eth/72 decoder would look for a fourth element that an eth/68 message does not have.
    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(
            () ->
                TransactionAnnouncementDecoder.getDecoder(EthProtocolVersion.V72.getCapability())
                    .decode(RLP.input(eth68), MAX_ANNOUNCEMENTS));

    assertThat(
            TransactionAnnouncementDecoder.getDecoder(EthProtocolVersion.V71.getCapability())
                .decode(RLP.input(eth68), MAX_ANNOUNCEMENTS))
        .hasSize(1);
  }
}
