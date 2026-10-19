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

import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class GetCellsMessageTest {

  private static final CellMask MASK =
      CellMask.fromBytes(Bytes.fromHexString("0x05" + "00".repeat(15)));

  private static Transaction tx(final long nonce) {
    return Transaction.builder()
        .type(TransactionType.FRONTIER)
        .nonce(nonce)
        .gasLimit(654321)
        .gasPrice(Wei.of(2))
        .value(Wei.of(1337))
        .payload(Bytes.EMPTY)
        .signAndBuild(SignatureAlgorithmFactory.getInstance().generateKeyPair());
  }

  @Test
  void roundTripsHashesAndCellMask() {
    final List<Transaction> txs = List.of(tx(1), tx(2));
    final GetCellsMessage msg = GetCellsMessage.create(txs, MASK);

    assertThat(msg.getCode()).isEqualTo(EthProtocolMessages.GET_CELLS);

    // Re-read from raw bytes so the encoded form, not the memoized fields, is exercised.
    final GetCellsMessage reparsed =
        GetCellsMessage.readFrom(new RawMessage(EthProtocolMessages.GET_CELLS, msg.getData()));

    assertThat(reparsed.pooledTransactions())
        .containsExactly(txs.get(0).getHash(), txs.get(1).getHash());
    assertThat(reparsed.cellMask()).isEqualTo(MASK);
  }

  @Test
  void roundTripsThroughRequestIdWrapping() {
    // GET_CELLS is a request-id message, so the encoded body has to survive being wrapped by
    // RequestManager and unwrapped on the responder side.
    final GetCellsMessage msg = GetCellsMessage.create(List.of(tx(1)), CellMask.FULL);
    final var unwrapped = msg.wrapMessageData(BigInteger.valueOf(42)).unwrapMessageData();

    assertThat(unwrapped.getKey()).isEqualTo(BigInteger.valueOf(42));
    final GetCellsMessage reparsed = GetCellsMessage.readFrom(unwrapped.getValue());
    assertThat(reparsed.cellMask()).isEqualTo(CellMask.FULL);
    assertThat(reparsed.pooledTransactions()).hasSize(1);
  }

  @Test
  void wrappedMessageHasTheDevp2pElementCount() {
    // devp2p: [request-id: P, [txhash...], cells: B_16] -- three elements once the request id is
    // prepended, so the body must be two sibling items rather than one nested list.
    final GetCellsMessage msg = GetCellsMessage.create(List.of(tx(1)), MASK);
    final Bytes wrapped = msg.wrapMessageData(BigInteger.valueOf(5)).getData();

    final RLPInput input = new BytesValueRLPInput(wrapped, false);
    assertThat(input.enterList()).isEqualTo(3);
    assertThat(input.readBigIntegerScalar()).isEqualTo(BigInteger.valueOf(5));
    assertThat(input.readList(rlp -> Hash.wrap(rlp.readBytes32()))).hasSize(1);
    assertThat(CellMask.fromBytes(input.readBytes())).isEqualTo(MASK);
    input.leaveList();
  }

  @Test
  void roundTripsEmptyHashList() {
    final GetCellsMessage msg = GetCellsMessage.create(List.of(), CellMask.EMPTY);
    final GetCellsMessage reparsed =
        GetCellsMessage.readFrom(new RawMessage(EthProtocolMessages.GET_CELLS, msg.getData()));
    assertThat(reparsed.pooledTransactions()).isEmpty();
    assertThat(reparsed.cellMask()).isEqualTo(CellMask.EMPTY);
  }

  @Test
  void readFromMessageWithWrongCodeThrows() {
    final RawMessage rawMsg = new RawMessage(EthProtocolMessages.BLOCK_HEADERS, Bytes.of(0));
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> GetCellsMessage.readFrom(rawMsg));
  }
}
