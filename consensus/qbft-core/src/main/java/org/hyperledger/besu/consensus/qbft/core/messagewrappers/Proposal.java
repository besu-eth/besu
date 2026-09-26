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
package org.hyperledger.besu.consensus.qbft.core.messagewrappers;

import org.hyperledger.besu.consensus.common.bft.messagewrappers.BftMessage;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.qbft.core.payload.PreparePayload;
import org.hyperledger.besu.consensus.qbft.core.payload.ProposalPayload;
import org.hyperledger.besu.consensus.qbft.core.payload.RoundChangePayload;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCodec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/** The Proposal. */
public class Proposal extends BftMessage<ProposalPayload> {

  private final List<SignedData<RoundChangePayload>> roundChanges;
  private final List<SignedData<PreparePayload>> prepares;

  /**
   * Instantiates a new Proposal.
   *
   * @param payload the payload
   * @param roundChanges the round changes
   * @param prepares the prepares
   */
  public Proposal(
      final SignedData<ProposalPayload> payload,
      final List<SignedData<RoundChangePayload>> roundChanges,
      final List<SignedData<PreparePayload>> prepares) {
    super(payload);
    this.roundChanges = roundChanges;
    this.prepares = prepares;
  }

  /**
   * Gets round changes.
   *
   * @return the round changes
   */
  public List<SignedData<RoundChangePayload>> getRoundChanges() {
    return roundChanges;
  }

  /**
   * Gets list of Prepare payload.
   *
   * @return the list of Prepare payload
   */
  public List<SignedData<PreparePayload>> getPrepares() {
    return prepares;
  }

  /**
   * Gets block.
   *
   * @return the block
   */
  public QbftBlock getBlock() {
    return getPayload().getProposedBlock();
  }

  /**
   * Gets block access list.
   *
   * @return the block access list
   */
  public Optional<BlockAccessList> getBlockAccessList() {
    return getPayload().getBlockAccessList();
  }

  @Override
  public Bytes encode() {
    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    rlpOut.startList();
    getSignedPayload().writeTo(rlpOut);

    rlpOut.startList();
    rlpOut.writeList(roundChanges, SignedData::writeTo);
    rlpOut.writeList(prepares, SignedData::writeTo);
    rlpOut.endList();

    rlpOut.endList();
    return rlpOut.encoded();
  }

  /**
   * Decode.
   *
   * @param data the data
   * @param blockEncoder the qbft block encoder
   * @return the proposal
   */
  public static Proposal decode(final Bytes data, final QbftBlockCodec blockEncoder) {
    return decode(data, blockEncoder, MAX_LIST_ENTRIES);
  }

  /**
   * Decode with an explicit cap on the certificate lists (round-changes and prepares). Use {@code
   * validators.size()} as the cap when decoding current-height messages to bound secp256k1 work.
   *
   * @param data the data
   * @param blockEncoder the qbft block encoder
   * @param maxCertEntries maximum permitted entries in each certificate list
   * @return the proposal
   */
  public static Proposal decode(
      final Bytes data, final QbftBlockCodec blockEncoder, final int maxCertEntries) {
    final RLPInput rlpIn = RLP.input(data);
    rlpIn.enterList();
    final SignedData<ProposalPayload> payload =
        readPayload(rlpIn, rlpInput -> ProposalPayload.readFrom(rlpInput, blockEncoder));

    rlpIn.enterList();
    final List<SignedData<RoundChangePayload>> roundChanges =
        rlpIn.readList(r -> readPayload(r, RoundChangePayload::readFrom), maxCertEntries);
    final List<SignedData<PreparePayload>> prepares =
        rlpIn.readList(r -> readPayload(r, PreparePayload::readFrom), maxCertEntries);
    rlpIn.leaveList();

    rlpIn.leaveList();
    return new Proposal(payload, roundChanges, prepares);
  }

  /**
   * Reads only the sequence number (block height) from the encoded message without full decode.
   *
   * @param data the raw encoded message bytes
   * @return the sequence number
   */
  public static long decodeSequence(final Bytes data) {
    final RLPInput rlp = RLP.input(data);
    rlp.enterList(); // outer Proposal list
    rlp.enterList(); // signed-data wrapper
    rlp.enterList(); // ProposalPayload
    return rlp.readLongScalar();
  }
}
