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
package org.hyperledger.besu.consensus.ibft.messagewrappers;

import org.hyperledger.besu.consensus.common.bft.BftBlockHeaderFunctions;
import org.hyperledger.besu.consensus.common.bft.messagewrappers.BftMessage;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.ibft.IbftExtraDataCodec;
import org.hyperledger.besu.consensus.ibft.payload.PayloadDeserializers;
import org.hyperledger.besu.consensus.ibft.payload.ProposalPayload;
import org.hyperledger.besu.consensus.ibft.payload.RoundChangeCertificate;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListDecoder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/** The Proposal. */
public class Proposal extends BftMessage<ProposalPayload> {

  private static final IbftExtraDataCodec BFT_EXTRA_DATA_ENCODER = new IbftExtraDataCodec();
  private final Block proposedBlock;
  private final Optional<BlockAccessList> blockAccessList;

  private final Optional<RoundChangeCertificate> roundChangeCertificate;
  private final boolean useLegacyEncoding;

  /**
   * Instantiates a new Proposal using current (26.1.0+) encoding.
   *
   * @param payload the payload
   * @param proposedBlock the proposed block
   * @param blockAccessList the block access list
   * @param certificate the certificate
   */
  public Proposal(
      final SignedData<ProposalPayload> payload,
      final Block proposedBlock,
      final Optional<BlockAccessList> blockAccessList,
      final Optional<RoundChangeCertificate> certificate) {
    this(payload, proposedBlock, blockAccessList, certificate, false);
  }

  /**
   * Creates a Proposal that encodes in pre-26.1.0 wire format (BAL slot omitted).
   *
   * @param payload the payload
   * @param proposedBlock the proposed block
   * @param blockAccessList the block access list
   * @param certificate the certificate
   * @return a legacy-encoding Proposal
   */
  public static Proposal withLegacyEncoding(
      final SignedData<ProposalPayload> payload,
      final Block proposedBlock,
      final Optional<BlockAccessList> blockAccessList,
      final Optional<RoundChangeCertificate> certificate) {
    return new Proposal(payload, proposedBlock, blockAccessList, certificate, true);
  }

  private Proposal(
      final SignedData<ProposalPayload> payload,
      final Block proposedBlock,
      final Optional<BlockAccessList> blockAccessList,
      final Optional<RoundChangeCertificate> certificate,
      final boolean useLegacyEncoding) {
    super(payload);
    this.proposedBlock = proposedBlock;
    this.blockAccessList = blockAccessList;
    this.roundChangeCertificate = certificate;
    this.useLegacyEncoding = useLegacyEncoding;
  }

  /**
   * Gets block.
   *
   * @return the block
   */
  public Block getBlock() {
    return proposedBlock;
  }

  /**
   * Gets block access list.
   *
   * @return the block access list
   */
  public Optional<BlockAccessList> getBlockAccessList() {
    return blockAccessList;
  }

  /**
   * Gets digest.
   *
   * @return the digest
   */
  public Hash getDigest() {
    return getPayload().getDigest();
  }

  /**
   * Gets round change certificate.
   *
   * @return the round change certificate
   */
  public Optional<RoundChangeCertificate> getRoundChangeCertificate() {
    return roundChangeCertificate;
  }

  @Override
  public Bytes encode() {
    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    rlpOut.startList();
    getSignedPayload().writeTo(rlpOut);
    proposedBlock.writeTo(rlpOut);
    if (roundChangeCertificate.isPresent()) {
      roundChangeCertificate.get().writeTo(rlpOut);
    } else {
      rlpOut.writeNull();
    }
    if (!useLegacyEncoding) {
      // Current 26.1.0+ format: write BAL or null slot
      blockAccessList.ifPresentOrElse((bal) -> bal.writeTo(rlpOut), rlpOut::writeNull);
    }
    // else: legacy mode — omit BAL entirely (pre-26.1.0 wire format, 3 items)
    rlpOut.endList();
    return rlpOut.encoded();
  }

  /**
   * Decode.
   *
   * @param data the data
   * @return the proposal
   */
  public static Proposal decode(final Bytes data) {
    final RLPInput rlpIn = RLP.input(data);
    rlpIn.enterList();
    final SignedData<ProposalPayload> payload =
        PayloadDeserializers.readSignedProposalPayloadFrom(rlpIn);
    final Block proposedBlock =
        Block.readFrom(rlpIn, BftBlockHeaderFunctions.forCommittedSeal(BFT_EXTRA_DATA_ENCODER));

    final Optional<RoundChangeCertificate> roundChangeCertificate =
        readRoundChangeCertificate(rlpIn);
    final Optional<BlockAccessList> blockAccessList = readBlockAccessList(rlpIn);

    rlpIn.leaveList();
    return new Proposal(payload, proposedBlock, blockAccessList, roundChangeCertificate);
  }

  private static Optional<RoundChangeCertificate> readRoundChangeCertificate(final RLPInput rlpIn) {
    RoundChangeCertificate roundChangeCertificate = null;
    if (!rlpIn.nextIsNull()) {
      roundChangeCertificate = RoundChangeCertificate.readFrom(rlpIn);
    } else {
      rlpIn.skipNext();
    }

    return Optional.ofNullable(roundChangeCertificate);
  }

  private static Optional<BlockAccessList> readBlockAccessList(final RLPInput rlpIn) {
    if (rlpIn.isEndOfCurrentList()) {
      // Backward compatibility: pre-26.1.0 messages do not include blockAccessList
      return Optional.empty();
    }
    if (!rlpIn.nextIsNull()) {
      return Optional.of(BlockAccessListDecoder.decode(rlpIn));
    }
    rlpIn.skipNext();
    return Optional.empty();
  }
}
