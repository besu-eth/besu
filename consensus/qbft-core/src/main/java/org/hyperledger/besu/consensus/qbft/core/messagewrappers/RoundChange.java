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
import org.hyperledger.besu.consensus.qbft.core.payload.PreparedRoundMetadata;
import org.hyperledger.besu.consensus.qbft.core.payload.RoundChangePayload;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCodec;
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListDecoder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/** The Round change payload message. */
public class RoundChange extends BftMessage<RoundChangePayload> {

  /**
   * Pre-26.1.0 RoundChange wire format: [SignedPayload, Block, Prepares]. The current 4-item format
   * adds a BAL slot before Prepares.
   */
  private static final int LEGACY_ROUND_CHANGE_ITEM_COUNT = 3;

  private final Optional<QbftBlock> proposedBlock;
  private final Optional<BlockAccessList> blockAccessList;
  private final QbftBlockCodec blockEncoder;
  private final List<SignedData<PreparePayload>> prepares;
  private final boolean useLegacyEncoding;

  /**
   * Instantiates a new Round change.
   *
   * @param payload the payload
   * @param proposedBlock the proposed block
   * @param blockAccessList the block access list
   * @param blockEncoder the qbft block encoder
   * @param prepares the prepares
   */
  public RoundChange(
      final SignedData<RoundChangePayload> payload,
      final Optional<QbftBlock> proposedBlock,
      final Optional<BlockAccessList> blockAccessList,
      final QbftBlockCodec blockEncoder,
      final List<SignedData<PreparePayload>> prepares) {
    this(payload, proposedBlock, blockAccessList, blockEncoder, prepares, false);
  }

  /**
   * Creates a RoundChange that encodes in pre-26.1.0 wire format (BAL slot omitted).
   *
   * @param payload the payload
   * @param proposedBlock the proposed block
   * @param blockAccessList the block access list
   * @param blockEncoder the qbft block encoder
   * @param prepares the prepares
   * @return a legacy-encoding RoundChange
   */
  public static RoundChange withLegacyEncoding(
      final SignedData<RoundChangePayload> payload,
      final Optional<QbftBlock> proposedBlock,
      final Optional<BlockAccessList> blockAccessList,
      final QbftBlockCodec blockEncoder,
      final List<SignedData<PreparePayload>> prepares) {
    return new RoundChange(payload, proposedBlock, blockAccessList, blockEncoder, prepares, true);
  }

  private RoundChange(
      final SignedData<RoundChangePayload> payload,
      final Optional<QbftBlock> proposedBlock,
      final Optional<BlockAccessList> blockAccessList,
      final QbftBlockCodec blockEncoder,
      final List<SignedData<PreparePayload>> prepares,
      final boolean useLegacyEncoding) {
    super(payload);
    this.proposedBlock = proposedBlock;
    this.blockAccessList = blockAccessList;
    this.blockEncoder = blockEncoder;
    this.prepares = prepares;
    this.useLegacyEncoding = useLegacyEncoding;
  }

  /**
   * Gets proposed block.
   *
   * @return the proposed block
   */
  public Optional<QbftBlock> getProposedBlock() {
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
   * Gets list of Prepare payload signed data.
   *
   * @return the prepares
   */
  public List<SignedData<PreparePayload>> getPrepares() {
    return prepares;
  }

  /**
   * Gets prepared round metadata.
   *
   * @return the prepared round metadata
   */
  public Optional<PreparedRoundMetadata> getPreparedRoundMetadata() {
    return getPayload().getPreparedRoundMetadata();
  }

  /**
   * Gets prepared round.
   *
   * @return the prepared round
   */
  public Optional<Integer> getPreparedRound() {
    return getPayload().getPreparedRoundMetadata().map(PreparedRoundMetadata::getPreparedRound);
  }

  @Override
  public Bytes encode() {
    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    rlpOut.startList();
    getSignedPayload().writeTo(rlpOut);
    proposedBlock.ifPresentOrElse(pb -> blockEncoder.writeTo(pb, rlpOut), rlpOut::writeEmptyList);
    if (!useLegacyEncoding) {
      // Current 26.1.0+ format: write BAL or null slot
      blockAccessList.ifPresentOrElse((bal) -> bal.writeTo(rlpOut), rlpOut::writeNull);
    }
    // else: legacy mode — omit BAL entirely (pre-26.1.0 wire format, 3 items)
    rlpOut.writeList(prepares, SignedData::writeTo);
    rlpOut.endList();
    return rlpOut.encoded();
  }

  /**
   * Decode.
   *
   * @param data the data
   * @param blockEncoder the qbft block encoder
   * @return the round change
   */
  public static RoundChange decode(final Bytes data, final QbftBlockCodec blockEncoder) {

    final RLPInput rlpIn = RLP.input(data);
    final int items = rlpIn.enterList();
    final SignedData<RoundChangePayload> payload = readPayload(rlpIn, RoundChangePayload::readFrom);

    final Optional<QbftBlock> block;
    if (rlpIn.nextIsList() && rlpIn.nextSize() == 0) {
      rlpIn.skipNext();
      block = Optional.empty();
    } else {
      block = Optional.of(blockEncoder.readFrom(rlpIn));
    }

    // Backward compatibility: pre-26.1.0 RoundChange has 3 items, [SignedPayload, Block, Prepares].
    // Current format has 4 items: [SignedPayload, Block, BAL-or-null, Prepares]. Because BAL sits
    // BEFORE Prepares, isEndOfCurrentList() inside readBlockAccessList alone cannot detect the
    // legacy shape (Prepares is still pending). Use the item count from enterList() to
    // disambiguate.
    final Optional<BlockAccessList> blockAccessList =
        (items == LEGACY_ROUND_CHANGE_ITEM_COUNT) ? Optional.empty() : readBlockAccessList(rlpIn);

    final List<SignedData<PreparePayload>> prepares =
        rlpIn.readList(r -> readPayload(r, PreparePayload::readFrom));
    rlpIn.leaveList();

    return new RoundChange(payload, block, blockAccessList, blockEncoder, prepares);
  }

  private static Optional<BlockAccessList> readBlockAccessList(final RLPInput rlpIn) {
    if (rlpIn.isEndOfCurrentList()) {
      return Optional.empty();
    }
    if (!rlpIn.nextIsNull()) {
      return Optional.of(BlockAccessListDecoder.decode(rlpIn));
    }
    rlpIn.skipNext();
    return Optional.empty();
  }
}
