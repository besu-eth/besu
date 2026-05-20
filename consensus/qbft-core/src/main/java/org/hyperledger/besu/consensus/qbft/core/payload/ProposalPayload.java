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
package org.hyperledger.besu.consensus.qbft.core.payload;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.qbft.core.messagedata.QbftV1;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCodec;
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListDecoder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;
import java.util.Optional;

import com.google.common.base.MoreObjects;

/** The Proposal payload. */
public class ProposalPayload extends QbftPayload {

  /**
   * Pre-26.1.0 ProposalPayload wire format: [sequence, round, block] (3 items, BAL omitted). The
   * current format adds a BAL slot (4 items). ConsensusRound is encoded as two scalars (sequence +
   * round), not as a sub-list - so the legacy item count is 3, not 2.
   */
  private static final int LEGACY_PROPOSAL_PAYLOAD_ITEM_COUNT = 3;

  private static final int TYPE = QbftV1.PROPOSAL;
  private final ConsensusRoundIdentifier roundIdentifier;
  private final QbftBlock proposedBlock;
  private final QbftBlockCodec blockEncoder;
  private final Optional<BlockAccessList> blockAccessList;
  private final boolean useLegacyEncoding;

  /**
   * Instantiates a new Proposal payload using current (26.1.0+) encoding without a block access
   * list.
   *
   * @param roundIdentifier the round identifier
   * @param proposedBlock the proposed block
   * @param blockEncoder the qbft block encoder
   */
  public ProposalPayload(
      final ConsensusRoundIdentifier roundIdentifier,
      final QbftBlock proposedBlock,
      final QbftBlockCodec blockEncoder) {
    this(roundIdentifier, proposedBlock, blockEncoder, Optional.empty(), false);
  }

  /**
   * Instantiates a new Proposal payload using current (26.1.0+) encoding.
   *
   * @param roundIdentifier the round identifier
   * @param proposedBlock the proposed block
   * @param blockEncoder the qbft block encoder
   * @param blockAccessList the block access list
   */
  public ProposalPayload(
      final ConsensusRoundIdentifier roundIdentifier,
      final QbftBlock proposedBlock,
      final QbftBlockCodec blockEncoder,
      final Optional<BlockAccessList> blockAccessList) {
    this(roundIdentifier, proposedBlock, blockEncoder, blockAccessList, false);
  }

  /**
   * Instantiates a new Proposal payload with explicit encoding mode.
   *
   * @param roundIdentifier the round identifier
   * @param proposedBlock the proposed block
   * @param blockEncoder the qbft block encoder
   * @param blockAccessList the block access list
   * @param useLegacyEncoding when true, omit the blockAccessList slot entirely (pre-26.1.0 2-field
   *     wire format). Pre-26.1.0 peers cannot decode BAL regardless, so BAL is always omitted in
   *     legacy mode. Use false for the current 26.1.0+ format.
   */
  public ProposalPayload(
      final ConsensusRoundIdentifier roundIdentifier,
      final QbftBlock proposedBlock,
      final QbftBlockCodec blockEncoder,
      final Optional<BlockAccessList> blockAccessList,
      final boolean useLegacyEncoding) {
    this.roundIdentifier = roundIdentifier;
    this.proposedBlock = proposedBlock;
    this.blockEncoder = blockEncoder;
    this.blockAccessList = blockAccessList;
    this.useLegacyEncoding = useLegacyEncoding;
  }

  /**
   * Read from rlp input and return proposal payload.
   *
   * @param rlpInput the rlp input
   * @param blockEncoder the qbft block encoder
   * @return the proposal payload
   */
  public static ProposalPayload readFrom(
      final RLPInput rlpInput, final QbftBlockCodec blockEncoder) {
    final int items = rlpInput.enterList();
    final ConsensusRoundIdentifier roundIdentifier = readConsensusRound(rlpInput);
    final QbftBlock proposedBlock = blockEncoder.readFrom(rlpInput);
    // Backward compatibility: pre-26.1.0 ProposalPayload is 3 items [sequence, round, block].
    // Current format is 4 items: [sequence, round, block, BAL-or-null]. The decoded payload's
    // encoding mode must match the wire format so that signature verification (which re-encodes
    // via writeTo to compute hashForSignature) produces the same bytes the sender signed.
    final boolean wasLegacyEncoded = (items == LEGACY_PROPOSAL_PAYLOAD_ITEM_COUNT);
    final Optional<BlockAccessList> blockAccessList =
        wasLegacyEncoded ? Optional.empty() : readBlockAccessList(rlpInput);
    rlpInput.leaveList();

    return new ProposalPayload(
        roundIdentifier, proposedBlock, blockEncoder, blockAccessList, wasLegacyEncoded);
  }

  @Override
  public void writeTo(final RLPOutput rlpOutput) {
    rlpOutput.startList();
    writeConsensusRound(rlpOutput);
    blockEncoder.writeTo(proposedBlock, rlpOutput);
    if (!useLegacyEncoding) {
      // Current 26.1.0+ format: write BAL or null slot
      blockAccessList.ifPresentOrElse((bal) -> bal.writeTo(rlpOutput), rlpOutput::writeNull);
    }
    // else: legacy mode — omit BAL entirely (pre-26.1.0 wire format, 2 fields)
    rlpOutput.endList();
  }

  /**
   * Gets proposed block.
   *
   * @return the proposed block
   */
  public QbftBlock getProposedBlock() {
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

  @Override
  public int getMessageType() {
    return TYPE;
  }

  @Override
  public ConsensusRoundIdentifier getRoundIdentifier() {
    return roundIdentifier;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProposalPayload that = (ProposalPayload) o;
    return Objects.equals(roundIdentifier, that.roundIdentifier)
        && Objects.equals(proposedBlock, that.proposedBlock)
        && Objects.equals(blockAccessList, that.blockAccessList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(roundIdentifier, proposedBlock, blockAccessList);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("roundIdentifier", roundIdentifier)
        .add("proposedBlock", proposedBlock)
        .add("blockAccessList", blockAccessList)
        .toString();
  }

  private static Optional<BlockAccessList> readBlockAccessList(final RLPInput rlpInput) {
    if (rlpInput.isEndOfCurrentList()) {
      // Backward compatibility: pre-26.1.0 messages do not include blockAccessList
      return Optional.empty();
    }
    if (!rlpInput.nextIsNull()) {
      return Optional.of(BlockAccessListDecoder.decode(rlpInput));
    }
    rlpInput.skipNext();
    return Optional.empty();
  }
}
