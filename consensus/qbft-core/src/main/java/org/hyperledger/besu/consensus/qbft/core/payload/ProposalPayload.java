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

  private static final int TYPE = QbftV1.PROPOSAL;
  private final ConsensusRoundIdentifier roundIdentifier;
  private final QbftBlock proposedBlock;
  private final QbftBlockCodec blockEncoder;
  private final Optional<BlockAccessList> blockAccessList;

  /**
   * Instantiates a new Proposal payload.
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
    this.roundIdentifier = roundIdentifier;
    this.proposedBlock = proposedBlock;
    this.blockEncoder = blockEncoder;
    this.blockAccessList = blockAccessList;
  }

  /**
   * Instantiates a new Proposal payload.
   *
   * @param roundIdentifier the round identifier
   * @param proposedBlock the proposed block
   * @param blockEncoder the qbft block encoder
   */
  public ProposalPayload(
      final ConsensusRoundIdentifier roundIdentifier,
      final QbftBlock proposedBlock,
      final QbftBlockCodec blockEncoder) {
    this(roundIdentifier, proposedBlock, blockEncoder, Optional.empty());
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
    rlpInput.enterList();
    final ConsensusRoundIdentifier roundIdentifier = readConsensusRound(rlpInput);
    final QbftBlock proposedBlock = blockEncoder.readFrom(rlpInput);
    final Optional<BlockAccessList> blockAccessList = readBlockAccessList(rlpInput);
    rlpInput.leaveList();

    return new ProposalPayload(roundIdentifier, proposedBlock, blockEncoder, blockAccessList);
  }

  @Override
  public void writeTo(final RLPOutput rlpOutput) {
    rlpOutput.startList();
    writeConsensusRound(rlpOutput);
    blockEncoder.writeTo(proposedBlock, rlpOutput);
    blockAccessList.ifPresentOrElse((bal) -> bal.writeTo(rlpOutput), rlpOutput::writeNull);
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
