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
import org.hyperledger.besu.consensus.common.bft.payload.Payload;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.Commit;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.Prepare;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.Proposal;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.RoundChange;
import org.hyperledger.besu.consensus.qbft.core.statemachine.PreparedCertificate;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCodec;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;

/** The Message factory. */
public class MessageFactory {

  private final NodeKey nodeKey;
  private final QbftBlockCodec blockEncoder;
  private final boolean useLegacyEncoding;

  /**
   * Instantiates a new Message factory using current (26.1.0+) encoding.
   *
   * @param nodeKey the node key
   * @param blockEncoder the block encoder
   */
  public MessageFactory(final NodeKey nodeKey, final QbftBlockCodec blockEncoder) {
    this(nodeKey, blockEncoder, false);
  }

  /**
   * Instantiates a new Message factory.
   *
   * @param nodeKey the node key
   * @param blockEncoder the block encoder
   * @param useLegacyEncoding when true, the encoder omits the BAL slot entirely (pre-26.1.0 wire
   *     format). Use false for the current 26.1.0+ format.
   */
  public MessageFactory(
      final NodeKey nodeKey, final QbftBlockCodec blockEncoder, final boolean useLegacyEncoding) {
    this.nodeKey = nodeKey;
    this.blockEncoder = blockEncoder;
    this.useLegacyEncoding = useLegacyEncoding;
  }

  /**
   * Creates a MessageFactory that encodes messages in pre-26.1.0 wire format (BAL slot omitted).
   *
   * @param nodeKey the node key
   * @param blockEncoder the block encoder
   * @return a legacy-encoding MessageFactory
   */
  public static MessageFactory withLegacyEncoding(
      final NodeKey nodeKey, final QbftBlockCodec blockEncoder) {
    return new MessageFactory(nodeKey, blockEncoder, true);
  }

  /**
   * Create proposal.
   *
   * @param roundIdentifier the round identifier
   * @param block the block
   * @param blockAccessList the block access list
   * @param roundChanges the round changes
   * @param prepares the prepares
   * @return the proposal
   */
  public Proposal createProposal(
      final ConsensusRoundIdentifier roundIdentifier,
      final QbftBlock block,
      final Optional<BlockAccessList> blockAccessList,
      final List<SignedData<RoundChangePayload>> roundChanges,
      final List<SignedData<PreparePayload>> prepares) {

    final ProposalPayload payload =
        new ProposalPayload(
            roundIdentifier, block, blockEncoder, blockAccessList, useLegacyEncoding);

    return new Proposal(createSignedMessage(payload), roundChanges, prepares);
  }

  /**
   * Create proposal.
   *
   * @param roundIdentifier the round identifier
   * @param block the block
   * @param roundChanges the round changes
   * @param prepares the prepares
   * @return the proposal
   */
  public Proposal createProposal(
      final ConsensusRoundIdentifier roundIdentifier,
      final QbftBlock block,
      final List<SignedData<RoundChangePayload>> roundChanges,
      final List<SignedData<PreparePayload>> prepares) {
    return createProposal(roundIdentifier, block, Optional.empty(), roundChanges, prepares);
  }

  /**
   * Create Prepare payload.
   *
   * @param roundIdentifier the round identifier
   * @param digest the digest
   * @return the prepare
   */
  public Prepare createPrepare(final ConsensusRoundIdentifier roundIdentifier, final Hash digest) {
    final PreparePayload payload = new PreparePayload(roundIdentifier, digest);
    return new Prepare(createSignedMessage(payload));
  }

  /**
   * Create commit payload.
   *
   * @param roundIdentifier the round identifier
   * @param digest the digest
   * @param commitSeal the commit seal
   * @return the commit
   */
  public Commit createCommit(
      final ConsensusRoundIdentifier roundIdentifier,
      final Hash digest,
      final SECPSignature commitSeal) {
    final CommitPayload payload = new CommitPayload(roundIdentifier, digest, commitSeal);
    return new Commit(createSignedMessage(payload));
  }

  /**
   * Create round change payload.
   *
   * @param roundIdentifier the round identifier
   * @param preparedRoundData the prepared round data
   * @return the round change
   */
  public RoundChange createRoundChange(
      final ConsensusRoundIdentifier roundIdentifier,
      final Optional<PreparedCertificate> preparedRoundData) {

    final RoundChangePayload payload;
    if (preparedRoundData.isPresent()) {

      final QbftBlock preparedBlock = preparedRoundData.get().getBlock();
      payload =
          new RoundChangePayload(
              roundIdentifier,
              Optional.of(
                  new PreparedRoundMetadata(
                      preparedBlock.getHash(), preparedRoundData.get().getRound())));

      return new RoundChange(
          createSignedMessage(payload),
          Optional.of(preparedBlock),
          preparedRoundData.get().getBlockAccessList(),
          blockEncoder,
          preparedRoundData.get().getPrepares(),
          useLegacyEncoding);

    } else {
      payload = new RoundChangePayload(roundIdentifier, Optional.empty());
      return new RoundChange(
          createSignedMessage(payload),
          Optional.empty(),
          Optional.empty(),
          blockEncoder,
          Collections.emptyList(),
          useLegacyEncoding);
    }
  }

  private <M extends Payload> SignedData<M> createSignedMessage(final M payload) {
    final SECPSignature signature =
        nodeKey.sign(Bytes32.wrap(payload.hashForSignature().getBytes()));
    return SignedData.create(payload, signature);
  }
}
