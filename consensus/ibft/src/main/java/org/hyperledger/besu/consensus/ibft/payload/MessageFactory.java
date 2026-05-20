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
package org.hyperledger.besu.consensus.ibft.payload;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.payload.Payload;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Commit;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Prepare;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Proposal;
import org.hyperledger.besu.consensus.ibft.messagewrappers.RoundChange;
import org.hyperledger.besu.consensus.ibft.statemachine.PreparedRoundArtifacts;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;

/** The Message factory. */
public class MessageFactory {

  private final NodeKey nodeKey;
  private final boolean useLegacyEncoding;

  /**
   * Instantiates a new Message factory using current (26.1.0+) encoding.
   *
   * @param nodeKey the node key
   */
  public MessageFactory(final NodeKey nodeKey) {
    this(nodeKey, false);
  }

  /**
   * Instantiates a new Message factory.
   *
   * @param nodeKey the node key
   * @param useLegacyEncoding when true, the encoder omits the BAL slot entirely (pre-26.1.0 wire
   *     format). Use false for the current 26.1.0+ format.
   */
  public MessageFactory(final NodeKey nodeKey, final boolean useLegacyEncoding) {
    this.nodeKey = nodeKey;
    this.useLegacyEncoding = useLegacyEncoding;
  }

  /**
   * Creates a MessageFactory that encodes messages in pre-26.1.0 wire format (BAL slot omitted).
   *
   * @param nodeKey the node key
   * @return a legacy-encoding MessageFactory
   */
  public static MessageFactory withLegacyEncoding(final NodeKey nodeKey) {
    return new MessageFactory(nodeKey, true);
  }

  /**
   * Create proposal.
   *
   * @param roundIdentifier the round identifier
   * @param block the block
   * @param blockAccessList the block access list
   * @param roundChangeCertificate the round change certificate
   * @return the proposal
   */
  public Proposal createProposal(
      final ConsensusRoundIdentifier roundIdentifier,
      final Block block,
      final Optional<BlockAccessList> blockAccessList,
      final Optional<RoundChangeCertificate> roundChangeCertificate) {

    final ProposalPayload payload = new ProposalPayload(roundIdentifier, block.getHash());

    return new Proposal(
        createSignedMessage(payload),
        block,
        blockAccessList,
        roundChangeCertificate,
        useLegacyEncoding);
  }

  /**
   * Create proposal.
   *
   * @param roundIdentifier the round identifier
   * @param block the block
   * @param roundChangeCertificate the round change certificate
   * @return the proposal
   */
  public Proposal createProposal(
      final ConsensusRoundIdentifier roundIdentifier,
      final Block block,
      final Optional<RoundChangeCertificate> roundChangeCertificate) {
    return createProposal(roundIdentifier, block, Optional.empty(), roundChangeCertificate);
  }

  /**
   * Create prepare.
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
   * Create commit.
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
   * Create round change.
   *
   * @param roundIdentifier the round identifier
   * @param preparedRoundArtifacts the prepared round artifacts
   * @return the round change
   */
  public RoundChange createRoundChange(
      final ConsensusRoundIdentifier roundIdentifier,
      final Optional<PreparedRoundArtifacts> preparedRoundArtifacts) {

    final RoundChangePayload payload =
        new RoundChangePayload(
            roundIdentifier,
            preparedRoundArtifacts.map(PreparedRoundArtifacts::getPreparedCertificate));
    return new RoundChange(
        createSignedMessage(payload),
        preparedRoundArtifacts.map(PreparedRoundArtifacts::getBlock),
        preparedRoundArtifacts.flatMap(PreparedRoundArtifacts::getBlockAccessList),
        useLegacyEncoding);
  }

  private <M extends Payload> SignedData<M> createSignedMessage(final M payload) {
    final SECPSignature signature =
        nodeKey.sign(Bytes32.wrap(payload.hashForSignature().getBytes()));
    return SignedData.create(payload, signature);
  }
}
