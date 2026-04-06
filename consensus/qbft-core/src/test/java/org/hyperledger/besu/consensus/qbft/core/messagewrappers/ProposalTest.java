/*
 * Copyright 2020 ConsenSys AG.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.qbft.core.QbftBlockTestFixture;
import org.hyperledger.besu.consensus.qbft.core.messagedata.QbftV1;
import org.hyperledger.besu.consensus.qbft.core.payload.PreparePayload;
import org.hyperledger.besu.consensus.qbft.core.payload.PreparedRoundMetadata;
import org.hyperledger.besu.consensus.qbft.core.payload.ProposalPayload;
import org.hyperledger.besu.consensus.qbft.core.payload.RoundChangePayload;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCodec;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ProposalTest {
  @Mock private QbftBlockCodec blockEncoder;

  private static final QbftBlock BLOCK = new QbftBlockTestFixture().build();

  @Test
  public void canRoundTripProposalMessage() {
    testRoundTripProposal(Optional.empty());
  }

  @Test
  public void canRoundTripProposalMessageWithBlockAccessList() {
    testRoundTripProposal(Optional.of(BlockAccessList.builder().build()));
  }

  private void testRoundTripProposal(final Optional<BlockAccessList> blockAccessList) {
    when(blockEncoder.readFrom(any())).thenReturn(BLOCK);

    final NodeKey nodeKey = NodeKeyUtils.generate();
    final Address addr = Util.publicKeyToAddress(nodeKey.getPublicKey());

    final ProposalPayload payload = createProposalPayload(blockAccessList);
    final SignedData<ProposalPayload> signedPayload =
        SignedData.create(
            payload, nodeKey.sign(Bytes32.wrap(payload.hashForSignature().getBytes())));

    final SignedData<PreparePayload> prepare = createPrepare(nodeKey);
    final SignedData<RoundChangePayload> roundChange = createRoundChange(nodeKey);

    final Proposal proposal = new Proposal(signedPayload, List.of(roundChange), List.of(prepare));
    final Proposal decodedProposal = Proposal.decode(proposal.encode(), blockEncoder);

    assertProposal(decodedProposal, addr, prepare, roundChange, payload, blockAccessList);
  }

  private ProposalPayload createProposalPayload(final Optional<BlockAccessList> blockAccessList) {
    if (blockAccessList.isPresent()) {
      return new ProposalPayload(
          new ConsensusRoundIdentifier(1, 1), BLOCK, blockEncoder, blockAccessList);
    }
    return new ProposalPayload(new ConsensusRoundIdentifier(1, 1), BLOCK, blockEncoder);
  }

  private SignedData<PreparePayload> createPrepare(final NodeKey nodeKey) {
    final PreparePayload preparePayload =
        new PreparePayload(new ConsensusRoundIdentifier(1, 0), BLOCK.getHash());
    return SignedData.create(
        preparePayload, nodeKey.sign(Bytes32.wrap(preparePayload.hashForSignature().getBytes())));
  }

  private SignedData<RoundChangePayload> createRoundChange(final NodeKey nodeKey) {
    final RoundChangePayload roundChangePayload =
        new RoundChangePayload(
            new ConsensusRoundIdentifier(1, 0),
            Optional.of(new PreparedRoundMetadata(BLOCK.getHash(), 0)));
    return SignedData.create(
        roundChangePayload,
        nodeKey.sign(Bytes32.wrap(roundChangePayload.hashForSignature().getBytes())));
  }

  @Test
  public void canDecodeProposalMessageFromLegacyNodeWithoutBlockAccessList() {
    // Simulate a pre-26.1.0 validator that encodes ProposalPayload WITHOUT the blockAccessList
    // field.
    // Old format payload list: [seqNum, roundNum, block]       (3 items)
    // New format payload list: [seqNum, roundNum, block, null] (4 items)

    // The mock must consume the block bytes written by the legacy writeTo override, otherwise the
    // RLP cursor remains on the block hash and readBlockAccessList sees it instead of end-of-list.
    when(blockEncoder.readFrom(any()))
        .thenAnswer(
            inv -> {
              inv.<RLPInput>getArgument(0).skipNext();
              return BLOCK;
            });

    final NodeKey nodeKey = NodeKeyUtils.generate();
    final ConsensusRoundIdentifier roundIdentifier = new ConsensusRoundIdentifier(1, 1);

    // ProposalPayload subclass that overrides writeTo() to omit the blockAccessList field,
    // reproducing the pre-26.1.0 wire format
    final ProposalPayload oldFormatPayload =
        new ProposalPayload(roundIdentifier, BLOCK, blockEncoder) {
          @Override
          public void writeTo(final RLPOutput output) {
            output.startList();
            writeConsensusRound(output);
            output.writeBytes(BLOCK.getHash().getBytes());
            // No blockAccessList field — simulates pre-26.1.0 encoding
            output.endList();
          }
        };

    final SignedData<ProposalPayload> signedPayload =
        SignedData.create(
            oldFormatPayload,
            nodeKey.sign(Bytes32.wrap(oldFormatPayload.hashForSignature().getBytes())));

    final Proposal proposal =
        Proposal.decode(new Proposal(signedPayload, List.of(), List.of()).encode(), blockEncoder);

    assertThat(proposal.getBlockAccessList()).isEmpty();
    assertThat(proposal.getSignedPayload().getPayload().getRoundIdentifier())
        .isEqualTo(roundIdentifier);
    assertThat(proposal.getSignedPayload().getPayload().getProposedBlock().getHash())
        .isEqualTo(BLOCK.getHash());
  }

  private void assertProposal(
      final Proposal decodedProposal,
      final Address expectedAddr,
      final SignedData<PreparePayload> expectedPrepare,
      final SignedData<RoundChangePayload> expectedRoundChange,
      final ProposalPayload originalPayload,
      final Optional<BlockAccessList> expectedBlockAccessList) {

    assertThat(decodedProposal.getAuthor()).isEqualTo(expectedAddr);
    assertThat(decodedProposal.getMessageType()).isEqualTo(QbftV1.PROPOSAL);
    assertThat(decodedProposal.getPrepares()).hasSize(1);
    assertThat(decodedProposal.getPrepares().getFirst())
        .isEqualToComparingFieldByField(expectedPrepare);
    assertThat(decodedProposal.getRoundChanges()).hasSize(1);
    assertThat(decodedProposal.getRoundChanges().getFirst())
        .isEqualToComparingFieldByField(expectedRoundChange);
    assertThat(decodedProposal.getSignedPayload().getPayload().getProposedBlock()).isEqualTo(BLOCK);
    assertThat(decodedProposal.getSignedPayload().getPayload().getRoundIdentifier())
        .isEqualTo(originalPayload.getRoundIdentifier());

    if (expectedBlockAccessList.isPresent()) {
      assertThat(decodedProposal.getBlockAccessList()).isPresent();
      assertThat(decodedProposal.getSignedPayload().getPayload().getBlockAccessList()).isPresent();
    }
  }
}
