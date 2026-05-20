/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.consensus.qbft.core.validation;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundHelpers;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.qbft.core.QbftBlockTestFixture;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.Proposal;
import org.hyperledger.besu.consensus.qbft.core.payload.MessageFactory;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCodec;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockHeader;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockValidator;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.Util;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ProposalPayloadValidatorTest {

  @Mock private QbftBlockValidator blockValidator;
  @Mock private QbftBlockCodec blockEncoder;

  private static final int CHAIN_HEIGHT = 3;
  private final ConsensusRoundIdentifier targetRound =
      new ConsensusRoundIdentifier(CHAIN_HEIGHT, 3);

  private final NodeKey nodeKey = NodeKeyUtils.generate();
  private final Address expectedProposer = Util.publicKeyToAddress(nodeKey.getPublicKey());
  private MessageFactory messageFactory;
  final ConsensusRoundIdentifier roundIdentifier =
      ConsensusRoundHelpers.createFrom(targetRound, 1, 0);

  @BeforeEach
  public void setup() {
    messageFactory = new MessageFactory(nodeKey, blockEncoder);
  }

  @Test
  public void validationPassesWhenProposerAndRoundMatchAndBlockIsValid() {
    final ProposalPayloadValidator payloadValidator =
        new ProposalPayloadValidator(expectedProposer, roundIdentifier, blockValidator);
    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture().number(roundIdentifier.getSequenceNumber()).buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();
    final Proposal proposal =
        messageFactory.createProposal(
            roundIdentifier, block, Optional.empty(), emptyList(), emptyList());

    when(blockValidator.validateBlock(eq(block), eq(Optional.empty())))
        .thenReturn(new QbftBlockValidator.ValidationResult(true, Optional.empty()));

    assertThat(payloadValidator.validate(proposal.getSignedPayload())).isTrue();
  }

  @Test
  public void validationPassesWhenBlockRoundDoesNotMatchProposalRound() {
    final ProposalPayloadValidator payloadValidator =
        new ProposalPayloadValidator(expectedProposer, roundIdentifier, blockValidator);

    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture().number(roundIdentifier.getSequenceNumber()).buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();
    final Proposal proposal =
        messageFactory.createProposal(
            roundIdentifier, block, Optional.empty(), emptyList(), emptyList());

    when(blockValidator.validateBlock(eq(block), eq(Optional.empty())))
        .thenReturn(new QbftBlockValidator.ValidationResult(true, Optional.empty()));

    assertThat(payloadValidator.validate(proposal.getSignedPayload())).isTrue();
  }

  @Test
  public void validationFailsWhenBlockFailsValidation() {
    final ConsensusRoundIdentifier roundIdentifier =
        ConsensusRoundHelpers.createFrom(targetRound, 1, 0);

    final ProposalPayloadValidator payloadValidator =
        new ProposalPayloadValidator(expectedProposer, roundIdentifier, blockValidator);
    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture().number(roundIdentifier.getSequenceNumber()).buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();
    final Proposal proposal =
        messageFactory.createProposal(
            roundIdentifier, block, Optional.empty(), emptyList(), emptyList());

    when(blockValidator.validateBlock(eq(block), eq(Optional.empty())))
        .thenReturn(new QbftBlockValidator.ValidationResult(false, Optional.empty()));

    assertThat(payloadValidator.validate(proposal.getSignedPayload())).isFalse();
  }

  @Test
  public void validationFailsWhenExpectedProposerDoesNotMatchPayloadsAuthor() {
    final ProposalPayloadValidator payloadValidator =
        new ProposalPayloadValidator(Address.fromHexString("0x1"), roundIdentifier, blockValidator);
    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture().number(roundIdentifier.getSequenceNumber()).buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();
    final Proposal proposal =
        messageFactory.createProposal(
            roundIdentifier, block, Optional.empty(), emptyList(), emptyList());

    assertThat(payloadValidator.validate(proposal.getSignedPayload())).isFalse();
    verifyNoMoreInteractions(blockValidator);
  }

  @Test
  public void validationFailsWhenMessageMismatchesExpectedRound() {
    final ProposalPayloadValidator payloadValidator =
        new ProposalPayloadValidator(expectedProposer, roundIdentifier, blockValidator);

    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture().number(roundIdentifier.getSequenceNumber()).buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();
    final Proposal proposal =
        messageFactory.createProposal(
            ConsensusRoundHelpers.createFrom(roundIdentifier, 0, +1),
            block,
            Optional.empty(),
            emptyList(),
            emptyList());

    assertThat(payloadValidator.validate(proposal.getSignedPayload())).isFalse();
    verifyNoMoreInteractions(blockValidator);
  }

  @Test
  public void validationFailsWhenMessageMismatchesExpectedHeight() {
    final ProposalPayloadValidator payloadValidator =
        new ProposalPayloadValidator(expectedProposer, roundIdentifier, blockValidator);

    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture().number(roundIdentifier.getSequenceNumber()).buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();
    final Proposal proposal =
        messageFactory.createProposal(
            ConsensusRoundHelpers.createFrom(roundIdentifier, +1, 0),
            block,
            Optional.empty(),
            emptyList(),
            emptyList());

    assertThat(payloadValidator.validate(proposal.getSignedPayload())).isFalse();
    verifyNoMoreInteractions(blockValidator);
  }

  @Test
  public void validationFailsForBlockWithIncorrectHeight() {
    final ProposalPayloadValidator payloadValidator =
        new ProposalPayloadValidator(expectedProposer, roundIdentifier, blockValidator);
    final QbftBlockHeader header =
        new QbftBlockHeaderTestFixture()
            .number(roundIdentifier.getSequenceNumber() + 1)
            .buildHeader();
    final QbftBlock block = new QbftBlockTestFixture().blockHeader(header).build();
    final Proposal proposal =
        messageFactory.createProposal(
            roundIdentifier, block, Optional.empty(), emptyList(), emptyList());

    when(blockValidator.validateBlock(eq(block), eq(Optional.empty())))
        .thenReturn(new QbftBlockValidator.ValidationResult(true, Optional.empty()));

    assertThat(payloadValidator.validate(proposal.getSignedPayload())).isFalse();
  }
}
