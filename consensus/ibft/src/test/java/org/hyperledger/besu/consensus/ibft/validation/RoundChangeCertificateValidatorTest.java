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
package org.hyperledger.besu.consensus.ibft.validation;

import static java.util.Collections.singletonList;
import static java.util.Optional.empty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.BftBlockInterface;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundHelpers;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.ProposedBlockHelpers;
import org.hyperledger.besu.consensus.ibft.IbftExtraDataCodec;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Proposal;
import org.hyperledger.besu.consensus.ibft.payload.MessageFactory;
import org.hyperledger.besu.consensus.ibft.payload.PreparedCertificate;
import org.hyperledger.besu.consensus.ibft.payload.RoundChangeCertificate;
import org.hyperledger.besu.consensus.ibft.statemachine.PreparedRoundArtifacts;
import org.hyperledger.besu.consensus.ibft.validation.RoundChangePayloadValidator.MessageValidatorForHeightFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.Util;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("DirectInvocationOnMock")
public class RoundChangeCertificateValidatorTest {

  private final NodeKey proposerKey = NodeKeyUtils.generate();
  private final NodeKey validatorKey = NodeKeyUtils.generate();
  private final NodeKey otherValidatorKey = NodeKeyUtils.generate();
  private final MessageFactory proposerMessageFactory = new MessageFactory(proposerKey);
  private final MessageFactory validatorMessageFactory = new MessageFactory(validatorKey);
  private final List<Address> validators = Lists.newArrayList();
  private final long chainHeight = 2;
  private final ConsensusRoundIdentifier roundIdentifier =
      new ConsensusRoundIdentifier(chainHeight, 4);
  private RoundChangeCertificateValidator validator;

  private final MessageValidatorForHeightFactory validatorFactory =
      mock(MessageValidatorForHeightFactory.class);
  final IbftExtraDataCodec bftExtraDataEncoder = new IbftExtraDataCodec();
  final BftBlockInterface bftBlockInterface = new BftBlockInterface(bftExtraDataEncoder);

  private Block proposedBlock;

  @BeforeEach
  public void setup() {
    validators.add(Util.publicKeyToAddress(proposerKey.getPublicKey()));
    validators.add(Util.publicKeyToAddress(validatorKey.getPublicKey()));
    validators.add(Util.publicKeyToAddress(otherValidatorKey.getPublicKey()));

    proposedBlock =
        ProposedBlockHelpers.createProposalBlock(validators, roundIdentifier, bftExtraDataEncoder);

    validator =
        new RoundChangeCertificateValidator(
            validators, validatorFactory, 5, bftExtraDataEncoder, bftBlockInterface);
  }

  @Test
  public void proposalWithEmptyRoundChangeCertificateFails() {
    final RoundChangeCertificate cert = new RoundChangeCertificate(Collections.emptyList());

    assertThat(
            validator.validateRoundChangeMessagesAndEnsureTargetRoundMatchesRoot(
                roundIdentifier, cert))
        .isFalse();
  }

  @Test
  public void roundChangeMessagesDoNotAllTargetRoundFails() {
    final ConsensusRoundIdentifier prevRound =
        ConsensusRoundHelpers.createFrom(roundIdentifier, 0, -1);

    final RoundChangeCertificate.Builder roundChangeBuilder = new RoundChangeCertificate.Builder();
    roundChangeBuilder.appendRoundChangeMessage(
        proposerMessageFactory.createRoundChange(roundIdentifier, empty()));
    roundChangeBuilder.appendRoundChangeMessage(
        proposerMessageFactory.createRoundChange(prevRound, empty()));

    assertThat(
            validator.validateRoundChangeMessagesAndEnsureTargetRoundMatchesRoot(
                roundIdentifier, roundChangeBuilder.buildCertificate()))
        .isFalse();
  }

  @Test
  public void invalidPrepareMessageInOnePrepareCertificateFails() {
    final ConsensusRoundIdentifier prevRound =
        ConsensusRoundHelpers.createFrom(roundIdentifier, 0, -1);

    final RoundChangeCertificate.Builder roundChangeBuilder = new RoundChangeCertificate.Builder();
    roundChangeBuilder.appendRoundChangeMessage(
        proposerMessageFactory.createRoundChange(
            roundIdentifier,
            Optional.of(
                new PreparedRoundArtifacts(
                    proposerMessageFactory.createProposal(prevRound, proposedBlock, empty()),
                    Lists.newArrayList(
                        validatorMessageFactory.createPrepare(
                            prevRound, proposedBlock.getHash()))))));

    assertThat(
            validator.validateRoundChangeMessagesAndEnsureTargetRoundMatchesRoot(
                roundIdentifier, roundChangeBuilder.buildCertificate()))
        .isFalse();
  }

  @Test
  public void detectsTheSuppliedBlockIsNotInLatestPrepareCertificate() {
    final ConsensusRoundIdentifier preparedRound =
        ConsensusRoundHelpers.createFrom(roundIdentifier, 0, -1);
    // The previous proposedBlock has been constructed with less validators, so is thus not
    // identical
    // to the proposedBlock in the new proposal (so should fail).
    final Block prevProposedBlock =
        ProposedBlockHelpers.createProposalBlock(
            validators.subList(0, 1), preparedRound, bftExtraDataEncoder);

    final PreparedRoundArtifacts mismatchedRoundArtefacts =
        new PreparedRoundArtifacts(
            proposerMessageFactory.createProposal(preparedRound, prevProposedBlock, empty()),
            singletonList(
                validatorMessageFactory.createPrepare(preparedRound, prevProposedBlock.getHash())));

    final RoundChangeCertificate roundChangeCert =
        new RoundChangeCertificate(
            singletonList(
                validatorMessageFactory
                    .createRoundChange(roundIdentifier, Optional.of(mismatchedRoundArtefacts))
                    .getSignedPayload()));

    assertThat(
            validator.validateProposalMessageMatchesLatestPrepareCertificate(
                roundChangeCert, proposedBlock))
        .isFalse();
  }

  @Test
  public void correctlyMatchesBlockAgainstLatestInRoundChangeCertificate() {
    final ConsensusRoundIdentifier latterPrepareRound =
        ConsensusRoundHelpers.createFrom(roundIdentifier, 0, -1);
    final Block latterBlock =
        ProposedBlockHelpers.createProposalBlock(
            validators, latterPrepareRound, bftExtraDataEncoder);
    final Proposal latterProposal =
        proposerMessageFactory.createProposal(latterPrepareRound, latterBlock, empty());
    final Optional<PreparedRoundArtifacts> latterTerminatedRoundArtefacts =
        Optional.of(
            new PreparedRoundArtifacts(
                latterProposal,
                org.assertj.core.util.Lists.newArrayList(
                    validatorMessageFactory.createPrepare(
                        latterPrepareRound, proposedBlock.getHash()))));

    // An earlier PrepareCert is added to ensure the path to find the latest PrepareCert
    // is correctly followed.
    final ConsensusRoundIdentifier earlierPreparedRound =
        new ConsensusRoundIdentifier(
            roundIdentifier.getSequenceNumber(), roundIdentifier.getRoundNumber() - 2);
    final Block earlierBlock =
        ProposedBlockHelpers.createProposalBlock(
            validators.subList(0, 1), earlierPreparedRound, bftExtraDataEncoder);
    final Proposal earlierProposal =
        proposerMessageFactory.createProposal(earlierPreparedRound, earlierBlock, empty());
    final Optional<PreparedRoundArtifacts> earlierTerminatedRoundArtefacts =
        Optional.of(
            new PreparedRoundArtifacts(
                earlierProposal,
                org.assertj.core.util.Lists.newArrayList(
                    validatorMessageFactory.createPrepare(
                        earlierPreparedRound, earlierBlock.getHash()))));

    final RoundChangeCertificate roundChangeCert =
        new RoundChangeCertificate(
            org.assertj.core.util.Lists.newArrayList(
                proposerMessageFactory
                    .createRoundChange(roundIdentifier, earlierTerminatedRoundArtefacts)
                    .getSignedPayload(),
                validatorMessageFactory
                    .createRoundChange(roundIdentifier, latterTerminatedRoundArtefacts)
                    .getSignedPayload()));

    assertThat(
            validator.validateProposalMessageMatchesLatestPrepareCertificate(
                roundChangeCert, earlierBlock))
        .isFalse();

    assertThat(
            validator.validateProposalMessageMatchesLatestPrepareCertificate(
                roundChangeCert, latterBlock))
        .isTrue();
  }

  @Test
  public void roundChangeCertificateWithTwoRoundChangesFromTheSameAuthorFailsValidation() {

    final RoundChangeCertificate roundChangeCert =
        new RoundChangeCertificate(
            org.assertj.core.util.Lists.newArrayList(
                proposerMessageFactory
                    .createRoundChange(roundIdentifier, empty())
                    .getSignedPayload(),
                proposerMessageFactory
                    .createRoundChange(roundIdentifier, empty())
                    .getSignedPayload()));

    assertThat(
            validator.validateRoundChangeMessagesAndEnsureTargetRoundMatchesRoot(
                roundIdentifier, roundChangeCert))
        .isFalse();
  }

  @Test
  public void latestPreparedCertificateIsExtractedFromRoundChangeCertificate() {
    // NOTE: This function does not validate that all RoundCHanges/Prepares etc. come from valid
    // sources, it is only responsible for determine which of the list or RoundChange messages
    // contains the newest
    // NOTE: This capability is tested as part of the NewRoundMessageValidationTests.
    final NodeKey proposerKey = NodeKeyUtils.generate();
    final MessageFactory proposerMessageFactory = new MessageFactory(proposerKey);
    final Block proposedBlock = mock(Block.class);
    when(proposedBlock.getHash()).thenReturn(Hash.fromHexStringLenient("1"));
    final ConsensusRoundIdentifier roundIdentifier = new ConsensusRoundIdentifier(1, 4);

    final ConsensusRoundIdentifier preparedRound =
        ConsensusRoundHelpers.createFrom(roundIdentifier, 0, -1);
    final Proposal differentProposal =
        proposerMessageFactory.createProposal(preparedRound, proposedBlock, Optional.empty());

    final Optional<PreparedRoundArtifacts> latterPreparedRoundArtifacts =
        Optional.of(
            new PreparedRoundArtifacts(
                differentProposal,
                Lists.newArrayList(
                    proposerMessageFactory.createPrepare(roundIdentifier, proposedBlock.getHash()),
                    proposerMessageFactory.createPrepare(
                        roundIdentifier, proposedBlock.getHash()))));

    // An earlier PrepareCert is added to ensure the path to find the latest PrepareCert
    // is correctly followed.
    final ConsensusRoundIdentifier earlierPreparedRound =
        ConsensusRoundHelpers.createFrom(roundIdentifier, 0, -2);
    final Proposal earlierProposal =
        proposerMessageFactory.createProposal(
            earlierPreparedRound, proposedBlock, Optional.empty());
    final Optional<PreparedRoundArtifacts> earlierPreparedRoundArtifacts =
        Optional.of(
            new PreparedRoundArtifacts(
                earlierProposal,
                Lists.newArrayList(
                    proposerMessageFactory.createPrepare(
                        earlierPreparedRound, proposedBlock.getHash()),
                    proposerMessageFactory.createPrepare(
                        earlierPreparedRound, proposedBlock.getHash()))));

    final Optional<PreparedCertificate> newestCert =
        RoundChangeCertificateValidator.findLatestPreparedCertificate(
            Lists.newArrayList(
                proposerMessageFactory
                    .createRoundChange(roundIdentifier, earlierPreparedRoundArtifacts)
                    .getSignedPayload(),
                proposerMessageFactory
                    .createRoundChange(roundIdentifier, latterPreparedRoundArtifacts)
                    .getSignedPayload()));

    assertThat(newestCert.get())
        .isEqualTo(latterPreparedRoundArtifacts.get().getPreparedCertificate());
  }

  @Test
  public void allRoundChangeHaveNoPreparedReturnsEmptyOptional() {
    final NodeKey proposerKey = NodeKeyUtils.generate();
    final MessageFactory proposerMessageFactory = new MessageFactory(proposerKey);
    final ConsensusRoundIdentifier roundIdentifier = new ConsensusRoundIdentifier(1, 4);

    final Optional<PreparedCertificate> newestCert =
        RoundChangeCertificateValidator.findLatestPreparedCertificate(
            Lists.newArrayList(
                proposerMessageFactory
                    .createRoundChange(roundIdentifier, Optional.empty())
                    .getSignedPayload(),
                proposerMessageFactory
                    .createRoundChange(roundIdentifier, Optional.empty())
                    .getSignedPayload()));

    assertThat(newestCert).isEmpty();
  }
}
