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
package org.hyperledger.besu.consensus.qbft.core.statemachine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.messagewrappers.BftMessage;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.Commit;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.Prepare;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.Proposal;
import org.hyperledger.besu.consensus.qbft.core.payload.MessageFactory;
import org.hyperledger.besu.consensus.qbft.core.payload.PreparePayload;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCodec;
import org.hyperledger.besu.consensus.qbft.core.validation.MessageValidator;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Hash;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RoundStateTest {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  private final List<MessageFactory> validatorMessageFactories = Lists.newArrayList();
  private final ConsensusRoundIdentifier roundIdentifier = new ConsensusRoundIdentifier(1, 1);
  private final Hash blockHash = Hash.fromHexStringLenient("1");

  @Mock private MessageValidator messageValidator;
  @Mock private QbftBlock block;
  @Mock private QbftBlockCodec blockEncoder;

  @BeforeEach
  public void setup() {
    for (int i = 0; i < 3; i++) {
      final NodeKey newNodeKey = NodeKeyUtils.generate();
      validatorMessageFactories.add(new MessageFactory(newNodeKey, blockEncoder));
    }
  }

  @Test
  public void defaultRoundIsNotPreparedOrCommittedAndHasNoPreparedCertificate() {
    final RoundState roundState = new RoundState(roundIdentifier, 1, messageValidator);

    assertThat(roundState.isPrepared()).isFalse();
    assertThat(roundState.isCommitted()).isFalse();
    assertThat(roundState.constructPreparedCertificate()).isEmpty();
  }

  @Test
  public void ifProposalMessageFailsValidationMethodReturnsFalse() {
    when(messageValidator.validateProposal(any())).thenReturn(false);
    final RoundState roundState = new RoundState(roundIdentifier, 1, messageValidator);

    final Proposal proposal =
        validatorMessageFactories
            .getFirst()
            .createProposal(
                roundIdentifier, block, Collections.emptyList(), Collections.emptyList());

    assertThat(roundState.setProposedBlock(proposal)).isFalse();
    assertThat(roundState.isPrepared()).isFalse();
    assertThat(roundState.isCommitted()).isFalse();
    assertThat(roundState.constructPreparedCertificate()).isEmpty();
  }

  @Test
  public void singleValidatorRequiresCommitMessageToBeCommitted() {
    when(messageValidator.validateProposal(any())).thenReturn(true);
    when(messageValidator.validateCommit(any())).thenReturn(true);

    final RoundState roundState = new RoundState(roundIdentifier, 1, messageValidator);

    final Proposal proposal =
        validatorMessageFactories
            .getFirst()
            .createProposal(
                roundIdentifier, block, Collections.emptyList(), Collections.emptyList());

    assertThat(roundState.setProposedBlock(proposal)).isTrue();
    assertThat(roundState.isPrepared()).isFalse();
    assertThat(roundState.isCommitted()).isFalse();

    final Commit commit =
        validatorMessageFactories
            .getFirst()
            .createCommit(
                roundIdentifier,
                blockHash,
                SIGNATURE_ALGORITHM.createSignature(BigInteger.ONE, BigInteger.ONE, (byte) 1));

    roundState.addCommitMessage(commit);
    assertThat(roundState.isPrepared()).isFalse();
    assertThat(roundState.isCommitted()).isTrue();
    assertThat(roundState.constructPreparedCertificate()).isEmpty();
  }

  @Test
  public void prepareMessagesCanBeReceivedPriorToProposal() {
    when(messageValidator.validateProposal(any())).thenReturn(true);
    when(messageValidator.validatePrepare(any())).thenReturn(true);

    final RoundState roundState = new RoundState(roundIdentifier, 3, messageValidator);

    final Prepare firstPrepare =
        validatorMessageFactories.get(1).createPrepare(roundIdentifier, blockHash);

    final Prepare secondPrepare =
        validatorMessageFactories.get(2).createPrepare(roundIdentifier, blockHash);

    final Prepare thirdPrepare =
        validatorMessageFactories.get(0).createPrepare(roundIdentifier, blockHash);

    roundState.addPrepareMessage(firstPrepare);
    assertThat(roundState.isPrepared()).isFalse();
    assertThat(roundState.isCommitted()).isFalse();
    assertThat(roundState.constructPreparedCertificate()).isEmpty();

    roundState.addPrepareMessage(secondPrepare);
    assertThat(roundState.isPrepared()).isFalse();
    assertThat(roundState.isCommitted()).isFalse();
    assertThat(roundState.constructPreparedCertificate()).isEmpty();

    roundState.addPrepareMessage(thirdPrepare);
    assertThat(roundState.isPrepared()).isFalse();
    assertThat(roundState.isCommitted()).isFalse();
    assertThat(roundState.constructPreparedCertificate()).isEmpty();

    final Proposal proposal =
        validatorMessageFactories
            .getFirst()
            .createProposal(
                roundIdentifier, block, Collections.emptyList(), Collections.emptyList());
    assertThat(roundState.setProposedBlock(proposal)).isTrue();
    assertThat(roundState.isPrepared()).isTrue();
    assertThat(roundState.isCommitted()).isFalse();
    assertThat(roundState.constructPreparedCertificate()).isNotEmpty();
  }

  @Test
  public void invalidPriorPrepareMessagesAreDiscardedUponSubsequentProposal() {
    final Prepare validPrepare =
        validatorMessageFactories.get(1).createPrepare(roundIdentifier, blockHash);
    final Prepare invalidPrepare =
        validatorMessageFactories.get(2).createPrepare(roundIdentifier, blockHash);
    final Prepare validPrepare2 =
        validatorMessageFactories.get(0).createPrepare(roundIdentifier, blockHash);

    final RoundState roundState = new RoundState(roundIdentifier, 2, messageValidator);

    when(messageValidator.validateProposal(any())).thenReturn(true);
    when(messageValidator.validatePrepare(validPrepare)).thenReturn(true);
    when(messageValidator.validatePrepare(invalidPrepare)).thenReturn(false);
    when(messageValidator.validatePrepare(validPrepare2)).thenReturn(true);

    roundState.addPrepareMessage(invalidPrepare);
    roundState.addPrepareMessage(validPrepare);
    verify(messageValidator, never()).validatePrepare(any());

    final Proposal proposal =
        validatorMessageFactories
            .getFirst()
            .createProposal(
                roundIdentifier, block, Collections.emptyList(), Collections.emptyList());

    assertThat(roundState.setProposedBlock(proposal)).isTrue();
    assertThat(roundState.isPrepared()).isFalse();
    assertThat(roundState.isCommitted()).isFalse();
    assertThat(roundState.constructPreparedCertificate()).isEmpty();

    roundState.addPrepareMessage(validPrepare2);
    assertThat(roundState.isPrepared()).isTrue();

    final Optional<PreparedCertificate> preparedCertificate =
        roundState.constructPreparedCertificate();
    assertThat(preparedCertificate).isNotEmpty();

    final List<SignedData<PreparePayload>> expectedPrepares =
        Stream.of(validPrepare, validPrepare2)
            .map(BftMessage::getSignedPayload)
            .collect(Collectors.toList());
    assertThat(preparedCertificate.get().getPrepares()).isEqualTo(expectedPrepares);
  }

  @Test
  public void invalidPriorCommitMessagesAreDiscardedUponSubsequentProposal() {
    final Commit validCommit =
        validatorMessageFactories
            .get(1)
            .createCommit(
                roundIdentifier,
                blockHash,
                SIGNATURE_ALGORITHM.createSignature(BigInteger.ONE, BigInteger.TEN, (byte) 1));

    final Commit invalidCommit =
        validatorMessageFactories
            .get(2)
            .createCommit(
                roundIdentifier,
                blockHash,
                SIGNATURE_ALGORITHM.createSignature(BigInteger.TEN, BigInteger.TEN, (byte) 1));

    // RoundState has a quorum size of 3, meaning 1 proposal and 2 commits are required to be
    // 'committed'.
    final RoundState roundState = new RoundState(roundIdentifier, 3, messageValidator);

    when(messageValidator.validateProposal(any())).thenReturn(true);
    when(messageValidator.validateCommit(validCommit)).thenReturn(true);
    when(messageValidator.validateCommit(invalidCommit)).thenReturn(false);

    roundState.addCommitMessage(validCommit);
    roundState.addCommitMessage(invalidCommit);
    verify(messageValidator, never()).validateCommit(any());

    final Proposal proposal =
        validatorMessageFactories
            .getFirst()
            .createProposal(
                roundIdentifier, block, Collections.emptyList(), Collections.emptyList());

    assertThat(roundState.setProposedBlock(proposal)).isTrue();
    assertThat(roundState.isPrepared()).isFalse();
    assertThat(roundState.isCommitted()).isFalse();
    assertThat(roundState.getCommitSeals()).containsOnly(validCommit.getCommitSeal());
  }

  @Test
  public void prepareMessageIsValidatedAgainstExistingProposal() {
    final RoundState roundState = new RoundState(roundIdentifier, 2, messageValidator);

    final Prepare firstPrepare =
        validatorMessageFactories.get(1).createPrepare(roundIdentifier, blockHash);

    final Prepare secondPrepare =
        validatorMessageFactories.get(2).createPrepare(roundIdentifier, blockHash);

    final Prepare thirdPrepare =
        validatorMessageFactories.get(0).createPrepare(roundIdentifier, blockHash);

    final Proposal proposal =
        validatorMessageFactories
            .get(0)
            .createProposal(
                roundIdentifier, block, Collections.emptyList(), Collections.emptyList());

    when(messageValidator.validateProposal(any())).thenReturn(true);
    when(messageValidator.validatePrepare(firstPrepare)).thenReturn(false);
    when(messageValidator.validatePrepare(secondPrepare)).thenReturn(true);
    when(messageValidator.validatePrepare(thirdPrepare)).thenReturn(true);

    roundState.setProposedBlock(proposal);
    assertThat(roundState.isPrepared()).isFalse();

    roundState.addPrepareMessage(firstPrepare);
    assertThat(roundState.isPrepared()).isFalse();

    roundState.addPrepareMessage(secondPrepare);
    assertThat(roundState.isPrepared()).isFalse();

    roundState.addPrepareMessage(thirdPrepare);
    assertThat(roundState.isPrepared()).isTrue();
  }

  @Test
  public void commitSealsAreExtractedFromReceivedMessages() {
    when(messageValidator.validateProposal(any())).thenReturn(true);
    when(messageValidator.validateCommit(any())).thenReturn(true);

    final RoundState roundState = new RoundState(roundIdentifier, 2, messageValidator);

    final Commit firstCommit =
        validatorMessageFactories
            .get(1)
            .createCommit(
                roundIdentifier,
                blockHash,
                SIGNATURE_ALGORITHM.createSignature(BigInteger.ONE, BigInteger.TEN, (byte) 1));

    final Commit secondCommit =
        validatorMessageFactories
            .get(2)
            .createCommit(
                roundIdentifier,
                blockHash,
                SIGNATURE_ALGORITHM.createSignature(BigInteger.TEN, BigInteger.TEN, (byte) 1));

    final Proposal proposal =
        validatorMessageFactories
            .get(0)
            .createProposal(
                roundIdentifier, block, Collections.emptyList(), Collections.emptyList());

    roundState.setProposedBlock(proposal);
    roundState.addCommitMessage(firstCommit);
    assertThat(roundState.isCommitted()).isFalse();
    roundState.addCommitMessage(secondCommit);
    assertThat(roundState.isCommitted()).isTrue();

    assertThat(roundState.getCommitSeals())
        .containsOnly(firstCommit.getCommitSeal(), secondCommit.getCommitSeal());
  }

  @Test
  public void duplicatePreparesAreNotIncludedInRoundChangeMessage() {
    final RoundState roundState = new RoundState(roundIdentifier, 2, messageValidator);

    final Prepare firstPrepare =
        validatorMessageFactories.get(1).createPrepare(roundIdentifier, blockHash);

    final Prepare secondPrepare =
        validatorMessageFactories.get(2).createPrepare(roundIdentifier, blockHash);

    final Prepare duplicatePrepare =
        validatorMessageFactories.get(2).createPrepare(roundIdentifier, blockHash);

    final Proposal proposal =
        validatorMessageFactories
            .get(0)
            .createProposal(
                roundIdentifier, block, Collections.emptyList(), Collections.emptyList());

    when(messageValidator.validateProposal(any())).thenReturn(true);
    when(messageValidator.validatePrepare(any())).thenReturn(true);

    roundState.setProposedBlock(proposal);
    roundState.addPrepareMessage(firstPrepare);
    roundState.addPrepareMessage(secondPrepare);
    roundState.addPrepareMessage(duplicatePrepare);

    assertThat(roundState.isPrepared()).isTrue();
    assertThat(roundState.isCommitted()).isFalse();

    final Optional<PreparedCertificate> preparedCertificate =
        roundState.constructPreparedCertificate();
    assertThat(preparedCertificate).isNotEmpty();
    assertThat(preparedCertificate.get().getRound()).isEqualTo(roundIdentifier.getRoundNumber());
    assertThat(preparedCertificate.get().getBlock()).isEqualTo(block);

    final List<SignedData<PreparePayload>> expectedPrepares =
        Stream.of(firstPrepare, secondPrepare)
            .map(BftMessage::getSignedPayload)
            .collect(Collectors.toList());
    assertThat(preparedCertificate.get().getPrepares()).isEqualTo(expectedPrepares);
  }
}
