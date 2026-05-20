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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Prepare;
import org.hyperledger.besu.consensus.ibft.messagewrappers.RoundChange;
import org.hyperledger.besu.consensus.ibft.payload.MessageFactory;
import org.hyperledger.besu.consensus.ibft.payload.PreparedCertificate;
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
public class RoundChangeSignedDataValidatorTest {

  private final NodeKey proposerKey = NodeKeyUtils.generate();
  private final NodeKey validatorKey = NodeKeyUtils.generate();
  private final NodeKey nonValidatorKey = NodeKeyUtils.generate();
  private final MessageFactory proposerMessageFactory = new MessageFactory(proposerKey);
  private final MessageFactory validatorMessageFactory = new MessageFactory(validatorKey);
  private final MessageFactory nonValidatorMessageFactory = new MessageFactory(nonValidatorKey);

  private final long chainHeight = 2;
  private final ConsensusRoundIdentifier currentRound =
      new ConsensusRoundIdentifier(chainHeight, 3);
  private final ConsensusRoundIdentifier targetRound = new ConsensusRoundIdentifier(chainHeight, 4);

  private final Block block = mock(Block.class);

  private final SignedDataValidator basicValidator = mock(SignedDataValidator.class);
  private final List<Address> validators = Lists.newArrayList();

  private final MessageValidatorForHeightFactory validatorFactory =
      mock(MessageValidatorForHeightFactory.class);
  private final RoundChangePayloadValidator validator =
      new RoundChangePayloadValidator(validatorFactory, validators, 1, chainHeight);

  @BeforeEach
  public void setup() {
    validators.add(Util.publicKeyToAddress(proposerKey.getPublicKey()));
    validators.add(Util.publicKeyToAddress(validatorKey.getPublicKey()));

    when(block.getHash()).thenReturn(Hash.fromHexStringLenient("1"));
    when(validatorFactory.createAt(any())).thenReturn(basicValidator);

    // By default, have all basic messages being valid thus any failures are attributed to logic
    // in the RoundChangePayloadValidator
    when(basicValidator.validateProposal(any())).thenReturn(true);
    when(basicValidator.validatePrepare(any())).thenReturn(true);
  }

  @Test
  public void roundChangeSentByNonValidatorFails() {
    final RoundChange msg =
        nonValidatorMessageFactory.createRoundChange(targetRound, Optional.empty());
    assertThat(validator.validateRoundChange(msg.getSignedPayload())).isFalse();
  }

  @Test
  public void roundChangeContainingNoCertificateIsSuccessful() {
    final RoundChange msg = proposerMessageFactory.createRoundChange(targetRound, Optional.empty());

    assertThat(validator.validateRoundChange(msg.getSignedPayload())).isTrue();
  }

  @Test
  public void roundChangeContainingInvalidProposalFails() {
    final PreparedRoundArtifacts preparedRoundArtifacts =
        new PreparedRoundArtifacts(
            proposerMessageFactory.createProposal(currentRound, block, Optional.empty()),
            Collections.emptyList());

    final PreparedCertificate prepareCertificate = preparedRoundArtifacts.getPreparedCertificate();

    final RoundChange msg =
        proposerMessageFactory.createRoundChange(targetRound, Optional.of(preparedRoundArtifacts));

    when(basicValidator.validateProposal(any())).thenReturn(false);

    assertThat(validator.validateRoundChange(msg.getSignedPayload())).isFalse();
    verify(validatorFactory, times(1))
        .createAt(prepareCertificate.getProposalPayload().getPayload().getRoundIdentifier());
    verify(basicValidator, times(1)).validateProposal(prepareCertificate.getProposalPayload());
    verify(basicValidator, never()).validatePrepare(any());
    verify(basicValidator, never()).validateCommit(any());
  }

  @Test
  public void roundChangeContainingValidProposalButNoPrepareMessagesFails() {
    final PreparedRoundArtifacts preparedRoundArtifacts =
        new PreparedRoundArtifacts(
            proposerMessageFactory.createProposal(currentRound, block, Optional.empty()),
            Collections.emptyList());

    final RoundChange msg =
        proposerMessageFactory.createRoundChange(targetRound, Optional.of(preparedRoundArtifacts));

    when(basicValidator.validateProposal(any())).thenReturn(true);
    assertThat(validator.validateRoundChange(msg.getSignedPayload())).isFalse();
  }

  @Test
  public void roundChangeInvalidPrepareMessageFromProposerFails() {
    final Prepare prepareMsg = validatorMessageFactory.createPrepare(currentRound, block.getHash());
    final PreparedRoundArtifacts preparedRoundArtifacts =
        new PreparedRoundArtifacts(
            proposerMessageFactory.createProposal(currentRound, block, Optional.empty()),
            Lists.newArrayList(prepareMsg));

    when(basicValidator.validateProposal(any())).thenReturn(true);
    when(basicValidator.validatePrepare(any())).thenReturn(false);

    final RoundChange msg =
        proposerMessageFactory.createRoundChange(targetRound, Optional.of(preparedRoundArtifacts));

    assertThat(validator.validateRoundChange(msg.getSignedPayload())).isFalse();

    verify(basicValidator, times(1)).validatePrepare(prepareMsg.getSignedPayload());
    verify(basicValidator, never()).validateCommit(any());
  }

  @Test
  public void roundChangeWithDifferentSequenceNumberFails() {
    final ConsensusRoundIdentifier latterRoundIdentifier =
        new ConsensusRoundIdentifier(currentRound.getSequenceNumber() + 1, 1);

    final RoundChange msg =
        proposerMessageFactory.createRoundChange(latterRoundIdentifier, Optional.empty());

    assertThat(validator.validateRoundChange(msg.getSignedPayload())).isFalse();
    verify(basicValidator, never()).validatePrepare(any());
  }

  @Test
  public void roundChangeWithProposalFromARoundAheadOfRoundChangeTargetFails() {
    final ConsensusRoundIdentifier futureRound =
        new ConsensusRoundIdentifier(
            currentRound.getSequenceNumber(), currentRound.getRoundNumber() + 2);

    final Prepare prepareMsg = validatorMessageFactory.createPrepare(futureRound, block.getHash());
    final PreparedRoundArtifacts preparedRoundArtifacts =
        new PreparedRoundArtifacts(
            proposerMessageFactory.createProposal(futureRound, block, Optional.empty()),
            Lists.newArrayList(prepareMsg));

    final RoundChange msg =
        proposerMessageFactory.createRoundChange(targetRound, Optional.of(preparedRoundArtifacts));

    assertThat(validator.validateRoundChange(msg.getSignedPayload())).isFalse();
    verify(validatorFactory, never()).createAt(any());
    verify(basicValidator, never()).validatePrepare(prepareMsg.getSignedPayload());
    verify(basicValidator, never()).validateCommit(any());
  }

  @Test
  public void roundChangeWithPastProposalForCurrentHeightIsSuccessful() {
    final Prepare prepareMsg = validatorMessageFactory.createPrepare(currentRound, block.getHash());
    final PreparedRoundArtifacts preparedRoundArtifacts =
        new PreparedRoundArtifacts(
            proposerMessageFactory.createProposal(currentRound, block, Optional.empty()),
            Lists.newArrayList(prepareMsg));

    final PreparedCertificate prepareCertificate = preparedRoundArtifacts.getPreparedCertificate();

    final RoundChange msg =
        proposerMessageFactory.createRoundChange(targetRound, Optional.of(preparedRoundArtifacts));

    when(basicValidator.validateProposal(prepareCertificate.getProposalPayload())).thenReturn(true);
    when(basicValidator.validatePrepare(prepareMsg.getSignedPayload())).thenReturn(true);

    assertThat(validator.validateRoundChange(msg.getSignedPayload())).isTrue();
    verify(validatorFactory, times(1))
        .createAt(prepareCertificate.getProposalPayload().getPayload().getRoundIdentifier());
    verify(basicValidator, times(1)).validateProposal(prepareCertificate.getProposalPayload());
    verify(basicValidator, times(1)).validatePrepare(prepareMsg.getSignedPayload());
  }

  @Test
  public void roundChangeWithDuplicatedPreparesFails() {
    final RoundChangePayloadValidator validatorRequiringTwoPrepares =
        new RoundChangePayloadValidator(validatorFactory, validators, 2, chainHeight);

    final Prepare prepareMsg = validatorMessageFactory.createPrepare(currentRound, block.getHash());
    final PreparedRoundArtifacts preparedRoundArtifacts =
        new PreparedRoundArtifacts(
            proposerMessageFactory.createProposal(currentRound, block, Optional.empty()),
            Lists.newArrayList(prepareMsg, prepareMsg));
    final PreparedCertificate prepareCertificate = preparedRoundArtifacts.getPreparedCertificate();
    final RoundChange msg =
        proposerMessageFactory.createRoundChange(targetRound, Optional.of(preparedRoundArtifacts));

    when(basicValidator.validateProposal(prepareCertificate.getProposalPayload())).thenReturn(true);
    when(basicValidator.validatePrepare(prepareMsg.getSignedPayload())).thenReturn(true);

    assertThat(validatorRequiringTwoPrepares.validateRoundChange(msg.getSignedPayload())).isFalse();
  }
}
