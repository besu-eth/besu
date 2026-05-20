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

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.ProposedBlockHelpers;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Commit;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Prepare;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Proposal;
import org.hyperledger.besu.consensus.ibft.payload.MessageFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.Util;

import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("DirectInvocationOnMock")
public class SignedDataValidatorTest {

  private final NodeKey proposerKey = NodeKeyUtils.generate();
  private final NodeKey validatorKey = NodeKeyUtils.generate();
  private final NodeKey nonValidatorKey = NodeKeyUtils.generate();
  private final MessageFactory proposerMessageFactory = new MessageFactory(proposerKey);
  private final MessageFactory validatorMessageFactory = new MessageFactory(validatorKey);
  private final MessageFactory nonValidatorMessageFactory = new MessageFactory(nonValidatorKey);

  private final List<Address> validators = Lists.newArrayList();

  private final ConsensusRoundIdentifier roundIdentifier = new ConsensusRoundIdentifier(2, 0);
  private SignedDataValidator validator;

  private final Block block = mock(Block.class);

  @BeforeEach
  public void setup() {
    validators.add(Util.publicKeyToAddress(proposerKey.getPublicKey()));
    validators.add(Util.publicKeyToAddress(validatorKey.getPublicKey()));

    validator =
        new SignedDataValidator(
            validators, Util.publicKeyToAddress(proposerKey.getPublicKey()), roundIdentifier);

    when(block.getHash()).thenReturn(Hash.fromHexStringLenient("1"));
  }

  @Test
  public void receivingAPrepareMessageBeforeProposalFails() {
    final Prepare prepareMsg = proposerMessageFactory.createPrepare(roundIdentifier, Hash.ZERO);

    assertThat(validator.validatePrepare(prepareMsg.getSignedPayload())).isFalse();
  }

  @Test
  public void receivingACommitMessageBeforeProposalFails() {
    final Commit commitMsg =
        proposerMessageFactory.createCommit(
            roundIdentifier, Hash.ZERO, proposerKey.sign(Bytes32.wrap(block.getHash().getBytes())));

    assertThat(validator.validateCommit(commitMsg.getSignedPayload())).isFalse();
  }

  @Test
  public void receivingProposalMessageFromNonProposerFails() {
    final Block block = ProposedBlockHelpers.createProposalBlock(emptyList(), roundIdentifier);
    final Proposal proposalMsg =
        validatorMessageFactory.createProposal(roundIdentifier, block, Optional.empty());

    assertThat(validator.validateProposal(proposalMsg.getSignedPayload())).isFalse();
  }

  @Test
  public void receivingPrepareFromProposerFails() {
    final Proposal proposalMsg =
        proposerMessageFactory.createProposal(roundIdentifier, block, Optional.empty());

    final Prepare prepareMsg =
        proposerMessageFactory.createPrepare(roundIdentifier, block.getHash());

    assertThat(validator.validateProposal(proposalMsg.getSignedPayload())).isTrue();
    assertThat(validator.validatePrepare(prepareMsg.getSignedPayload())).isFalse();
  }

  @Test
  public void receivingPrepareFromNonValidatorFails() {
    final Proposal proposalMsg =
        proposerMessageFactory.createProposal(roundIdentifier, block, Optional.empty());

    final Prepare prepareMsg =
        nonValidatorMessageFactory.createPrepare(roundIdentifier, block.getHash());

    assertThat(validator.validateProposal(proposalMsg.getSignedPayload())).isTrue();
    assertThat(validator.validatePrepare(prepareMsg.getSignedPayload())).isFalse();
  }

  @Test
  public void receivingMessagesWithDifferentRoundIdFromProposalFails() {
    final Proposal proposalMsg =
        proposerMessageFactory.createProposal(roundIdentifier, block, Optional.empty());

    final ConsensusRoundIdentifier invalidRoundIdentifier =
        new ConsensusRoundIdentifier(
            roundIdentifier.getSequenceNumber(), roundIdentifier.getRoundNumber() + 1);
    final Prepare prepareMsg =
        validatorMessageFactory.createPrepare(invalidRoundIdentifier, block.getHash());
    final Commit commitMsg =
        validatorMessageFactory.createCommit(
            invalidRoundIdentifier,
            block.getHash(),
            proposerKey.sign(Bytes32.wrap(block.getHash().getBytes())));

    assertThat(validator.validateProposal(proposalMsg.getSignedPayload())).isTrue();
    assertThat(validator.validatePrepare(prepareMsg.getSignedPayload())).isFalse();
    assertThat(validator.validateCommit(commitMsg.getSignedPayload())).isFalse();
  }

  @Test
  public void receivingPrepareNonProposerValidatorWithCorrectRoundIsSuccessful() {
    final Proposal proposalMsg =
        proposerMessageFactory.createProposal(roundIdentifier, block, Optional.empty());
    final Prepare prepareMsg =
        validatorMessageFactory.createPrepare(roundIdentifier, block.getHash());

    assertThat(validator.validateProposal(proposalMsg.getSignedPayload())).isTrue();
    assertThat(validator.validatePrepare(prepareMsg.getSignedPayload())).isTrue();
  }

  @Test
  public void receivingACommitMessageWithAnInvalidCommitSealFails() {
    final Proposal proposalMsg =
        proposerMessageFactory.createProposal(roundIdentifier, block, Optional.empty());

    final Commit commitMsg =
        proposerMessageFactory.createCommit(
            roundIdentifier,
            block.getHash(),
            nonValidatorKey.sign(Bytes32.wrap(block.getHash().getBytes())));

    assertThat(validator.validateProposal(proposalMsg.getSignedPayload())).isTrue();
    assertThat(validator.validateCommit(commitMsg.getSignedPayload())).isFalse();
  }

  @Test
  public void commitMessageContainingValidSealFromValidatorIsSuccessful() {
    final Proposal proposalMsg =
        proposerMessageFactory.createProposal(roundIdentifier, block, Optional.empty());

    final Commit proposerCommitMsg =
        proposerMessageFactory.createCommit(
            roundIdentifier,
            block.getHash(),
            proposerKey.sign(Bytes32.wrap(block.getHash().getBytes())));

    final Commit validatorCommitMsg =
        validatorMessageFactory.createCommit(
            roundIdentifier,
            block.getHash(),
            validatorKey.sign(Bytes32.wrap(block.getHash().getBytes())));

    assertThat(validator.validateProposal(proposalMsg.getSignedPayload())).isTrue();
    assertThat(validator.validateCommit(proposerCommitMsg.getSignedPayload())).isTrue();
    assertThat(validator.validateCommit(validatorCommitMsg.getSignedPayload())).isTrue();
  }

  @Test
  public void subsequentProposalHasDifferentSenderFails() {
    final Proposal proposalMsg =
        proposerMessageFactory.createProposal(roundIdentifier, block, Optional.empty());
    assertThat(validator.validateProposal(proposalMsg.getSignedPayload())).isTrue();

    final Proposal secondProposalMsg =
        validatorMessageFactory.createProposal(roundIdentifier, block, Optional.empty());
    assertThat(validator.validateProposal(secondProposalMsg.getSignedPayload())).isFalse();
  }

  @Test
  public void subsequentProposalHasDifferentContentFails() {
    final Proposal proposalMsg =
        proposerMessageFactory.createProposal(roundIdentifier, block, Optional.empty());
    assertThat(validator.validateProposal(proposalMsg.getSignedPayload())).isTrue();

    final ConsensusRoundIdentifier newRoundIdentifier = new ConsensusRoundIdentifier(3, 0);
    final Proposal secondProposalMsg =
        proposerMessageFactory.createProposal(newRoundIdentifier, block, Optional.empty());
    assertThat(validator.validateProposal(secondProposalMsg.getSignedPayload())).isFalse();
  }

  @Test
  public void subsequentProposalHasIdenticalSenderAndContentIsSuccessful() {
    final Proposal proposalMsg =
        proposerMessageFactory.createProposal(roundIdentifier, block, Optional.empty());
    assertThat(validator.validateProposal(proposalMsg.getSignedPayload())).isTrue();

    final Proposal secondProposalMsg =
        proposerMessageFactory.createProposal(roundIdentifier, block, Optional.empty());
    assertThat(validator.validateProposal(secondProposalMsg.getSignedPayload())).isTrue();
  }
}
