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
package org.hyperledger.besu.consensus.ibft.statemachine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.BftBlockInterface;
import org.hyperledger.besu.consensus.common.bft.BftHelpers;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundHelpers;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.ProposedBlockHelpers;
import org.hyperledger.besu.consensus.ibft.IbftExtraDataCodec;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Prepare;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Proposal;
import org.hyperledger.besu.consensus.ibft.messagewrappers.RoundChange;
import org.hyperledger.besu.consensus.ibft.payload.MessageFactory;
import org.hyperledger.besu.consensus.ibft.validation.ProposalBlockConsistencyValidator;
import org.hyperledger.besu.consensus.ibft.validation.RoundChangeMessageValidator;
import org.hyperledger.besu.consensus.ibft.validation.RoundChangePayloadValidator;
import org.hyperledger.besu.consensus.ibft.validation.SignedDataValidator;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.Util;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RoundChangeManagerTest {

  private RoundChangeManager manager;

  private final NodeKey proposerKey = NodeKeyUtils.generate();
  private final NodeKey validator1Key = NodeKeyUtils.generate();
  private final NodeKey validator2Key = NodeKeyUtils.generate();
  private final NodeKey nonValidatorKey = NodeKeyUtils.generate();

  private final ConsensusRoundIdentifier ri1 = new ConsensusRoundIdentifier(2, 1);
  private final ConsensusRoundIdentifier ri2 = new ConsensusRoundIdentifier(2, 2);
  private final ConsensusRoundIdentifier ri3 = new ConsensusRoundIdentifier(2, 3);
  private final List<Address> validators = Lists.newArrayList();
  private final ProposalBlockConsistencyValidator proposalConsistencyValidator =
      mock(ProposalBlockConsistencyValidator.class);
  private final BftBlockInterface bftBlockInterface =
      new BftBlockInterface(new IbftExtraDataCodec());

  @BeforeEach
  public void setup() {

    validators.add(Util.publicKeyToAddress(proposerKey.getPublicKey()));
    validators.add(Util.publicKeyToAddress(validator1Key.getPublicKey()));
    validators.add(Util.publicKeyToAddress(validator2Key.getPublicKey()));

    final RoundChangePayloadValidator.MessageValidatorForHeightFactory messageValidatorFactory =
        mock(RoundChangePayloadValidator.MessageValidatorForHeightFactory.class);

    when(messageValidatorFactory.createAt(ri1))
        .thenAnswer(
            invocation ->
                new SignedDataValidator(
                    validators, Util.publicKeyToAddress(proposerKey.getPublicKey()), ri1));
    when(messageValidatorFactory.createAt(ri2))
        .thenAnswer(
            invocation ->
                new SignedDataValidator(
                    validators, Util.publicKeyToAddress(validator1Key.getPublicKey()), ri2));
    when(messageValidatorFactory.createAt(ri3))
        .thenAnswer(
            invocation ->
                new SignedDataValidator(
                    validators, Util.publicKeyToAddress(validator2Key.getPublicKey()), ri3));

    final RoundChangeMessageValidator roundChangeMessageValidator =
        new RoundChangeMessageValidator(
            new RoundChangePayloadValidator(
                messageValidatorFactory,
                validators,
                BftHelpers.calculateRequiredValidatorQuorum(
                    BftHelpers.calculateRequiredValidatorQuorum(validators.size())),
                2),
            proposalConsistencyValidator,
            bftBlockInterface);
    manager = new RoundChangeManager(2, roundChangeMessageValidator);

    when(proposalConsistencyValidator.validateProposalMatchesBlock(any(), any(), any()))
        .thenReturn(true);
  }

  private RoundChange makeRoundChangeMessage(
      final NodeKey key, final ConsensusRoundIdentifier round) {
    final MessageFactory messageFactory = new MessageFactory(key);
    return messageFactory.createRoundChange(round, Optional.empty());
  }

  private RoundChange makeRoundChangeMessageWithPreparedCert(
      final NodeKey key,
      final ConsensusRoundIdentifier round,
      final List<NodeKey> prepareProviders) {
    Preconditions.checkArgument(!prepareProviders.contains(key));

    final MessageFactory messageFactory = new MessageFactory(key);

    final ConsensusRoundIdentifier proposalRound = ConsensusRoundHelpers.createFrom(round, 0, -1);
    final Block block = ProposedBlockHelpers.createProposalBlock(validators, proposalRound);
    // Proposal must come from an earlier round.
    final Proposal proposal = messageFactory.createProposal(proposalRound, block, Optional.empty());

    final List<Prepare> preparePayloads =
        prepareProviders.stream()
            .map(
                k -> {
                  final MessageFactory prepareFactory = new MessageFactory(k);
                  return prepareFactory.createPrepare(proposalRound, block.getHash());
                })
            .collect(Collectors.toList());

    final PreparedRoundArtifacts preparedRoundArtifacts =
        new PreparedRoundArtifacts(proposal, preparePayloads);

    return messageFactory.createRoundChange(round, Optional.of(preparedRoundArtifacts));
  }

  @Test
  public void rejectsInvalidRoundChangeMessage() {
    final RoundChange roundChangeData = makeRoundChangeMessage(nonValidatorKey, ri1);
    assertThat(manager.appendRoundChangeMessage(roundChangeData)).isEmpty();
    assertThat(manager.roundChangeCache.get(ri1)).isNull();
  }

  @Test
  public void acceptsValidRoundChangeMessage() {
    final RoundChange roundChangeData = makeRoundChangeMessage(proposerKey, ri2);
    assertThat(manager.appendRoundChangeMessage(roundChangeData)).isEmpty();
    assertThat(manager.roundChangeCache.get(ri2).receivedMessages.size()).isEqualTo(1);
  }

  @Test
  public void doesntAcceptDuplicateValidRoundChangeMessage() {
    final RoundChange roundChangeData = makeRoundChangeMessage(proposerKey, ri2);
    assertThat(manager.appendRoundChangeMessage(roundChangeData)).isEmpty();
    assertThat(manager.appendRoundChangeMessage(roundChangeData)).isEmpty();
    assertThat(manager.roundChangeCache.get(ri2).receivedMessages.size()).isEqualTo(1);
  }

  @Test
  public void becomesReadyAtThreshold() {
    final RoundChange roundChangeDataProposer = makeRoundChangeMessage(proposerKey, ri2);
    final RoundChange roundChangeDataValidator1 = makeRoundChangeMessage(validator1Key, ri2);
    assertThat(manager.appendRoundChangeMessage(roundChangeDataProposer))
        .isEqualTo(Optional.empty());
    assertThat(manager.appendRoundChangeMessage(roundChangeDataValidator1).isPresent()).isTrue();
  }

  @Test
  public void doesntReachReadyWhenSuppliedWithDifferentRounds() {
    final RoundChange roundChangeDataProposer = makeRoundChangeMessage(proposerKey, ri2);
    final RoundChange roundChangeDataValidator1 = makeRoundChangeMessage(validator1Key, ri3);
    assertThat(manager.appendRoundChangeMessage(roundChangeDataProposer))
        .isEqualTo(Optional.empty());
    assertThat(manager.appendRoundChangeMessage(roundChangeDataValidator1))
        .isEqualTo(Optional.empty());
    assertThat(manager.roundChangeCache.get(ri2).receivedMessages.size()).isEqualTo(1);
    assertThat(manager.roundChangeCache.get(ri3).receivedMessages.size()).isEqualTo(1);
  }

  @Test
  public void discardsRoundPreviousToThatRequested() {
    final RoundChange roundChangeDataProposer = makeRoundChangeMessage(proposerKey, ri1);
    final RoundChange roundChangeDataValidator1 = makeRoundChangeMessage(validator1Key, ri2);
    final RoundChange roundChangeDataValidator2 = makeRoundChangeMessage(validator2Key, ri3);
    assertThat(manager.appendRoundChangeMessage(roundChangeDataProposer))
        .isEqualTo(Optional.empty());
    assertThat(manager.appendRoundChangeMessage(roundChangeDataValidator1))
        .isEqualTo(Optional.empty());
    assertThat(manager.appendRoundChangeMessage(roundChangeDataValidator2))
        .isEqualTo(Optional.empty());
    manager.discardRoundsPriorTo(ri2);
    assertThat(manager.roundChangeCache.get(ri1)).isNull();
    assertThat(manager.roundChangeCache.get(ri2).receivedMessages.size()).isEqualTo(1);
    assertThat(manager.roundChangeCache.get(ri3).receivedMessages.size()).isEqualTo(1);
  }

  @Test
  public void stopsAcceptingMessagesAfterReady() {
    final RoundChange roundChangeDataProposer = makeRoundChangeMessage(proposerKey, ri2);
    final RoundChange roundChangeDataValidator1 = makeRoundChangeMessage(validator1Key, ri2);
    final RoundChange roundChangeDataValidator2 = makeRoundChangeMessage(validator2Key, ri2);
    assertThat(manager.appendRoundChangeMessage(roundChangeDataProposer))
        .isEqualTo(Optional.empty());
    assertThat(manager.appendRoundChangeMessage(roundChangeDataValidator1).isPresent()).isTrue();
    assertThat(manager.roundChangeCache.get(ri2).receivedMessages.size()).isEqualTo(2);
    assertThat(manager.appendRoundChangeMessage(roundChangeDataValidator2))
        .isEqualTo(Optional.empty());
    assertThat(manager.roundChangeCache.get(ri2).receivedMessages.size()).isEqualTo(2);
  }

  @Test
  public void roundChangeMessagesWithPreparedCertificateMustHaveSufficientPrepareMessages() {
    // Specifically, prepareMessage count is ONE LESS than the calculated quorum size (as the
    // proposal acts as the extra msg).
    // There are 3 validators, therefore, should only need 2 prepare message to be acceptable.

    // These tests are run at ri2, such that validators can be found for past round at ri1.
    RoundChange roundChangeData =
        makeRoundChangeMessageWithPreparedCert(proposerKey, ri2, Collections.emptyList());
    assertThat(manager.appendRoundChangeMessage(roundChangeData)).isEmpty();
    assertThat(manager.roundChangeCache.get(ri2)).isNull();

    roundChangeData =
        makeRoundChangeMessageWithPreparedCert(
            proposerKey, ri2, Lists.newArrayList(validator1Key, validator2Key));
    assertThat(manager.appendRoundChangeMessage(roundChangeData)).isEmpty();
    assertThat(manager.roundChangeCache.get(ri2).receivedMessages.size()).isEqualTo(1);
  }
}
