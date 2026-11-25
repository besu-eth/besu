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
package org.hyperledger.besu.consensus.qbft.core.statemachine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.BlockTimer;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.RoundTimer;
import org.hyperledger.besu.consensus.common.bft.events.RoundExpiry;
import org.hyperledger.besu.consensus.common.bft.network.ValidatorMulticaster;
import org.hyperledger.besu.consensus.qbft.core.QbftBlockTestFixture;
import org.hyperledger.besu.consensus.qbft.core.messagedata.RoundChangeMessageData;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.RoundChange;
import org.hyperledger.besu.consensus.qbft.core.network.QbftMessageTransmitter;
import org.hyperledger.besu.consensus.qbft.core.payload.MessageFactory;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCodec;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCreator;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockHeader;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockInterface;
import org.hyperledger.besu.consensus.qbft.core.types.QbftFinalState;
import org.hyperledger.besu.consensus.qbft.core.types.QbftProtocolSchedule;
import org.hyperledger.besu.consensus.qbft.core.types.QbftValidatorModeTransitionLogger;
import org.hyperledger.besu.consensus.qbft.core.types.QbftValidatorProvider;
import org.hyperledger.besu.consensus.qbft.core.validation.FutureRoundProposalMessageValidator;
import org.hyperledger.besu.consensus.qbft.core.validation.MessageValidator;
import org.hyperledger.besu.consensus.qbft.core.validation.MessageValidatorFactory;
import org.hyperledger.besu.consensus.qbft.core.validation.QbftBlockHeaderTestFixture;
import org.hyperledger.besu.consensus.qbft.core.validation.RoundChangeMessageValidator;
import org.hyperledger.besu.consensus.qbft.core.validation.RoundChangePayloadValidator;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.util.Subscribers;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class QbftBlockHeightManagerFactoryEarlyRoundChangeTest {

  private static final int VALIDATOR_COUNT = 7;
  private static final int QUORUM = 5;
  private static final long CHAIN_HEIGHT = 1;

  private final QbftBlockHeader parentHeader = new QbftBlockHeaderTestFixture().buildHeader();
  private final QbftBlock createdBlock = new QbftBlockTestFixture().build();

  // Index 0 is the local node.
  private final List<NodeKey> validatorKeys = new ArrayList<>();
  private final List<Address> validators = new ArrayList<>();
  private final List<MessageFactory> validatorMessageFactories = new ArrayList<>();
  private MessageFactory localMessageFactory;
  private Address localAddress;
  private NodeKey localNodeKey;

  @Mock private QbftFinalState finalState;
  @Mock private QbftMessageTransmitter roundTransmitter;
  @Mock private QbftRoundFactory roundFactory;
  @Mock private Clock clock;
  @Mock private MessageValidatorFactory messageValidatorFactory;
  @Mock private QbftBlockCreator blockCreator;
  @Mock private BlockTimer blockTimer;
  @Mock private RoundTimer roundTimer;
  @Mock private FutureRoundProposalMessageValidator futureRoundProposalMessageValidator;
  @Mock private ValidatorMulticaster validatorMulticaster;
  @Mock private QbftProtocolSchedule protocolSchedule;
  @Mock private QbftBlockCodec blockEncoder;
  @Mock private QbftBlockInterface blockInterface;
  @Mock private QbftValidatorProvider validatorProvider;
  @Mock private QbftValidatorModeTransitionLogger validatorModeTransitionLogger;

  @BeforeEach
  public void setup() {
    for (int i = 0; i < VALIDATOR_COUNT; i++) {
      final NodeKey key = NodeKeyUtils.generate();
      validatorKeys.add(key);
      validators.add(Util.publicKeyToAddress(key.getPublicKey()));
      validatorMessageFactories.add(new MessageFactory(key, blockEncoder));
    }
    localNodeKey = validatorKeys.get(0);
    localAddress = validators.get(0);
    localMessageFactory = validatorMessageFactories.get(0);

    when(finalState.getBlockTimer()).thenReturn(blockTimer);
    when(finalState.getRoundTimer()).thenReturn(roundTimer);
    when(finalState.getQuorum()).thenReturn(QUORUM);
    when(finalState.getValidators()).thenReturn(validators);
    when(finalState.getLocalAddress()).thenReturn(localAddress);
    when(finalState.getValidatorMulticaster()).thenReturn(validatorMulticaster);
    when(finalState.getClock()).thenReturn(clock);
    when(finalState.isLocalNodeValidator()).thenReturn(true);
    when(finalState.isLocalNodeProposerForRound(any())).thenReturn(false);

    // When the round-0 block timer fires and we are not the proposer, stay in round 0.
    when(blockTimer.checkEmptyBlockExpired(any(), anyLong())).thenReturn(true);
    when(blockCreator.createBlock(anyLong(), any()))
        .thenReturn(new QbftBlockCreator.BlockCreationResult(createdBlock, Optional.empty()));
    when(blockInterface.replaceRoundForCommitBlock(any(), anyInt())).thenReturn(createdBlock);

    final MessageValidator messageValidator = mock(MessageValidator.class);
    when(messageValidator.validateProposal(any())).thenReturn(true);
    when(messageValidator.validatePrepare(any())).thenReturn(true);
    when(messageValidator.validateCommit(any())).thenReturn(true);
    when(futureRoundProposalMessageValidator.validateProposalMessage(any())).thenReturn(true);
    when(messageValidatorFactory.createFutureRoundProposalMessageValidator(anyLong(), any()))
        .thenReturn(futureRoundProposalMessageValidator);
    when(messageValidatorFactory.createMessageValidator(any(), any())).thenReturn(messageValidator);
    // Real round-change validator: rejects non-validators, other heights and round <= 0.
    when(messageValidatorFactory.createRoundChangeMessageValidator(anyLong(), any()))
        .thenAnswer(
            invocation ->
                new RoundChangeMessageValidator(
                    new RoundChangePayloadValidator(validators, CHAIN_HEIGHT),
                    QUORUM,
                    CHAIN_HEIGHT,
                    validators,
                    protocolSchedule));

    when(roundFactory.createNewRound(any(), anyInt()))
        .thenAnswer(
            invocation -> {
              final int round = invocation.getArgument(1);
              final ConsensusRoundIdentifier roundId =
                  new ConsensusRoundIdentifier(CHAIN_HEIGHT, round);
              return newRound(new RoundState(roundId, QUORUM, messageValidator));
            });
    when(roundFactory.createNewRoundWithState(any(), any()))
        .thenAnswer(invocation -> newRound(invocation.getArgument(1)));
  }

  private QbftRound newRound(final RoundState roundState) {
    return new QbftRound(
        roundState,
        blockCreator,
        blockInterface,
        protocolSchedule,
        Subscribers.create(),
        localNodeKey,
        localAddress,
        localMessageFactory,
        roundTransmitter,
        roundTimer,
        parentHeader);
  }

  private QbftBlockHeightManager createViaFactory(final boolean earlyRoundChange) {
    final QbftBlockHeightManagerFactory factory =
        new QbftBlockHeightManagerFactory(
            finalState,
            roundFactory,
            messageValidatorFactory,
            localMessageFactory,
            validatorProvider,
            validatorModeTransitionLogger);
    factory.isEarlyRoundChangeEnabled(earlyRoundChange);
    final BaseQbftBlockHeightManager created = factory.create(parentHeader);
    assertThat(created).isInstanceOf(QbftBlockHeightManager.class);
    return (QbftBlockHeightManager) created;
  }

  /** Creates a manager via the factory and moves it from round 0 to round 1 with one expiry. */
  private QbftBlockHeightManager managerAtRoundOne(final boolean earlyRoundChange) {
    final QbftBlockHeightManager manager = createViaFactory(earlyRoundChange);
    final ConsensusRoundIdentifier roundZero = new ConsensusRoundIdentifier(CHAIN_HEIGHT, 0);
    manager.handleBlockTimerExpiry(roundZero);
    assertThat(currentRoundNumber(manager)).isEqualTo(0);
    manager.roundExpired(new RoundExpiry(roundZero));
    assertThat(currentRoundNumber(manager)).isEqualTo(1);
    reset(validatorMulticaster);
    reset(roundTransmitter);
    return manager;
  }

  private RoundChange roundChangeFrom(final int validatorIndex, final int round) {
    return validatorMessageFactories
        .get(validatorIndex)
        .createRoundChange(new ConsensusRoundIdentifier(CHAIN_HEIGHT, round), Optional.empty());
  }

  private static int currentRoundNumber(final QbftBlockHeightManager manager) {
    return manager.getCurrentRound().orElseThrow().getRoundIdentifier().getRoundNumber();
  }

  /** Round numbers of round-change messages multicast by the height manager. */
  private List<Integer> multicastRoundChangeRounds() {
    final ArgumentCaptor<MessageData> captor = ArgumentCaptor.forClass(MessageData.class);
    verify(validatorMulticaster, atLeastOnce()).send(captor.capture());
    return captor.getAllValues().stream()
        .filter(m -> m instanceof RoundChangeMessageData)
        .map(m -> ((RoundChangeMessageData) m).decode(blockEncoder))
        .map(rc -> rc.getRoundIdentifier().getRoundNumber())
        .collect(Collectors.toList());
  }

  // ---------------------------------------------------------------------------------------------
  // Factory: early round change enabled vs disabled
  // ---------------------------------------------------------------------------------------------

  @Test
  public void earlyFactoryManagerMovesToFutureRoundOnFPlusOneRoundChangesAndSendsOwnRoundChange() {
    final QbftBlockHeightManager manager = managerAtRoundOne(true);

    // With 7 validators f = (7 - 1) / 3 = 2, so the early round change threshold is f + 1 = 3.
    // After round changes for round 3 from only two validators (validators 1 and 2) the threshold
    // is not reached yet, so the current round number is still 1.
    manager.handleRoundChangePayload(roundChangeFrom(1, 3));
    manager.handleRoundChangePayload(roundChangeFrom(2, 3));
    assertThat(currentRoundNumber(manager)).isEqualTo(1);

    // The round change from validator 3 is the third (f + 1) one for round 3, so the manager jumps
    // straight to round 3 and multicasts its own round change for that round.
    manager.handleRoundChangePayload(roundChangeFrom(3, 3));

    assertThat(currentRoundNumber(manager)).isEqualTo(3);
    assertThat(multicastRoundChangeRounds()).containsExactly(3);
  }

  @Test
  public void nonEarlyFactoryManagerStaysOnSameInput() {
    final QbftBlockHeightManager manager = managerAtRoundOne(false);

    // Without early round change, f + 1 round changes have no effect: the manager stays on round 1
    // for every round change below the quorum (QUORUM - 1 = 4 validators).
    for (int validator = 1; validator < QUORUM; validator++) {
      manager.handleRoundChangePayload(roundChangeFrom(validator, 3));
      assertThat(currentRoundNumber(manager)).isEqualTo(1);
    }
    verify(validatorMulticaster, never()).send(any());

    // The quorum-th round change completes a round change certificate, which moves the manager to
    // round 3 through the normal (non-early) round change path.
    manager.handleRoundChangePayload(roundChangeFrom(QUORUM, 3));
    assertThat(currentRoundNumber(manager)).isEqualTo(3);
  }

  // ---------------------------------------------------------------------------------------------
  // f+1 threshold boundaries
  // ---------------------------------------------------------------------------------------------

  @Test
  public void fRoundChangesAreNotEnough() {
    final QbftBlockHeightManager manager = managerAtRoundOne(true);

    manager.handleRoundChangePayload(roundChangeFrom(1, 3));
    manager.handleRoundChangePayload(roundChangeFrom(2, 3));

    assertThat(currentRoundNumber(manager)).isEqualTo(1);
    verify(validatorMulticaster, never()).send(any());
  }

  @Test
  public void sameValidatorTwiceCountsOnce() {
    final QbftBlockHeightManager manager = managerAtRoundOne(true);

    manager.handleRoundChangePayload(roundChangeFrom(1, 3));
    manager.handleRoundChangePayload(roundChangeFrom(2, 3));
    manager.handleRoundChangePayload(roundChangeFrom(2, 4));

    assertThat(currentRoundNumber(manager)).isEqualTo(1);
    verify(validatorMulticaster, never()).send(any());
  }

  @Test
  public void exactDuplicateRoundChangeCountsOnce() {
    final QbftBlockHeightManager manager = managerAtRoundOne(true);
    final RoundChange fromValidatorTwo = roundChangeFrom(2, 3);

    manager.handleRoundChangePayload(roundChangeFrom(1, 3));
    manager.handleRoundChangePayload(fromValidatorTwo);
    // The exact same message (same validator, same round) received again must not count as a
    // third round change.
    manager.handleRoundChangePayload(fromValidatorTwo);

    assertThat(currentRoundNumber(manager)).isEqualTo(1);
    verify(validatorMulticaster, never()).send(any());

    // A round change from a third distinct validator still reaches f + 1 and triggers the jump.
    manager.handleRoundChangePayload(roundChangeFrom(3, 3));
    assertThat(currentRoundNumber(manager)).isEqualTo(3);
  }

  @Test
  public void roundChangeFromNonValidatorIsNotCounted() {
    final QbftBlockHeightManager manager = managerAtRoundOne(true);
    final MessageFactory outsider = new MessageFactory(NodeKeyUtils.generate(), blockEncoder);

    manager.handleRoundChangePayload(roundChangeFrom(1, 3));
    manager.handleRoundChangePayload(roundChangeFrom(2, 3));
    manager.handleRoundChangePayload(
        outsider.createRoundChange(
            new ConsensusRoundIdentifier(CHAIN_HEIGHT, 3), Optional.empty()));

    assertThat(currentRoundNumber(manager)).isEqualTo(1);
    verify(validatorMulticaster, never()).send(any());

    // A third real validator now triggers the jump, showing the outsider was what held it back.
    manager.handleRoundChangePayload(roundChangeFrom(3, 3));
    assertThat(currentRoundNumber(manager)).isEqualTo(3);
  }
}
