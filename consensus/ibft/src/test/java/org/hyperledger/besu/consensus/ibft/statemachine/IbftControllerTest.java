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

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Lists.newArrayList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.EthSynchronizerUpdater;
import org.hyperledger.besu.consensus.common.bft.MessageTracker;
import org.hyperledger.besu.consensus.common.bft.events.BftReceivedMessageEvent;
import org.hyperledger.besu.consensus.common.bft.events.BlockTimerExpiry;
import org.hyperledger.besu.consensus.common.bft.events.NewChainHead;
import org.hyperledger.besu.consensus.common.bft.events.RoundExpiry;
import org.hyperledger.besu.consensus.common.bft.statemachine.BftFinalState;
import org.hyperledger.besu.consensus.common.bft.statemachine.FutureMessageBuffer;
import org.hyperledger.besu.consensus.ibft.IbftGossip;
import org.hyperledger.besu.consensus.ibft.messagedata.CommitMessageData;
import org.hyperledger.besu.consensus.ibft.messagedata.IbftV2;
import org.hyperledger.besu.consensus.ibft.messagedata.PrepareMessageData;
import org.hyperledger.besu.consensus.ibft.messagedata.ProposalMessageData;
import org.hyperledger.besu.consensus.ibft.messagedata.RoundChangeMessageData;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Commit;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Prepare;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Proposal;
import org.hyperledger.besu.consensus.ibft.messagewrappers.RoundChange;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.DefaultMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class IbftControllerTest {
  @Mock private Blockchain blockChain;
  @Mock private BftFinalState bftFinalState;
  @Mock private IbftBlockHeightManagerFactory blockHeightManagerFactory;
  @Mock private BlockHeader chainHeadBlockHeader;
  @Mock private BlockHeader nextBlock;
  @Mock private BaseIbftBlockHeightManager blockHeightManager;

  @Mock private Proposal proposal;
  private Message proposalMessage;
  @Mock private ProposalMessageData proposalMessageData;

  @Mock private Prepare prepare;
  private Message prepareMessage;
  @Mock private PrepareMessageData prepareMessageData;

  @Mock private Commit commit;
  private Message commitMessage;
  @Mock private CommitMessageData commitMessageData;

  @Mock private RoundChange roundChange;
  private Message roundChangeMessage;
  @Mock private RoundChangeMessageData roundChangeMessageData;

  @Mock private MessageTracker messageTracker;
  private final Address validator = Address.fromHexString("0x0");
  private final Address unknownValidator = Address.fromHexString("0x2");
  private final ConsensusRoundIdentifier futureRoundIdentifier = new ConsensusRoundIdentifier(5, 0);
  private final ConsensusRoundIdentifier roundIdentifier = new ConsensusRoundIdentifier(4, 0);
  private final ConsensusRoundIdentifier pastRoundIdentifier = new ConsensusRoundIdentifier(3, 0);
  @Mock private IbftGossip ibftGossip;
  @Mock private FutureMessageBuffer<Message> futureMessageBuffer;
  private IbftController ibftController;

  @BeforeEach
  public void setup() {
    when(blockChain.getChainHeadHeader()).thenReturn(chainHeadBlockHeader);
    lenient().when(blockChain.getChainHeadBlockNumber()).thenReturn(3L);
    lenient().when(blockHeightManagerFactory.create(any())).thenReturn(blockHeightManager);
    lenient().when(bftFinalState.getValidators()).thenReturn(ImmutableList.of(validator));

    lenient().when(chainHeadBlockHeader.getNumber()).thenReturn(3L);
    lenient().when(chainHeadBlockHeader.getHash()).thenReturn(Hash.ZERO);

    lenient().when(blockHeightManager.getParentBlockHeader()).thenReturn(chainHeadBlockHeader);
    when(blockHeightManager.getChainHeight()).thenReturn(4L); // one greater than blockchain

    lenient().when(nextBlock.getNumber()).thenReturn(5L);

    lenient().when(bftFinalState.isLocalNodeValidator()).thenReturn(true);
    lenient().when(messageTracker.hasSeenMessage(any())).thenReturn(false);
  }

  private void constructIbftController() {
    ibftController =
        new IbftController(
            blockChain,
            bftFinalState,
            blockHeightManagerFactory,
            ibftGossip,
            messageTracker,
            futureMessageBuffer,
            mock(EthSynchronizerUpdater.class));
  }

  @Test
  public void createsNewBlockHeightManagerWhenStarted() {
    constructIbftController();
    verify(blockHeightManagerFactory, never()).create(chainHeadBlockHeader);
    ibftController.start();

    verify(futureMessageBuffer, never()).addMessage(anyLong(), any());
    verify(blockHeightManagerFactory).create(chainHeadBlockHeader);
  }

  @Test
  public void startsNewBlockHeightManagerAndReplaysFutureMessages() {
    setupPrepare(futureRoundIdentifier, validator);
    setupCommit(futureRoundIdentifier, validator);
    setupRoundChange(futureRoundIdentifier, validator);

    final List<Message> height2Msgs =
        newArrayList(prepareMessage, commitMessage, roundChangeMessage);
    when(blockHeightManager.getChainHeight()).thenReturn(5L);
    when(futureMessageBuffer.retrieveMessagesForHeight(5L)).thenReturn(height2Msgs);

    constructIbftController();
    ibftController.start();

    verify(futureMessageBuffer).retrieveMessagesForHeight(5L);
    verify(futureMessageBuffer, never()).retrieveMessagesForHeight(6L);
    verify(blockHeightManagerFactory).create(chainHeadBlockHeader);
    verify(blockHeightManager, atLeastOnce()).getChainHeight();
    verify(blockHeightManager, never()).handleProposalPayload(proposal);
    verify(blockHeightManager).handlePreparePayload(prepare);
    verify(ibftGossip).send(prepareMessage);
    verify(blockHeightManager).handleCommitPayload(commit);
    verify(ibftGossip).send(commitMessage);
    verify(blockHeightManager).handleRoundChangePayload(roundChange);
    verify(ibftGossip).send(roundChangeMessage);
  }

  @Test
  public void createsNewBlockHeightManagerAndReplaysFutureMessagesOnNewChainHeadEvent() {
    setupPrepare(futureRoundIdentifier, validator);
    setupProposal(futureRoundIdentifier, validator);
    setupCommit(futureRoundIdentifier, validator);
    setupRoundChange(futureRoundIdentifier, validator);

    when(futureMessageBuffer.retrieveMessagesForHeight(5L))
        .thenReturn(
            ImmutableList.of(prepareMessage, proposalMessage, commitMessage, roundChangeMessage))
        .thenReturn(Collections.emptyList());
    when(blockHeightManager.getChainHeight()).thenReturn(5L);

    constructIbftController();
    ibftController.start();
    final NewChainHead newChainHead = new NewChainHead(nextBlock);
    ibftController.handleNewBlockEvent(newChainHead);

    verify(blockHeightManagerFactory).create(nextBlock);
    verify(blockHeightManager, atLeastOnce()).getChainHeight();
    verify(futureMessageBuffer, times(2)).retrieveMessagesForHeight(5L);
    verify(blockHeightManager).handleProposalPayload(proposal);
    verify(ibftGossip).send(proposalMessage);
    verify(blockHeightManager).handlePreparePayload(prepare);
    verify(ibftGossip).send(prepareMessage);
    verify(blockHeightManager).handleCommitPayload(commit);
    verify(ibftGossip).send(commitMessage);
    verify(blockHeightManager).handleRoundChangePayload(roundChange);
    verify(ibftGossip).send(roundChangeMessage);
  }

  @Test
  public void newBlockForCurrentOrPreviousHeightTriggersNoChange() {
    constructIbftController();
    ibftController.start();
    long chainHeadHeight = 3;
    when(nextBlock.getNumber()).thenReturn(chainHeadHeight);
    when(nextBlock.getHash()).thenReturn(Hash.ZERO);
    final NewChainHead sameHeightBlock = new NewChainHead(nextBlock);
    ibftController.handleNewBlockEvent(sameHeightBlock);
    verify(blockHeightManagerFactory, times(1)).create(any()); // initial creation

    when(nextBlock.getNumber()).thenReturn(chainHeadHeight - 1);
    final NewChainHead priorBlock = new NewChainHead(nextBlock);
    ibftController.handleNewBlockEvent(priorBlock);
    verify(blockHeightManagerFactory, times(1)).create(any());
  }

  @Test
  public void handlesRoundExpiry() {
    final RoundExpiry roundExpiry = new RoundExpiry(roundIdentifier);

    constructIbftController();
    ibftController.start();
    ibftController.handleRoundExpiry(roundExpiry);

    verify(blockHeightManager).roundExpired(roundExpiry);
  }

  @Test
  public void handlesBlockTimerExpiry() {
    final BlockTimerExpiry blockTimerExpiry = new BlockTimerExpiry(roundIdentifier);

    constructIbftController();
    ibftController.start();
    ibftController.handleBlockTimerExpiry(blockTimerExpiry);

    verify(blockHeightManager).handleBlockTimerExpiry(roundIdentifier);
  }

  @Test
  public void proposalForCurrentHeightIsPassedToBlockHeightManager() {
    setupProposal(roundIdentifier, validator);
    constructIbftController();
    ibftController.start();
    ibftController.handleMessageEvent(new BftReceivedMessageEvent(proposalMessage));

    verify(futureMessageBuffer, never()).addMessage(anyLong(), any());
    verify(blockHeightManager).handleProposalPayload(proposal);
    verify(ibftGossip).send(proposalMessage);
    verify(blockHeightManager, atLeastOnce()).getChainHeight();
    verifyNoMoreInteractions(blockHeightManager);
  }

  @Test
  public void prepareForCurrentHeightIsPassedToBlockHeightManager() {
    setupPrepare(roundIdentifier, validator);
    constructIbftController();
    ibftController.start();
    ibftController.handleMessageEvent(new BftReceivedMessageEvent(prepareMessage));

    verify(futureMessageBuffer, never()).addMessage(anyLong(), any());
    verify(blockHeightManager).handlePreparePayload(prepare);
    verify(ibftGossip).send(prepareMessage);
    verify(blockHeightManager, atLeastOnce()).getChainHeight();
    verifyNoMoreInteractions(blockHeightManager);
  }

  @Test
  public void commitForCurrentHeightIsPassedToBlockHeightManager() {
    setupCommit(roundIdentifier, validator);
    constructIbftController();
    ibftController.start();
    ibftController.handleMessageEvent(new BftReceivedMessageEvent(commitMessage));

    verify(futureMessageBuffer, never()).addMessage(anyLong(), any());
    verify(blockHeightManager).handleCommitPayload(commit);
    verify(ibftGossip).send(commitMessage);
    verify(blockHeightManager, atLeastOnce()).getChainHeight();
    verifyNoMoreInteractions(blockHeightManager);
  }

  @Test
  public void roundChangeForCurrentHeightIsPassedToBlockHeightManager() {
    setupRoundChange(roundIdentifier, validator);
    constructIbftController();
    ibftController.start();
    ibftController.handleMessageEvent(new BftReceivedMessageEvent(roundChangeMessage));

    verify(futureMessageBuffer, never()).addMessage(anyLong(), any());
    verify(blockHeightManager).handleRoundChangePayload(roundChange);
    verify(ibftGossip).send(roundChangeMessage);
    verify(blockHeightManager, atLeastOnce()).getChainHeight();
    verifyNoMoreInteractions(blockHeightManager);
  }

  @Test
  public void proposalForPastHeightIsDiscarded() {
    setupProposal(pastRoundIdentifier, validator);
    verifyNotHandledAndNoFutureMsgs(new BftReceivedMessageEvent(proposalMessage));
  }

  @Test
  public void prepareForPastHeightIsDiscarded() {
    setupPrepare(pastRoundIdentifier, validator);
    verifyNotHandledAndNoFutureMsgs(new BftReceivedMessageEvent(prepareMessage));
  }

  @Test
  public void commitForPastHeightIsDiscarded() {
    setupCommit(pastRoundIdentifier, validator);
    verifyNotHandledAndNoFutureMsgs(new BftReceivedMessageEvent(commitMessage));
  }

  @Test
  public void roundChangeForPastHeightIsDiscarded() {
    setupRoundChange(pastRoundIdentifier, validator);
    verifyNotHandledAndNoFutureMsgs(new BftReceivedMessageEvent(roundChangeMessage));
  }

  @Test
  public void roundExpiryForPastHeightIsDiscarded() {
    final RoundExpiry roundExpiry = new RoundExpiry(pastRoundIdentifier);
    constructIbftController();
    ibftController.start();
    ibftController.handleRoundExpiry(roundExpiry);
    verify(futureMessageBuffer, never()).addMessage(anyLong(), any());
    verify(blockHeightManager, never()).roundExpired(any());
  }

  @Test
  public void blockTimerForPastHeightIsDiscarded() {
    final BlockTimerExpiry blockTimerExpiry = new BlockTimerExpiry(pastRoundIdentifier);
    constructIbftController();
    ibftController.start();
    ibftController.handleBlockTimerExpiry(blockTimerExpiry);
    verify(futureMessageBuffer, never()).addMessage(anyLong(), any());
    verify(blockHeightManager, never()).handleBlockTimerExpiry(any());
  }

  @Test
  public void proposalForUnknownValidatorIsDiscarded() {
    setupProposal(roundIdentifier, unknownValidator);
    verifyNotHandledAndNoFutureMsgs(new BftReceivedMessageEvent(proposalMessage));
  }

  @Test
  public void prepareForUnknownValidatorIsDiscarded() {
    setupPrepare(roundIdentifier, unknownValidator);
    verifyNotHandledAndNoFutureMsgs(new BftReceivedMessageEvent(prepareMessage));
  }

  @Test
  public void commitForUnknownValidatorIsDiscarded() {
    setupCommit(roundIdentifier, unknownValidator);
    verifyNotHandledAndNoFutureMsgs(new BftReceivedMessageEvent(commitMessage));
  }

  @Test
  public void roundChangeForUnknownValidatorIsDiscarded() {
    setupRoundChange(roundIdentifier, unknownValidator);
    verifyNotHandledAndNoFutureMsgs(new BftReceivedMessageEvent(roundChangeMessage));
  }

  @Test
  public void proposalForFutureHeightIsBuffered() {
    setupProposal(futureRoundIdentifier, validator);
    verifyHasFutureMessages(futureRoundIdentifier.getSequenceNumber(), proposalMessage);
  }

  @Test
  public void prepareForFutureHeightIsBuffered() {
    setupPrepare(futureRoundIdentifier, validator);
    verifyHasFutureMessages(futureRoundIdentifier.getSequenceNumber(), prepareMessage);
  }

  @Test
  public void commitForFutureHeightIsBuffered() {
    setupCommit(futureRoundIdentifier, validator);
    verifyHasFutureMessages(futureRoundIdentifier.getSequenceNumber(), commitMessage);
  }

  @Test
  public void roundChangeForFutureHeightIsBuffered() {
    setupRoundChange(futureRoundIdentifier, validator);
    verifyHasFutureMessages(futureRoundIdentifier.getSequenceNumber(), roundChangeMessage);
  }

  @Test
  public void duplicatedMessagesAreNotProcessed() {
    when(messageTracker.hasSeenMessage(proposalMessageData)).thenReturn(true);
    setupProposal(roundIdentifier, validator);
    verifyNotHandledAndNoFutureMsgs(new BftReceivedMessageEvent(proposalMessage));
    verify(messageTracker, never()).addSeenMessage(proposalMessageData);
  }

  @Test
  public void uniqueMessagesAreAddedAsSeen() {
    when(messageTracker.hasSeenMessage(proposalMessageData)).thenReturn(false);
    setupProposal(roundIdentifier, validator);
    constructIbftController();
    ibftController.start();
    ibftController.handleMessageEvent(new BftReceivedMessageEvent(proposalMessage));

    verify(messageTracker).addSeenMessage(proposalMessageData);
  }

  @Test
  public void messagesWhichAreAboveHeightManagerButBelowBlockChainLengthAreDiscarded() {
    // NOTE: for this to occur, the system would need to be synchronising - i.e. blockchain is
    // moving up faster than ibft loop is handling NewBlock messages
    final long blockchainLength = 10L;
    final long blockHeightManagerTargettingBlock = 6L;
    final long messageHeight = 8L;
    setupProposal(new ConsensusRoundIdentifier(messageHeight, 0), validator);

    when(blockChain.getChainHeadHeader()).thenReturn(chainHeadBlockHeader);
    when(blockChain.getChainHeadBlockNumber()).thenReturn(blockchainLength);
    when(blockHeightManagerFactory.create(any())).thenReturn(blockHeightManager);
    when(blockHeightManager.getChainHeight()).thenReturn(blockHeightManagerTargettingBlock);

    constructIbftController();
    ibftController.start();
    ibftController.handleMessageEvent(new BftReceivedMessageEvent(proposalMessage));
    verify(futureMessageBuffer, never()).addMessage(anyLong(), any());
    verify(blockHeightManager, never()).handleProposalPayload(any());
  }

  private void verifyNotHandledAndNoFutureMsgs(final BftReceivedMessageEvent msg) {
    constructIbftController();
    ibftController.start();
    ibftController.handleMessageEvent(msg);

    verify(futureMessageBuffer, never()).addMessage(anyLong(), any());
    verify(blockHeightManager, atLeastOnce()).getChainHeight();
    verifyNoMoreInteractions(blockHeightManager);
  }

  private void verifyHasFutureMessages(final long msgHeight, final Message message) {
    constructIbftController();
    ibftController.start();
    ibftController.handleMessageEvent(new BftReceivedMessageEvent(message));

    verify(futureMessageBuffer).addMessage(msgHeight, message);
    verify(blockHeightManager, atLeastOnce()).getChainHeight();
    verifyNoMoreInteractions(blockHeightManager);
  }

  private void setupProposal(
      final ConsensusRoundIdentifier roundIdentifier, final Address validator) {
    lenient().when(proposal.getAuthor()).thenReturn(validator);
    lenient().when(proposal.getRoundIdentifier()).thenReturn(roundIdentifier);
    lenient().when(proposalMessageData.getCode()).thenReturn(IbftV2.PROPOSAL);
    lenient().when(proposalMessageData.decode(anyInt())).thenReturn(proposal);
    proposalMessage = new DefaultMessage(null, proposalMessageData);
  }

  private void setupPrepare(
      final ConsensusRoundIdentifier roundIdentifier, final Address validator) {
    lenient().when(prepare.getAuthor()).thenReturn(validator);
    when(prepare.getRoundIdentifier()).thenReturn(roundIdentifier);
    when(prepareMessageData.getCode()).thenReturn(IbftV2.PREPARE);
    when(prepareMessageData.decode()).thenReturn(prepare);
    prepareMessage = new DefaultMessage(null, prepareMessageData);
  }

  private void setupCommit(
      final ConsensusRoundIdentifier roundIdentifier, final Address validator) {
    lenient().when(commit.getAuthor()).thenReturn(validator);
    when(commit.getRoundIdentifier()).thenReturn(roundIdentifier);
    when(commitMessageData.getCode()).thenReturn(IbftV2.COMMIT);
    when(commitMessageData.decode()).thenReturn(commit);
    commitMessage = new DefaultMessage(null, commitMessageData);
  }

  private void setupRoundChange(
      final ConsensusRoundIdentifier roundIdentifier, final Address validator) {
    lenient().when(roundChange.getAuthor()).thenReturn(validator);
    when(roundChange.getRoundIdentifier()).thenReturn(roundIdentifier);
    when(roundChangeMessageData.getCode()).thenReturn(IbftV2.ROUND_CHANGE);
    when(roundChangeMessageData.decode()).thenReturn(roundChange);
    roundChangeMessage = new DefaultMessage(null, roundChangeMessageData);
  }

  @Test
  public void heightManagerCanOnlyBeStartedOnceIfNotStopped() {
    constructIbftController();
    ibftController.start();
    assertThatThrownBy(() -> ibftController.start())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Attempt to start new height manager without stopping previous manager");
  }

  @Test
  public void heightManagerCanBeRestartedIfStopped() {
    constructIbftController();
    ibftController.start();
    ibftController.stop();
    assertThatNoException().isThrownBy(() -> ibftController.start());
  }
}
