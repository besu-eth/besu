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
package org.hyperledger.besu.consensus.ibft.tests;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import org.hyperledger.besu.consensus.common.bft.BftHelpers;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.events.NewChainHead;
import org.hyperledger.besu.consensus.ibft.IbftExtraDataCodec;
import org.hyperledger.besu.consensus.ibft.messagedata.ProposalMessageData;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Commit;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Prepare;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Proposal;
import org.hyperledger.besu.consensus.ibft.messagewrappers.RoundChange;
import org.hyperledger.besu.consensus.ibft.payload.MessageFactory;
import org.hyperledger.besu.consensus.ibft.payload.RoundChangeCertificate;
import org.hyperledger.besu.consensus.ibft.support.RoundSpecificPeers;
import org.hyperledger.besu.consensus.ibft.support.TestContext;
import org.hyperledger.besu.consensus.ibft.support.TestContextBuilder;
import org.hyperledger.besu.consensus.ibft.support.ValidatorPeer;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.ethereum.core.Block;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GossipTest {

  private final long blockTimeStamp = 100;
  private final Clock fixedClock =
      Clock.fixed(Instant.ofEpochSecond(blockTimeStamp), ZoneId.systemDefault());

  private final int NETWORK_SIZE = 5;

  private final TestContext context =
      new TestContextBuilder()
          .validatorCount(NETWORK_SIZE)
          .indexOfFirstLocallyProposedBlock(0)
          .clock(fixedClock)
          .useGossip(true)
          .buildAndStart();

  private final ConsensusRoundIdentifier roundId = new ConsensusRoundIdentifier(1, 0);
  private final RoundSpecificPeers peers = context.roundSpecificPeers(roundId);
  private Block block;
  private ValidatorPeer sender;
  private MessageFactory msgFactory;

  @BeforeEach
  public void setup() {
    block = context.createBlockForProposalFromChainHead(roundId.getRoundNumber(), 30);
    sender = peers.getProposer();
    msgFactory = sender.getMessageFactory();
  }

  @Test
  public void gossipMessagesToPeers() {
    final Prepare localPrepare =
        context.getLocalNodeMessageFactory().createPrepare(roundId, block.getHash());
    peers.verifyNoMessagesReceivedNonProposing();

    final Proposal proposal = sender.injectProposal(roundId, block);
    // sender node will have a prepare message as an effect of the proposal being sent
    peers.verifyMessagesReceivedNonPropsing(proposal, localPrepare);
    peers.verifyMessagesReceivedProposer(localPrepare);

    final Prepare prepare = sender.injectPrepare(roundId, block.getHash());
    peers.verifyMessagesReceivedNonPropsing(prepare);
    peers.verifyNoMessagesReceivedProposer();

    final Commit commit = sender.injectCommit(roundId, block.getHash());
    peers.verifyMessagesReceivedNonPropsing(commit);
    peers.verifyNoMessagesReceivedProposer();

    final RoundChange roundChange = msgFactory.createRoundChange(roundId, Optional.empty());
    final RoundChangeCertificate roundChangeCert =
        new RoundChangeCertificate(singletonList(roundChange.getSignedPayload()));

    final Proposal nextRoundProposal =
        sender.injectProposalForFutureRound(roundId, roundChangeCert, proposal.getBlock());
    peers.verifyMessagesReceivedNonPropsing(nextRoundProposal);
    peers.verifyNoMessagesReceivedProposer();

    sender.injectRoundChange(roundId, Optional.empty());
    peers.verifyMessagesReceivedNonPropsing(roundChange);
    peers.verifyNoMessagesReceivedProposer();
  }

  @Test
  public void onlyGossipOnce() {
    final Prepare prepare = sender.injectPrepare(roundId, block.getHash());
    peers.verifyMessagesReceivedNonPropsing(prepare);

    sender.injectPrepare(roundId, block.getHash());
    peers.verifyNoMessagesReceivedNonProposing();

    sender.injectPrepare(roundId, block.getHash());
    peers.verifyNoMessagesReceivedNonProposing();
  }

  @Test
  public void messageWithUnknownValidatorIsNotGossiped() {
    final MessageFactory unknownMsgFactory = new MessageFactory(NodeKeyUtils.generate());
    final Proposal unknownProposal =
        unknownMsgFactory.createProposal(roundId, block, Optional.empty());

    sender.injectMessage(ProposalMessageData.create(unknownProposal));
    peers.verifyNoMessagesReceived();
  }

  @Test
  public void messageIsNotGossipedToSenderOrCreator() {
    final ValidatorPeer msgCreator = peers.getFirstNonProposer();
    final MessageFactory peerMsgFactory = msgCreator.getMessageFactory();
    final Proposal proposalFromPeer =
        peerMsgFactory.createProposal(roundId, block, Optional.empty());

    sender.injectMessage(ProposalMessageData.create(proposalFromPeer));

    peers.verifyMessagesReceivedNonPropsingExcluding(msgCreator, proposalFromPeer);
    peers.verifyNoMessagesReceivedProposer();
  }

  @Test
  public void futureMessageIsNotGossipedImmediately() {
    ConsensusRoundIdentifier futureRoundId = new ConsensusRoundIdentifier(2, 0);
    msgFactory.createProposal(futureRoundId, block, Optional.empty());

    sender.injectProposal(futureRoundId, block);
    peers.verifyNoMessagesReceived();
  }

  @Test
  public void previousHeightMessageIsNotGossiped() {
    final ConsensusRoundIdentifier futureRoundId = new ConsensusRoundIdentifier(0, 0);
    sender.injectProposal(futureRoundId, block);
    peers.verifyNoMessagesReceived();
  }

  @Test
  public void futureMessageGetGossipedLater() {
    final Block signedCurrentHeightBlock =
        BftHelpers.createSealedBlock(
            new IbftExtraDataCodec(), block, 0, peers.sign(block.getHash()));

    ConsensusRoundIdentifier futureRoundId = new ConsensusRoundIdentifier(2, 0);
    Prepare futurePrepare = sender.injectPrepare(futureRoundId, block.getHash());
    peers.verifyNoMessagesReceivedNonProposing();
    ;

    // add block to chain so we can move to next block height
    context.getBlockchain().appendBlock(signedCurrentHeightBlock, emptyList());
    context
        .getController()
        .handleNewBlockEvent(new NewChainHead(signedCurrentHeightBlock.getHeader()));

    peers.verifyMessagesReceivedNonPropsing(futurePrepare);
  }
}
