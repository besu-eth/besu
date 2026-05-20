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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.consensus.ibft.support.IntegrationTestHelpers.createSignedCommitPayload;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.inttest.NodeParams;
import org.hyperledger.besu.consensus.ibft.messagedata.IbftV2;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Commit;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Prepare;
import org.hyperledger.besu.consensus.ibft.payload.MessageFactory;
import org.hyperledger.besu.consensus.ibft.support.RoundSpecificPeers;
import org.hyperledger.besu.consensus.ibft.support.TestContext;
import org.hyperledger.besu.consensus.ibft.support.TestContextBuilder;
import org.hyperledger.besu.consensus.ibft.support.ValidatorPeer;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SpuriousBehaviourTest {

  private final long blockTimeStamp = 100;
  private final Clock fixedClock =
      Clock.fixed(Instant.ofEpochSecond(blockTimeStamp), ZoneId.systemDefault());

  // Test is configured such that a remote peer is responsible for proposing a block
  private final int NETWORK_SIZE = 5;

  // Configuration ensures remote peer will provide proposal for first block
  private final TestContext context =
      new TestContextBuilder()
          .validatorCount(NETWORK_SIZE)
          .indexOfFirstLocallyProposedBlock(0)
          .clock(fixedClock)
          .buildAndStart();
  private final ConsensusRoundIdentifier roundId = new ConsensusRoundIdentifier(1, 0);
  private final RoundSpecificPeers peers = context.roundSpecificPeers(roundId);

  private final Block proposedBlock = context.createBlockForProposalFromChainHead(0, 30);
  private Prepare expectedPrepare;
  private Commit expectedCommit;

  @BeforeEach
  public void setup() {

    expectedPrepare =
        context.getLocalNodeMessageFactory().createPrepare(roundId, proposedBlock.getHash());
    expectedCommit =
        new Commit(
            createSignedCommitPayload(
                roundId, proposedBlock, context.getLocalNodeParams().getNodeKey()));
  }

  @Test
  public void badlyFormedRlpDoesNotPreventOngoingIbftOperation() {
    final MessageData illegalCommitMsg = new RawMessage(IbftV2.PREPARE, Bytes.EMPTY);
    peers.getNonProposing(0).injectMessage(illegalCommitMsg);

    peers.getProposer().injectProposal(roundId, proposedBlock);
    peers.verifyMessagesReceived(expectedPrepare);
  }

  @Test
  public void messageWithIllegalMessageCodeAreDiscardedAndDoNotPreventOngoingIbftOperation() {
    final MessageData illegalCommitMsg = new RawMessage(IbftV2.MESSAGE_SPACE, Bytes.EMPTY);
    peers.getNonProposing(0).injectMessage(illegalCommitMsg);

    peers.getProposer().injectProposal(roundId, proposedBlock);
    peers.verifyMessagesReceived(expectedPrepare);
  }

  @Test
  public void nonValidatorsCannotTriggerResponses() {
    final NodeKey nonValidatorNodeKey = NodeKeyUtils.generate();
    final NodeParams nonValidatorParams =
        new NodeParams(
            Util.publicKeyToAddress(nonValidatorNodeKey.getPublicKey()), nonValidatorNodeKey);

    final ValidatorPeer nonvalidator =
        new ValidatorPeer(
            nonValidatorParams,
            new MessageFactory(nonValidatorParams.getNodeKey()),
            context.getEventMultiplexer());

    nonvalidator.injectProposal(new ConsensusRoundIdentifier(1, 0), proposedBlock);

    peers.verifyNoMessagesReceived();
  }

  @Test
  public void preparesWithMisMatchedDigestAreNotRepondedTo() {
    peers.getProposer().injectProposal(roundId, proposedBlock);
    peers.verifyMessagesReceived(expectedPrepare);

    peers.prepareForNonProposing(roundId, Hash.ZERO);
    peers.verifyNoMessagesReceived();

    peers.prepareForNonProposing(roundId, proposedBlock.getHash());
    peers.verifyMessagesReceived(expectedCommit);

    peers.prepareForNonProposing(roundId, Hash.ZERO);
    assertThat(context.getCurrentChainHeight()).isEqualTo(0);

    peers.commitForNonProposing(roundId, proposedBlock.getHash());
    assertThat(context.getCurrentChainHeight()).isEqualTo(1);
  }

  @Test
  public void oneCommitSealIsIllegalPreventsImport() {
    peers.getProposer().injectProposal(roundId, proposedBlock);
    peers.verifyMessagesReceived(expectedPrepare);

    peers.prepareForNonProposing(roundId, proposedBlock.getHash());

    // for a network of 5, 4 seals are required (local + 3 remote)
    peers.getNonProposing(0).injectCommit(roundId, proposedBlock.getHash());
    peers.getNonProposing(1).injectCommit(roundId, proposedBlock.getHash());

    // nonProposer-2 will generate an invalid seal
    final ValidatorPeer badSealPeer = peers.getNonProposing(2);
    final SECPSignature illegalSeal = badSealPeer.getnodeKey().sign(Bytes32.ZERO);

    badSealPeer.injectCommit(roundId, proposedBlock.getHash(), illegalSeal);
    assertThat(context.getCurrentChainHeight()).isEqualTo(0);

    // Now inject the REAL commit message
    badSealPeer.injectCommit(roundId, proposedBlock.getHash());
    assertThat(context.getCurrentChainHeight()).isEqualTo(1);
  }
}
