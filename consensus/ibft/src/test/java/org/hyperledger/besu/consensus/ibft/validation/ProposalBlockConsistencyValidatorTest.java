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

import org.hyperledger.besu.consensus.common.bft.BftBlockHeaderFunctions;
import org.hyperledger.besu.consensus.common.bft.BftBlockInterface;
import org.hyperledger.besu.consensus.common.bft.BftExtraDataCodec;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundHelpers;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.ProposedBlockHelpers;
import org.hyperledger.besu.consensus.ibft.IbftExtraDataCodec;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Proposal;
import org.hyperledger.besu.consensus.ibft.payload.MessageFactory;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.ethereum.core.Block;

import java.util.Collections;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ProposalBlockConsistencyValidatorTest {

  private final MessageFactory proposerMessageFactory = new MessageFactory(NodeKeyUtils.generate());
  private final long chainHeight = 2;
  private final ConsensusRoundIdentifier roundIdentifier =
      new ConsensusRoundIdentifier(chainHeight, 4);

  private final BftExtraDataCodec bftExtraDataCodec = new IbftExtraDataCodec();
  private final BftBlockInterface bftBlockInterface = new BftBlockInterface(bftExtraDataCodec);
  private final Block block =
      ProposedBlockHelpers.createProposalBlock(
          Collections.emptyList(), roundIdentifier, bftExtraDataCodec);
  private ProposalBlockConsistencyValidator consistencyChecker;

  @BeforeEach
  public void setup() {

    consistencyChecker = new ProposalBlockConsistencyValidator();
  }

  @Test
  public void blockDigestMisMatchWithMessageRoundFails() {
    final Proposal proposalMsg =
        proposerMessageFactory.createProposal(roundIdentifier, block, Optional.empty());

    final IbftExtraDataCodec bftExtraDataEncoder = new IbftExtraDataCodec();
    final Block misMatchedBlock =
        bftBlockInterface.replaceRoundInBlock(
            block,
            roundIdentifier.getRoundNumber() + 1,
            BftBlockHeaderFunctions.forCommittedSeal(bftExtraDataEncoder));

    assertThat(
            consistencyChecker.validateProposalMatchesBlock(
                proposalMsg.getSignedPayload(), misMatchedBlock, bftBlockInterface))
        .isFalse();
  }

  @Test
  public void blockDigestMatchesButRoundDiffersFails() {
    final ConsensusRoundIdentifier futureRound =
        ConsensusRoundHelpers.createFrom(roundIdentifier, 0, +1);
    final Proposal proposalMsg =
        proposerMessageFactory.createProposal(futureRound, block, Optional.empty());

    assertThat(
            consistencyChecker.validateProposalMatchesBlock(
                proposalMsg.getSignedPayload(), block, bftBlockInterface))
        .isFalse();
  }

  @Test
  public void blockWithMismatchedNumberFails() {
    final ConsensusRoundIdentifier futureHeight =
        ConsensusRoundHelpers.createFrom(roundIdentifier, +1, 0);
    final Proposal proposalMsg =
        proposerMessageFactory.createProposal(futureHeight, block, Optional.empty());

    assertThat(
            consistencyChecker.validateProposalMatchesBlock(
                proposalMsg.getSignedPayload(), block, bftBlockInterface))
        .isFalse();
  }
}
