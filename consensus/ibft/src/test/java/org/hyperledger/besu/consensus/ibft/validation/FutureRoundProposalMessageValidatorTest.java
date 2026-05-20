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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundHelpers;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.ProposedBlockHelpers;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Proposal;
import org.hyperledger.besu.consensus.ibft.payload.MessageFactory;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FutureRoundProposalMessageValidatorTest {

  private final MessageFactory messageFactoy = new MessageFactory(NodeKeyUtils.generate());
  private final ConsensusRoundIdentifier roundIdentifier = new ConsensusRoundIdentifier(1, 1);
  private final Block proposedBlock =
      ProposedBlockHelpers.createProposalBlock(emptyList(), roundIdentifier);

  private FutureRoundProposalMessageValidator validator;

  private final MessageValidatorFactory messageValidatorFactory =
      mock(MessageValidatorFactory.class);
  private final MessageValidator messageValidator = mock(MessageValidator.class);

  @BeforeEach
  public void setup() {

    when(messageValidatorFactory.createMessageValidator(any(), any())).thenReturn(messageValidator);
    when(messageValidator.validateProposal(any())).thenReturn(true);

    final BlockHeader parentHeader = mock(BlockHeader.class);

    validator =
        new FutureRoundProposalMessageValidator(
            messageValidatorFactory, roundIdentifier.getSequenceNumber(), parentHeader);
  }

  @Test
  public void validProposalMatchingCurrentChainHeightPassesValidation() {
    final Proposal proposal =
        messageFactoy.createProposal(roundIdentifier, proposedBlock, Optional.empty());

    assertThat(validator.validateProposalMessage(proposal)).isTrue();
  }

  @Test
  public void proposalTargettingDifferentChainHeightFailsValidation() {
    final ConsensusRoundIdentifier futureChainIdentifier =
        ConsensusRoundHelpers.createFrom(roundIdentifier, 1, 0);
    final Proposal proposal =
        messageFactoy.createProposal(futureChainIdentifier, proposedBlock, Optional.empty());

    assertThat(validator.validateProposalMessage(proposal)).isFalse();
  }

  @Test
  public void proposalWhichFailsMessageValidationFailsFutureRoundValidation() {
    final Proposal proposal =
        messageFactoy.createProposal(roundIdentifier, proposedBlock, Optional.empty());
    when(messageValidator.validateProposal(any())).thenReturn(false);

    assertThat(validator.validateProposalMessage(proposal)).isFalse();
  }
}
