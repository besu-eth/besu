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

import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.ProposedBlockHelpers;
import org.hyperledger.besu.consensus.ibft.messagewrappers.RoundChange;
import org.hyperledger.besu.consensus.ibft.payload.MessageFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.ethereum.core.Block;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RoundChangeArtifactsTest {

  private final List<MessageFactory> messageFactories = Lists.newArrayList();

  private final long chainHeight = 5;
  private final ConsensusRoundIdentifier targetRound = new ConsensusRoundIdentifier(chainHeight, 5);

  @BeforeEach
  public void setup() {
    for (int i = 0; i < 4; i++) {
      final NodeKey nodeKey = NodeKeyUtils.generate();
      final MessageFactory messageFactory = new MessageFactory(nodeKey);
      messageFactories.add(messageFactory);
    }
  }

  private PreparedRoundArtifacts createPreparedRoundArtefacts(final int fromRound) {

    final ConsensusRoundIdentifier preparedRound =
        new ConsensusRoundIdentifier(chainHeight, fromRound);
    final Block block = ProposedBlockHelpers.createProposalBlock(emptyList(), preparedRound);

    return new PreparedRoundArtifacts(
        messageFactories.get(0).createProposal(preparedRound, block, Optional.empty()),
        messageFactories.stream()
            .map(factory -> factory.createPrepare(preparedRound, block.getHash()))
            .collect(Collectors.toList()));
  }

  private RoundChange createRoundChange(
      final int fromRound, final boolean containsPrepareCertificate) {
    if (containsPrepareCertificate) {
      return messageFactories
          .get(0)
          .createRoundChange(targetRound, Optional.of(createPreparedRoundArtefacts(fromRound)));
    } else {
      return messageFactories.get(0).createRoundChange(targetRound, empty());
    }
  }

  @Test
  public void newestBlockIsExtractedFromListOfRoundChangeMessages() {
    final List<RoundChange> roundChanges =
        Lists.newArrayList(
            createRoundChange(1, true), createRoundChange(2, true), createRoundChange(3, false));

    RoundChangeArtifacts artifacts = RoundChangeArtifacts.create(roundChanges);

    assertThat(artifacts.getBlock()).isEqualTo(roundChanges.get(1).getProposedBlock());

    roundChanges.add(createRoundChange(4, true));
    artifacts = RoundChangeArtifacts.create(roundChanges);
    assertThat(artifacts.getBlock()).isEqualTo(roundChanges.get(3).getProposedBlock());
    assertThat(artifacts.getRoundChangeCertificate().getRoundChangePayloads())
        .containsExactly(
            roundChanges.get(0).getSignedPayload(),
            roundChanges.get(1).getSignedPayload(),
            roundChanges.get(2).getSignedPayload(),
            roundChanges.get(3).getSignedPayload());
  }

  @Test
  public void noRoundChangesPreparedThereforeReportedBlockIsEmpty() {
    final List<RoundChange> roundChanges =
        Lists.newArrayList(
            createRoundChange(1, false), createRoundChange(2, false), createRoundChange(3, false));

    final RoundChangeArtifacts artifacts = RoundChangeArtifacts.create(roundChanges);

    assertThat(artifacts.getBlock()).isEmpty();
  }
}
