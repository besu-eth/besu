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
package org.hyperledger.besu.consensus.ibft;

import static java.util.Collections.singletonList;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.ProposedBlockHelpers;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Proposal;
import org.hyperledger.besu.consensus.ibft.messagewrappers.RoundChange;
import org.hyperledger.besu.consensus.ibft.payload.MessageFactory;
import org.hyperledger.besu.consensus.ibft.statemachine.PreparedRoundArtifacts;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.core.AddressHelpers;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.Optional;

public class TestHelpers {

  public static Proposal createSignedProposalPayload(final NodeKey signerNodeKey) {
    return createSignedProposalPayloadWithRound(signerNodeKey, 0xFEDCBA98, Optional.empty());
  }

  public static Proposal createSignedProposalPayload(
      final NodeKey signerNodeKey, final Optional<BlockAccessList> blockAccessList) {
    return createSignedProposalPayloadWithRound(signerNodeKey, 0xFEDCBA98, blockAccessList);
  }

  public static Proposal createSignedProposalPayloadWithRound(
      final NodeKey signerNodeKey,
      final int round,
      final Optional<BlockAccessList> blockAccessList) {
    final MessageFactory messageFactory = new MessageFactory(signerNodeKey);
    final ConsensusRoundIdentifier roundIdentifier =
        new ConsensusRoundIdentifier(0x1234567890ABCDEFL, round);
    final Block block =
        ProposedBlockHelpers.createProposalBlock(
            singletonList(AddressHelpers.ofValue(1)), roundIdentifier);
    return messageFactory.createProposal(roundIdentifier, block, blockAccessList, Optional.empty());
  }

  public static RoundChange createSignedRoundChangePayload(final NodeKey signerKeys) {
    final MessageFactory messageFactory = new MessageFactory(signerKeys);
    final ConsensusRoundIdentifier roundIdentifier =
        new ConsensusRoundIdentifier(0x1234567890ABCDEFL, 0xFEDCBA98);
    return messageFactory.createRoundChange(roundIdentifier, Optional.empty());
  }

  public static RoundChange createSignedRoundChangePayload(
      final NodeKey signerKeys, final Optional<PreparedRoundArtifacts> preparedRoundArtifacts) {
    final MessageFactory messageFactory = new MessageFactory(signerKeys);
    final ConsensusRoundIdentifier roundIdentifier =
        new ConsensusRoundIdentifier(0x1234567890ABCDEFL, 0xFEDCBA98);
    return messageFactory.createRoundChange(roundIdentifier, preparedRoundArtifacts);
  }
}
