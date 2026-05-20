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
package org.hyperledger.besu.consensus.ibft.support;

import org.hyperledger.besu.consensus.common.bft.BftBlockHashing;
import org.hyperledger.besu.consensus.common.bft.BftExtraData;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.ibft.IbftExtraDataCodec;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Prepare;
import org.hyperledger.besu.consensus.ibft.payload.CommitPayload;
import org.hyperledger.besu.consensus.ibft.payload.MessageFactory;
import org.hyperledger.besu.consensus.ibft.statemachine.PreparedRoundArtifacts;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.core.Block;

import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes32;

public class IntegrationTestHelpers {

  public static SignedData<CommitPayload> createSignedCommitPayload(
      final ConsensusRoundIdentifier roundId, final Block block, final NodeKey nodeKey) {

    final IbftExtraDataCodec ibftExtraDataEncoder = new IbftExtraDataCodec();
    final BftExtraData extraData = ibftExtraDataEncoder.decode(block.getHeader());

    final SECPSignature commitSeal =
        nodeKey.sign(
            Bytes32.wrap(
                new BftBlockHashing(ibftExtraDataEncoder)
                    .calculateDataHashForCommittedSeal(block.getHeader(), extraData)
                    .getBytes()));

    final MessageFactory messageFactory = new MessageFactory(nodeKey);

    return messageFactory.createCommit(roundId, block.getHash(), commitSeal).getSignedPayload();
  }

  public static PreparedRoundArtifacts createValidPreparedRoundArtifacts(
      final TestContext context, final ConsensusRoundIdentifier preparedRound, final Block block) {
    final RoundSpecificPeers peers = context.roundSpecificPeers(preparedRound);

    return new PreparedRoundArtifacts(
        peers
            .getProposer()
            .getMessageFactory()
            .createProposal(preparedRound, block, Optional.empty()),
        peers.createSignedPreparePayloadOfNonProposing(preparedRound, block.getHash()).stream()
            .map(Prepare::new)
            .collect(Collectors.toList()));
  }
}
