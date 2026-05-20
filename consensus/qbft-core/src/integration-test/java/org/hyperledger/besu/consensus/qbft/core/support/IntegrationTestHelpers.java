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
package org.hyperledger.besu.consensus.qbft.core.support;

import org.hyperledger.besu.consensus.common.bft.BftBlockInterface;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.qbft.QbftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.adaptor.QbftBlockInterfaceAdaptor;
import org.hyperledger.besu.consensus.qbft.core.payload.CommitPayload;
import org.hyperledger.besu.consensus.qbft.core.payload.MessageFactory;
import org.hyperledger.besu.consensus.qbft.core.statemachine.PreparedCertificate;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCodec;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockInterface;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.cryptoservices.NodeKey;

import org.apache.tuweni.bytes.Bytes32;

public class IntegrationTestHelpers {

  public static SignedData<CommitPayload> createSignedCommitPayload(
      final ConsensusRoundIdentifier roundId,
      final QbftBlock block,
      final NodeKey nodeKey,
      final QbftBlockCodec blockEncoder) {

    final QbftBlock commitBlock =
        createCommitBlockFromProposalBlock(block, roundId.getRoundNumber());
    final SECPSignature commitSeal = nodeKey.sign(Bytes32.wrap(commitBlock.getHash().getBytes()));

    final MessageFactory messageFactory = new MessageFactory(nodeKey, blockEncoder);

    return messageFactory.createCommit(roundId, block.getHash(), commitSeal).getSignedPayload();
  }

  public static PreparedCertificate createValidPreparedCertificate(
      final TestContext context,
      final ConsensusRoundIdentifier preparedRound,
      final QbftBlock block) {
    final RoundSpecificPeers peers = context.roundSpecificPeers(preparedRound);

    return new PreparedCertificate(
        block,
        peers.createSignedPreparePayloadOfAllPeers(preparedRound, block.getHash()),
        preparedRound.getRoundNumber());
  }

  public static QbftBlock createCommitBlockFromProposalBlock(
      final QbftBlock proposalBlock, final int round) {
    final QbftExtraDataCodec bftExtraDataCodec = new QbftExtraDataCodec();
    final BftBlockInterface bftBlockInterface = new BftBlockInterface(bftExtraDataCodec);
    final QbftBlockInterface qbftBlockInterface = new QbftBlockInterfaceAdaptor(bftBlockInterface);
    return qbftBlockInterface.replaceRoundForCommitBlock(proposalBlock, round);
  }
}
