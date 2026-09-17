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
package org.hyperledger.besu.ethereum.eth.manager.peertask.task;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.peertask.InvalidPeerTaskResponseException;
import org.hyperledger.besu.ethereum.eth.manager.task.BodyIdentifier;
import org.hyperledger.besu.ethereum.eth.messages.BlockBodiesMessage;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements PeerTask for getting block bodies from peers, and matches headers to bodies to supply
 * full blocks
 */
public class GetBodiesFromPeerTask extends AbstractGetBodiesFromPeerTask<Block> {

  private static final Logger LOG = LoggerFactory.getLogger(GetBodiesFromPeerTask.class);

  private static final int DEFAULT_RETRIES_AGAINST_OTHER_PEERS = 5;

  private final List<Block> blocks = new ArrayList<>();

  public GetBodiesFromPeerTask(
      final List<BlockHeader> blockHeaders, final ProtocolSchedule protocolSchedule) {
    this(blockHeaders, protocolSchedule, DEFAULT_RETRIES_AGAINST_OTHER_PEERS);
  }

  public GetBodiesFromPeerTask(
      final List<BlockHeader> blockHeaders,
      final ProtocolSchedule protocolSchedule,
      final int allowedRetriesAgainstOtherPeers) {
    super(blockHeaders, protocolSchedule, allowedRetriesAgainstOtherPeers);
  }

  @Override
  public List<Block> processResponse(
      final MessageData messageData, final Set<Capability> agreedCapabilities)
      throws InvalidPeerTaskResponseException {
    // Blocks returned by this method are in the same order as the headers, but might not be
    // complete
    if (messageData == null) {
      throw new InvalidPeerTaskResponseException();
    }
    final BlockBodiesMessage blocksMessage = BlockBodiesMessage.readFrom(messageData);
    final List<BlockBody> blockBodies = blocksMessage.bodies(protocolSchedule);
    if (blockBodies.isEmpty() || blockBodies.size() > blockHeaders.size()) {
      throw new InvalidPeerTaskResponseException();
    }

    for (int i = 0; i < blockBodies.size(); i++) {
      final BlockBody blockBody = blockBodies.get(i);
      final BlockHeader blockHeader = blockHeaders.get(i);
      if (!blockBodyMatchesBlockHeader(blockBody, blockHeader)) {
        LOG.atDebug()
            .setMessage("Received block body does not match block header: {}")
            .addArgument(blockHeader.getBlockHash())
            .log();
        throw new InvalidPeerTaskResponseException();
      }

      blocks.add(new Block(blockHeader, blockBody));
    }
    return blocks;
  }

  private boolean blockBodyMatchesBlockHeader(
      final BlockBody blockBody, final BlockHeader blockHeader) {
    final BodyIdentifier headerBlockId = new BodyIdentifier(blockHeader);
    final BodyIdentifier bodyBlockId = new BodyIdentifier(blockBody);
    return headerBlockId.equals(bodyBlockId);
  }
}
