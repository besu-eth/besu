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

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskValidationResponse;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockBodiesMessage;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Implements PeerTask for getting block bodies from peers, and matches headers to bodies to supply
 * full blocks
 */
public abstract class AbstractGetBodiesFromPeerTask<T> implements PeerTask<List<T>> {

  final List<BlockHeader> blockHeaders;
  final ProtocolSchedule protocolSchedule;
  private final int allowedRetriesAgainstOtherPeers;

  private final long requiredBlockchainHeight;
  private final boolean isPoS;

  public AbstractGetBodiesFromPeerTask(
      final List<BlockHeader> blockHeaders,
      final ProtocolSchedule protocolSchedule,
      final int allowedRetriesAgainstOtherPeers) {
    if (blockHeaders == null || blockHeaders.isEmpty()) {
      throw new IllegalArgumentException("Block headers must not be empty");
    }

    this.blockHeaders = blockHeaders;
    this.protocolSchedule = protocolSchedule;
    this.allowedRetriesAgainstOtherPeers = allowedRetriesAgainstOtherPeers;

    this.requiredBlockchainHeight =
        blockHeaders.stream()
            .mapToLong(BlockHeader::getNumber)
            .max()
            .orElse(BlockHeader.GENESIS_BLOCK_NUMBER);
    this.isPoS = protocolSchedule.getByBlockHeader(blockHeaders.getLast()).isPoS();
  }

  @Override
  public SubProtocol getSubProtocol() {
    return EthProtocol.get();
  }

  @Override
  public MessageData getRequestMessage(final Set<Capability> agreedCapabilities) {
    return GetBlockBodiesMessage.create(
        blockHeaders.stream().map(BlockHeader::getBlockHash).toList());
  }

  @Override
  public int getRetriesWithOtherPeer() {
    return allowedRetriesAgainstOtherPeers;
  }

  @Override
  public Predicate<EthPeerImmutableAttributes> getPeerRequirementFilter() {
    return (ethPeer) -> isPoS || ethPeer.estimatedChainHeight() >= requiredBlockchainHeight;
  }

  @Override
  public PeerTaskValidationResponse validateResult(final List<T> result) {
    if (result.isEmpty()) {
      return PeerTaskValidationResponse.NO_RESULTS_RETURNED;
    }
    return PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD;
  }

  public List<BlockHeader> getBlockHeaders() {
    return blockHeaders;
  }
}
