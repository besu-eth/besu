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

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.peertask.InvalidPeerTaskResponseException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskValidationResponse;
import org.hyperledger.besu.ethereum.eth.messages.BlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockAccessListsMessage;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** PeerTask for requesting and validating block access lists against block headers. */
public class GetBlockAccessListsFromPeerTask implements PeerTask<List<BlockAccessList>> {

  private static final Logger LOG = LoggerFactory.getLogger(GetBlockAccessListsFromPeerTask.class);

  private final List<BlockHeader> blockHeaders;
  private final long requiredBlockchainHeight;

  public GetBlockAccessListsFromPeerTask(final List<BlockHeader> blockHeaders) {
    checkArgument(
        blockHeaders != null && !blockHeaders.isEmpty(), "Block headers must not be empty");
    checkArgument(
        blockHeaders.stream().allMatch(header -> header.getBalHash().isPresent()),
        "All headers must contain a block access list hash");
    this.blockHeaders = blockHeaders;
    this.requiredBlockchainHeight =
        blockHeaders.stream()
            .mapToLong(BlockHeader::getNumber)
            .max()
            .orElse(BlockHeader.GENESIS_BLOCK_NUMBER);
  }

  @Override
  public SubProtocol getSubProtocol() {
    return EthProtocol.get();
  }

  @Override
  public MessageData getRequestMessage(final Set<Capability> agreedCapabilities) {
    return GetBlockAccessListsMessage.create(
        blockHeaders.stream().map(BlockHeader::getBlockHash).toList());
  }

  @Override
  public List<BlockAccessList> processResponse(
      final MessageData messageData, final Set<Capability> agreedCapabilities)
      throws InvalidPeerTaskResponseException {
    if (messageData == null) {
      LOG.atDebug().setMessage("Received null response while waiting for block access lists").log();
      throw new InvalidPeerTaskResponseException("Null message data");
    }
    final BlockAccessListsMessage balMessage = BlockAccessListsMessage.readFrom(messageData);
    final List<BlockAccessList> blockAccessLists = new ArrayList<>();
    balMessage.blockAccessLists().forEach(blockAccessLists::add);
    return blockAccessLists;
  }

  @Override
  public PeerTaskValidationResponse validateResult(final List<BlockAccessList> result) {
    if (result.isEmpty()) {
      return PeerTaskValidationResponse.NO_RESULTS_RETURNED;
    }

    if (result.size() > blockHeaders.size()) {
      LOG.atDebug()
          .setMessage("Received invalid block access list response size: received={}, requested={}")
          .addArgument(result::size)
          .addArgument(blockHeaders::size)
          .log();
      return PeerTaskValidationResponse.TOO_MANY_RESULTS_RETURNED;
    }

    for (int i = 0; i < result.size(); i++) {
      final Hash expectedBalHash = blockHeaders.get(i).getBalHash().orElse(null);
      final Hash actualBalHash = BodyValidation.balHash(result.get(i));
      if (expectedBalHash == null || !expectedBalHash.equals(actualBalHash)) {
        LOG.atDebug()
            .setMessage(
                "Received mismatched block access list at index {}: expected hash {}, actual hash {}")
            .addArgument(i)
            .addArgument(expectedBalHash)
            .addArgument(actualBalHash)
            .log();
        return PeerTaskValidationResponse.RESULTS_DO_NOT_MATCH_QUERY;
      }
    }

    return PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD;
  }

  @Override
  public Predicate<EthPeerImmutableAttributes> getPeerRequirementFilter() {
    return ethPeer -> ethPeer.estimatedChainHeight() >= requiredBlockchainHeight;
  }
}
