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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.peertask.InvalidPeerTaskResponseException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskValidationResponse;
import org.hyperledger.besu.ethereum.eth.messages.BlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockHeadersMessage;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetHeadersFromPeerTask implements PeerTask<List<BlockHeader>> {
  private static final Logger LOG = LoggerFactory.getLogger(GetHeadersFromPeerTask.class);

  private static final int DEFAULT_MAXIMUM_RETRIES_AGAINST_DIFFERENT_PEERS = 5;

  private final long blockNumber;
  private final Hash blockHash;
  private final int maxHeaders;
  private final int skip;
  private final Direction direction;
  private final int maximumRetriesAgainstDifferentPeers;
  private final ProtocolSchedule protocolSchedule;
  private final boolean isPoS;

  public GetHeadersFromPeerTask(
      final long blockNumber,
      final int maxHeaders,
      final int skip,
      final Direction direction,
      final ProtocolSchedule protocolSchedule) {
    this(
        null,
        blockNumber,
        maxHeaders,
        skip,
        direction,
        DEFAULT_MAXIMUM_RETRIES_AGAINST_DIFFERENT_PEERS,
        protocolSchedule);
  }

  public GetHeadersFromPeerTask(
      final Hash blockHash,
      final long blockNumber,
      final int maxHeaders,
      final int skip,
      final Direction direction,
      final ProtocolSchedule protocolSchedule) {
    this(
        blockHash,
        blockNumber,
        maxHeaders,
        skip,
        direction,
        DEFAULT_MAXIMUM_RETRIES_AGAINST_DIFFERENT_PEERS,
        protocolSchedule);
  }

  public GetHeadersFromPeerTask(
      final long blockNumber,
      final int maxHeaders,
      final int skip,
      final Direction direction,
      final int maximumRetriesAgainstDifferentPeers,
      final ProtocolSchedule protocolSchedule) {
    this(
        null,
        blockNumber,
        maxHeaders,
        skip,
        direction,
        maximumRetriesAgainstDifferentPeers,
        protocolSchedule);
  }

  public GetHeadersFromPeerTask(
      final Hash blockHash,
      final int maxHeaders,
      final int skip,
      final Direction direction,
      final int maximumRetriesAgainstDifferentPeers,
      final ProtocolSchedule protocolSchedule) {
    this(
        blockHash,
        -1,
        maxHeaders,
        skip,
        direction,
        maximumRetriesAgainstDifferentPeers,
        protocolSchedule);
  }

  public GetHeadersFromPeerTask(
      final Hash blockHash,
      final long blockNumber,
      final int maxHeaders,
      final int skip,
      final Direction direction,
      final int maximumRetriesAgainstDifferentPeers,
      final ProtocolSchedule protocolSchedule) {
    LOG.debug(
        "Constructing GetHeadersFromPeerTask with hash {}, number {}, maxHeaders {}, skip {}, direction {}",
        blockHash,
        blockNumber,
        maxHeaders,
        skip,
        direction);
    this.blockHash = blockHash;
    this.blockNumber = blockNumber;
    this.maxHeaders = maxHeaders;
    this.skip = skip;
    this.direction = direction;
    this.maximumRetriesAgainstDifferentPeers = maximumRetriesAgainstDifferentPeers;
    this.protocolSchedule = protocolSchedule;

    isPoS = protocolSchedule.anyMatch((ps) -> ps.spec().isPoS());
  }

  @Override
  public SubProtocol getSubProtocol() {
    return EthProtocol.get();
  }

  @Override
  public MessageData getRequestMessage(final Set<Capability> agreedCapabilities) {
    if (blockHash != null) {
      return GetBlockHeadersMessage.create(
          blockHash, maxHeaders, skip, direction == Direction.REVERSE);
    } else {
      return GetBlockHeadersMessage.create(
          blockNumber, maxHeaders, skip, direction == Direction.REVERSE);
    }
  }

  @Override
  public List<BlockHeader> processResponse(
      final MessageData messageData, final Set<Capability> agreedCapabilities)
      throws InvalidPeerTaskResponseException {
    if (messageData == null) {
      throw new InvalidPeerTaskResponseException("Response MessageData is null");
    }
    return BlockHeadersMessage.readFrom(messageData).getHeaders(protocolSchedule);
  }

  @Override
  public Predicate<EthPeerImmutableAttributes> getPeerRequirementFilter() {
    return (ethPeer) -> {
      return isPoS || ethPeer.estimatedChainHeight() >= blockNumber;
    };
  }

  @Override
  public PeerTaskValidationResponse validateResult(final List<BlockHeader> blockHeaders) {
    if (blockHeaders.isEmpty()) {
      // Message contains no data - nothing to do
      LOG.debug(
          "No blockheaders returned for query starting at {}",
          blockHash != null ? blockHash : blockNumber);
      return PeerTaskValidationResponse.NO_RESULTS_RETURNED;
    }

    if (blockHeaders.size() > maxHeaders) {
      // Too many headers - this isn't our response
      LOG.debug(
          "Too many blockheaders returned for query starting at {}",
          blockHash != null ? blockHash : blockNumber);
      return PeerTaskValidationResponse.TOO_MANY_RESULTS_RETURNED;
    }

    if ((blockHash != null && !blockHeaders.getFirst().getHash().equals(blockHash))
        || (blockHash == null && blockHeaders.getFirst().getNumber() != blockNumber)) {
      // This isn't our message - nothing to do
      LOG.debug(
          "First header returned doesn't match query starting at {}",
          blockHash != null ? blockHash : blockNumber);
      return PeerTaskValidationResponse.RESULTS_DO_NOT_MATCH_QUERY;
    }

    if (!isBlockHeadersMatchingRequest(blockHeaders)) {
      LOG.debug(
          "Blockheaders do not match expected headers from request for query starting at {}",
          blockHash != null ? blockHash : blockNumber);
      return PeerTaskValidationResponse.RESULTS_DO_NOT_MATCH_QUERY;
    }

    if (blockHeaders.size() >= 2 && skip == 0) {
      // headers are supposed to be sequential and at least 2 have been returned, check if a chain
      // is formed
      for (int i = 0; i < blockHeaders.size() - 1; i++) {
        BlockHeader parentHeader = null;
        BlockHeader childHeader = null;
        switch (direction) {
          case FORWARD:
            parentHeader = blockHeaders.get(i);
            childHeader = blockHeaders.get(i + 1);
            break;
          case REVERSE:
            childHeader = blockHeaders.get(i);
            parentHeader = blockHeaders.get(i + 1);
            break;
        }
        if (!parentHeader.getHash().equals(childHeader.getParentHash())) {
          LOG.warn(
              "Blockheaders were non-sequential for query starting at {}",
              blockHash != null ? blockHash : blockNumber);
          return PeerTaskValidationResponse.NON_SEQUENTIAL_HEADERS_RETURNED;
        }
      }
    }
    return PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD;
  }

  @Override
  public void postProcessResult(final PeerTaskExecutorResult<List<BlockHeader>> result) {
    if (!result.ethPeers().isEmpty() && result.result().isPresent()) {
      result.result().get().stream()
          .max(Comparator.comparingLong(ProcessableBlockHeader::getNumber))
          .ifPresent(
              (highestBlockHeader) ->
                  result.ethPeers().getLast().chainState().update(highestBlockHeader));
    }
  }

  @Override
  public int getRetriesWithOtherPeer() {
    return maximumRetriesAgainstDifferentPeers;
  }

  @Override
  public Duration getDelayBetweenSamePeerRetries() {
    return Duration.ZERO;
  }

  public Long getBlockNumber() {
    return blockNumber;
  }

  public Hash getBlockHash() {
    return blockHash;
  }

  public int getMaxHeaders() {
    return maxHeaders;
  }

  public int getSkip() {
    return skip;
  }

  public Direction getDirection() {
    return direction;
  }

  public enum Direction {
    FORWARD,
    REVERSE
  }

  private boolean isBlockHeadersMatchingRequest(final List<BlockHeader> blockHeaders) {
    BlockHeader prevBlockHeader = blockHeaders.getFirst();
    final int expectedDelta = direction == Direction.REVERSE ? -(skip + 1) : (skip + 1);
    BlockHeader header;
    for (int i = 1; i < blockHeaders.size(); i++) {
      header = blockHeaders.get(i);
      if (header.getNumber() != prevBlockHeader.getNumber() + expectedDelta) {
        // Skip doesn't match, this isn't our data
        return false;
      }
      prevBlockHeader = header;
    }
    return true;
  }
}
