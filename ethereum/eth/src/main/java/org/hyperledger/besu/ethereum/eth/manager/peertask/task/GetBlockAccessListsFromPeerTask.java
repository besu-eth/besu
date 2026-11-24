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
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListDecoder;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.peertask.InvalidPeerTaskResponseException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.MalformedRlpFromPeerException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskValidationResponse;
import org.hyperledger.besu.ethereum.eth.messages.BlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockAccessListsMessage;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** PeerTask for requesting and validating block access lists against block headers. */
public class GetBlockAccessListsFromPeerTask implements PeerTask<List<Optional<BlockAccessList>>> {

  private static final Logger LOG = LoggerFactory.getLogger(GetBlockAccessListsFromPeerTask.class);

  private final List<BlockHeader> blockHeaders;
  private final long requiredBlockchainHeight;
  private final boolean singleAttempt;

  public GetBlockAccessListsFromPeerTask(final List<BlockHeader> blockHeaders) {
    this(blockHeaders, false);
  }

  /**
   * @param blockHeaders headers of the requested BALs, all advertising a BAL hash
   * @param singleAttempt when true, the request is sent once to a single peer without retries, for
   *     best-effort callers that can reconstruct missing BALs
   */
  public GetBlockAccessListsFromPeerTask(
      final List<BlockHeader> blockHeaders, final boolean singleAttempt) {
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
    this.singleAttempt = singleAttempt;
  }

  @Override
  public SubProtocol getSubProtocol() {
    return EthProtocol.get();
  }

  @Override
  public int getRetriesWithOtherPeer() {
    return singleAttempt ? 0 : PeerTask.super.getRetriesWithOtherPeer();
  }

  @Override
  public int getRetriesWithSamePeer() {
    return singleAttempt ? 0 : PeerTask.super.getRetriesWithSamePeer();
  }

  @Override
  public MessageData getRequestMessage(final Set<Capability> agreedCapabilities) {
    return GetBlockAccessListsMessage.create(
        blockHeaders.stream().map(BlockHeader::getBlockHash).toList());
  }

  @Override
  public List<Optional<BlockAccessList>> processResponse(
      final MessageData messageData, final Set<Capability> agreedCapabilities)
      throws InvalidPeerTaskResponseException, MalformedRlpFromPeerException {
    if (messageData == null) {
      LOG.atDebug().setMessage("Received null response while waiting for block access lists").log();
      throw new InvalidPeerTaskResponseException("Null message data");
    }
    final BlockAccessListsMessage balMessage = BlockAccessListsMessage.readFrom(messageData);
    try {
      final List<Optional<BlockAccessList>> result = new ArrayList<>();
      int noOfBalsReceived = 0;
      for (final Bytes rawBalRlp : balMessage.blockAccessListsRaw()) {
        if (noOfBalsReceived >= blockHeaders.size()) {
          throw new MalformedRlpFromPeerException(
              "Too many block access lists returned", messageData.getData());
        }
        if (RLP.NULL.equals(rawBalRlp)) {
          // The peer may legitimately not have a BAL beyond the history expiry window.
          // TODO: Verify legitimacy of BAL unavailability.
          result.add(Optional.empty());
          noOfBalsReceived++;
          continue;
        }

        final Hash expectedBalHash = blockHeaders.get(noOfBalsReceived).getBalHash().orElseThrow();
        final Hash actualBalHash = BodyValidation.balHash(rawBalRlp);
        if (!expectedBalHash.equals(actualBalHash)) {
          LOG.atDebug()
              .setMessage(
                  "Received mismatched block access list at index {}: expected hash {}, actual hash {}")
              .addArgument(noOfBalsReceived)
              .addArgument(expectedBalHash)
              .addArgument(actualBalHash)
              .log();
          throw new MalformedRlpFromPeerException(
              "Block access list hash does not match query", messageData.getData());
        }
        result.add(Optional.of(BlockAccessListDecoder.decode(RLP.input(rawBalRlp))));
        noOfBalsReceived++;
      }
      return result;
    } catch (RLPException e) {
      throw new MalformedRlpFromPeerException(e, messageData.getData());
    }
  }

  @Override
  public PeerTaskValidationResponse validateResult(final List<Optional<BlockAccessList>> result) {
    if (result.isEmpty()) {
      return PeerTaskValidationResponse.NO_RESULTS_RETURNED;
    }

    return PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD;
  }

  @Override
  public Predicate<EthPeerImmutableAttributes> getPeerRequirementFilter() {
    return ethPeer ->
        ethPeer.estimatedChainHeight() >= requiredBlockchainHeight
            && ethPeer.ethPeer().getAgreedCapabilities().stream()
                .anyMatch(EthProtocol::isEth71Compatible);
  }
}
