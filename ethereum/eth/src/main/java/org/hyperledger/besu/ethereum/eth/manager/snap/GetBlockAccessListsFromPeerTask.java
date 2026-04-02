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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import static java.util.Collections.emptyList;
import static org.slf4j.LoggerFactory.getLogger;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.PeerRequest;
import org.hyperledger.besu.ethereum.eth.manager.PendingPeerRequest;
import org.hyperledger.besu.ethereum.eth.manager.RequestManager;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.ProtocolViolationException;
import org.hyperledger.besu.ethereum.eth.manager.task.AbstractPeerRequestTask;
import org.hyperledger.besu.ethereum.eth.messages.snap.BlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV2;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;

public class GetBlockAccessListsFromPeerTask
    extends AbstractPeerRequestTask<List<BlockAccessList>> {

  private static final Logger LOG = getLogger(GetBlockAccessListsFromPeerTask.class);

  private final List<BlockHeader> blockHeaders;

  private GetBlockAccessListsFromPeerTask(
      final EthContext ethContext,
      final List<BlockHeader> blockHeaders,
      final MetricsSystem metricsSystem) {
    super(ethContext, SnapProtocol.NAME, SnapV2.BLOCK_ACCESS_LISTS, metricsSystem);
    this.blockHeaders = blockHeaders;
  }

  public static GetBlockAccessListsFromPeerTask forBlockAccessLists(
      final EthContext ethContext,
      final List<BlockHeader> blockHeaders,
      final MetricsSystem metricsSystem) {
    return new GetBlockAccessListsFromPeerTask(ethContext, blockHeaders, metricsSystem);
  }

  @Override
  protected PendingPeerRequest sendRequest() {
    return sendRequestToPeer(
        new PeerRequest() {
          @Override
          public RequestManager.ResponseStream sendRequest(final EthPeer peer)
              throws PeerConnection.PeerNotConnected {
            LOG.atTrace()
                .setMessage("Requesting {} block access lists from {} .")
                .addArgument(blockHeaders::size)
                .addArgument(peer)
                .log();
            if (!peer.isServingSnap()) {
              LOG.atDebug()
                  .setMessage("EthPeer that is not serving snap called in {}, peer: {}")
                  .addArgument(GetBlockAccessListsFromPeerTask.class)
                  .addArgument(peer)
                  .log();
              throw new RuntimeException(
                  "EthPeer that is not serving snap called in "
                      + GetBlockAccessListsFromPeerTask.class);
            }
            if (!peer.getAgreedCapabilities().contains(SnapProtocol.SNAP2)) {
              LOG.atDebug()
                  .setMessage("EthPeer does not support snap/2 in {}, peer: {}")
                  .addArgument(GetBlockAccessListsFromPeerTask.class)
                  .addArgument(peer)
                  .log();
              throw new RuntimeException(
                  "EthPeer does not support snap/2 in " + GetBlockAccessListsFromPeerTask.class);
            }
            return peer.getSnapBlockAccessLists(
                blockHeaders.stream().map(BlockHeader::getBlockHash).toList());
          }

          @Override
          public boolean isEthPeerSuitable(final EthPeerImmutableAttributes ethPeer) {
            return ethPeer.isServingSnap()
                && ethPeer.ethPeer().getAgreedCapabilities().contains(SnapProtocol.SNAP2);
          }
        },
        blockHeaders.stream()
            .mapToLong(BlockHeader::getNumber)
            .max()
            .orElse(BlockHeader.GENESIS_BLOCK_NUMBER));
  }

  @Override
  protected Optional<List<BlockAccessList>> processResponse(
      final boolean streamClosed, final MessageData message, final EthPeer peer) {
    if (streamClosed) {
      return Optional.of(emptyList());
    }

    final List<BlockAccessList> result = new ArrayList<>();
    int index = 0;
    for (final BlockAccessList bal :
        BlockAccessListsMessage.readFrom(message).blockAccessLists(true)) {
      if (index >= blockHeaders.size()) {
        throw new ProtocolViolationException(
            "Received more block access lists than requested: expected %d but got at least %d"
                .formatted(blockHeaders.size(), index + 1));
      }
      if (!bal.isEmpty()) {
        final Hash expected = blockHeaders.get(index).getBalHash().orElseThrow();
        final Hash actual = BodyValidation.balHash(bal);
        if (!actual.equals(expected)) {
          throw new ProtocolViolationException(
              "Received block access list with invalid hash at index %d: expected %s, got %s"
                  .formatted(index, expected, actual));
        }
      }
      result.add(bal);
      index++;
    }
    return Optional.of(result);
  }
}
