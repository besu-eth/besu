/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.eth.sync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.manager.BlockRangeProvider;
import org.hyperledger.besu.ethereum.eth.manager.ChainState;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthMessage;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.PeerReputation;
import org.hyperledger.besu.ethereum.eth.messages.BlockRangeUpdateMessage;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;

import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BlockRangeBroadcasterTest {

  @Mock private Blockchain blockchain;
  @Mock private EthContext ethContext;
  @Mock private EthPeers ethPeers;
  @Mock private EthPeer ethPeerWithSupport;
  @Mock private EthPeer ethPeerWithoutSupport;
  @Mock private BlockRangeProvider blockRangeProvider;

  private BlockRangeBroadcaster blockRangeBroadcaster;

  @BeforeEach
  public void setup() {
    when(ethContext.getEthMessages()).thenReturn(mock(EthMessages.class));
    when(ethContext.getScheduler()).thenReturn(mock(EthScheduler.class));
    blockRangeBroadcaster = new BlockRangeBroadcaster(ethContext, blockchain, blockRangeProvider);
  }

  @Test
  public void shouldSendBlockRangeOnlyToEth69Peers() throws PeerConnection.PeerNotConnected {
    setupPeers(ethPeerWithoutSupport, ethPeerWithSupport);
    when(ethPeerWithSupport.hasSupportForMessage(EthProtocolMessages.BLOCK_RANGE_UPDATE))
        .thenReturn(true);
    when(ethPeerWithoutSupport.hasSupportForMessage(EthProtocolMessages.BLOCK_RANGE_UPDATE))
        .thenReturn(false);
    broadcastBlockRange();
    verify(ethPeerWithoutSupport, never()).send(any(BlockRangeUpdateMessage.class));
    verify(ethPeerWithSupport, times(1)).send(any(BlockRangeUpdateMessage.class));
  }

  private void setupPeers(final EthPeer... peers) {
    when(ethContext.getEthPeers()).thenReturn(ethPeers);
    when(ethPeers.streamAvailablePeers())
        .thenReturn(Stream.of(peers).map(EthPeerImmutableAttributes::from));
    for (EthPeer ethPeer : peers) {
      ChainState chainState = Mockito.mock(ChainState.class);

      Mockito.when(ethPeer.chainState()).thenReturn(chainState);
      Mockito.when(chainState.getEstimatedHeight()).thenReturn(0L);
      Mockito.when(chainState.getEstimatedTotalDifficulty()).thenReturn(Difficulty.of(0));
      Mockito.when(ethPeer.getReputation()).thenReturn(new PeerReputation());
      PeerConnection connection = mock(PeerConnection.class);
      Mockito.when(ethPeer.getConnection()).thenReturn(connection);
    }
  }

  private void broadcastBlockRange() {
    long startBlockNumber = 0L;
    long endBlockNumber = 1L;
    Hash endBlockHash = Hash.ZERO;
    blockRangeBroadcaster.broadcastBlockRange(startBlockNumber, endBlockNumber, endBlockHash);
  }

  @Test
  public void shouldSendSnapshotBlockRangeToPeers() throws PeerConnection.PeerNotConnected {
    final long expectedEarliestBlock = 10L;
    final long expectedLatestBlockNumber = 20L;
    final Hash expectedBlockHash = Hash.wrap(Bytes32.fromHexString("0x0B"));
    final BlockHeader latestBlockHeader = mock(BlockHeader.class);
    when(latestBlockHeader.getNumber()).thenReturn(expectedLatestBlockNumber);
    when(latestBlockHeader.getHash()).thenReturn(expectedBlockHash);
    when(blockchain.getChainHeadHeader()).thenReturn(latestBlockHeader);
    setupPeers(ethPeerWithSupport);
    when(ethPeerWithSupport.hasSupportForMessage(EthProtocolMessages.BLOCK_RANGE_UPDATE))
        .thenReturn(true);

    when(blockRangeProvider.getEarliestBlockNumber(expectedLatestBlockNumber))
        .thenReturn(Optional.of(expectedEarliestBlock));
    blockRangeBroadcaster.broadcastBlockRange();

    final ArgumentCaptor<BlockRangeUpdateMessage> message =
        ArgumentCaptor.forClass(BlockRangeUpdateMessage.class);
    verify(ethPeerWithSupport).send(message.capture());
    assertThat(message.getValue().getEarliestBlockNumber()).isEqualTo(expectedEarliestBlock);
    assertThat(message.getValue().getLatestBlockNumber()).isEqualTo(expectedLatestBlockNumber);
    assertThat(message.getValue().getBlockHash()).isEqualTo(expectedBlockHash);
  }

  @Test
  public void shouldSendTheGenesisRangeWhenTheEarliestBlockIsAboveTheChainHead()
      throws PeerConnection.PeerNotConnected {
    blockRangeBroadcaster =
        new BlockRangeBroadcaster(
            ethContext, blockchain, new BlockRangeProvider(blockchain, Optional.empty()));
    setupPeers(ethPeerWithSupport);
    when(ethPeerWithSupport.hasSupportForMessage(EthProtocolMessages.BLOCK_RANGE_UPDATE))
        .thenReturn(true);
    final BlockHeader chainHead = mock(BlockHeader.class);
    when(chainHead.getNumber()).thenReturn(20L);
    when(blockchain.getChainHeadHeader()).thenReturn(chainHead);
    // a sync moved the chain head up to the bodies it recorded after the head was read
    when(blockchain.getEarliestBlockNumber(20L)).thenCallRealMethod();
    when(blockchain.getEarliestBlockNumber()).thenReturn(Optional.of(30L));
    final BlockHeader genesis = mock(BlockHeader.class);
    when(genesis.getHash()).thenReturn(Hash.ZERO);
    when(blockchain.getGenesisBlockHeader()).thenReturn(genesis);

    blockRangeBroadcaster.broadcastBlockRange();

    final ArgumentCaptor<BlockRangeUpdateMessage> message =
        ArgumentCaptor.forClass(BlockRangeUpdateMessage.class);
    verify(ethPeerWithSupport).send(message.capture());
    assertThat(message.getValue().getEarliestBlockNumber()).isZero();
    assertThat(message.getValue().getLatestBlockNumber()).isZero();
    assertThat(message.getValue().getBlockHash()).isEqualTo(Hash.ZERO);
  }

  @Test
  public void shouldSendGenesisRangeWhenSnapshotIsEmpty() throws PeerConnection.PeerNotConnected {
    final Hash genesisHash = Hash.wrap(Bytes32.fromHexString("0x0B"));
    final BlockHeader genesisHeader = mock(BlockHeader.class);
    when(genesisHeader.getNumber()).thenReturn(0L);
    when(genesisHeader.getHash()).thenReturn(genesisHash);
    when(blockchain.getGenesisBlockHeader()).thenReturn(genesisHeader);
    when(blockchain.getChainHeadHeader()).thenReturn(genesisHeader);
    setupPeers(ethPeerWithSupport);
    when(ethPeerWithSupport.hasSupportForMessage(EthProtocolMessages.BLOCK_RANGE_UPDATE))
        .thenReturn(true);

    when(blockRangeProvider.getEarliestBlockNumber(0L)).thenReturn(Optional.empty());
    blockRangeBroadcaster.broadcastBlockRange();

    final ArgumentCaptor<BlockRangeUpdateMessage> message =
        ArgumentCaptor.forClass(BlockRangeUpdateMessage.class);
    verify(ethPeerWithSupport).send(message.capture());
    assertThat(message.getValue().getEarliestBlockNumber()).isZero();
    assertThat(message.getValue().getLatestBlockNumber()).isZero();
    assertThat(message.getValue().getBlockHash()).isEqualTo(genesisHash);
  }

  @Test
  public void shouldNotDisconnectIfLatestBlockNumberIsGreaterThanEarliest() {
    final EthPeer peer = mock(EthPeer.class);
    handleBlockRangeUpdateMessage(peer, 0L, 1L);
    verify(peer, never()).disconnect(any());
    verify(peer).registerKnownBlock(Hash.ZERO);
    verify(peer).registerBlockRange(Hash.ZERO, 1L, 0L);
  }

  @Test
  public void shouldNotDisconnectIfLatestBlockNumberIsEqualToEarliest() {
    final EthPeer peer = mock(EthPeer.class);
    handleBlockRangeUpdateMessage(peer, 0L, 0L);
    verify(peer, never()).disconnect(any());
    verify(peer).registerKnownBlock(Hash.ZERO);
    verify(peer).registerBlockRange(Hash.ZERO, 0L, 0L);
  }

  @Test
  public void shouldDisconnectIfLatestBlockNumberIsLessThanEarliest() {
    final EthPeer peer = mock(EthPeer.class);
    handleBlockRangeUpdateMessage(peer, 1L, 0L);
    verify(peer)
        .disconnect(DisconnectMessage.DisconnectReason.SUBPROTOCOL_TRIGGERED_INVALID_BLOCK_RANGE);
  }

  private void handleBlockRangeUpdateMessage(
      final EthPeer peer, final long earliestBlockNumber, final long latestBlockNumber) {
    BlockRangeUpdateMessage message =
        BlockRangeUpdateMessage.create(earliestBlockNumber, latestBlockNumber, Hash.ZERO);
    EthMessage ethMessage = new EthMessage(peer, message);
    blockRangeBroadcaster.handleBlockRangeUpdateMessage(ethMessage);
  }
}
