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
package org.hyperledger.besu.ethereum.eth.sync.tasks;

import static java.util.Arrays.asList;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsProcessor;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CompleteBlocksWithPeerTaskTest {

  private long blockNumber;

  @BeforeEach
  public void setUp() {
    blockNumber = 1;
  }

  @Test
  public void shouldFailWhenEmptyHeaders() {
    assertThatThrownBy(() -> new CompleteBlocksWithPeerTask(null, Collections.emptyList(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Must supply a non-empty headers list");
  }

  @Test
  public void shouldReturnEmptyBlock() {
    final ProtocolSchedule protocolSchedule = getProtocolScheduleMock();
    final BlockHeader blockHeader = getEmptyBlockHeaderMock();
    final PeerTaskExecutor peerTaskExecutor = mock(PeerTaskExecutor.class);

    CompleteBlocksWithPeerTask completeBlocksWithPeerTask =
        new CompleteBlocksWithPeerTask(protocolSchedule, List.of(blockHeader), peerTaskExecutor);
    final List<Block> blocks = completeBlocksWithPeerTask.retrieveBlocksFromPeers();
    assertThat(blocks).isNotEmpty();
    assertThat(blocks.size()).isEqualTo(1);
    assertThat(BlockHeader.hasEmptyBlock(blocks.get(0).getHeader())).isTrue();

    verify(peerTaskExecutor, Mockito.never()).execute(any());
  }

  @Test
  public void shouldCreateWithdrawalsAwareEmptyBlock_whenWithdrawalsAreEnabled() {
    final ProtocolSchedule mockProtocolSchedule = Mockito.mock(ProtocolSchedule.class);
    final ProtocolSpec mockParisSpec = Mockito.mock(ProtocolSpec.class);
    final ProtocolSpec mockShanghaiSpec = Mockito.mock(ProtocolSpec.class);
    final WithdrawalsProcessor mockWithdrawalsProcessor = Mockito.mock(WithdrawalsProcessor.class);

    final BlockHeader header1 =
        new BlockHeaderTestFixture().number(1).withdrawalsRoot(null).buildHeader();
    final BlockHeader header2 =
        new BlockHeaderTestFixture().number(2).withdrawalsRoot(Hash.EMPTY_TRIE_HASH).buildHeader();

    when(mockProtocolSchedule.getByBlockHeader((eq(header1)))).thenReturn(mockParisSpec);
    when(mockParisSpec.getWithdrawalsProcessor()).thenReturn(Optional.empty());
    when(mockProtocolSchedule.getByBlockHeader((eq(header2)))).thenReturn(mockShanghaiSpec);
    when(mockShanghaiSpec.getWithdrawalsProcessor())
        .thenReturn(Optional.of(mockWithdrawalsProcessor));

    final List<Block> expectedBlocks = getExpectedBlocks(header1, header2);

    final PeerTaskExecutor peerTaskExecutor = mock(PeerTaskExecutor.class);
    when(peerTaskExecutor.execute(any()))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(expectedBlocks),
                PeerTaskExecutorResponseCode.SUCCESS,
                Collections.emptyList()));

    final CompleteBlocksWithPeerTask task =
        new CompleteBlocksWithPeerTask(
            mockProtocolSchedule, asList(header1, header2), peerTaskExecutor);
    final List<Block> blocks = task.retrieveBlocksFromPeers();

    assertThat(blocks).isEqualTo(expectedBlocks);
  }

  @Test
  public void shouldReturnNonEmptyBlock() {
    final Block block = mock(Block.class);
    final ProtocolSchedule protocolSchedule = getProtocolScheduleMock();
    final PeerTaskExecutor peerTaskExecutor = mock(PeerTaskExecutor.class);
    final BlockHeader nonEmptyBlockHeaderMock = getNonEmptyBlockHeaderMock("0x01", "0x02");
    when(peerTaskExecutor.execute(any()))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(block)),
                PeerTaskExecutorResponseCode.SUCCESS,
                Collections.emptyList()));

    CompleteBlocksWithPeerTask completeBlocksWithPeerTask =
        new CompleteBlocksWithPeerTask(
            protocolSchedule, List.of(nonEmptyBlockHeaderMock), peerTaskExecutor);

    final List<Block> blocks = completeBlocksWithPeerTask.retrieveBlocksFromPeers();
    assertThat(blocks).isNotEmpty();
    assertThat(blocks.size()).isEqualTo(1);
    assertThat(blocks.get(0)).isEqualTo(block);
  }

  @Test
  public void shouldReturnBlocksInRightOrderWhenEmptyAndNonEmptyBlocksRequested() {
    final Block block1 = mock(Block.class);
    final Block block3 = mock(Block.class);
    final BlockHeader emptyBlockHeaderMock = getEmptyBlockHeaderMock();
    final BlockHeader nonEmptyBlockHeaderMock1 = getNonEmptyBlockHeaderMock("0x01", "0x02");
    final BlockHeader nonEmptyBlockHeaderMock3 = getNonEmptyBlockHeaderMock("0x03", "0x04");

    final ProtocolSchedule protocolSchedule = getProtocolScheduleMock();
    final PeerTaskExecutor peerTaskExecutor = mock(PeerTaskExecutor.class);
    when(peerTaskExecutor.execute(any()))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(block1, block3)),
                PeerTaskExecutorResponseCode.SUCCESS,
                Collections.emptyList()));

    CompleteBlocksWithPeerTask completeBlocksWithPeerTask =
        new CompleteBlocksWithPeerTask(
            protocolSchedule,
            List.of(
                nonEmptyBlockHeaderMock1,
                emptyBlockHeaderMock,
                nonEmptyBlockHeaderMock3,
                emptyBlockHeaderMock),
            peerTaskExecutor);

    final List<Block> blocks = completeBlocksWithPeerTask.retrieveBlocksFromPeers();
    assertThat(blocks).isNotEmpty();
    assertThat(blocks.size()).isEqualTo(4);
    assertThat(blocks.get(0)).isEqualTo(block1);
    assertThat(BlockHeader.hasEmptyBlock(blocks.get(1).getHeader())).isTrue();
    assertThat(blocks.get(2)).isEqualTo(block3);
    assertThat(BlockHeader.hasEmptyBlock(blocks.get(3).getHeader())).isTrue();
  }

  @Test
  public void shouldRequestMoreBodiesUntilFinished() {
    final Block block1 = mock(Block.class);
    final Block block3 = mock(Block.class);
    final BlockHeader emptyBlockHeaderMock = getEmptyBlockHeaderMock();
    final BlockHeader nonEmptyBlockHeaderMock1 = getNonEmptyBlockHeaderMock("0x01", "0x02");
    final BlockHeader nonEmptyBlockHeaderMock3 = getNonEmptyBlockHeaderMock("0x03", "0x04");

    final ProtocolSchedule protocolSchedule = getProtocolScheduleMock();
    final PeerTaskExecutor peerTaskExecutor = mock(PeerTaskExecutor.class);
    when(peerTaskExecutor.execute(any()))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(block1)),
                PeerTaskExecutorResponseCode.SUCCESS,
                Collections.emptyList()))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(block3)),
                PeerTaskExecutorResponseCode.SUCCESS,
                Collections.emptyList()));

    CompleteBlocksWithPeerTask completeBlocksWithPeerTask =
        new CompleteBlocksWithPeerTask(
            protocolSchedule,
            List.of(
                nonEmptyBlockHeaderMock1,
                emptyBlockHeaderMock,
                nonEmptyBlockHeaderMock3,
                emptyBlockHeaderMock),
            peerTaskExecutor);

    final List<Block> blocks = completeBlocksWithPeerTask.retrieveBlocksFromPeers();
    assertThat(blocks).isNotEmpty();
    assertThat(blocks.size()).isEqualTo(4);
    assertThat(blocks.get(0)).isEqualTo(block1);
    assertThat(BlockHeader.hasEmptyBlock(blocks.get(1).getHeader())).isTrue();
    assertThat(blocks.get(2)).isEqualTo(block3);
    assertThat(BlockHeader.hasEmptyBlock(blocks.get(3).getHeader())).isTrue();
  }

  @Test
  public void shouldThrowExceptionWhenNoPeersAvailable() {
    final ProtocolSchedule protocolSchedule = getProtocolScheduleMock();
    final PeerTaskExecutor peerTaskExecutor = mock(PeerTaskExecutor.class);
    final BlockHeader header1 = getNonEmptyBlockHeaderMock("0x01", "0x02");
    final BlockHeader header2 = getNonEmptyBlockHeaderMock("0x03", "0x05");

    Mockito.when(peerTaskExecutor.execute(any(GetBodiesFromPeerTask.class)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.empty(),
                PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE,
                Collections.emptyList()));

    CompleteBlocksWithPeerTask task =
        new CompleteBlocksWithPeerTask(
            protocolSchedule, List.of(header1, header2), peerTaskExecutor);
    Assertions.assertThrows(
        RuntimeException.class,
        task::retrieveBlocksFromPeers,
        "Unable to retrieve blocks for block numbers: 1 to 2");
  }

  private static ProtocolSchedule getProtocolScheduleMock() {
    final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
    final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
    final Optional<WithdrawalsProcessor> optional = Optional.of(mock(WithdrawalsProcessor.class));
    when(protocolSpec.getWithdrawalsProcessor()).thenReturn(optional);
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    return protocolSchedule;
  }

  private BlockHeader getEmptyBlockHeaderMock() {
    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getNumber()).thenReturn(blockNumber++);
    when(blockHeader.getTransactionsRoot()).thenReturn(Hash.EMPTY_TRIE_HASH);
    when(blockHeader.getOmmersHash()).thenReturn(Hash.EMPTY_LIST_HASH);
    when(blockHeader.getWithdrawalsRoot()).thenReturn(Optional.empty());
    return blockHeader;
  }

  private BlockHeader getNonEmptyBlockHeaderMock(
      final String transactionsRootHexString, final String ommersHash) {
    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getNumber()).thenReturn(blockNumber++);
    when(blockHeader.getTransactionsRoot())
        .thenReturn(Hash.fromHexStringLenient(transactionsRootHexString));
    when(blockHeader.getOmmersHash()).thenReturn(Hash.fromHexStringLenient(ommersHash));
    when(blockHeader.getWithdrawalsRoot()).thenReturn(Optional.empty());
    return blockHeader;
  }

  private static List<Block> getExpectedBlocks(
      final BlockHeader header1, final BlockHeader header2) {
    final Block block1 =
        new Block(
            header1,
            new BlockBody(Collections.emptyList(), Collections.emptyList(), Optional.empty()));
    final Block block2 =
        new Block(
            header2,
            new BlockBody(
                Collections.emptyList(),
                Collections.emptyList(),
                Optional.of(Collections.emptyList())));

    return asList(block1, block2);
  }
}
