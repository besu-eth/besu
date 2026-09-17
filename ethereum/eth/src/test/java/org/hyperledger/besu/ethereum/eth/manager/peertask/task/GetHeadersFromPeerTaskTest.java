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
package org.hyperledger.besu.ethereum.eth.manager.peertask.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.ChainState;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.PeerReputation;
import org.hyperledger.besu.ethereum.eth.manager.peertask.InvalidPeerTaskResponseException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskValidationResponse;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask.Direction;
import org.hyperledger.besu.ethereum.eth.messages.BlockHeadersMessage;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class GetHeadersFromPeerTaskTest {
  private static final Set<Capability> AGREED_CAPABILITIES = Set.of(EthProtocol.LATEST);

  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);

  @Test
  public void testGetSubProtocol() {
    GetHeadersFromPeerTask task =
        new GetHeadersFromPeerTask(0, 1, 0, Direction.FORWARD, protocolSchedule);
    assertEquals(EthProtocol.get(), task.getSubProtocol());
  }

  @Test
  public void testGetRequestMessageForHash() {
    GetHeadersFromPeerTask task =
        new GetHeadersFromPeerTask(Hash.ZERO, 0, 1, 0, Direction.FORWARD, protocolSchedule);
    MessageData requestMessageData = task.getRequestMessage(AGREED_CAPABILITIES);
    assertEquals(
        "0xe4a00000000000000000000000000000000000000000000000000000000000000000018080",
        requestMessageData.getData().toHexString());
  }

  @Test
  public void testGetRequestMessageForBlockNumber() {
    GetHeadersFromPeerTask task =
        new GetHeadersFromPeerTask(123, 1, 0, Direction.FORWARD, protocolSchedule);
    MessageData requestMessageData = task.getRequestMessage(AGREED_CAPABILITIES);
    assertEquals("0xc47b018080", requestMessageData.getData().toHexString());
  }

  @Test
  public void testGetRequestMessageForHashWhenBlockNumberAlsoProvided() {
    GetHeadersFromPeerTask task =
        new GetHeadersFromPeerTask(Hash.ZERO, 123, 1, 0, Direction.FORWARD, protocolSchedule);
    MessageData requestMessageData = task.getRequestMessage(AGREED_CAPABILITIES);
    assertEquals(
        "0xe4a00000000000000000000000000000000000000000000000000000000000000000018080",
        requestMessageData.getData().toHexString());
  }

  @Test
  public void testProcessResponseWithNullMessageData() {
    GetHeadersFromPeerTask task =
        new GetHeadersFromPeerTask(0, 1, 0, Direction.FORWARD, protocolSchedule);
    assertThrows(
        InvalidPeerTaskResponseException.class,
        () -> task.processResponse(null, Set.of()),
        "Response MessageData is null");
  }

  @Test
  public void testProcessResponse() throws InvalidPeerTaskResponseException {
    final BlockchainSetupUtil blockchainSetupUtil =
        BlockchainSetupUtil.forTesting(DataStorageFormat.FOREST);
    blockchainSetupUtil.importAllBlocks();
    Blockchain blockchain = blockchainSetupUtil.getBlockchain();
    BlockHeader blockHeader = blockchain.getChainHeadHeader();
    BlockHeadersMessage responseMessage = BlockHeadersMessage.create(blockHeader);

    GetHeadersFromPeerTask task =
        new GetHeadersFromPeerTask(
            blockHeader.getBlockHash(),
            0,
            1,
            0,
            Direction.FORWARD,
            blockchainSetupUtil.getProtocolSchedule());

    assertEquals(
        List.of(blockchain.getChainHeadHeader()),
        task.processResponse(responseMessage, AGREED_CAPABILITIES));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testGetPeerRequirementFilter(final boolean isPoS) {
    reset(protocolSchedule);
    when(protocolSchedule.anyMatch(any())).thenReturn(isPoS);

    GetHeadersFromPeerTask task =
        new GetHeadersFromPeerTask(5, 1, 0, Direction.FORWARD, protocolSchedule);

    EthPeer failForShortChainHeight = mockPeer(1);
    EthPeer successfulCandidate = mockPeer(5);

    assertEquals(
        isPoS,
        task.getPeerRequirementFilter()
            .test(EthPeerImmutableAttributes.from(failForShortChainHeight)));
    assertTrue(
        task.getPeerRequirementFilter().test(EthPeerImmutableAttributes.from(successfulCandidate)));
  }

  @Test
  public void testValidateResultForEmptyResult() {
    GetHeadersFromPeerTask task =
        new GetHeadersFromPeerTask(5, 1, 0, Direction.FORWARD, protocolSchedule);
    assertEquals(
        PeerTaskValidationResponse.NO_RESULTS_RETURNED,
        task.validateResult(Collections.emptyList()));
  }

  @Test
  public void testShouldDisconnectPeerForTooManyHeadersReturned() {
    GetHeadersFromPeerTask task =
        new GetHeadersFromPeerTask(5, 1, 1, Direction.FORWARD, protocolSchedule);

    BlockHeader header1 = mock(BlockHeader.class);
    BlockHeader header2 = mock(BlockHeader.class);
    BlockHeader header3 = mock(BlockHeader.class);

    assertEquals(
        PeerTaskValidationResponse.TOO_MANY_RESULTS_RETURNED,
        task.validateResult(List.of(header1, header2, header3)));
  }

  @Test
  public void testValidateResultForNonSequentialHeaders() {
    GetHeadersFromPeerTask task =
        new GetHeadersFromPeerTask(1, 3, 0, Direction.FORWARD, protocolSchedule);

    Hash block1Hash = Hash.fromHexStringLenient("01");
    Hash block2Hash = Hash.fromHexStringLenient("02");
    BlockHeader header1 = mock(BlockHeader.class);
    when(header1.getNumber()).thenReturn(1L);
    when(header1.getHash()).thenReturn(block1Hash);
    BlockHeader header2 = mock(BlockHeader.class);
    when(header2.getNumber()).thenReturn(2L);
    when(header2.getHash()).thenReturn(block2Hash);
    when(header2.getParentHash()).thenReturn(block1Hash);
    BlockHeader header3 = mock(BlockHeader.class);
    when(header3.getNumber()).thenReturn(3L);
    when(header3.getParentHash()).thenReturn(Hash.ZERO);

    assertEquals(
        PeerTaskValidationResponse.NON_SEQUENTIAL_HEADERS_RETURNED,
        task.validateResult(List.of(header1, header2, header3)));
  }

  private EthPeer mockPeer(final long chainHeight) {
    EthPeer ethPeer = mock(EthPeer.class);
    ChainState chainState = mock(ChainState.class);

    when(ethPeer.chainState()).thenReturn(chainState);
    when(chainState.getEstimatedHeight()).thenReturn(chainHeight);
    when(chainState.getEstimatedTotalDifficulty()).thenReturn(Difficulty.of(0));
    when(ethPeer.getReputation()).thenReturn(new PeerReputation());
    PeerConnection connection = mock(PeerConnection.class);
    when(ethPeer.getConnection()).thenReturn(connection);
    return ethPeer;
  }
}
