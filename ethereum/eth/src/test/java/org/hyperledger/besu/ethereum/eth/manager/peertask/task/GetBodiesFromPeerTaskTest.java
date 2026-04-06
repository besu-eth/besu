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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.ChainState;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.PeerReputation;
import org.hyperledger.besu.ethereum.eth.manager.peertask.InvalidPeerTaskResponseException;
import org.hyperledger.besu.ethereum.eth.messages.BlockBodiesMessage;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockBodiesMessage;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class GetBodiesFromPeerTaskTest {

  private static final String FRONTIER_TX_RLP =
      "0xf901fc8032830138808080b901ae60056013565b6101918061001d6000396000f35b3360008190555056006001600060e060020a6000350480630a874df61461003a57806341c0e1b514610058578063a02b161e14610066578063dbbdf0831461007757005b610045600435610149565b80600160a060020a031660005260206000f35b610060610161565b60006000f35b6100716004356100d4565b60006000f35b61008560043560243561008b565b60006000f35b600054600160a060020a031632600160a060020a031614156100ac576100b1565b6100d0565b8060018360005260205260406000208190555081600060005260206000a15b5050565b600054600160a060020a031633600160a060020a031614158015610118575033600160a060020a0316600182600052602052604060002054600160a060020a031614155b61012157610126565b610146565b600060018260005260205260406000208190555080600060005260206000a15b50565b60006001826000526020526040600020549050919050565b600054600160a060020a031633600160a060020a0316146101815761018f565b600054600160a060020a0316ff5b561ca0c5689ed1ad124753d54576dfb4b571465a41900a1dff4058d8adf16f752013d0a01221cbd70ec28c94a3b55ec771bcbc70778d6ee0b51ca7ea9514594c861b1884";

  private static final Transaction TX =
      TransactionDecoder.decodeRLP(
          new BytesValueRLPInput(Bytes.fromHexString(FRONTIER_TX_RLP), false),
          EncodingContext.BLOCK_BODY);
  public static final List<Transaction> TRANSACTION_LIST = List.of(TX);
  public static final BlockBody BLOCK_BODY =
      new BlockBody(TRANSACTION_LIST, Collections.emptyList(), Optional.empty());
  private static final Set<Capability> AGREED_CAPABILITIES = Set.of(EthProtocol.LATEST);
  private static ProtocolSchedule protocolSchedule;

  @BeforeAll
  public static void setup() {
    protocolSchedule = mock(ProtocolSchedule.class);
    final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
    when(protocolSpec.isPoS()).thenReturn(true);
    when(protocolSchedule.getByBlockHeader(Mockito.any())).thenReturn(protocolSpec);
  }

  @Test
  public void testGetSubProtocol() {

    GetBodiesFromPeerTask task =
        new GetBodiesFromPeerTask(List.of(mockBlockHeader(0)), protocolSchedule);
    Assertions.assertEquals(EthProtocol.get(), task.getSubProtocol());
  }

  @Test
  public void testGetRequestMessage() {
    GetBodiesFromPeerTask task =
        new GetBodiesFromPeerTask(
            List.of(mockBlockHeader(1), mockBlockHeader(2), mockBlockHeader(3)), protocolSchedule);

    MessageData messageData = task.getRequestMessage(AGREED_CAPABILITIES);
    GetBlockBodiesMessage getBlockBodiesMessage = GetBlockBodiesMessage.readFrom(messageData);

    Assertions.assertEquals(EthProtocolMessages.GET_BLOCK_BODIES, getBlockBodiesMessage.getCode());
    Iterable<Hash> hashesInMessage = getBlockBodiesMessage.hashes();
    List<Hash> expectedHashes =
        List.of(
            Hash.fromHexString(StringUtils.repeat("00", 31) + "11"),
            Hash.fromHexString(StringUtils.repeat("00", 31) + "21"),
            Hash.fromHexString(StringUtils.repeat("00", 31) + "31"));
    List<Hash> actualHashes = new ArrayList<>();
    hashesInMessage.forEach(actualHashes::add);

    Assertions.assertEquals(3, actualHashes.size());
    Assertions.assertEquals(
        expectedHashes.stream().sorted().toList(), actualHashes.stream().sorted().toList());
  }

  @Test
  public void testParseResponseWithNullResponseMessage() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new GetBodiesFromPeerTask(Collections.emptyList(), protocolSchedule));
  }

  @Test
  public void testParseResponseForInvalidResponse() {
    GetBodiesFromPeerTask task =
        new GetBodiesFromPeerTask(List.of(mockBlockHeader(1)), protocolSchedule);
    // body does not match header
    BlockBodiesMessage bodiesMessage = BlockBodiesMessage.create(List.of(BLOCK_BODY));

    Assertions.assertThrows(
        InvalidPeerTaskResponseException.class,
        () -> task.processResponse(bodiesMessage, Set.of()));
  }

  @Test
  public void testParseResponse() throws InvalidPeerTaskResponseException {
    final BlockHeader nonEmptyBlockHeaderMock =
        getNonEmptyBlockHeaderMock(BodyValidation.transactionsRoot(TRANSACTION_LIST).toString());

    GetBodiesFromPeerTask task =
        new GetBodiesFromPeerTask(List.of(nonEmptyBlockHeaderMock), protocolSchedule);

    final BlockBodiesMessage blockBodiesMessage = BlockBodiesMessage.create(List.of(BLOCK_BODY));

    List<Block> result = task.processResponse(blockBodiesMessage, Set.of());

    assertThat(result.size()).isEqualTo(1);
    assertThat(result.getFirst().getBody().getTransactions()).isEqualTo(TRANSACTION_LIST);
  }

  @Test
  public void testGetPeerRequirementFilter() {
    BlockHeader blockHeader1 = mockBlockHeader(1);
    BlockHeader blockHeader2 = mockBlockHeader(2);
    BlockHeader blockHeader3 = mockBlockHeader(3);

    GetBodiesFromPeerTask task =
        new GetBodiesFromPeerTask(
            List.of(blockHeader1, blockHeader2, blockHeader3), protocolSchedule);

    EthPeer successfulCandidate = mockPeer(5);

    Assertions.assertTrue(
        task.getPeerRequirementFilter().test(EthPeerImmutableAttributes.from(successfulCandidate)));
  }

  private BlockHeader mockBlockHeader(final long blockNumber) {
    BlockHeader blockHeader = Mockito.mock(BlockHeader.class);
    Mockito.when(blockHeader.getNumber()).thenReturn(blockNumber);
    // second to last hex digit indicates the blockNumber, last hex digit indicates the usage of the
    // hash
    Mockito.when(blockHeader.getHash())
        .thenReturn(Hash.fromHexString(StringUtils.repeat("00", 31) + blockNumber + "1"));
    Mockito.when(blockHeader.getBlockHash())
        .thenReturn(Hash.fromHexString(StringUtils.repeat("00", 31) + blockNumber + "1"));
    Mockito.when(blockHeader.getReceiptsRoot())
        .thenReturn(Hash.fromHexString(StringUtils.repeat("00", 31) + blockNumber + "2"));

    return blockHeader;
  }

  private static BlockHeader getNonEmptyBlockHeaderMock(final String transactionsRootHexString) {
    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getTransactionsRoot())
        .thenReturn(Hash.fromHexStringLenient(transactionsRootHexString));
    when(blockHeader.getOmmersHash()).thenReturn(Hash.EMPTY_LIST_HASH);
    when(blockHeader.getWithdrawalsRoot()).thenReturn(Optional.empty());
    return blockHeader;
  }

  private EthPeer mockPeer(final long chainHeight) {
    EthPeer ethPeer = Mockito.mock(EthPeer.class);
    ChainState chainState = Mockito.mock(ChainState.class);

    Mockito.when(ethPeer.chainState()).thenReturn(chainState);
    Mockito.when(chainState.getEstimatedHeight()).thenReturn(chainHeight);
    Mockito.when(chainState.getEstimatedTotalDifficulty()).thenReturn(Difficulty.of(0));
    Mockito.when(ethPeer.getReputation()).thenReturn(new PeerReputation());
    PeerConnection connection = mock(PeerConnection.class);
    Mockito.when(ethPeer.getConnection()).thenReturn(connection);
    return ethPeer;
  }
}
