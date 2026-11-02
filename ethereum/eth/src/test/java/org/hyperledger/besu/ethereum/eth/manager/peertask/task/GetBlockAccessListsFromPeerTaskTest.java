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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.EthProtocolVersion;
import org.hyperledger.besu.ethereum.eth.manager.ChainState;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.PeerReputation;
import org.hyperledger.besu.ethereum.eth.manager.peertask.InvalidPeerTaskResponseException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.MalformedRlpFromPeerException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskValidationResponse;
import org.hyperledger.besu.ethereum.eth.messages.BlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockAccessListsMessage;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class GetBlockAccessListsFromPeerTaskTest {

  private static final Set<org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability>
      AGREED_CAPABILITIES = Set.of(EthProtocol.LATEST);
  private static final BlockDataGenerator dataGenerator = new BlockDataGenerator(1);

  @Test
  void testGetRequestMessage() {
    final Hash hash1 = Hash.fromHexString(StringUtils.repeat("00", 31) + "11");
    final Hash hash2 = Hash.fromHexString(StringUtils.repeat("00", 31) + "21");

    final BlockHeader header1 = mockBlockHeader(1, new BlockAccessList(List.of()), hash1);
    final BlockHeader header2 = mockBlockHeader(2, new BlockAccessList(List.of()), hash2);

    final GetBlockAccessListsFromPeerTask task =
        new GetBlockAccessListsFromPeerTask(List.of(header1, header2));

    final MessageData messageData = task.getRequestMessage(AGREED_CAPABILITIES);
    final GetBlockAccessListsMessage message = GetBlockAccessListsMessage.readFrom(messageData);

    assertThat(message.getCode()).isEqualTo(EthProtocolMessages.GET_BLOCK_ACCESS_LISTS);
    assertThat(message.blockHashes())
        .containsExactly(Hash.fromHexStringLenient("0x11"), Hash.fromHexStringLenient("0x21"));
  }

  @Test
  void testProcessResponseWithMatchingData()
      throws InvalidPeerTaskResponseException, MalformedRlpFromPeerException {
    final BlockAccessList blockAccessList = new BlockAccessList(List.of());
    final BlockHeader header = mockBlockHeader(1, blockAccessList);

    final GetBlockAccessListsFromPeerTask task =
        new GetBlockAccessListsFromPeerTask(List.of(header));

    final List<Optional<BlockAccessList>> response =
        task.processResponse(
            BlockAccessListsMessage.createFromBlockAccessLists(List.of(blockAccessList)), Set.of());

    assertThat(response).containsExactly(Optional.of(blockAccessList));
  }

  @Test
  void testProcessResponseWithUnavailableEntry()
      throws InvalidPeerTaskResponseException, MalformedRlpFromPeerException {
    final BlockHeader header = mockBlockHeader(1, new BlockAccessList(List.of()));
    final GetBlockAccessListsFromPeerTask task =
        new GetBlockAccessListsFromPeerTask(List.of(header));

    // an empty string is the legitimate "BAL not available" marker, not malformed data
    final List<Optional<BlockAccessList>> response =
        task.processResponse(
            BlockAccessListsMessage.createUnsafe(rlpList(out -> out.writeBytes(Bytes.EMPTY))),
            Set.of());

    assertThat(response).containsExactly(Optional.empty());
  }

  @Test
  void testProcessResponseWithTruncatedListIsMalformed() {
    final GetBlockAccessListsFromPeerTask task = taskForOneHeader();
    // a list header that declares more payload than is present
    final Bytes truncated = Bytes.fromHexString("0xc5c0");

    assertMalformed(task, truncated);
  }

  @Test
  void testProcessResponseWithNonListPayloadIsMalformed() {
    final GetBlockAccessListsFromPeerTask task = taskForOneHeader();
    final Bytes notAList = Bytes.fromHexString("0x83010203");

    assertMalformed(task, notAList);
  }

  @Test
  void testProcessResponseWithMalformedEntryIsMalformed() {
    final GetBlockAccessListsFromPeerTask task = taskForOneHeader();
    // [[[0x01]]]: one BAL with one account whose address is a single byte instead of 20
    final Bytes badAddress =
        rlpList(
            bal -> {
              bal.startList();
              bal.startList();
              bal.writeBytes(Bytes.of(1));
              bal.endList();
              bal.endList();
            });

    assertMalformed(task, badAddress);
  }

  @Test
  void testProcessResponseWithMalformedEntryAfterValidOneIsMalformed() {
    final BlockAccessList valid = new BlockAccessList(List.of());
    final GetBlockAccessListsFromPeerTask task =
        new GetBlockAccessListsFromPeerTask(
            List.of(mockBlockHeader(1, valid), mockBlockHeader(2, valid)));
    // entries are decoded lazily, so a bad entry behind a good one must still be caught
    final Bytes validThenBad =
        rlpList(
            out -> {
              out.startList();
              out.endList();
              out.writeBytes(Bytes.of(1, 2, 3));
            });

    assertMalformed(task, validThenBad);
  }

  private GetBlockAccessListsFromPeerTask taskForOneHeader() {
    return new GetBlockAccessListsFromPeerTask(
        List.of(mockBlockHeader(1, new BlockAccessList(List.of()))));
  }

  private static void assertMalformed(
      final GetBlockAccessListsFromPeerTask task, final Bytes payload) {
    final MalformedRlpFromPeerException exception =
        assertThrows(
            MalformedRlpFromPeerException.class,
            () -> task.processResponse(BlockAccessListsMessage.createUnsafe(payload), Set.of()));
    assertThat(exception.getCause()).isInstanceOf(RLPException.class);
    assertThat(exception.getMessageData()).isEqualTo(payload);
  }

  private static Bytes rlpList(final java.util.function.Consumer<BytesValueRLPOutput> entries) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    entries.accept(out);
    out.endList();
    return out.encoded();
  }

  @Test
  void testValidateResultWithMismatchedBal() {
    final BlockAccessList expectedBal = new BlockAccessList(List.of());
    final BlockHeader header = mockBlockHeader(1, expectedBal);

    final BlockAccessList mismatchingBal = dataGenerator.blockAccessList(1);

    final GetBlockAccessListsFromPeerTask task =
        new GetBlockAccessListsFromPeerTask(List.of(header));

    assertThat(task.validateResult(List.of(Optional.of(mismatchingBal))))
        .isEqualTo(PeerTaskValidationResponse.RESULTS_DO_NOT_MATCH_QUERY);
  }

  @Test
  void testRequiresNonEmptyHeaders() {
    assertThrows(
        IllegalArgumentException.class, () -> new GetBlockAccessListsFromPeerTask(List.of()));
  }

  @Test
  void testRequiresHeadersWithBalHash() {
    final BlockHeader headerWithoutBalHash = Mockito.mock(BlockHeader.class);
    Mockito.when(headerWithoutBalHash.getBalHash()).thenReturn(java.util.Optional.empty());

    assertThrows(
        IllegalArgumentException.class,
        () -> new GetBlockAccessListsFromPeerTask(List.of(headerWithoutBalHash)));
  }

  @Test
  void testSingleAttemptDisablesRetries() {
    final BlockHeader header = mockBlockHeader(3, new BlockAccessList(List.of()));

    final GetBlockAccessListsFromPeerTask defaultTask =
        new GetBlockAccessListsFromPeerTask(List.of(header));
    assertThat(defaultTask.getRetriesWithOtherPeer()).isPositive();
    assertThat(defaultTask.getRetriesWithSamePeer()).isPositive();

    final GetBlockAccessListsFromPeerTask singleAttemptTask =
        new GetBlockAccessListsFromPeerTask(List.of(header), true);
    assertThat(singleAttemptTask.getRetriesWithOtherPeer()).isZero();
    assertThat(singleAttemptTask.getRetriesWithSamePeer()).isZero();
  }

  @Test
  void testGetPeerRequirementFilter() {
    final BlockHeader header = mockBlockHeader(3, new BlockAccessList(List.of()));
    final GetBlockAccessListsFromPeerTask task =
        new GetBlockAccessListsFromPeerTask(List.of(header));

    final EthPeer successfulCandidate = mockPeer(5, true);
    final EthPeer lowHeightCandidate = mockPeer(2, true);
    final EthPeer nonEth71Candidate = mockPeer(5, false);

    assertThat(
            task.getPeerRequirementFilter()
                .test(EthPeerImmutableAttributes.from(successfulCandidate)))
        .isTrue();
    assertThat(
            task.getPeerRequirementFilter()
                .test(EthPeerImmutableAttributes.from(lowHeightCandidate)))
        .isFalse();
    assertThat(
            task.getPeerRequirementFilter()
                .test(EthPeerImmutableAttributes.from(nonEth71Candidate)))
        .isFalse();
  }

  @Test
  void testValidateResult() {
    final BlockHeader header = mockBlockHeader(1, new BlockAccessList(List.of()));
    final GetBlockAccessListsFromPeerTask task =
        new GetBlockAccessListsFromPeerTask(List.of(header));

    assertThat(task.validateResult(List.of()))
        .isEqualTo(PeerTaskValidationResponse.NO_RESULTS_RETURNED);
    assertThat(task.validateResult(List.of(Optional.empty())))
        .isEqualTo(PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD);
    assertThat(task.validateResult(List.of(Optional.of(dataGenerator.emptyBlockAccessList()))))
        .isEqualTo(PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD);
    assertThat(
            task.validateResult(
                List.of(
                    Optional.of(new BlockAccessList(List.of())),
                    Optional.of(new BlockAccessList(List.of())))))
        .isEqualTo(PeerTaskValidationResponse.TOO_MANY_RESULTS_RETURNED);
  }

  private BlockHeader mockBlockHeader(final long blockNumber, final BlockAccessList bal) {
    return mockBlockHeader(blockNumber, bal, Hash.ZERO);
  }

  private BlockHeader mockBlockHeader(
      final long blockNumber, final BlockAccessList bal, final Hash hash) {
    final BlockHeader blockHeader = Mockito.mock(BlockHeader.class);
    Mockito.when(blockHeader.getNumber()).thenReturn(blockNumber);
    Mockito.when(blockHeader.getBlockHash()).thenReturn(hash);
    Mockito.when(blockHeader.getBalHash())
        .thenReturn(java.util.Optional.of(BodyValidation.balHash(bal)));
    return blockHeader;
  }

  private EthPeer mockPeer(final long chainHeight, final boolean eth71Compatible) {
    final EthPeer ethPeer = Mockito.mock(EthPeer.class);
    final ChainState chainState = Mockito.mock(ChainState.class);

    Mockito.when(ethPeer.chainState()).thenReturn(chainState);
    Mockito.when(chainState.getEstimatedHeight()).thenReturn(chainHeight);
    Mockito.when(chainState.getEstimatedTotalDifficulty()).thenReturn(Difficulty.of(0));
    Mockito.when(ethPeer.getReputation()).thenReturn(new PeerReputation());
    final PeerConnection connection = Mockito.mock(PeerConnection.class);
    Mockito.when(ethPeer.getConnection()).thenReturn(connection);
    Mockito.when(ethPeer.getAgreedCapabilities())
        .thenReturn(
            eth71Compatible
                ? Set.of(EthProtocolVersion.V71.getCapability())
                : Set.of(EthProtocolVersion.V69.getCapability()));
    return ethPeer;
  }
}
