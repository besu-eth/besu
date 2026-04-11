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

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.eth.core.Utils.receiptToSyncReceipt;
import static org.hyperledger.besu.ethereum.eth.core.Utils.serializeReceiptsList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.core.SyncBlockBody;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.SyncTransactionReceiptDecoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.SyncTransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.core.Utils;
import org.hyperledger.besu.ethereum.eth.manager.ChainState;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.PeerReputation;
import org.hyperledger.besu.ethereum.eth.manager.peertask.InvalidPeerTaskResponseException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.MalformedRlpFromPeerException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskValidationResponse;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncReceiptsFromPeerTask.Request;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncReceiptsFromPeerTask.Response;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.messages.GetPaginatedReceiptsMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetReceiptsMessage;
import org.hyperledger.besu.ethereum.eth.messages.ReceiptsMessage;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.SimpleNoCopyRlpEncoder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class GetSyncReceiptsFromPeerTaskTest {
  private static final Set<Capability> AGREED_CAPABILITIES_ETH69 = Set.of(EthProtocol.ETH69);
  private static final Set<Capability> AGREED_CAPABILITIES_LATEST = Set.of(EthProtocol.LATEST);
  private static ProtocolSchedule protocolSchedule;

  @BeforeAll
  public static void setup() {
    protocolSchedule = mock(ProtocolSchedule.class);
    final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
    final GasLimitCalculator gasLimitCalculator = mock(GasLimitCalculator.class);
    when(gasLimitCalculator.transactionGasLimitCap()).thenReturn(Long.MAX_VALUE);
    when(protocolSpec.getGasLimitCalculator()).thenReturn(gasLimitCalculator);
    when(protocolSpec.isPoS()).thenReturn(false);
    when(protocolSchedule.getByBlockHeader(Mockito.any())).thenReturn(protocolSpec);
    when(protocolSchedule.anyMatch(Mockito.any())).thenReturn(false);
  }

  @Test
  public void testGetSubProtocol() {
    final var task =
        createTask(new Request(List.of(mockBlock(1, 1).block), List.of()), protocolSchedule);
    assertEquals(EthProtocol.get(), task.getSubProtocol());
  }

  @Test
  public void testGetRequestMessageETH69() {
    final List<MockedBlock> mockedBlocks =
        List.of(mockBlock(1, 2), mockBlock(2, 1), mockBlock(3, 1));

    final GetSyncReceiptsFromPeerTask task =
        createTask(
            new Request(mockedBlocks.stream().map(MockedBlock::block).toList(), List.of()),
            protocolSchedule);

    final MessageData messageData = task.getRequestMessage(AGREED_CAPABILITIES_ETH69);
    final GetReceiptsMessage getReceiptsMessage = GetReceiptsMessage.readFrom(messageData);

    assertEquals(EthProtocolMessages.GET_RECEIPTS, getReceiptsMessage.getCode());

    final List<Hash> hashesInMessage = getReceiptsMessage.blockHashes();
    final List<Hash> expectedHashes =
        mockedBlocks.stream()
            .map(MockedBlock::block)
            .map(SyncBlock::getHeader)
            .map(BlockHeader::getHash)
            .toList();

    assertThat(expectedHashes).containsExactlyElementsOf(hashesInMessage);
  }

  @Test
  public void testGetRequestMessageLatest() {
    final List<MockedBlock> mockedBlocks =
        List.of(mockBlock(1, 2), mockBlock(2, 1), mockBlock(3, 1));

    final GetSyncReceiptsFromPeerTask task =
        createTask(
            new Request(
                mockedBlocks.stream().map(MockedBlock::block).toList(),
                List.of(toResponseReceipt(mockedBlocks.getFirst().receipts.getFirst()))),
            protocolSchedule);

    final MessageData messageData = task.getRequestMessage(AGREED_CAPABILITIES_LATEST);
    final GetPaginatedReceiptsMessage getReceiptsMessage =
        GetPaginatedReceiptsMessage.readFrom(messageData);

    assertEquals(EthProtocolMessages.GET_RECEIPTS, getReceiptsMessage.getCode());

    final List<Hash> hashesInMessage = getReceiptsMessage.blockHashes();
    final List<Hash> expectedHashes =
        mockedBlocks.stream()
            .map(MockedBlock::block)
            .map(SyncBlock::getHeader)
            .map(BlockHeader::getHash)
            .toList();

    assertThat(expectedHashes).containsExactlyElementsOf(hashesInMessage);

    assertThat(getReceiptsMessage.firstBlockReceiptIndex()).isEqualTo(1);
  }

  @Test
  public void testParseResponseWithNullResponseMessage() {
    final GetSyncReceiptsFromPeerTask task =
        createTask(new Request(List.of(mockBlock(1, 2).block), List.of()), protocolSchedule);
    assertThrows(
        InvalidPeerTaskResponseException.class, () -> task.processResponse(null, Set.of()));
  }

  @Test
  public void testParseResponse()
      throws InvalidPeerTaskResponseException, MalformedRlpFromPeerException {
    final List<MockedBlock> mockedBlocks =
        List.of(mockBlock(1, 1), mockBlock(2, 1), mockBlock(3, 1), mockBlock(4, 0));

    final GetSyncReceiptsFromPeerTask task =
        createTask(
            new Request(mockedBlocks.stream().map(MockedBlock::block).toList(), List.of()),
            protocolSchedule);

    final ReceiptsMessage receiptsMessage =
        ReceiptsMessage.createUnsafe(
            serializeReceiptsList(
                mockedBlocks.stream().map(MockedBlock::receipts).toList(),
                TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION));
    final MessageData rawMsg =
        new RawMessage(EthProtocolMessages.RECEIPTS, receiptsMessage.getData());

    final Response response = task.processResponse(rawMsg, AGREED_CAPABILITIES_ETH69);

    assertThat(response.completeReceiptsByBlock().values())
        .usingElementComparator(this::receiptsComparator)
        .containsExactlyInAnyOrder(
            toResponseReceipts(mockedBlocks.get(0).receipts),
            toResponseReceipts(mockedBlocks.get(1).receipts),
            toResponseReceipts(mockedBlocks.get(2).receipts),
            toResponseReceipts(mockedBlocks.get(3).receipts));
  }

  /** Builds a MessageData in eth/70 wire format: {@code <scalar(flag)> <list of receipt-lists>} */
  private MessageData buildEth70ReceiptsMessage(
      final List<List<TransactionReceipt>> receiptsByBlock, final boolean lastBlockIncomplete) {
    final BytesValueRLPOutput rlp = new BytesValueRLPOutput();
    rlp.writeLongScalar(lastBlockIncomplete ? 1 : 0);
    final Bytes serializedReceiptsList =
        serializeReceiptsList(
            receiptsByBlock, TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION);
    return new RawMessage(
        EthProtocolMessages.RECEIPTS, Bytes.concatenate(rlp.encoded(), serializedReceiptsList));
  }

  @Test
  public void testParseResponseWithEth70PaginatedLastBlockIncomplete()
      throws InvalidPeerTaskResponseException, MalformedRlpFromPeerException {
    // Block 1 has 2 receipts (complete), block 2 has 3 receipts but only 1 is returned (partial)
    final MockedBlock block1 = mockBlock(1, 2);
    final MockedBlock block2 = mockBlock(2, 3);

    final GetSyncReceiptsFromPeerTask task =
        createTask(new Request(List.of(block1.block, block2.block), List.of()), protocolSchedule);

    // Server returns block1 fully and 1 receipt from block2 (lastBlockIncomplete=true)
    final MessageData receiptsMessage =
        buildEth70ReceiptsMessage(
            List.of(block1.receipts, List.of(block2.receipts.getFirst())), true);

    final Response response = task.processResponse(receiptsMessage, AGREED_CAPABILITIES_LATEST);

    // Block1 is complete → in completeReceiptsByBlock
    assertThat(response.completeReceiptsByBlock()).hasSize(1);
    assertThat(response.completeReceiptsByBlock()).containsKey(block1.block);

    // Block2 is partial → in lastBlockPartialReceipts
    assertThat(response.lastBlockPartialReceipts()).hasSize(1);
    assertThat(response.lastBlockPartialReceipts().getFirst().getRlpBytes())
        .isEqualTo(toResponseReceipt(block2.receipts.getFirst()).getRlpBytes());
  }

  @Test
  public void testParseResponseWithEth70AllReceiptsCompleteLastBlockIncompleteFalse()
      throws InvalidPeerTaskResponseException, MalformedRlpFromPeerException {
    final MockedBlock block1 = mockBlock(1, 1);
    final MockedBlock block2 = mockBlock(2, 2);

    final GetSyncReceiptsFromPeerTask task =
        createTask(new Request(List.of(block1.block, block2.block), List.of()), protocolSchedule);

    // Server returns both blocks fully (lastBlockIncomplete=false)
    final MessageData receiptsMessage =
        buildEth70ReceiptsMessage(List.of(block1.receipts, block2.receipts), false);

    final Response response = task.processResponse(receiptsMessage, AGREED_CAPABILITIES_LATEST);

    assertThat(response.lastBlockPartialReceipts()).isEmpty();
    assertThat(response.completeReceiptsByBlock()).containsOnlyKeys(block1.block, block2.block);
  }

  @Test
  public void testParseResponseCombinesPartialReceiptsFromPreviousRequest()
      throws InvalidPeerTaskResponseException, MalformedRlpFromPeerException {
    // Block has 3 receipts; previous request delivered receipts[0], now we get receipts[1..2]
    final MockedBlock block = mockBlock(1, 3);

    final List<SyncTransactionReceipt> alreadyFetched =
        List.of(toResponseReceipt(block.receipts.getFirst()));

    final GetSyncReceiptsFromPeerTask task =
        createTask(new Request(List.of(block.block), alreadyFetched), protocolSchedule);

    // Server returns receipts[1..2] (eth/70 format, lastBlockIncomplete=false)
    final MessageData receiptsMessage =
        buildEth70ReceiptsMessage(List.of(block.receipts.subList(1, 3)), false);

    final Response response = task.processResponse(receiptsMessage, AGREED_CAPABILITIES_LATEST);

    // completeFirstBlock() should prepend alreadyFetched and produce all 3 receipts
    assertThat(response.lastBlockPartialReceipts()).isEmpty();
    assertThat(response.completeReceiptsByBlock()).containsKey(block.block);
    assertThat(response.completeReceiptsByBlock().get(block.block)).hasSize(3);
  }

  @Test
  public void testParseResponseWithEth70LastBlockIncompleteTrueAndEmptyListThrows() {
    final MockedBlock block = mockBlock(1, 2);

    final GetSyncReceiptsFromPeerTask task =
        createTask(new Request(List.of(block.block), List.of()), protocolSchedule);

    // Malicious server sends lastBlockIncomplete=1 but empty receipt list
    final MessageData receiptsMessage = buildEth70ReceiptsMessage(List.of(), true);

    assertThatThrownBy(() -> task.processResponse(receiptsMessage, AGREED_CAPABILITIES_LATEST))
        .isInstanceOf(InvalidPeerTaskResponseException.class);
  }

  @Test
  public void testParseResponseFailsWhenReceiptsForTooManyBlocksAreReturned() {
    final List<MockedBlock> mockedBlocks =
        List.of(mockBlock(1, 1), mockBlock(2, 1), mockBlock(3, 1));

    final GetSyncReceiptsFromPeerTask task =
        createTask(
            new Request(mockedBlocks.stream().map(MockedBlock::block).toList(), List.of()),
            protocolSchedule);

    final MockedBlock extraMockedBlock = mockBlock(4, 1);

    final ReceiptsMessage receiptsMessage =
        ReceiptsMessage.createUnsafe(
            serializeReceiptsList(
                Stream.concat(mockedBlocks.stream(), Stream.of(extraMockedBlock))
                    .map(MockedBlock::receipts)
                    .toList(),
                TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION));
    final MessageData rawMsg =
        new RawMessage(EthProtocolMessages.RECEIPTS, receiptsMessage.getData());

    assertThatThrownBy(() -> task.processResponse(rawMsg, AGREED_CAPABILITIES_ETH69))
        .isInstanceOf(InvalidPeerTaskResponseException.class)
        .hasMessageContaining("Too many result returned");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testGetPeerRequirementFilter(final boolean isPoS) {
    reset(protocolSchedule);
    when(protocolSchedule.anyMatch(any())).thenReturn(isPoS);
    final List<MockedBlock> mockedBlocks = List.of(mockBlock(1, 1), mockBlock(2, 1));

    final GetSyncReceiptsFromPeerTask task =
        createTask(
            new Request(mockedBlocks.stream().map(MockedBlock::block).toList(), List.of()),
            protocolSchedule);

    EthPeer failForShortChainHeight = mockPeer(1);
    EthPeer successfulCandidate = mockPeer(5);

    assertThat(
            task.getPeerRequirementFilter()
                .test(EthPeerImmutableAttributes.from(failForShortChainHeight)))
        .isEqualTo(isPoS);
    Assertions.assertTrue(
        task.getPeerRequirementFilter().test(EthPeerImmutableAttributes.from(successfulCandidate)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void validateResultFailsWhenNoResultAreReturned(
      final boolean hasFirstBlockPartialReceipts) {
    final MockedBlock mockedBlock = mockBlock(1, 1);
    final GetSyncReceiptsFromPeerTask task =
        createTask(
            new Request(
                List.of(mockedBlock.block),
                hasFirstBlockPartialReceipts
                    ? toResponseReceipts(mockedBlock.receipts)
                    : emptyList()),
            protocolSchedule);

    assertEquals(
        PeerTaskValidationResponse.NO_RESULTS_RETURNED,
        task.validateResult(new Response(Map.of(), List.of())));
  }

  static List<Arguments> validateResultProvider() {
    return List.of(
        Arguments.of(false, false),
        Arguments.of(false, true),
        Arguments.of(true, false),
        Arguments.of(true, true));
  }

  @ParameterizedTest
  @MethodSource("validateResultProvider")
  public void testValidateResultForFullSuccess(
      final boolean hasFirstBlockPartialReceipts, final boolean lastBlockIncomplete) {
    final MockedBlock mockedBlock = mockBlock(1, 1);
    final MockedBlock lastMockedBlock = mockBlock(2, 3);

    final GetSyncReceiptsFromPeerTask task =
        createTask(
            new Request(
                List.of(mockedBlock.block, lastMockedBlock.block),
                hasFirstBlockPartialReceipts
                    ? List.of(toResponseReceipt(lastMockedBlock.receipts.getFirst()))
                    : emptyList()),
            protocolSchedule);

    final List<SyncTransactionReceipt> expectedLastBlockPartialReceipts =
        lastBlockIncomplete
            ? toResponseReceipts(lastMockedBlock.receipts).subList(0, 1)
            : List.of();

    final Map<SyncBlock, List<SyncTransactionReceipt>> expectedCompletedBlocks = new HashMap<>();
    expectedCompletedBlocks.put(mockedBlock.block, toResponseReceipts(mockedBlock.receipts));
    if (!lastBlockIncomplete) {
      expectedCompletedBlocks.put(
          lastMockedBlock.block, toResponseReceipts(lastMockedBlock.receipts));
    }

    assertEquals(
        PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD,
        task.validateResult(
            new Response(expectedCompletedBlocks, expectedLastBlockPartialReceipts)));
  }

  @Test
  public void validateResultFailsReceiptRootDoesNotMatch() {
    final MockedBlock mockedRequestedBlock = mockBlock(1, 1);

    final GetSyncReceiptsFromPeerTask task =
        createTask(new Request(List.of(mockedRequestedBlock.block), List.of()), protocolSchedule);

    final List<TransactionReceipt> anotherBlockReceipts = mockBlock(2, 1).receipts;

    assertEquals(
        PeerTaskValidationResponse.RESULTS_DO_NOT_MATCH_QUERY,
        task.validateResult(
            new Response(
                // for the requested block, receipts returned are from another block
                Map.of(mockedRequestedBlock.block, toResponseReceipts(anotherBlockReceipts)),
                List.of())));
  }

  @Test
  public void validateResultPassesForFrontierReceiptsAfterDecoderRoundTrip() {
    // Regression test for eth/69 Frontier receipt root calculation.
    // Exercises the full decode path (encodeForRootCalculation is invoked) rather than
    // the single-arg constructor path used by toResponseReceipt() in other tests.
    final MockedBlock block = mockBlock(1, 2);

    final GetSyncReceiptsFromPeerTask task =
        createTask(new Request(List.of(block.block), List.of()), protocolSchedule);

    final SyncTransactionReceiptDecoder decoder = new SyncTransactionReceiptDecoder();
    final List<SyncTransactionReceipt> decodedReceipts =
        block.receipts.stream()
            .map(
                receipt -> {
                  final BytesValueRLPOutput out = new BytesValueRLPOutput();
                  TransactionReceiptEncoder.writeTo(
                      receipt,
                      out,
                      TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION);
                  return decoder.decode(out.encoded());
                })
            .toList();

    assertEquals(
        PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD,
        task.validateResult(new Response(Map.of(block.block, decodedReceipts), List.of())));
  }

  @Test
  public void validateResultSuccessWhenPartialBlockIsIncomplete() {
    final MockedBlock mockedBlock = mockBlock(1, 3);

    final GetSyncReceiptsFromPeerTask task =
        createTask(
            new Request(
                List.of(mockedBlock.block),
                List.of(toResponseReceipt(mockedBlock.receipts.getFirst()))),
            protocolSchedule);

    assertEquals(
        PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD,
        task.validateResult(
            new Response(Map.of(), toResponseReceipts(mockedBlock.receipts).subList(0, 2))));
  }

  @Test
  public void validateResultFailsWhenPartialReceiptSizeExceedsTxGasLimitBound() {
    // txGasLimitCap=800 → per-receipt threshold = 100 bytes; a 101-byte receipt must be rejected
    final MockedBlock block = mockBlockWithGasLimit(1, 1, 30_000_000L);
    final GetSyncReceiptsFromPeerTask task =
        createTask(
            new Request(List.of(block.block), List.of()),
            createProtocolScheduleWithTxGasLimitCap(800L));

    final SyncTransactionReceipt oversizedReceipt =
        new SyncTransactionReceipt(Bytes.of(new byte[101]));

    assertEquals(
        PeerTaskValidationResponse.INVALID_RECEIPT_RETURNED,
        task.validateResult(new Response(Map.of(), List.of(oversizedReceipt))));
  }

  @Test
  public void validateResultFailsWhenCumulativePartialReceiptSizeExceedsBlockGasLimitBound() {
    // blockGasLimit=800 → cumulative threshold = 100 bytes; two 60-byte receipts (total 120) must
    // be rejected even though each individually is within the per-receipt bound
    final MockedBlock block = mockBlockWithGasLimit(1, 2, 800L);
    final GetSyncReceiptsFromPeerTask task =
        createTask(
            new Request(List.of(block.block), List.of()),
            createProtocolScheduleWithTxGasLimitCap(Long.MAX_VALUE));

    final List<SyncTransactionReceipt> partialReceipts =
        List.of(
            new SyncTransactionReceipt(Bytes.of(new byte[60])),
            new SyncTransactionReceipt(Bytes.of(new byte[60])));

    assertEquals(
        PeerTaskValidationResponse.INVALID_RECEIPT_RETURNED,
        task.validateResult(new Response(Map.of(), partialReceipts)));
  }

  @Test
  public void validateResultPassesWhenPartialReceiptSizesAreWithinBounds() {
    // txGasLimitCap=800 → per-receipt threshold=100; blockGasLimit=800 → cumulative threshold=100
    // Two 40-byte receipts: each 40 < 100, cumulative 80 < 100 → valid
    final MockedBlock block = mockBlockWithGasLimit(1, 2, 800L);
    final GetSyncReceiptsFromPeerTask task =
        createTask(
            new Request(List.of(block.block), List.of()),
            createProtocolScheduleWithTxGasLimitCap(800L));

    final List<SyncTransactionReceipt> partialReceipts =
        List.of(
            new SyncTransactionReceipt(Bytes.of(new byte[40])),
            new SyncTransactionReceipt(Bytes.of(new byte[40])));

    assertEquals(
        PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD,
        task.validateResult(new Response(Map.of(), partialReceipts)));
  }

  @Test
  public void validateResultChecksAllPartialReceiptsEvenWhenFirstRequestBlockIsComplete() {
    // Regression test: when the first block in the request becomes complete and the partial
    // block is a later one, the size loop must start from index 0 (not
    // firstBlockPartialReceipts.size()) so none of the later block's receipts are skipped.
    final MockedBlock blockA = mockBlock(1, 1);
    final MockedBlock blockB = mockBlockWithGasLimit(2, 1, 800L);
    final SyncTransactionReceipt receiptForA = toResponseReceipt(blockA.receipts.getFirst());

    final GetSyncReceiptsFromPeerTask task =
        createTask(
            new Request(List.of(blockA.block, blockB.block), List.of(receiptForA)),
            createProtocolScheduleWithTxGasLimitCap(800L));

    // blockA is complete (receipt root matches), blockB has a single oversized partial receipt
    final SyncTransactionReceipt oversizedReceiptForB =
        new SyncTransactionReceipt(Bytes.of(new byte[101]));

    assertEquals(
        PeerTaskValidationResponse.INVALID_RECEIPT_RETURNED,
        task.validateResult(
            new Response(
                Map.of(blockA.block, List.of(receiptForA)), List.of(oversizedReceiptForB))));
  }

  private static BlockHeader mockBlockHeader(
      final long blockNumber, final List<TransactionReceipt> receipts) {
    BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getNumber()).thenReturn(blockNumber);
    // second to last hex digit indicates the blockNumber,
    // last hex digit indicates the usage of the hash
    when(blockHeader.getHash())
        .thenReturn(Hash.fromHexString(StringUtils.repeat("00", 31) + blockNumber + "1"));
    when(blockHeader.getReceiptsRoot()).thenReturn(BodyValidation.receiptsRoot(receipts));
    when(blockHeader.getGasLimit()).thenReturn(30_000_000L);

    return blockHeader;
  }

  private static List<TransactionReceipt> mockTransactionReceipts(
      final long blockNumber, final int count) {

    return IntStream.rangeClosed(1, count)
        .mapToObj(
            i -> new TransactionReceipt(1, 123L * i + blockNumber, emptyList(), Optional.empty()))
        .toList();
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

  private GetSyncReceiptsFromPeerTask createTask(
      final Request request, final ProtocolSchedule protocolSchedule) {
    return new GetSyncReceiptsFromPeerTask(
        request, protocolSchedule, new SyncTransactionReceiptEncoder(new SimpleNoCopyRlpEncoder()));
  }

  private SyncTransactionReceipt toResponseReceipt(final TransactionReceipt receipt) {
    return receiptToSyncReceipt(receipt, TransactionReceiptEncodingConfiguration.DEFAULT);
  }

  private List<SyncTransactionReceipt> toResponseReceipts(final List<TransactionReceipt> receipts) {
    return receipts.stream().map(this::toResponseReceipt).toList();
  }

  private int receiptsComparator(
      final List<SyncTransactionReceipt> receipts1, final List<SyncTransactionReceipt> receipts2) {
    if (receipts1.size() != receipts2.size()) {
      return receipts1.size() - receipts2.size();
    }
    for (int i = 0; i < receipts1.size(); i++) {
      if (Utils.compareSyncReceipts(receipts1.get(i), receipts2.get(i)) != 0) {
        // quick tiebreak since we are not interested in the order here
        return receipts1.hashCode() - receipts2.hashCode();
      }
    }
    return 0;
  }

  private MockedBlock mockBlock(final long number, final int txCount) {
    final SyncBlockBody body = mock(SyncBlockBody.class);
    when(body.getTransactionCount()).thenReturn(txCount);
    final List<TransactionReceipt> receipts = mockTransactionReceipts(number, txCount);
    return new MockedBlock(new SyncBlock(mockBlockHeader(number, receipts), body), receipts);
  }

  private MockedBlock mockBlockWithGasLimit(
      final long number, final int txCount, final long blockGasLimit) {
    final SyncBlockBody body = mock(SyncBlockBody.class);
    when(body.getTransactionCount()).thenReturn(txCount);
    final List<TransactionReceipt> receipts = mockTransactionReceipts(number, txCount);
    final BlockHeader header = mockBlockHeader(number, receipts);
    when(header.getGasLimit()).thenReturn(blockGasLimit);
    return new MockedBlock(new SyncBlock(header, body), receipts);
  }

  private ProtocolSchedule createProtocolScheduleWithTxGasLimitCap(final long txGasLimitCap) {
    final ProtocolSchedule ps = mock(ProtocolSchedule.class);
    final ProtocolSpec spec = mock(ProtocolSpec.class);
    final GasLimitCalculator calc = mock(GasLimitCalculator.class);
    when(calc.transactionGasLimitCap()).thenReturn(txGasLimitCap);
    when(spec.getGasLimitCalculator()).thenReturn(calc);
    when(spec.isPoS()).thenReturn(false);
    when(ps.getByBlockHeader(any())).thenReturn(spec);
    when(ps.anyMatch(any())).thenReturn(false);
    return ps;
  }

  private record MockedBlock(SyncBlock block, List<TransactionReceipt> receipts) {}
}
