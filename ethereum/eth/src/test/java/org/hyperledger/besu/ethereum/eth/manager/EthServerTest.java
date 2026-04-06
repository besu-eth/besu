/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.manager;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.eth.core.Utils.serializeReceiptsList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.ImmutableEthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.ProtocolViolationException;
import org.hyperledger.besu.ethereum.eth.messages.BlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.messages.BlockBodiesMessage;
import org.hyperledger.besu.ethereum.eth.messages.BlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockBodiesMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetNodeDataMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetPaginatedReceiptsMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetPooledTransactionsMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetReceiptsMessage;
import org.hyperledger.besu.ethereum.eth.messages.NodeDataMessage;
import org.hyperledger.besu.ethereum.eth.messages.PaginatedReceiptsMessage;
import org.hyperledger.besu.ethereum.eth.messages.PooledTransactionsMessage;
import org.hyperledger.besu.ethereum.eth.messages.ReceiptsMessage;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class EthServerTest {

  private final BlockDataGenerator dataGenerator = new BlockDataGenerator(0);
  private final Blockchain blockchain = mock(Blockchain.class);
  private final WorldStateArchive worldStateArchive = mock(WorldStateArchive.class);
  private final TransactionPool transactionPool = mock(TransactionPool.class);
  private final EthPeer ethPeer = mock(EthPeer.class);
  private final EthMessages ethMessages = new EthMessages();

  @Test
  public void shouldHandleDataBeingUnavailableWhenRespondingToNodeDataRequests() {
    final Map<Hash, Bytes> nodeData = setupNodeData(1);
    setupEthServer();

    final List<Hash> hashes = new ArrayList<>(nodeData.keySet());
    hashes.add(dataGenerator.hash()); // Add unknown hash
    final List<Bytes> expectedResult = new ArrayList<>(nodeData.values());

    assertThat(
            ethMessages.dispatch(
                new EthMessage(ethPeer, GetNodeDataMessage.create(hashes)), EthProtocol.LATEST))
        .contains(NodeDataMessage.create(expectedResult));
  }

  @Test
  public void shouldLimitNumberOfResponsesToNodeDataRequests() {
    final int limit = 2;
    final Map<Hash, Bytes> nodeData = setupNodeData(3);
    setupEthServer(b -> b.maxGetNodeData(limit));

    final List<Hash> hashes = new ArrayList<>(nodeData.keySet());
    final List<Bytes> expectedResult =
        hashes.stream().limit(limit).map(nodeData::get).collect(Collectors.toList());

    assertThat(
            ethMessages.dispatch(
                new EthMessage(ethPeer, GetNodeDataMessage.create(hashes)), EthProtocol.LATEST))
        .contains(NodeDataMessage.create(expectedResult));
  }

  @Test
  public void shouldLimitTheNumberOfNodeDataResponsesLookedUpNotTheNumberReturned() {
    final Map<Hash, Bytes> nodeData = setupNodeData(2);
    setupEthServer(b -> b.maxGetNodeData(2));

    final List<Hash> knownHashes = new ArrayList<>(nodeData.keySet());
    final List<Hash> hashes =
        List.of(
            knownHashes.get(0),
            dataGenerator.hash(), // Insert a hash that will return an empty response
            knownHashes.get(1));
    final List<Bytes> expectedResult = singletonList(nodeData.get(knownHashes.get(0)));

    assertThat(
            ethMessages.dispatch(
                new EthMessage(ethPeer, GetNodeDataMessage.create(hashes)), EthProtocol.LATEST))
        .contains(NodeDataMessage.create(expectedResult));
  }

  @Test
  public void shouldLimitNodeDataByMessageSize() {
    final Map<Hash, Bytes> nodeData = setupNodeData(10);
    final List<Hash> hashes = new ArrayList<>(nodeData.keySet());
    int sizeLimit = RLP.MAX_PREFIX_SIZE;
    final List<Bytes> expectedResult = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      final Bytes data = nodeData.get(hashes.get(i));
      expectedResult.add(data);
      sizeLimit += calculateRlpEncodedSize(data);
    }

    final int messageSizeLimit = sizeLimit;
    setupEthServer(b -> b.maxMessageSize(messageSizeLimit));

    assertThat(
            ethMessages.dispatch(
                new EthMessage(ethPeer, GetNodeDataMessage.create(hashes)), EthProtocol.LATEST))
        .contains(NodeDataMessage.create(expectedResult));
  }

  @Test
  public void shouldLimitBlockHeadersByMessageSize() {
    final List<Block> blocks = setupBlocks(10);
    final List<BlockHeader> expectedHeaders = new ArrayList<>();
    int sizeLimit = RLP.MAX_PREFIX_SIZE;
    for (int i = 0; i < 4; i++) {
      final BlockHeader header = blocks.get(i).getHeader();
      sizeLimit += calculateRlpEncodedSize(header);
      expectedHeaders.add(header);
    }

    final int msgSizeLimit = sizeLimit;
    setupEthServer(b -> b.maxMessageSize(msgSizeLimit));

    // Request all blocks, which will exceed the limit
    final BlockHeader firstHeader = blocks.get(0).getHeader();
    final GetBlockHeadersMessage headersMsg =
        GetBlockHeadersMessage.create(firstHeader.getHash(), blocks.size(), 0, false);
    final EthMessage ethMsg = new EthMessage(ethPeer, headersMsg);

    // Check response
    final BlockHeadersMessage expectedMsg = BlockHeadersMessage.create(expectedHeaders);
    final Optional<MessageData> result = ethMessages.dispatch(ethMsg, EthProtocol.LATEST);
    assertThat(result).contains(expectedMsg);
  }

  @Test
  public void shouldLimitBlockHeadersByCount() {
    final int blockCount = 10;
    final int limit = 6;
    final List<Block> blocks = setupBlocks(blockCount);
    final List<BlockHeader> expectedHeaders =
        blocks.stream().limit(limit).map(Block::getHeader).collect(Collectors.toList());

    setupEthServer(b -> b.maxGetBlockHeaders(limit));

    // Request all blocks, which will exceed the limit
    final BlockHeader firstHeader = blocks.get(0).getHeader();
    final GetBlockHeadersMessage headersMsg =
        GetBlockHeadersMessage.create(firstHeader.getHash(), blocks.size(), 0, false);
    final EthMessage ethMsg = new EthMessage(ethPeer, headersMsg);

    // Check response
    final BlockHeadersMessage expectedMsg = BlockHeadersMessage.create(expectedHeaders);
    final Optional<MessageData> result = ethMessages.dispatch(ethMsg, EthProtocol.LATEST);
    assertThat(result).contains(expectedMsg);
  }

  @Test
  public void shouldLimitBlockBodiesByMessageSize() {
    final List<Block> blocks = setupBlocks(10);
    final List<BlockBody> expectedBodies = new ArrayList<>();
    int sizeLimit = RLP.MAX_PREFIX_SIZE;
    for (int i = 0; i < 4; i++) {
      final BlockBody body = blocks.get(i).getBody();
      sizeLimit += calculateRlpEncodedSize(body);
      expectedBodies.add(body);
    }

    final int msgSizeLimit = sizeLimit;
    setupEthServer(b -> b.maxMessageSize(msgSizeLimit));

    // Request all blocks, which will exceed the limit
    final List<Hash> blockHashes = blocks.stream().map(Block::getHash).collect(Collectors.toList());
    final GetBlockBodiesMessage bodiesMsg = GetBlockBodiesMessage.create(blockHashes);
    final EthMessage ethMsg = new EthMessage(ethPeer, bodiesMsg);

    // Check response
    final BlockBodiesMessage expectedMsg = BlockBodiesMessage.create(expectedBodies);
    final Optional<MessageData> result = ethMessages.dispatch(ethMsg, EthProtocol.LATEST);
    assertThat(result).contains(expectedMsg);
  }

  @Test
  public void shouldLimitBlockBodiesByCount() {
    final int blockCount = 10;
    final int limit = 6;
    final List<Block> blocks = setupBlocks(blockCount);
    final List<BlockBody> expectedBodies =
        blocks.stream().limit(limit).map(Block::getBody).collect(Collectors.toList());

    setupEthServer(b -> b.maxGetBlockBodies(limit));

    // Request all blocks, which will exceed the limit
    final List<Hash> blockHashes = blocks.stream().map(Block::getHash).collect(Collectors.toList());
    final GetBlockBodiesMessage bodiesMsg = GetBlockBodiesMessage.create(blockHashes);
    final EthMessage ethMsg = new EthMessage(ethPeer, bodiesMsg);

    // Check response
    final BlockBodiesMessage expectedMsg = BlockBodiesMessage.create(expectedBodies);
    final Optional<MessageData> result = ethMessages.dispatch(ethMsg, EthProtocol.LATEST);
    assertThat(result).contains(expectedMsg);
  }

  @Test
  public void shouldLimitTxReceiptsByMessageSize() {
    final Map<Hash, List<TransactionReceipt>> receiptsByHash = setupBlockReceipts(10);
    final List<Hash> hashes = new ArrayList<>(receiptsByHash.keySet());
    final List<List<TransactionReceipt>> expectedResults = new ArrayList<>();
    int sizeLimit = RLP.MAX_PREFIX_SIZE;
    for (int i = 0; i < 4; i++) {
      final List<TransactionReceipt> receipts = receiptsByHash.get(hashes.get(i));
      sizeLimit += calculateRlpEncodedSize(receipts);
      expectedResults.add(receipts);
    }

    final int msgSizeLimit = sizeLimit;
    setupEthServer(b -> b.maxMessageSize(msgSizeLimit));

    // Request all records, which will exceed the limit
    final GetReceiptsMessage bodiesMsg = GetReceiptsMessage.create(hashes);
    final EthMessage ethMsg = new EthMessage(ethPeer, bodiesMsg);

    // Check response
    final ReceiptsMessage expectedMsg =
        ReceiptsMessage.createUnsafe(
            serializeReceiptsList(
                expectedResults,
                TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION));
    final Optional<MessageData> result = ethMessages.dispatch(ethMsg, EthProtocol.ETH69);
    assertThat(result).contains(expectedMsg);
  }

  @Test
  public void shouldLimitTxReceiptsByCount() {
    final int limit = 6;
    final Map<Hash, List<TransactionReceipt>> receiptsByHash = setupBlockReceipts(10);
    final List<Hash> hashes = new ArrayList<>(receiptsByHash.keySet());
    final List<List<TransactionReceipt>> expectedResults =
        receiptsByHash.values().stream().limit(limit).collect(Collectors.toList());

    setupEthServer(b -> b.maxGetReceipts(limit));

    // Request all records, which will exceed the limit
    final GetReceiptsMessage bodiesMsg = GetReceiptsMessage.create(hashes);
    final EthMessage ethMsg = new EthMessage(ethPeer, bodiesMsg);

    // Check response
    final ReceiptsMessage expectedMsg =
        ReceiptsMessage.createUnsafe(
            serializeReceiptsList(
                expectedResults,
                TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION));
    final Optional<MessageData> result = ethMessages.dispatch(ethMsg, EthProtocol.ETH69);
    assertThat(result).contains(expectedMsg);
  }

  @Test
  public void shouldLimitTransactionsByMessageSize() {
    final List<Transaction> transactions = setupTransactions(10);
    final List<Transaction> expectedResult = new ArrayList<>();
    int sizeLimit = RLP.MAX_PREFIX_SIZE;
    for (int i = 0; i < 4; i++) {
      final Transaction tx = transactions.get(i);
      sizeLimit += calculateRlpEncodedSize(tx);
      expectedResult.add(tx);
    }

    final int msgSizeLimit = sizeLimit;
    setupEthServer(b -> b.maxMessageSize(msgSizeLimit));

    // Request all hashes, which will exceed the limit
    final List<Hash> hashes =
        transactions.stream().map(Transaction::getHash).collect(Collectors.toList());
    final GetPooledTransactionsMessage msgData = GetPooledTransactionsMessage.create(hashes);
    final EthMessage ethMsg = new EthMessage(ethPeer, msgData);

    // Check response
    final PooledTransactionsMessage expectedMsg = PooledTransactionsMessage.create(expectedResult);
    final Optional<MessageData> result = ethMessages.dispatch(ethMsg, EthProtocol.LATEST);
    assertThat(result).contains(expectedMsg);
  }

  @Test
  public void shouldLimitTransactionsByCount() {
    final int limit = 6;
    final List<Transaction> transactions = setupTransactions(10);
    final List<Transaction> expectedResult =
        transactions.stream().limit(limit).collect(Collectors.toList());

    setupEthServer(b -> b.maxGetPooledTransactions(limit));

    // Request all hashes, which will exceed the limit
    final List<Hash> hashes =
        transactions.stream().map(Transaction::getHash).collect(Collectors.toList());
    final GetPooledTransactionsMessage msgData = GetPooledTransactionsMessage.create(hashes);
    final EthMessage ethMsg = new EthMessage(ethPeer, msgData);

    // Check response
    final PooledTransactionsMessage expectedMsg = PooledTransactionsMessage.create(expectedResult);
    final Optional<MessageData> result = ethMessages.dispatch(ethMsg, EthProtocol.LATEST);
    assertThat(result).contains(expectedMsg);
  }

  @Test
  public void shouldReturnAllPaginatedReceiptsForKnownBlocks() {
    // Use explicit receipts to guarantee each block has at least 1 receipt: blocks with 0
    // receipts from blockSequence would trigger skipBefore(0) >= size(0) → ProtocolViolation.
    final Hash hash0 = dataGenerator.hash();
    final Hash hash1 = dataGenerator.hash();
    final Hash hash2 = dataGenerator.hash();
    final List<TransactionReceipt> receipts0 = List.of(dataGenerator.receipt());
    final List<TransactionReceipt> receipts1 = List.of(dataGenerator.receipt());
    final List<TransactionReceipt> receipts2 = List.of(dataGenerator.receipt());
    when(blockchain.getTxReceipts(hash0)).thenReturn(Optional.of(receipts0));
    when(blockchain.getTxReceipts(hash1)).thenReturn(Optional.of(receipts1));
    when(blockchain.getTxReceipts(hash2)).thenReturn(Optional.of(receipts2));
    setupEthServer();

    final List<Hash> hashes = List.of(hash0, hash1, hash2);
    final GetPaginatedReceiptsMessage msg = GetPaginatedReceiptsMessage.create(hashes, 0);

    final PaginatedReceiptsMessage expectedMsg =
        PaginatedReceiptsMessage.createUnsafe(
            serializePaginatedReceiptsList(List.of(receipts0, receipts1, receipts2), false), false);

    final Optional<MessageData> result =
        ethMessages.dispatch(new EthMessage(ethPeer, msg), EthProtocol.ETH70);
    assertThat(result).contains(expectedMsg);
  }

  @Test
  public void shouldLimitPaginatedReceiptsByCount() {
    final int limit = 6;
    final int blockCount = 10;
    // Explicit receipts (1 per block) to avoid 0-receipt blocks from blockSequence.
    final List<Hash> hashes = new ArrayList<>();
    final List<List<TransactionReceipt>> allReceipts = new ArrayList<>();
    for (int i = 0; i < blockCount; i++) {
      final Hash hash = dataGenerator.hash();
      final List<TransactionReceipt> receipts = List.of(dataGenerator.receipt());
      hashes.add(hash);
      allReceipts.add(receipts);
      when(blockchain.getTxReceipts(hash)).thenReturn(Optional.of(receipts));
    }
    setupEthServer(b -> b.maxGetReceipts(limit));

    final GetPaginatedReceiptsMessage msg = GetPaginatedReceiptsMessage.create(hashes, 0);

    final PaginatedReceiptsMessage expectedMsg =
        PaginatedReceiptsMessage.createUnsafe(
            serializePaginatedReceiptsList(allReceipts.subList(0, limit), false), false);

    final Optional<MessageData> result =
        ethMessages.dispatch(new EthMessage(ethPeer, msg), EthProtocol.ETH70);
    assertThat(result).contains(expectedMsg);
  }

  @Test
  public void shouldLimitPaginatedReceiptsByMessageSize() {
    // Use explicit receipts (not blockSequence) to avoid blocks with 0 transactions, which would
    // be silently included in the response without setting lastBlockIncomplete, causing an
    // extra empty-list block entry and making the expected vs actual sizes diverge.
    final Hash block0Hash = dataGenerator.hash();
    final Hash block1Hash = dataGenerator.hash();
    final TransactionReceipt receipt0 = dataGenerator.receipt();
    final TransactionReceipt receipt1 = dataGenerator.receipt();
    when(blockchain.getTxReceipts(block0Hash)).thenReturn(Optional.of(List.of(receipt0)));
    when(blockchain.getTxReceipts(block1Hash)).thenReturn(Optional.of(List.of(receipt1)));

    // Size limit: exactly fits receipt0 from block 0 but not receipt1 from block 1.
    // The server initialises responseSizeEstimate = MAX_PREFIX + 2 (outer list + scalar), then adds
    // MAX_PREFIX per block (block list header), and checks:
    //   responseSizeEstimate + receiptSize + MAX_PREFIX_SIZE > maxMessageSize.
    // With sizeLimit = 3*MAX_PREFIX + 2 + size(receipt0):
    //   block0 header added → estimate = 2*MAX_PREFIX + 2
    //   receipt0 check: 2*MAX_PREFIX + 2 + size(receipt0) + MAX_PREFIX = sizeLimit → not > → FITS
    //   after receipt0: estimate = 2*MAX_PREFIX + 2 + size(receipt0)
    //   block1 header added → estimate = 3*MAX_PREFIX + 2 + size(receipt0)
    //   receipt1 check: 3*MAX_PREFIX + 2 + size(receipt0) + size(receipt1) + MAX_PREFIX > sizeLimit
    //                 = size(receipt1) + MAX_PREFIX > 0 → always true → lastBlockIncomplete = true
    final int sizeLimit =
        3 * RLP.MAX_PREFIX_SIZE + 2 + calculatePaginatedReceiptEncodedSize(receipt0);
    setupEthServer(b -> b.maxMessageSize(sizeLimit));

    final List<Hash> hashes = List.of(block0Hash, block1Hash);
    final GetPaginatedReceiptsMessage msg = GetPaginatedReceiptsMessage.create(hashes, 0);

    // block 0: receipt0 fits; block 1: starts but receipt1 doesn't fit → empty list
    final PaginatedReceiptsMessage expectedMsg =
        PaginatedReceiptsMessage.createUnsafe(
            serializePaginatedReceiptsList(List.of(List.of(receipt0), List.of()), true), true);

    final Optional<MessageData> result =
        ethMessages.dispatch(new EthMessage(ethPeer, msg), EthProtocol.ETH70);
    assertThat(result).contains(expectedMsg);
  }

  @Test
  public void shouldPaginateWithFirstBlockReceiptIndex() {
    // Create receipts directly rather than deriving them from blockSequence, which can produce
    // blocks with 0 transactions (and thus 0 receipts), causing subList(firstIndex, 0) to throw.
    final Hash firstBlockHash = dataGenerator.hash();
    final Hash secondBlockHash = dataGenerator.hash();
    final List<TransactionReceipt> firstBlockReceipts =
        List.of(dataGenerator.receipt(), dataGenerator.receipt());
    final List<TransactionReceipt> secondBlockReceipts =
        List.of(dataGenerator.receipt(), dataGenerator.receipt());
    when(blockchain.getTxReceipts(firstBlockHash)).thenReturn(Optional.of(firstBlockReceipts));
    when(blockchain.getTxReceipts(secondBlockHash)).thenReturn(Optional.of(secondBlockReceipts));
    setupEthServer();

    final int firstIndex = 1;
    // Use List.of to guarantee ordering: firstBlockHash is always the block that gets paginated
    final List<Hash> hashes = List.of(firstBlockHash, secondBlockHash);
    final GetPaginatedReceiptsMessage msg = GetPaginatedReceiptsMessage.create(hashes, firstIndex);
    final EthMessage ethMsg = new EthMessage(ethPeer, msg);

    // firstIndex skips the first receipt of the first block only; subsequent blocks are unaffected
    final List<List<TransactionReceipt>> expectedReceipts =
        List.of(
            firstBlockReceipts.subList(firstIndex, firstBlockReceipts.size()), secondBlockReceipts);
    final PaginatedReceiptsMessage expectedMsg =
        PaginatedReceiptsMessage.createUnsafe(
            serializePaginatedReceiptsList(expectedReceipts, false), false);

    final Optional<MessageData> result = ethMessages.dispatch(ethMsg, EthProtocol.ETH70);
    assertThat(result).contains(expectedMsg);
  }

  @Test
  public void shouldReturnCollectedReceiptsUpToUnknownBlockInPaginatedRequest() {
    // Setup one known block followed by an unknown one
    final Hash knownHash = dataGenerator.hash();
    final TransactionReceipt receipt = dataGenerator.receipt();
    when(blockchain.getTxReceipts(knownHash)).thenReturn(Optional.of(List.of(receipt)));
    final Hash unknownHash = dataGenerator.hash();
    when(blockchain.getTxReceipts(unknownHash)).thenReturn(Optional.empty());
    setupEthServer();

    final GetPaginatedReceiptsMessage msg =
        GetPaginatedReceiptsMessage.create(List.of(knownHash, unknownHash), 0);
    final Optional<MessageData> result =
        ethMessages.dispatch(new EthMessage(ethPeer, msg), EthProtocol.ETH70);

    // Server stops at the unknown block and returns what was collected before it
    final PaginatedReceiptsMessage expectedMsg =
        PaginatedReceiptsMessage.createUnsafe(
            serializePaginatedReceiptsList(List.of(List.of(receipt)), false), false);
    assertThat(result).contains(expectedMsg);
  }

  @Test
  public void shouldThrowProtocolViolationForInvalidFirstBlockReceiptIndex() {
    // Block has 2 receipts; firstBlockReceiptIndex = 3 triggers skipBefore(3) > size(2) → throws
    final Hash blockHash = dataGenerator.hash();
    final List<TransactionReceipt> receipts =
        List.of(dataGenerator.receipt(), dataGenerator.receipt());
    when(blockchain.getTxReceipts(blockHash)).thenReturn(Optional.of(receipts));
    setupEthServer();

    final GetPaginatedReceiptsMessage msg =
        GetPaginatedReceiptsMessage.create(List.of(blockHash), 3);

    assertThatThrownBy(() -> ethMessages.dispatch(new EthMessage(ethPeer, msg), EthProtocol.ETH70))
        .isInstanceOf(ProtocolViolationException.class);
  }

  @Test
  public void shouldTreatFirstBlockReceiptIndexEqualToSizeAsValidAndReturnEmptyFirstBlock() {
    // skipBefore == blockReceipts.size() is valid (condition is strictly >):
    // the first block contributes an empty receipt list and subsequent blocks are returned
    // normally.
    final Hash block0Hash = dataGenerator.hash();
    final Hash block1Hash = dataGenerator.hash();
    final List<TransactionReceipt> block0Receipts =
        List.of(dataGenerator.receipt(), dataGenerator.receipt());
    final List<TransactionReceipt> block1Receipts = List.of(dataGenerator.receipt());
    when(blockchain.getTxReceipts(block0Hash)).thenReturn(Optional.of(block0Receipts));
    when(blockchain.getTxReceipts(block1Hash)).thenReturn(Optional.of(block1Receipts));
    setupEthServer();

    // firstBlockReceiptIndex == size(block0) == 2: skip all receipts → empty list for block0
    final GetPaginatedReceiptsMessage msg =
        GetPaginatedReceiptsMessage.create(List.of(block0Hash, block1Hash), 2);

    final PaginatedReceiptsMessage expectedMsg =
        PaginatedReceiptsMessage.createUnsafe(
            serializePaginatedReceiptsList(List.of(List.of(), block1Receipts), false), false);

    final Optional<MessageData> result =
        ethMessages.dispatch(new EthMessage(ethPeer, msg), EthProtocol.ETH70);
    assertThat(result).contains(expectedMsg);
  }

  @Test
  public void shouldTruncateResponseWhenFirstBlockIndexAndMessageSizeLimitInteract() {
    // Covers the interaction between firstBlockReceiptIndex > 0 (skipping receipts in block 0)
    // and the message size limit cutting off block 1 mid-way ("double pagination").
    final Hash block0Hash = dataGenerator.hash();
    final Hash block1Hash = dataGenerator.hash();
    final TransactionReceipt r0 = dataGenerator.receipt(); // skipped via firstBlockReceiptIndex
    final TransactionReceipt r1 = dataGenerator.receipt(); // included from block 0
    final TransactionReceipt r2 = dataGenerator.receipt(); // would be from block 1, doesn't fit
    when(blockchain.getTxReceipts(block0Hash)).thenReturn(Optional.of(List.of(r0, r1)));
    when(blockchain.getTxReceipts(block1Hash)).thenReturn(Optional.of(List.of(r2)));

    // Size limit: fits only r1 (from block 0 after skipping r0); r2 from block 1 won't fit.
    // Uses the same accounting as shouldLimitPaginatedReceiptsByMessageSize:
    //   3*MAX_PREFIX + 2 + size(r1) is exactly the boundary where r1 fits but r2 doesn't.
    final int sizeLimit = 3 * RLP.MAX_PREFIX_SIZE + 2 + calculatePaginatedReceiptEncodedSize(r1);
    setupEthServer(b -> b.maxMessageSize(sizeLimit));

    final GetPaginatedReceiptsMessage msg =
        GetPaginatedReceiptsMessage.create(List.of(block0Hash, block1Hash), 1);

    // block 0: r0 skipped, r1 fits; block 1: starts but r2 doesn't fit → empty list
    final PaginatedReceiptsMessage expectedMsg =
        PaginatedReceiptsMessage.createUnsafe(
            serializePaginatedReceiptsList(List.of(List.of(r1), List.of()), true), true);

    final Optional<MessageData> result =
        ethMessages.dispatch(new EthMessage(ethPeer, msg), EthProtocol.ETH70);
    assertThat(result).contains(expectedMsg);
  }

  @Test
  public void shouldIncludeEmptyEntryForUnavailableBlockAccessList() {
    setupEthServer();

    final Hash availableHash = dataGenerator.hash();
    final Hash unavailableHash = dataGenerator.hash();
    final BlockAccessList available = dataGenerator.blockAccessList();

    when(blockchain.getBlockAccessList(availableHash)).thenReturn(Optional.of(available));
    when(blockchain.getBlockAccessList(unavailableHash)).thenReturn(Optional.empty());

    final GetBlockAccessListsMessage request =
        GetBlockAccessListsMessage.create(List.of(availableHash, unavailableHash));

    final BlockAccessListsMessage expected =
        BlockAccessListsMessage.create(List.of(available, new BlockAccessList(List.of())));

    assertThat(ethMessages.dispatch(new EthMessage(ethPeer, request), EthProtocol.LATEST))
        .contains(expected);
  }

  @Test
  public void shouldLimitBlockAccessListsByCount() {
    final int count = 10;
    final int limit = 6;
    setupEthServer(b -> b.maxGetBlockAccessLists(limit));

    final List<Hash> hashes = new ArrayList<>(count);
    final List<BlockAccessList> accessLists = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      final Hash h = dataGenerator.hash();
      final BlockAccessList bal = dataGenerator.blockAccessList();
      hashes.add(h);
      accessLists.add(bal);
      when(blockchain.getBlockAccessList(h)).thenReturn(Optional.of(bal));
    }

    final GetBlockAccessListsMessage request = GetBlockAccessListsMessage.create(hashes);

    final BlockAccessListsMessage expected =
        BlockAccessListsMessage.create(accessLists.subList(0, limit));

    assertThat(ethMessages.dispatch(new EthMessage(ethPeer, request), EthProtocol.LATEST))
        .contains(expected);
  }

  @Test
  public void shouldLimitBlockAccessListsByMessageSize() {
    final int count = 10;
    setupEthServer();

    final List<Hash> hashes = new ArrayList<>(count);
    final List<BlockAccessList> accessLists = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      final Hash h = dataGenerator.hash();
      final BlockAccessList bal = dataGenerator.blockAccessList();
      hashes.add(h);
      accessLists.add(bal);
      when(blockchain.getBlockAccessList(h)).thenReturn(Optional.of(bal));
    }

    int sizeLimit = RLP.MAX_PREFIX_SIZE;
    final List<BlockAccessList> expectedAccessLists = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      final BlockAccessList bal = accessLists.get(i);
      expectedAccessLists.add(bal);
      sizeLimit += calculateRlpEncodedSize(bal);
    }

    final int messageSizeLimit = sizeLimit;
    setupEthServer(b -> b.maxMessageSize(messageSizeLimit));

    final GetBlockAccessListsMessage request = GetBlockAccessListsMessage.create(hashes);
    final BlockAccessListsMessage expected = BlockAccessListsMessage.create(expectedAccessLists);

    assertThat(ethMessages.dispatch(new EthMessage(ethPeer, request), EthProtocol.LATEST))
        .contains(expected);
  }

  @Test
  public void
      shouldLimitTheNumberOfBlockAccessListsLookedUpByRequestLimitEvenWhenSomeAreUnavailable() {
    final int requestLimit = 2;
    setupEthServer(b -> b.maxGetBlockAccessLists(requestLimit));

    final Hash firstAvailableHash = dataGenerator.hash();
    final Hash unavailableHash = dataGenerator.hash();
    final Hash secondAvailableHash = dataGenerator.hash(); // should NOT be processed

    final BlockAccessList firstAvailable = dataGenerator.blockAccessList();
    final BlockAccessList secondAvailable = dataGenerator.blockAccessList();

    when(blockchain.getBlockAccessList(firstAvailableHash)).thenReturn(Optional.of(firstAvailable));
    when(blockchain.getBlockAccessList(unavailableHash)).thenReturn(Optional.empty());
    when(blockchain.getBlockAccessList(secondAvailableHash))
        .thenReturn(Optional.of(secondAvailable));

    final GetBlockAccessListsMessage request =
        GetBlockAccessListsMessage.create(
            List.of(firstAvailableHash, unavailableHash, secondAvailableHash));

    // With requestLimit=2, the 3rd hash must not be looked up or included.
    final BlockAccessListsMessage expected =
        BlockAccessListsMessage.create(List.of(firstAvailable, new BlockAccessList(List.of())));

    assertThat(ethMessages.dispatch(new EthMessage(ethPeer, request), EthProtocol.LATEST))
        .contains(expected);
    verify(blockchain, never()).getBlockAccessList(secondAvailableHash);
  }

  @Test
  public void shouldReturnEmptyResponseWhenFirstBlockAccessListWouldExceedMessageSize() {
    setupEthServer(b -> b.maxMessageSize(RLP.MAX_PREFIX_SIZE));

    final Hash firstHash = dataGenerator.hash();
    final Hash secondHash = dataGenerator.hash();

    final BlockAccessList firstBal = dataGenerator.blockAccessList();
    final BlockAccessList secondBal = dataGenerator.blockAccessList();

    when(blockchain.getBlockAccessList(firstHash)).thenReturn(Optional.of(firstBal));
    when(blockchain.getBlockAccessList(secondHash)).thenReturn(Optional.of(secondBal));

    final GetBlockAccessListsMessage request =
        GetBlockAccessListsMessage.create(List.of(firstHash, secondHash));

    assertThat(ethMessages.dispatch(new EthMessage(ethPeer, request), EthProtocol.LATEST))
        .contains(BlockAccessListsMessage.create(List.of()));
    verify(blockchain, never()).getBlockAccessList(secondHash);
  }

  private void setupEthServer() {
    setupEthServer(Function.identity());
  }

  private void setupEthServer(
      final Function<
              ImmutableEthProtocolConfiguration.Builder, ImmutableEthProtocolConfiguration.Builder>
          configModifier) {
    final var configBuilder = ImmutableEthProtocolConfiguration.builder();
    final EthProtocolConfiguration ethConfig = configModifier.apply(configBuilder).build();

    new EthServer(blockchain, worldStateArchive, transactionPool, ethMessages, ethConfig);
  }

  private Map<Hash, Bytes> setupNodeData(final int count) {
    // Return empty value unless otherwise specified
    when(worldStateArchive.getNodeData(any())).thenReturn(Optional.empty());

    final Map<Hash, Bytes> nodeDataByHash = new HashMap<>();
    for (int i = 0; i < count; i++) {
      final Hash hash = dataGenerator.hash();
      final Bytes data = dataGenerator.bytesValue(10, 30);
      when(worldStateArchive.getNodeData(hash)).thenReturn(Optional.of(data));
      nodeDataByHash.put(hash, data);
    }

    return nodeDataByHash;
  }

  private List<Block> setupBlocks(final int count) {
    final List<Block> blocks = dataGenerator.blockSequence(count);
    for (Block block : blocks) {
      when(blockchain.getBlockBody(block.getHash())).thenReturn(Optional.of(block.getBody()));
      when(blockchain.getBlockHeader(block.getHash())).thenReturn(Optional.of(block.getHeader()));
      when(blockchain.getBlockHeader(block.getHeader().getNumber()))
          .thenReturn(Optional.of(block.getHeader()));
    }

    return blocks;
  }

  private Map<Hash, List<TransactionReceipt>> setupBlockReceipts(final int count) {
    final Map<Hash, List<TransactionReceipt>> txReceiptsByHash = new HashMap<>();
    final List<Block> blocks = dataGenerator.blockSequence(count);
    for (Block block : blocks) {
      final List<TransactionReceipt> receipts = dataGenerator.receipts(block);
      when(blockchain.getTxReceipts(block.getHash())).thenReturn(Optional.of(receipts));
      txReceiptsByHash.put(block.getHash(), receipts);
    }

    return txReceiptsByHash;
  }

  private List<Transaction> setupTransactions(final int count) {
    List<Transaction> txs =
        Stream.generate(dataGenerator::transaction).limit(count).collect(Collectors.toList());

    for (Transaction tx : txs) {
      when(transactionPool.getTransactionByHash(tx.getHash())).thenReturn(Optional.of(tx));
    }

    return txs;
  }

  private int calculateRlpEncodedSize(final BlockBody blockBody) {
    return RLP.encode(blockBody::writeWrappedBodyTo).size();
  }

  private int calculateRlpEncodedSize(final BlockHeader header) {
    return RLP.encode(header::writeTo).size();
  }

  private int calculateRlpEncodedSize(final Transaction tx) {
    return RLP.encode(tx::writeTo).size();
  }

  private int calculateRlpEncodedSize(final Bytes data) {
    final BytesValueRLPOutput rlp = new BytesValueRLPOutput();
    rlp.writeBytes(data);
    return rlp.encodedSize();
  }

  private int calculateRlpEncodedSize(final List<TransactionReceipt> receipts) {
    final BytesValueRLPOutput rlp = new BytesValueRLPOutput();
    rlp.startList();
    receipts.forEach(
        r ->
            TransactionReceiptEncoder.writeTo(
                r, rlp, TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION));
    rlp.endList();
    return rlp.encodedSize();
  }

  private int calculatePaginatedReceiptEncodedSize(final TransactionReceipt receipt) {
    final BytesValueRLPOutput rlp = new BytesValueRLPOutput();
    TransactionReceiptEncoder.writeTo(
        receipt, rlp, TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION);
    return rlp.encodedSize();
  }

  private Bytes serializePaginatedReceiptsList(
      final List<List<TransactionReceipt>> receipts, final boolean lastBlockIncomplete) {
    final BytesValueRLPOutput rlp = new BytesValueRLPOutput();
    rlp.writeLongScalar(lastBlockIncomplete ? 1 : 0);
    rlp.startList();
    for (final List<TransactionReceipt> blockReceipts : receipts) {
      final BytesValueRLPOutput encodedBlockReceipts = new BytesValueRLPOutput();
      encodedBlockReceipts.startList();
      for (final TransactionReceipt receipt : blockReceipts) {
        final BytesValueRLPOutput encodedReceipt = new BytesValueRLPOutput();
        TransactionReceiptEncoder.writeTo(
            receipt,
            encodedReceipt,
            TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION);
        encodedBlockReceipts.writeRaw(encodedReceipt.encoded());
      }
      encodedBlockReceipts.endList();
      rlp.writeRaw(encodedBlockReceipts.encoded());
    }
    rlp.endList();
    return rlp.encoded();
  }

  private int calculateRlpEncodedSize(final BlockAccessList blockAccessList) {
    final BytesValueRLPOutput rlp = new BytesValueRLPOutput();
    BlockAccessListEncoder.encode(blockAccessList, rlp);
    return rlp.encodedSize();
  }
}
