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

import static org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason.INVALID_FIRST_BLOCK_RECEIPT_INDEX;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListEncoder;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.ProtocolViolationException;
import org.hyperledger.besu.ethereum.eth.messages.BlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.messages.BlockBodiesMessage;
import org.hyperledger.besu.ethereum.eth.messages.BlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
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
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EthServer {
  private static final Logger LOG = LoggerFactory.getLogger(EthServer.class);
  private final Blockchain blockchain;
  private final WorldStateArchive worldStateArchive;
  private final TransactionPool transactionPool;
  private final EthMessages ethMessages;
  private final EthProtocolConfiguration ethereumWireProtocolConfiguration;

  EthServer(
      final Blockchain blockchain,
      final WorldStateArchive worldStateArchive,
      final TransactionPool transactionPool,
      final EthMessages ethMessages,
      final EthProtocolConfiguration ethereumWireProtocolConfiguration) {
    this.blockchain = blockchain;
    this.worldStateArchive = worldStateArchive;
    this.transactionPool = transactionPool;
    this.ethMessages = ethMessages;
    this.ethereumWireProtocolConfiguration = ethereumWireProtocolConfiguration;
    this.registerResponseConstructors();
  }

  private void registerResponseConstructors() {
    final int maxMessageSize = ethereumWireProtocolConfiguration.getMaxMessageSize();

    ethMessages.registerResponseConstructor(
        EthProtocolMessages.GET_BLOCK_HEADERS,
        (peer, messageData, capability) ->
            constructGetHeadersResponse(
                blockchain,
                messageData,
                ethereumWireProtocolConfiguration.getMaxGetBlockHeaders(),
                maxMessageSize));
    ethMessages.registerResponseConstructor(
        EthProtocolMessages.GET_BLOCK_BODIES,
        (peer, messageData, capability) ->
            constructGetBodiesResponse(
                blockchain,
                messageData,
                ethereumWireProtocolConfiguration.getMaxGetBlockBodies(),
                maxMessageSize));
    ethMessages.registerResponseConstructor(
        EthProtocolMessages.GET_RECEIPTS,
        (peer, messageData, capability) -> {
          if (EthProtocol.isEth70Compatible(capability)) {
            return constructGetPaginatedReceiptsResponse(
                peer,
                blockchain,
                messageData,
                ethereumWireProtocolConfiguration.getMaxGetReceipts(),
                maxMessageSize);
          }
          return constructGetReceiptsResponse(
              blockchain,
              messageData,
              ethereumWireProtocolConfiguration.getMaxGetReceipts(),
              maxMessageSize,
              capability);
        });
    ethMessages.registerResponseConstructor(
        EthProtocolMessages.GET_NODE_DATA,
        (peer, messageData, capability) ->
            constructGetNodeDataResponse(
                worldStateArchive,
                messageData,
                ethereumWireProtocolConfiguration.getMaxGetNodeData(),
                maxMessageSize));
    ethMessages.registerResponseConstructor(
        EthProtocolMessages.GET_POOLED_TRANSACTIONS,
        (peer, messageData, capability) ->
            constructGetPooledTransactionsResponse(
                transactionPool,
                peer,
                messageData,
                ethereumWireProtocolConfiguration.getMaxGetPooledTransactions(),
                maxMessageSize));
    ethMessages.registerResponseConstructor(
        EthProtocolMessages.GET_BLOCK_ACCESS_LISTS,
        (peer, messageData, capability) ->
            constructGetBlockAccessListsResponse(
                blockchain,
                messageData,
                ethereumWireProtocolConfiguration.getMaxGetBlockAccessLists(),
                maxMessageSize));
  }

  static MessageData constructGetHeadersResponse(
      final Blockchain blockchain,
      final MessageData message,
      final int requestLimit,
      final int maxMessageSize) {
    // Extract parameters from request
    final GetBlockHeadersMessage getHeaders = GetBlockHeadersMessage.readFrom(message);
    final Optional<Hash> hash = getHeaders.hash();
    final int skip = getHeaders.skip();
    final int maxHeaders = Math.min(requestLimit, getHeaders.maxHeaders());
    final boolean reversed = getHeaders.reverse();
    final BlockHeader firstHeader;
    // Query first header by hash or number depending on request arguments
    if (hash.isPresent()) {
      final Hash startHash = hash.get();
      firstHeader = blockchain.getBlockHeader(startHash).orElse(null);
    } else {
      final long firstNumber = getHeaders.blockNumber().getAsLong();
      firstHeader = blockchain.getBlockHeader(firstNumber).orElse(null);
    }

    // The initial header was not found, nothing to return
    if (firstHeader == null) {
      return BlockHeadersMessage.create(Collections.emptyList());
    }

    // Encode the first header
    int responseSizeEstimate = RLP.MAX_PREFIX_SIZE;
    final BytesValueRLPOutput rlp = new BytesValueRLPOutput();
    rlp.startList();
    final Bytes firstEncodedHeader = RLP.encode(firstHeader::writeTo);
    if (responseSizeEstimate + firstEncodedHeader.size() > maxMessageSize) {
      return BlockHeadersMessage.create(Collections.emptyList());
    }
    responseSizeEstimate += firstEncodedHeader.size();
    rlp.writeRaw(firstEncodedHeader);
    // Collect and encode the remaining headers
    final long numberDelta = reversed ? -(skip + 1) : (skip + 1);
    for (int i = 1; i < maxHeaders; i++) {
      final long blockNumber = firstHeader.getNumber() + i * numberDelta;
      if (blockNumber < BlockHeader.GENESIS_BLOCK_NUMBER) {
        break;
      }
      final Optional<BlockHeader> maybeHeader = blockchain.getBlockHeader(blockNumber);
      if (maybeHeader.isEmpty()) {
        break;
      }
      final BytesValueRLPOutput headerRlp = new BytesValueRLPOutput();
      maybeHeader.get().writeTo(headerRlp);
      final int encodedSize = headerRlp.encodedSize();
      if (responseSizeEstimate + encodedSize > maxMessageSize) {
        break;
      }
      responseSizeEstimate += encodedSize;
      rlp.writeRaw(headerRlp.encoded());
    }
    rlp.endList();

    return BlockHeadersMessage.createUnsafe(rlp.encoded());
  }

  static MessageData constructGetBodiesResponse(
      final Blockchain blockchain,
      final MessageData message,
      final int requestLimit,
      final int maxMessageSize) {
    final GetBlockBodiesMessage getBlockBodiesMessage = GetBlockBodiesMessage.readFrom(message);
    final Iterable<Hash> hashes = getBlockBodiesMessage.hashes();

    int responseSizeEstimate = RLP.MAX_PREFIX_SIZE;
    final BytesValueRLPOutput rlp = new BytesValueRLPOutput();
    rlp.startList();
    int count = 0;
    for (final Hash hash : hashes) {
      if (count >= requestLimit) {
        break;
      }
      count++;
      final Optional<BlockBody> maybeBody = blockchain.getBlockBody(hash);
      if (maybeBody.isEmpty()) {
        continue;
      }

      final BlockBody body = maybeBody.get();
      final BytesValueRLPOutput bodyOutput = new BytesValueRLPOutput();
      body.writeWrappedBodyTo(bodyOutput);
      final int encodedSize = bodyOutput.encodedSize();
      if (responseSizeEstimate + encodedSize > maxMessageSize) {
        break;
      }
      responseSizeEstimate += encodedSize;
      rlp.writeRaw(bodyOutput.encoded());
    }
    rlp.endList();
    return BlockBodiesMessage.createUnsafe(rlp.encoded());
  }

  static MessageData constructGetReceiptsResponse(
      final Blockchain blockchain,
      final MessageData message,
      final int requestLimit,
      final int maxMessageSize,
      final Capability cap) {
    final GetReceiptsMessage getReceipts = GetReceiptsMessage.readFrom(message);
    final Iterable<Hash> blockHashes = getReceipts.blockHashes();

    int responseSizeEstimate = RLP.MAX_PREFIX_SIZE;
    final BytesValueRLPOutput rlp = new BytesValueRLPOutput();
    rlp.startList();
    int count = 0;
    for (final Hash blockHash : blockHashes) {
      if (count >= requestLimit) {
        break;
      }
      count++;
      final Optional<List<TransactionReceipt>> maybeReceipts = blockchain.getTxReceipts(blockHash);
      if (maybeReceipts.isEmpty()) {
        continue;
      }
      final BytesValueRLPOutput encodedReceipts = new BytesValueRLPOutput();
      encodedReceipts.startList();
      TransactionReceiptEncodingConfiguration encodingConfiguration =
          EthProtocol.isEth69Compatible(cap)
              ? TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION
              : TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION;
      maybeReceipts
          .get()
          .forEach(
              r -> TransactionReceiptEncoder.writeTo(r, encodedReceipts, encodingConfiguration));
      encodedReceipts.endList();
      final int encodedSize = encodedReceipts.encodedSize();
      if (responseSizeEstimate + encodedSize > maxMessageSize) {
        break;
      }

      responseSizeEstimate += encodedSize;
      rlp.writeRaw(encodedReceipts.encoded());
    }
    rlp.endList();

    return ReceiptsMessage.createUnsafe(rlp.encoded());
  }

  static MessageData constructGetPaginatedReceiptsResponse(
      final EthPeer peer,
      final Blockchain blockchain,
      final MessageData message,
      final int requestLimit,
      final int maxMessageSize) {
    final GetPaginatedReceiptsMessage getPaginatedReceipts =
        GetPaginatedReceiptsMessage.readFrom(message);
    final List<Hash> requestedBlockHashes = getPaginatedReceipts.blockHashes();
    final List<Hash> blockHashes;
    if (requestedBlockHashes.size() > requestLimit) {
      LOG.atDebug()
          .setMessage(
              "Requested receipts for {} blocks, more than allowed max number of {}, ignoring extra blocks")
          .addArgument(requestedBlockHashes::size)
          .addArgument(requestLimit)
          .log();
      blockHashes = requestedBlockHashes.subList(0, requestLimit);
    } else {
      blockHashes = requestedBlockHashes;
    }

    final var blockReceiptsRLPs = new ArrayList<BytesValueRLPOutput>(blockHashes.size());

    int skipBefore = getPaginatedReceipts.firstBlockReceiptIndex();
    LOG.trace(
        "Paginated receipt request for {} blocks with first block receipt index {}",
        blockHashes.size(),
        skipBefore);
    // Account for the outer list header and the lastBlockIncomplete scalar (max 2 bytes).
    int responseSizeEstimate = RLP.MAX_PREFIX_SIZE + 2;
    boolean lastBlockIncomplete = false;

    for (final Hash blockHash : blockHashes) {
      final Optional<List<TransactionReceipt>> maybeReceipts = blockchain.getTxReceipts(blockHash);
      if (maybeReceipts.isEmpty()) {
        LOG.debug(
            "Invalid request from peer {}, block {} does not exists, returning", peer, blockHash);
        break;
      }

      final List<TransactionReceipt> blockReceipts = maybeReceipts.get();
      final List<TransactionReceipt> requestedReceipts;

      if (skipBefore > blockReceipts.size()) {
        throw new ProtocolViolationException(
            "Invalid request from peer %s, firstBlockReceiptIndex %d is greater than or equal the receipt count of %d for block %s"
                .formatted(peer, skipBefore, blockReceipts.size(), blockHash),
            INVALID_FIRST_BLOCK_RECEIPT_INDEX);
      }

      if (skipBefore > 0) {
        requestedReceipts = blockReceipts.subList(skipBefore, blockReceipts.size());
        skipBefore = 0;
      } else {
        requestedReceipts = blockReceipts;
      }

      // Account for this block's own list header before processing its receipts.
      responseSizeEstimate += RLP.MAX_PREFIX_SIZE;

      final BytesValueRLPOutput encodedBlockReceipts = new BytesValueRLPOutput();
      encodedBlockReceipts.startList();

      for (final TransactionReceipt receipt : requestedReceipts) {
        final BytesValueRLPOutput encodedReceipt = new BytesValueRLPOutput();
        TransactionReceiptEncoder.writeTo(
            receipt,
            encodedReceipt,
            TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION);
        if (responseSizeEstimate + encodedReceipt.encodedSize() + RLP.MAX_PREFIX_SIZE
            > maxMessageSize) {
          lastBlockIncomplete = true;
          break;
        }
        responseSizeEstimate += encodedReceipt.encodedSize();
        encodedBlockReceipts.writeRaw(encodedReceipt.encoded());
      }

      encodedBlockReceipts.endList();
      blockReceiptsRLPs.add(encodedBlockReceipts);
      if (lastBlockIncomplete) {
        break;
      }
    }

    final BytesValueRLPOutput rlp = new BytesValueRLPOutput();
    rlp.writeLongScalar(lastBlockIncomplete ? 1 : 0);
    rlp.startList();
    blockReceiptsRLPs.forEach(r -> rlp.writeRaw(r.encoded()));
    rlp.endList();

    final Bytes encodedResponse = rlp.encoded();
    LOG.trace(
        "Returning paginated receipts for {} blocks, with last block incomplete {}, enconded size {}",
        blockHashes.size(),
        lastBlockIncomplete,
        encodedResponse.size());
    return PaginatedReceiptsMessage.createUnsafe(encodedResponse, lastBlockIncomplete);
  }

  static MessageData constructGetBlockAccessListsResponse(
      final Blockchain blockchain,
      final MessageData message,
      final int requestLimit,
      final int maxMessageSize) {
    final GetBlockAccessListsMessage getBlockAccessLists =
        GetBlockAccessListsMessage.readFrom(message);
    final Iterable<Hash> blockHashes = getBlockAccessLists.blockHashes();

    int responseSizeEstimate = RLP.MAX_PREFIX_SIZE;
    final BytesValueRLPOutput rlp = new BytesValueRLPOutput();
    rlp.startList();
    int count = 0;
    for (final Hash blockHash : blockHashes) {
      if (count >= requestLimit) {
        break;
      }
      count++;

      final Optional<BlockAccessList> maybeBlockAccessList =
          blockchain.getBlockAccessList(blockHash);
      final BytesValueRLPOutput balOutput = new BytesValueRLPOutput();
      if (maybeBlockAccessList.isPresent()) {
        BlockAccessListEncoder.encode(maybeBlockAccessList.get(), balOutput);
      } else {
        // EIP-8159: Empty lists are returned for blocks where the BAL is unavailable.
        balOutput.startList();
        balOutput.endList();
      }

      final int encodedSize = balOutput.encodedSize();
      if (responseSizeEstimate + encodedSize > maxMessageSize) {
        break;
      }
      responseSizeEstimate += encodedSize;
      rlp.writeRaw(balOutput.encoded());
    }
    rlp.endList();

    return BlockAccessListsMessage.createUnsafe(rlp.encoded());
  }

  static MessageData constructGetPooledTransactionsResponse(
      final TransactionPool transactionPool,
      final EthPeer peer,
      final MessageData message,
      final int requestLimit,
      final int maxMessageSize) {
    final GetPooledTransactionsMessage getPooledTransactions =
        GetPooledTransactionsMessage.readFrom(message);
    final List<Hash> hashes = getPooledTransactions.pooledTransactions();

    LOG.trace("Requested pooled transactions: peer={}, requested hashes={}", peer, hashes);

    final List<Hash> returnedHashes = new ArrayList<>(hashes.size());

    int responseSizeEstimate = RLP.MAX_PREFIX_SIZE;
    final BytesValueRLPOutput rlp = new BytesValueRLPOutput();
    rlp.startList();
    int count = 0;
    for (final Hash hash : hashes) {
      if (count >= requestLimit) {
        break;
      }
      count++;
      final Optional<Transaction> maybeTx = transactionPool.getTransactionByHash(hash);
      if (maybeTx.isEmpty()) {
        continue;
      }

      final BytesValueRLPOutput txRlp = new BytesValueRLPOutput();
      TransactionEncoder.encodeRLP(maybeTx.get(), txRlp, EncodingContext.POOLED_TRANSACTION);
      final int encodedSize = txRlp.encodedSize();
      if (responseSizeEstimate + encodedSize > maxMessageSize) {
        break;
      }

      responseSizeEstimate += encodedSize;
      rlp.writeRaw(txRlp.encoded());
      returnedHashes.add(hash);
    }
    rlp.endList();

    LOG.atTrace()
        .setMessage("Sending pooled transactions: peer={}, returned hashes={}, notFoundCount={}")
        .addArgument(peer)
        .addArgument(returnedHashes)
        .addArgument(() -> hashes.size() - returnedHashes.size())
        .log();

    return PooledTransactionsMessage.createUnsafe(rlp.encoded());
  }

  static MessageData constructGetNodeDataResponse(
      final WorldStateArchive worldStateArchive,
      final MessageData message,
      final int requestLimit,
      final int maxMessageSize) {
    final GetNodeDataMessage getNodeDataMessage = GetNodeDataMessage.readFrom(message);
    final Iterable<Hash> hashes = getNodeDataMessage.hashes();

    int responseSizeEstimate = RLP.MAX_PREFIX_SIZE;
    final BytesValueRLPOutput rlp = new BytesValueRLPOutput();
    rlp.startList();
    int count = 0;
    for (final Hash hash : hashes) {
      if (count >= requestLimit) {
        break;
      }
      count++;

      final Optional<Bytes> maybeNodeData = worldStateArchive.getNodeData(hash);
      if (maybeNodeData.isEmpty()) {
        continue;
      }

      final BytesValueRLPOutput rlpNodeData = new BytesValueRLPOutput();
      rlpNodeData.writeBytes(maybeNodeData.get());
      final int encodedSize = rlpNodeData.encodedSize();
      if (responseSizeEstimate + encodedSize > maxMessageSize) {
        break;
      }

      responseSizeEstimate += encodedSize;
      rlp.writeRaw(rlpNodeData.encoded());
    }
    rlp.endList();

    return NodeDataMessage.createUnsafe(rlp.encoded());
  }
}
