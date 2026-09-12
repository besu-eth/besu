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
package org.hyperledger.besu.ethereum.chain.pluginadapter;

import static org.hyperledger.besu.ethereum.core.plugins.Subscriptions.unsubscribeOnClose;
import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;
import org.hyperledger.besu.ethereum.core.PropagatedBlockSource;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.data.AddedBlockContext;
import org.hyperledger.besu.plugin.data.AddedBlockContext.EventType;
import org.hyperledger.besu.plugin.data.BlockBody;
import org.hyperledger.besu.plugin.data.BlockContext;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.data.PropagatedBlockContext;
import org.hyperledger.besu.plugin.data.TransactionReceipt;
import org.hyperledger.besu.plugin.services.BlockchainService;
import org.hyperledger.besu.plugin.services.Subscription;
import org.hyperledger.besu.plugin.services.chain.spi.BadBlockListener;
import org.hyperledger.besu.plugin.services.chain.spi.BlockAddedListener;
import org.hyperledger.besu.plugin.services.chain.spi.BlockPropagatedListener;
import org.hyperledger.besu.plugin.services.chain.spi.BlockReorgListener;
import org.hyperledger.besu.plugin.services.chain.spi.LogListener;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/** The Blockchain service implementation. */
@Unstable
public class BlockchainServiceImpl implements BlockchainService {

  private ProtocolSchedule protocolSchedule;
  private MutableBlockchain blockchain;
  private PropagatedBlockSource propagatedBlockSource;
  private BadBlockManager badBlockManager;

  /** Instantiates a new Blockchain service implementation. */
  public BlockchainServiceImpl() {}

  /**
   * Initialize the Blockchain service.
   *
   * @param blockchain the blockchain
   * @param protocolSchedule the protocol schedule
   * @param propagatedBlockSource the source of block-propagated events
   * @param badBlockManager the bad block manager
   */
  public void init(
      final MutableBlockchain blockchain,
      final ProtocolSchedule protocolSchedule,
      final PropagatedBlockSource propagatedBlockSource,
      final BadBlockManager badBlockManager) {
    this.protocolSchedule = protocolSchedule;
    this.blockchain = blockchain;
    this.propagatedBlockSource = propagatedBlockSource;
    this.badBlockManager = badBlockManager;
  }

  /**
   * Gets block by number
   *
   * @param number the block number
   * @return the BlockContext if block exists otherwise empty
   */
  @Override
  public Optional<BlockContext> getBlockByNumber(final long number) {
    return blockchain
        .getBlockByNumber(number)
        .map(block -> blockContext(block::getHeader, block::getBody));
  }

  /**
   * Gets block by hash
   *
   * @param hash the block hash
   * @return the BlockContext if block exists otherwise empty
   */
  @Override
  public Optional<BlockContext> getBlockByHash(final Hash hash) {
    return blockchain
        .getBlockByHash(hash)
        .map(block -> blockContext(block::getHeader, block::getBody));
  }

  /**
   * Gets block header by hash
   *
   * @param hash the block hash
   * @return the block header if block exists otherwise empty
   */
  @Override
  public Optional<BlockHeader> getBlockHeaderByHash(final Hash hash) {
    return blockchain.getBlockHeader(hash).map(BlockHeader.class::cast);
  }

  @Override
  public Hash getChainHeadHash() {
    return blockchain.getChainHeadHash();
  }

  @Override
  public BlockHeader getChainHeadHeader() {
    return blockchain.getChainHeadHeader();
  }

  @Override
  public Optional<Wei> getNextBlockBaseFee() {
    final var chainHeadHeader = blockchain.getChainHeadHeader();
    final var protocolSpec =
        protocolSchedule.getForNextBlockHeader(chainHeadHeader, System.currentTimeMillis());
    return Optional.of(protocolSpec.getFeeMarket())
        .filter(FeeMarket::implementsBaseFee)
        .map(BaseFeeMarket.class::cast)
        .map(
            feeMarket ->
                feeMarket.computeBaseFee(
                    chainHeadHeader.getNumber() + 1,
                    chainHeadHeader.getBaseFee().orElse(Wei.ZERO),
                    chainHeadHeader.getGasUsed(),
                    feeMarket.targetGasUsed(chainHeadHeader)));
  }

  @Override
  public Wei getBlobGasPrice(final BlockHeader blockHeader) {
    final var protocolSpec = protocolSchedule.getByBlockHeader(blockHeader);
    final var maybeParentHeader = blockchain.getBlockHeader(blockHeader.getParentHash());
    return protocolSpec
        .getFeeMarket()
        .blobGasPricePerGas(
            maybeParentHeader
                .map(parent -> calculateExcessBlobGasForParent(protocolSpec, parent))
                .orElse(BlobGas.ZERO));
  }

  @Override
  public Optional<Transaction> getTransactionByHash(final Hash transactionHash) {
    return blockchain.getTransactionByHash(transactionHash).map(Transaction.class::cast);
  }

  @Override
  public Optional<List<TransactionReceipt>> getReceiptsByBlockHash(final Hash blockHash) {
    return blockchain
        .getTxReceipts(blockHash)
        .map(
            list -> list.stream().map(TransactionReceipt.class::cast).collect(Collectors.toList()));
  }

  @Override
  public void storeBlock(
      final BlockHeader blockHeader,
      final BlockBody blockBody,
      final List<? extends TransactionReceipt> receipts) {
    final org.hyperledger.besu.ethereum.core.BlockHeader coreHeader =
        (org.hyperledger.besu.ethereum.core.BlockHeader) blockHeader;
    final org.hyperledger.besu.ethereum.core.BlockBody coreBody =
        (org.hyperledger.besu.ethereum.core.BlockBody) blockBody;
    final List<org.hyperledger.besu.ethereum.core.TransactionReceipt> coreReceipts =
        receipts.stream()
            .map(org.hyperledger.besu.ethereum.core.TransactionReceipt.class::cast)
            .toList();
    blockchain.unsafeImportBlock(
        new Block(coreHeader, coreBody),
        coreReceipts,
        Optional.ofNullable(blockchain.calculateTotalDifficulty(coreHeader)));
  }

  @Override
  public Optional<Hash> getSafeBlock() {
    return blockchain.getSafeBlock();
  }

  @Override
  public Optional<Hash> getFinalizedBlock() {
    return blockchain.getFinalized();
  }

  @Override
  public void setFinalizedBlock(final Hash blockHash) {
    final var protocolSpec = getProtocolSpec(blockHash);

    if (protocolSpec.isPoS()) {
      throw new UnsupportedOperationException(
          "Marking block as finalized is not supported for PoS networks");
    }
    blockchain.setFinalized(blockHash);
  }

  @Override
  public void setSafeBlock(final Hash blockHash) {
    final var protocolSpec = getProtocolSpec(blockHash);

    if (protocolSpec.isPoS()) {
      throw new UnsupportedOperationException(
          "Marking block as safe is not supported for PoS networks");
    }

    blockchain.setSafeBlock(blockHash);
  }

  private ProtocolSpec getProtocolSpec(final Hash blockHash) {
    return blockchain
        .getBlockByHash(blockHash)
        .map(Block::getHeader)
        .map(protocolSchedule::getByBlockHeader)
        .orElseThrow(() -> new IllegalArgumentException("Block not found: " + blockHash));
  }

  @Override
  public Subscription subscribeBlockPropagated(final BlockPropagatedListener listener) {
    final long id =
        propagatedBlockSource.subscribe(
            (block, totalDifficulty) ->
                listener.onBlockPropagated(
                    propagatedBlockContext(
                        block::getHeader, block::getBody, () -> totalDifficulty)));
    return unsubscribeOnClose(() -> propagatedBlockSource.unsubscribe(id));
  }

  @Override
  public Subscription subscribeBlockAdded(final BlockAddedListener listener) {
    final long id =
        blockchain.observeBlockAdded(
            event ->
                listener.onBlockAdded(
                    addedBlockContext(
                        event.getEventType(),
                        event::getHeader,
                        () -> event.getBlock().getBody(),
                        event::getTransactionReceipts)));
    return unsubscribeOnClose(() -> blockchain.removeObserver(id));
  }

  @Override
  public Subscription subscribeBlockReorg(final BlockReorgListener listener) {
    final long id =
        blockchain.observeChainReorg(
            (blockWithReceipts, chain) ->
                listener.onBlockReorg(
                    addedBlockContext(
                        EventType.CHAIN_REORG,
                        blockWithReceipts::getHeader,
                        blockWithReceipts.getBlock()::getBody,
                        blockWithReceipts::getReceipts)));
    return unsubscribeOnClose(() -> blockchain.removeChainReorgObserver(id));
  }

  @Override
  public Subscription subscribeLogs(
      final List<Address> addresses, final List<List<Bytes32>> topics, final LogListener listener) {
    final List<List<LogTopic>> topicCriteria =
        topics.stream()
            .map(
                position ->
                    position.stream()
                        .map(topic -> topic == null ? null : LogTopic.wrap(topic))
                        .toList())
            .toList();
    final long id =
        blockchain.observeLogs(
            logWithMetadata -> {
              if (matches(logWithMetadata, addresses, topicCriteria)) {
                listener.onLogEmitted(logWithMetadata);
              }
            });
    return unsubscribeOnClose(() -> blockchain.removeObserver(id));
  }

  @Override
  public Subscription subscribeBadBlock(final BadBlockListener listener) {
    final long id = badBlockManager.subscribeToBadBlocks(listener::onBadBlockAdded);
    return unsubscribeOnClose(() -> badBlockManager.unsubscribeFromBadBlocks(id));
  }

  // Same matching rules as eth_getLogs: any address when the list is empty, topics matched by
  // position, an empty or null-containing position accepting anything there.
  private static boolean matches(
      final LogWithMetadata log,
      final List<Address> addresses,
      final List<List<LogTopic>> topicCriteria) {
    if (!addresses.isEmpty() && !addresses.contains(log.getLogger())) {
      return false;
    }
    if (topicCriteria.isEmpty()) {
      return true;
    }
    final List<LogTopic> logTopics = log.getTopics();
    if (logTopics.size() < topicCriteria.size()) {
      return false;
    }
    for (int i = 0; i < topicCriteria.size(); i++) {
      final List<LogTopic> accepted = topicCriteria.get(i);
      if (!accepted.isEmpty() && !accepted.contains(null) && !accepted.contains(logTopics.get(i))) {
        return false;
      }
    }
    return true;
  }

  private static PropagatedBlockContext propagatedBlockContext(
      final Supplier<BlockHeader> blockHeaderSupplier,
      final Supplier<BlockBody> blockBodySupplier,
      final Supplier<Difficulty> totalDifficultySupplier) {
    return new PropagatedBlockContext() {
      @Override
      public BlockHeader getBlockHeader() {
        return blockHeaderSupplier.get();
      }

      @Override
      public BlockBody getBlockBody() {
        return blockBodySupplier.get();
      }

      @Override
      public UInt256 getTotalDifficulty() {
        return totalDifficultySupplier.get().toUInt256();
      }
    };
  }

  private static AddedBlockContext addedBlockContext(
      final EventType eventType,
      final Supplier<BlockHeader> blockHeaderSupplier,
      final Supplier<BlockBody> blockBodySupplier,
      final Supplier<List<? extends TransactionReceipt>> transactionReceiptsSupplier) {
    return new AddedBlockContext() {
      @Override
      public BlockHeader getBlockHeader() {
        return blockHeaderSupplier.get();
      }

      @Override
      public BlockBody getBlockBody() {
        return blockBodySupplier.get();
      }

      @Override
      public List<? extends TransactionReceipt> getTransactionReceipts() {
        return transactionReceiptsSupplier.get();
      }

      @Override
      public EventType getEventType() {
        return eventType;
      }
    };
  }

  private static BlockContext blockContext(
      final Supplier<BlockHeader> blockHeaderSupplier,
      final Supplier<BlockBody> blockBodySupplier) {
    return new BlockContext() {
      @Override
      public BlockHeader getBlockHeader() {
        return blockHeaderSupplier.get();
      }

      @Override
      public BlockBody getBlockBody() {
        return blockBodySupplier.get();
      }
    };
  }

  @Override
  public Optional<BigInteger> getChainId() {
    if (protocolSchedule == null) {
      return Optional.empty();
    }
    return protocolSchedule.getChainId();
  }

  @Override
  public HardforkId getHardforkId(final BlockHeader blockHeader) {
    return protocolSchedule.getByBlockHeader(blockHeader).getHardforkId();
  }

  @Override
  public HardforkId getHardforkId(final long blockNumber) {
    return blockchain
        .getBlockHeader(blockNumber)
        .map(this::getHardforkId)
        .orElseThrow(
            () -> new IllegalArgumentException("Block not found for number: " + blockNumber));
  }

  @Override
  public HardforkId getNextBlockHardforkId(
      final BlockHeader parentBlockHeader, final long timestampForNextBlock) {
    return protocolSchedule
        .getForNextBlockHeader(parentBlockHeader, timestampForNextBlock)
        .getHardforkId();
  }
}
