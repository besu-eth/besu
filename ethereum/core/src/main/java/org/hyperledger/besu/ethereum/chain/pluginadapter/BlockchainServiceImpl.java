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

import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.data.BlockBody;
import org.hyperledger.besu.plugin.data.BlockContext;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.data.TransactionReceipt;
import org.hyperledger.besu.plugin.services.BlockchainService;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** The Blockchain service implementation. */
@Unstable
public class BlockchainServiceImpl implements BlockchainService {

  private ProtocolSchedule protocolSchedule;
  private MutableBlockchain blockchain;

  /** Instantiates a new Blockchain service implementation. */
  public BlockchainServiceImpl() {}

  /**
   * Initialize the Blockchain service.
   *
   * @param blockchain the blockchain
   * @param protocolSchedule the protocol schedule
   */
  public void init(final MutableBlockchain blockchain, final ProtocolSchedule protocolSchedule) {
    this.protocolSchedule = protocolSchedule;
    this.blockchain = blockchain;
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
