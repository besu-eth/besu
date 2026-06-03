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
package org.hyperledger.besu.ethereum.chain;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/** An interface for reading data from the blockchain. */
public interface Blockchain {
  /**
   * Return the head (latest block hash and total difficulty) of the local canonical blockchain.
   *
   * @return The head of the blockchain.
   */
  ChainHead getChainHead();

  /**
   * Return the last finalized block hash if present.
   *
   * @return The hash of the last finalized block
   */
  Optional<Hash> getFinalized();

  /**
   * Return the last safe block hash if present.
   *
   * @return The hash of the last safe block
   */
  Optional<Hash> getSafeBlock();

  /**
   * Return the number of the earliest block in the canonical chain.
   *
   * @return The number of the earliest block in the chain.
   */
  Optional<Long> getEarliestBlockNumber();

  /**
   * Return the block number of the head of the canonical chain.
   *
   * @return The block number of the head of the chain.
   */
  long getChainHeadBlockNumber();

  /**
   * Return the hash of the head of the canonical chain.
   *
   * @return The hash of the head of the chain.
   */
  Hash getChainHeadHash();

  /**
   * Returns the header for the current chain head.
   *
   * @return the header for the current chain head
   */
  default BlockHeader getChainHeadHeader() {
    return getBlockHeader(getChainHeadHash())
        .orElseThrow(() -> new IllegalStateException("Missing chain head header."));
  }

  default Block getChainHeadBlock() {
    final Hash chainHeadHash = getChainHeadHash();
    final BlockHeader header =
        getBlockHeader(chainHeadHash)
            .orElseThrow(() -> new IllegalStateException("Missing chain head header."));
    final BlockBody body =
        getBlockBody(chainHeadHash)
            .orElseThrow(() -> new IllegalStateException("Missing chain head body."));
    return new Block(header, body);
  }

  default Block getGenesisBlock() {
    final Hash genesisHash =
        getBlockHashByNumber(BlockHeader.GENESIS_BLOCK_NUMBER)
            .orElseThrow(() -> new IllegalStateException("Missing genesis block."));
    return getBlockByHash(genesisHash)
        .orElseThrow(() -> new IllegalStateException("Missing genesis block."));
  }

  default BlockHeader getGenesisBlockHeader() {
    return getBlockHeader(BlockHeader.GENESIS_BLOCK_NUMBER)
        .orElseThrow(() -> new IllegalStateException("Missing genesis block header."));
  }

  default Optional<Block> getBlockByHash(final Hash blockHash) {
    return getBlockHeader(blockHash)
        .flatMap(header -> getBlockBody(blockHash).map(body -> new Block(header, body)));
  }

  default Optional<Block> getBlockByNumber(final long number) {
    return getBlockHashByNumber(number).flatMap(this::getBlockByHash);
  }

  /**
   * Checks whether the block corresponding to the given hash is on the canonical chain.
   *
   * @param blockHeaderHash The hash of the block to check.
   * @return true if the block corresponding to the hash is on the canonical chain.
   */
  default boolean blockIsOnCanonicalChain(final Hash blockHeaderHash) {
    return getBlockHeader(blockHeaderHash)
        .flatMap(h -> getBlockHashByNumber(h.getNumber()))
        .filter(h -> h.equals(blockHeaderHash))
        .isPresent();
  }

  /**
   * Returns the block header corresponding to the given block number on the canonical chain.
   *
   * @param blockNumber The reference block number whose header we want to retrieve.
   * @return The block header corresponding to this block number.
   */
  Optional<BlockHeader> getBlockHeader(long blockNumber);

  /**
   * Returns a list of block headers starting from the given block number.
   *
   * @param startBlockNumber The number of the first block header.
   * @param numberOfHeaders The number of blocks to retrieve.
   * @return The block headers.
   */
  List<BlockHeader> getBlockHeaders(long startBlockNumber, int numberOfHeaders);

  /**
   * Return true if the block corresponding the hash is present.
   *
   * @param blockHash The hash of the block to check.
   * @return true if the block is tracked.
   */
  default boolean contains(final Hash blockHash) {
    return getBlockHeader(blockHash).isPresent();
  }

  /**
   * Returns the block header corresponding to the given block hash. Associated block is not
   * necessarily on the canonical chain.
   *
   * @param blockHeaderHash The hash of the block whose header we want to retrieve.
   * @return The block header corresponding to this block hash.
   */
  Optional<BlockHeader> getBlockHeader(Hash blockHeaderHash);

  /**
   * Safe version of {@code getBlockHeader} (it should take any locks necessary to ensure any block
   * updates that might be taking place have been completed first)
   *
   * @param blockHeaderHash The hash of the block whose header we want to retrieve.
   * @return The block header corresponding to this block hash.
   */
  Optional<BlockHeader> getBlockHeaderSafe(Hash blockHeaderHash);

  /**
   * Returns the block body corresponding to the given block header hash. Associated block is not
   * necessarily on the canonical chain.
   *
   * @param blockHeaderHash The block header hash identifying the block whose body should be
   *     returned.
   * @return The block body corresponding to the target block.
   */
  Optional<BlockBody> getBlockBody(Hash blockHeaderHash);

  /**
   * Safe version of {@code getBlockBody} (it should take any locks necessary to ensure any block
   * updates that might be taking place have been completed first)
   *
   * @param blockHeaderHash The hash of the block whose header we want to retrieve.
   * @return The block body corresponding to this block hash.
   */
  Optional<BlockBody> getBlockBodySafe(Hash blockHeaderHash);

  Optional<BlockAccessList> getBlockAccessList(Hash blockHash);

  /**
   * Given a block's hash, returns the list of transaction receipts associated with this block's
   * transactions. Associated block is not necessarily on the canonical chain.
   *
   * @param blockHeaderHash The header hash of the block we're querying.
   * @return The transaction receipts corresponding to block hash.
   */
  Optional<List<TransactionReceipt>> getTxReceipts(Hash blockHeaderHash);

  /**
   * Retrieves the header hash of the block at the given height in the canonical chain.
   *
   * @param number The height of the block whose hash should be retrieved.
   * @return The hash of the block at the given height.
   */
  Optional<Hash> getBlockHashByNumber(long number);

  /**
   * Returns the total difficulty (cumulative difficulty up to and including the target block) of
   * the block corresponding to the given hash. Associated block is not necessarily on the canonical
   * chain.
   *
   * @param blockHeaderHash The hash of the block header being queried.
   * @return The total difficulty of the corresponding block.
   */
  Optional<Difficulty> getTotalDifficultyByHash(Hash blockHeaderHash);

  /**
   * Given a transaction hash, returns the location (block number and transaction index) of the
   * hashed transaction on the canonical chain.
   *
   * @param transactionHash A transaction hash.
   * @return The location of the hashed transaction.
   */
  Optional<Transaction> getTransactionByHash(Hash transactionHash);

  /**
   * Returns the transaction location associated with the corresponding hash.
   *
   * @param transactionHash A transaction hash.
   * @return The transaction location associated with the corresponding hash.
   */
  Optional<TransactionLocation> getTransactionLocation(Hash transactionHash);

  /**
   * Returns the transaction hash for a given sender address and nonce.
   *
   * @param sender The sender address.
   * @param nonce The transaction nonce.
   * @return The transaction hash, if indexed.
   */
  Optional<Hash> getTransactionHashBySenderAndNonce(Address sender, long nonce);

  /**
   * Adds an observer that will get called when a new block is added.
   *
   * <p><i>No guarantees are made about the order in which observers are invoked.</i>
   *
   * @param observer the observer to call
   * @return the observer ID that can be used to remove it later.
   */
  long observeBlockAdded(BlockAddedObserver observer);

  /** Removes all observers previously added via {@link #observeBlockAdded(BlockAddedObserver)}. */
  default void removeAllBlockAddedObservers() {}

  /**
   * Adds an observer that will get called on for every added and removed log when a new block is
   * added.
   *
   * <p><i>No guarantees are made about the order in which the observers are invoked.</i>
   *
   * @param logObserver the observer to call
   * @return the observer ID that can be used to remove it later.
   */
  default long observeLogs(final Consumer<LogWithMetadata> logObserver) {
    return observeBlockAdded((event -> event.getLogsWithMetadata().forEach(logObserver)));
  }

  /**
   * Removes a previously added observer of any type.
   *
   * @param observerId the ID of the observer to remove
   * @return {@code true} if the observer was removed; otherwise {@code false}
   */
  boolean removeObserver(long observerId);

  /**
   * Adds an observer that will get called when a new block is added after reorg.
   *
   * <p><i>No guarantees are made about the order in which observers are invoked.</i>
   *
   * @param observer the observer to call
   * @return the observer ID that can be used to remove it later.
   */
  long observeChainReorg(ChainReorgObserver observer);

  /**
   * Removes a previously added {@link ChainReorgObserver}.
   *
   * @param observerId the ID of the observer to remove
   * @return {@code true} if the observer was removed; otherwise {@code false}
   */
  boolean removeChainReorgObserver(long observerId);

  /**
   * Gets the current block choice rule. When presented with two block headers indicate which chain
   * is preferred. greater than zero: the first chain is preferred, less than zero: the second chain
   * is preferred, or zero: no preference.
   *
   * @return The preferred block header
   */
  Comparator<BlockHeader> getBlockChoiceRule();

  /**
   * Sets the current fork choice rule. When presented with two block headers indicate which chain
   * is more preferred. greater than zero: the first chain is more preferred, less than zero: the
   * second chain is more preferred, or zero: no preference.
   *
   * @param blockChoiceRule The new fork choice rule.
   */
  void setBlockChoiceRule(Comparator<BlockHeader> blockChoiceRule);
}
