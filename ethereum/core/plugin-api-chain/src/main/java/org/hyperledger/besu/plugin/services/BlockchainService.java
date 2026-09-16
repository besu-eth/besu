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
package org.hyperledger.besu.plugin.services;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.data.BlockBody;
import org.hyperledger.besu.plugin.data.BlockContext;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.data.TransactionReceipt;
import org.hyperledger.besu.plugin.services.chain.spi.BadBlockListener;
import org.hyperledger.besu.plugin.services.chain.spi.BlockAddedListener;
import org.hyperledger.besu.plugin.services.chain.spi.BlockPropagatedListener;
import org.hyperledger.besu.plugin.services.chain.spi.BlockReorgListener;
import org.hyperledger.besu.plugin.services.chain.spi.LogListener;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;

/**
 * A service for reading the blockchain: blocks, headers, receipts, transactions, the chain id and
 * fork identity. It also stores blocks and sets the safe and finalized block.
 */
@Unstable
public interface BlockchainService extends BesuService {
  /**
   * Gets block by number
   *
   * @param number the block number
   * @return the BlockContext
   */
  Optional<BlockContext> getBlockByNumber(final long number);

  /**
   * Gets block by hash
   *
   * @param hash the block hash
   * @return the BlockContext
   */
  Optional<BlockContext> getBlockByHash(final Hash hash);

  /**
   * Gets block header by hash
   *
   * @param hash the block hash
   * @return the block header if block exists otherwise empty
   */
  Optional<BlockHeader> getBlockHeaderByHash(final Hash hash);

  /**
   * Get the hash of the chain head
   *
   * @return chain head hash
   */
  Hash getChainHeadHash();

  /**
   * Return the blob gas price for the specified block
   *
   * @param blockHeader the block header
   * @return the block gas price or Wei.ZERO if blobs are not yet supported for that block header
   */
  Wei getBlobGasPrice(BlockHeader blockHeader);

  /**
   * Get a transaction by its hash
   *
   * @param transactionHash the transaction hash
   * @return the transaction
   */
  Optional<Transaction> getTransactionByHash(Hash transactionHash);

  /**
   * Get the receipts for a block by block hash
   *
   * @param blockHash the block hash
   * @return the transaction receipts
   */
  Optional<List<TransactionReceipt>> getReceiptsByBlockHash(Hash blockHash);

  /**
   * Store a block
   *
   * @param blockHeader the block header
   * @param blockBody the block body
   * @param receipts the transaction receipts
   */
  void storeBlock(
      BlockHeader blockHeader, BlockBody blockBody, List<? extends TransactionReceipt> receipts);

  /**
   * Get the block header of the chain head
   *
   * @return chain head block header
   */
  BlockHeader getChainHeadHeader();

  /**
   * Return the base fee for the next block
   *
   * @return base fee of the next block or empty if the fee market does not support base fee
   */
  Optional<Wei> getNextBlockBaseFee();

  /**
   * Get the block hash of the safe block
   *
   * @return the block hash of the safe block
   */
  Optional<Hash> getSafeBlock();

  /**
   * Get the block hash of the finalized block
   *
   * @return the block hash of the finalized block
   */
  Optional<Hash> getFinalizedBlock();

  /**
   * Set the finalized block for non-PoS networks
   *
   * @param blockHash Hash of the finalized block
   * @throws IllegalArgumentException if the block hash is not on the chain
   * @throws UnsupportedOperationException if the network is a PoS network
   */
  void setFinalizedBlock(Hash blockHash)
      throws IllegalArgumentException, UnsupportedOperationException;

  /**
   * Set the safe block for non-PoS networks
   *
   * @param blockHash Hash of the safe block
   * @throws IllegalArgumentException if the block hash is not on the chain
   * @throws UnsupportedOperationException if the network is a PoS network
   */
  void setSafeBlock(Hash blockHash) throws IllegalArgumentException, UnsupportedOperationException;

  /**
   * Get the chain id
   *
   * @return the chain id
   */
  Optional<BigInteger> getChainId();

  /**
   * Get the hardfork identifier for the given block header
   *
   * @param blockHeader the block header to determine the hardfork for
   * @return the hardfork identifier applicable to the given block
   */
  @Unstable
  HardforkId getHardforkId(BlockHeader blockHeader);

  /**
   * Get the hardfork identifier for the given block number
   *
   * @param blockNumber the block number to determine the hardfork for
   * @return the hardfork identifier applicable to the given block number
   * @throws IllegalArgumentException if no block with that number exists
   */
  @Unstable
  HardforkId getHardforkId(long blockNumber);

  /**
   * Get the hardfork identifier for the next block based on the parent block and timestamp
   *
   * @param parentBlockHeader the parent block header
   * @param timestampForNextBlock the timestamp for the next block
   * @return the hardfork identifier that will be applicable to the next block
   */
  @Unstable
  HardforkId getNextBlockHardforkId(BlockHeader parentBlockHeader, long timestampForNextBlock);

  /**
   * Subscribes to blocks being propagated: a block whose header has been received and validated and
   * is about to be sent to other peers, before its body has been evaluated. The block may not have
   * been imported yet and may fail later validation.
   *
   * @param listener the listener that receives each propagated block
   * @return the subscription; close it to stop receiving events
   */
  Subscription subscribeBlockPropagated(BlockPropagatedListener listener);

  /**
   * Subscribes to blocks added to the chain, after they have been evaluated and validated.
   *
   * @param listener the listener that receives each added block
   * @return the subscription; close it to stop receiving events
   */
  Subscription subscribeBlockAdded(BlockAddedListener listener);

  /**
   * Subscribes to reorgs: blocks added while the chain moves to a different head.
   *
   * @param listener the listener that receives each reorg block
   * @return the subscription; close it to stop receiving events
   */
  Subscription subscribeBlockReorg(BlockReorgListener listener);

  /**
   * Subscribes to logs, both added and removed, emitted by each new block and matching the given
   * filter. An empty address list matches any address. Topics are matched by position: the outer
   * list is the topic position, each inner list the accepted values at that position, and an empty
   * inner list accepts any value there.
   *
   * @param addresses the contract addresses to match, empty for any
   * @param topics the topics to match by position, empty for any
   * @param listener the listener that receives each matching log
   * @return the subscription; close it to stop receiving events
   */
  Subscription subscribeLogs(
      List<Address> addresses, List<List<Bytes32>> topics, LogListener listener);

  /**
   * Subscribes to bad blocks: blocks that failed validation, or that descend from one that did.
   *
   * @param listener the listener that receives each bad block
   * @return the subscription; close it to stop receiving events
   */
  Subscription subscribeBadBlock(BadBlockListener listener);
}
