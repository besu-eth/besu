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
package org.hyperledger.besu.ethereum.referencetests;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.ChainHead;
import org.hyperledger.besu.ethereum.chain.ChainReorgObserver;
import org.hyperledger.besu.ethereum.chain.TransactionLocation;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * A blockchain mock for the Ethereum reference tests.
 *
 * <p>Operations which would lead to non-deterministic behaviour if executed while processing
 * transactions throw {@link NonDeterministicOperationException}. For example all methods that
 * lookup blocks by number since the block being processed may not be on the canonical chain but
 * that must not affect the execution of its transactions.
 *
 * <p>The Ethereum reference tests for VM execution (VMTests) and transaction processing
 * (GeneralStateTests) require a block's hash to be the hash of the string of its block number.
 */
public class ReferenceTestBlockchain implements Blockchain {

  // Maximum number of blocks prior to the chain head that can be retrieved by
  // hash.
  private static final long MAXIMUM_BLOCKS_BEHIND_HEAD = 256;
  private static final String NUMBER_LOOKUP_ERROR =
      "Blocks must not be looked up by number in the EVM. The block being processed may not be on the canonical chain.";
  private static final String CHAIN_HEAD_ERROR =
      "Chain head is inherently non-deterministic. The block currently being processed should be treated as the chain head.";
  private static final String FINALIZED_ERROR =
      "Finalized block is inherently non-deterministic. The block currently being processed should be treated as the finalized block.";
  private static final String SAFE_BLOCK_ERROR =
      "Safe block is inherently non-deterministic. The block currently being processed should be treated as the safe block.";
  private final Map<Hash, BlockHeader> hashToHeader = new HashMap<>();

  public ReferenceTestBlockchain() {
    this(0);
  }

  public ReferenceTestBlockchain(final long chainHeadBlockNumber) {
    for (long blockNumber = Math.max(0L, chainHeadBlockNumber - MAXIMUM_BLOCKS_BEHIND_HEAD);
        blockNumber < chainHeadBlockNumber;
        blockNumber++) {
      final Hash hash = generateTestBlockHash(blockNumber);
      hashToHeader.put(
          hash,
          new BlockHeaderTestFixture()
              .number(blockNumber)
              .parentHash(generateTestBlockHash(blockNumber - 1))
              .buildHeader());
    }
  }

  public static Hash generateTestBlockHash(final long number) {
    final byte[] bytes = Long.toString(number).getBytes(UTF_8);
    return Hash.hash(Bytes.wrap(bytes));
  }

  @Override
  public Optional<Hash> getBlockHashByNumber(final long number) {
    throw new NonDeterministicOperationException(NUMBER_LOOKUP_ERROR);
  }

  @Override
  public ChainHead getChainHead() {
    throw new NonDeterministicOperationException(CHAIN_HEAD_ERROR);
  }

  @Override
  public Optional<Hash> getFinalized() {
    throw new NonDeterministicOperationException(FINALIZED_ERROR);
  }

  @Override
  public Optional<Hash> getSafeBlock() {
    throw new NonDeterministicOperationException(SAFE_BLOCK_ERROR);
  }

  @Override
  public Optional<Long> getEarliestBlockNumber() {
    // Deterministic, but just not implemented.
    throw new UnsupportedOperationException();
  }

  @Override
  public long getChainHeadBlockNumber() {
    throw new NonDeterministicOperationException(CHAIN_HEAD_ERROR);
  }

  @Override
  public Hash getChainHeadHash() {
    throw new NonDeterministicOperationException(CHAIN_HEAD_ERROR);
  }

  @Override
  public Optional<TransactionLocation> getTransactionLocation(final Hash transactionHash) {
    throw new NonDeterministicOperationException("Transaction location may be different on forks");
  }

  @Override
  public Optional<Hash> getTransactionHashBySenderAndNonce(final Address sender, final long nonce) {
    throw new NonDeterministicOperationException("Transaction location may be different on forks");
  }

  @Override
  public Optional<BlockHeader> getBlockHeader(final long blockNumber) {
    throw new NonDeterministicOperationException(NUMBER_LOOKUP_ERROR);
  }

  @Override
  public List<BlockHeader> getBlockHeaders(final long startBlockNumber, final int numberOfHeaders) {
    // Deterministic, but just not implemented.
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<BlockHeader> getBlockHeader(final Hash blockHeaderHash) {
    return Optional.ofNullable(hashToHeader.get(blockHeaderHash));
  }

  @Override
  public synchronized Optional<BlockHeader> getBlockHeaderSafe(final Hash blockHeaderHash) {
    return Optional.ofNullable(hashToHeader.get(blockHeaderHash));
  }

  @Override
  public Optional<BlockBody> getBlockBody(final Hash blockHeaderHash) {
    // Deterministic, but just not implemented.
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized Optional<BlockBody> getBlockBodySafe(final Hash blockHeaderHash) {
    return getBlockBody(blockHeaderHash);
  }

  @Override
  public Optional<List<TransactionReceipt>> getTxReceipts(final Hash blockHeaderHash) {
    // Deterministic, but just not implemented.
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<BlockAccessList> getBlockAccessList(final Hash blockHash) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Difficulty> getTotalDifficultyByHash(final Hash blockHeaderHash) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Transaction> getTransactionByHash(final Hash transactionHash) {
    throw new NonDeterministicOperationException(
        "Which transactions are on the chain may vary on different forks");
  }

  @Override
  public long observeBlockAdded(final BlockAddedObserver observer) {
    throw new NonDeterministicOperationException("Listening for new blocks is not deterministic");
  }

  @Override
  public boolean removeObserver(final long observerId) {
    throw new NonDeterministicOperationException("Listening for new blocks is not deterministic");
  }

  @Override
  public long observeChainReorg(final ChainReorgObserver observer) {
    throw new NonDeterministicOperationException("Listening for chain reorg is not deterministic");
  }

  @Override
  public boolean removeChainReorgObserver(final long observerId) {
    throw new NonDeterministicOperationException("Listening for chain reorg is not deterministic");
  }

  public static class NonDeterministicOperationException extends RuntimeException {
    NonDeterministicOperationException(final String message) {
      super(message);
    }
  }

  @Override
  @SuppressWarnings("unused")
  public Comparator<BlockHeader> getBlockChoiceRule() {
    return (a, b) -> {
      throw new NonDeterministicOperationException(
          "ReferenceTestBlockchain for VMTest Chains do not support fork choice rules");
    };
  }

  @Override
  public void setBlockChoiceRule(final Comparator<BlockHeader> blockChoiceRule) {
    throw new UnsupportedOperationException("Not Used for Reference Tests");
  }
}
