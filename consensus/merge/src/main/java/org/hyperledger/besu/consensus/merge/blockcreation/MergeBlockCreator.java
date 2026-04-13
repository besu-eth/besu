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
package org.hyperledger.besu.consensus.merge.blockcreation;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.blockcreation.AbstractBlockCreator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.SealableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;

/** The Merge block creator. */
class MergeBlockCreator extends AbstractBlockCreator {

  /**
   * Instantiates a new Merge block creator.
   *
   * @param miningConfiguration the mining parameters
   * @param extraDataCalculator the extra data calculator
   * @param transactionPool the pending transactions
   * @param protocolContext the protocol context
   * @param protocolSchedule the protocol schedule
   * @param parentHeader the parent header
   */
  public MergeBlockCreator(
      final MiningConfiguration miningConfiguration,
      final ExtraDataCalculator extraDataCalculator,
      final TransactionPool transactionPool,
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final BlockHeader parentHeader,
      final EthScheduler ethScheduler) {
    super(
        miningConfiguration,
        (__, ___) -> miningConfiguration.getCoinbase().orElseThrow(),
        extraDataCalculator,
        transactionPool,
        protocolContext,
        protocolSchedule,
        ethScheduler);
  }

  /**
   * Create block and return block creation result.
   *
   * @param maybeTransactions the maybe transactions
   * @param random the random
   * @param timestamp the timestamp
   * @param withdrawals optional list of withdrawals
   * @param parentBeaconBlockRoot optional root hash of the parent beacon block
   * @param slotNumber optional slot number (EIP-7843)
   * @param parentHeader the parent header
   * @return the block creation result
   */
  public BlockCreationResult createBlock(
      final Optional<List<Transaction>> maybeTransactions,
      final Bytes32 random,
      final long timestamp,
      final Optional<List<Withdrawal>> withdrawals,
      final Optional<Bytes32> parentBeaconBlockRoot,
      final Optional<Long> slotNumber,
      final BlockHeader parentHeader) {

    return createBlock(
        maybeTransactions,
        random,
        timestamp,
        withdrawals,
        parentBeaconBlockRoot,
        slotNumber,
        parentHeader,
        List.of());
  }

  /**
   * Create block with inclusion list support (EIP-7805).
   *
   * @param maybeTransactions the maybe transactions
   * @param random the random
   * @param timestamp the timestamp
   * @param withdrawals optional list of withdrawals
   * @param parentBeaconBlockRoot optional root hash of the parent beacon block
   * @param slotNumber optional slot number (EIP-7843)
   * @param parentHeader the parent header
   * @param inclusionListTransactions optional inclusion list transactions to prioritize
   * @return the block creation result
   */
  public BlockCreationResult createBlock(
      final Optional<List<Transaction>> maybeTransactions,
      final Bytes32 random,
      final long timestamp,
      final Optional<List<Withdrawal>> withdrawals,
      final Optional<Bytes32> parentBeaconBlockRoot,
      final Optional<Long> slotNumber,
      final BlockHeader parentHeader,
      final List<Transaction> inclusionListTransactions) {

    return createBlock(
        maybeTransactions,
        Optional.of(Collections.emptyList()),
        withdrawals,
        Optional.of(random),
        parentBeaconBlockRoot,
        slotNumber,
        timestamp,
        false,
        parentHeader,
        inclusionListTransactions);
  }

  @Override
  public BlockCreationResult createBlock(
      final Optional<List<Transaction>> maybeTransactions,
      final Optional<List<BlockHeader>> maybeOmmers,
      final long timestamp,
      final BlockHeader parentHeader) {
    throw new UnsupportedOperationException("random is required");
  }

  @Override
  protected BlockHeader createFinalBlockHeader(final SealableBlockHeader sealableBlockHeader) {
    return BlockHeaderBuilder.create()
        .difficulty(Difficulty.ZERO)
        .populateFrom(sealableBlockHeader)
        .nonce(0L)
        .blockHeaderFunctions(blockHeaderFunctions)
        .buildBlockHeader();
  }
}
