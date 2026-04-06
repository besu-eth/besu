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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.testing;

import org.hyperledger.besu.datatypes.Address;
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

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A block creator for testing purposes that only includes the provided transactions and does not
 * use the local transaction pool. When explicit transactions are provided, the
 * AbstractBlockCreator's evaluateTransactions method is used instead of
 * buildTransactionListForBlock, which means the transaction pool is not queried for additional
 * transactions.
 */
class TestingBlockCreator extends AbstractBlockCreator {

  public TestingBlockCreator(
      final MiningConfiguration miningConfiguration,
      final Address coinbase,
      final Bytes extraData,
      final TransactionPool transactionPool,
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final EthScheduler ethScheduler) {
    super(
        miningConfiguration,
        (__, ___) -> coinbase,
        parent -> extraData,
        transactionPool,
        protocolContext,
        protocolSchedule,
        ethScheduler);
  }

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
        Optional.of(Collections.emptyList()),
        withdrawals,
        Optional.of(random),
        parentBeaconBlockRoot,
        slotNumber,
        timestamp,
        false,
        parentHeader);
  }

  @Override
  public BlockCreationResult createBlock(
      final Optional<List<Transaction>> maybeTransactions,
      final Optional<List<BlockHeader>> maybeOmmers,
      final long timestamp,
      final BlockHeader parentHeader) {
    throw new UnsupportedOperationException("random is required for testing block creation");
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
