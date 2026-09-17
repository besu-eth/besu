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
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;

/** Utility class exposing {@link MergeBlockCreator} functionality for reference tests. */
public final class ReferenceTestMergeBlockCreator {

  private ReferenceTestMergeBlockCreator() {}

  public static Block createBlock(
      final MiningConfiguration miningConfiguration,
      final AbstractBlockCreator.ExtraDataCalculator extraDataCalculator,
      final TransactionPool transactionPool,
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final BlockHeader parentHeader,
      final EthScheduler ethScheduler,
      final Optional<List<Transaction>> transactions,
      final Optional<List<BlockHeader>> ommers,
      final Bytes32 random,
      final long timestamp,
      final Optional<List<Withdrawal>> withdrawals,
      final Optional<Bytes32> parentBeaconBlockRoot,
      final Optional<Long> slotNumber) {

    return new MergeBlockCreator(
            miningConfiguration,
            extraDataCalculator,
            transactionPool,
            protocolContext,
            protocolSchedule,
            parentHeader,
            ethScheduler)
        .createBlock(
            transactions,
            ommers,
            withdrawals,
            Optional.of(random),
            parentBeaconBlockRoot,
            slotNumber,
            Optional.empty(),
            timestamp,
            true,
            parentHeader)
        .getBlock();
  }
}
