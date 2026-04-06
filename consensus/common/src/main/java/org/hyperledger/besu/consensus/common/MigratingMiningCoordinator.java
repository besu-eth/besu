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
package org.hyperledger.besu.consensus.common;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Migrating mining coordinator. */
public class MigratingMiningCoordinator implements MiningCoordinator, BlockAddedObserver {

  private static final Logger LOG = LoggerFactory.getLogger(MigratingMiningCoordinator.class);

  private final ForksSchedule<MiningCoordinator> miningCoordinatorSchedule;
  private final Blockchain blockchain;
  private MiningCoordinator activeMiningCoordinator;
  private long blockAddedObserverId;

  /**
   * Instantiates a new Migrating mining coordinator.
   *
   * @param miningCoordinatorSchedule the mining coordinator schedule
   * @param blockchain the blockchain
   */
  public MigratingMiningCoordinator(
      final ForksSchedule<MiningCoordinator> miningCoordinatorSchedule,
      final Blockchain blockchain) {
    this.miningCoordinatorSchedule = miningCoordinatorSchedule;
    this.blockchain = blockchain;

    final BlockHeader chainHead = blockchain.getChainHeadHeader();

    this.activeMiningCoordinator =
        this.miningCoordinatorSchedule
            .getFork(chainHead.getNumber() + 1, chainHead.getTimestamp())
            .getValue();
  }

  @Override
  public void start() {
    blockAddedObserverId = blockchain.observeBlockAdded(this);
    startActiveMiningCoordinator();
  }

  private void startActiveMiningCoordinator() {
    activeMiningCoordinator.enable();
    activeMiningCoordinator.start();
    if (activeMiningCoordinator instanceof BlockAddedObserver) {
      ((BlockAddedObserver) activeMiningCoordinator).removeObserver();
    }
  }

  @Override
  public void stop() {
    blockchain.removeObserver(blockAddedObserverId);
    activeMiningCoordinator.stop();
  }

  @Override
  public void awaitStop() throws InterruptedException {
    activeMiningCoordinator.awaitStop();
  }

  @Override
  public boolean enable() {
    return activeMiningCoordinator.enable();
  }

  @Override
  public boolean disable() {
    return activeMiningCoordinator.disable();
  }

  @Override
  public boolean isMining() {
    return activeMiningCoordinator.isMining();
  }

  @Override
  public Wei getMinTransactionGasPrice() {
    return activeMiningCoordinator.getMinTransactionGasPrice();
  }

  @Override
  public Wei getMinPriorityFeePerGas() {
    return activeMiningCoordinator.getMinPriorityFeePerGas();
  }

  @Override
  public Optional<Address> getCoinbase() {
    return activeMiningCoordinator.getCoinbase();
  }

  @Override
  public Optional<Block> createBlock(
      final BlockHeader parentHeader,
      final List<Transaction> transactions,
      final List<BlockHeader> ommers) {
    return activeMiningCoordinator.createBlock(parentHeader, transactions, ommers);
  }

  @Override
  public Optional<Block> createBlock(final BlockHeader parentHeader, final long timestamp) {
    return Optional.empty();
  }

  @Override
  public void changeTargetGasLimit(final Long targetGasLimit) {
    activeMiningCoordinator.changeTargetGasLimit(targetGasLimit);
  }

  @Override
  public void onBlockAdded(final BlockAddedEvent event) {
    final long currentBlock = event.getHeader().getNumber();
    final long parentTimestamp = event.getHeader().getTimestamp();
    final MiningCoordinator nextMiningCoordinator =
        miningCoordinatorSchedule.getFork(currentBlock + 1, parentTimestamp).getValue();

    if (activeMiningCoordinator != nextMiningCoordinator) {
      LOG.trace(
          "Migrating mining coordinator at block {} from {} to {}",
          currentBlock,
          activeMiningCoordinator.getClass().getSimpleName(),
          nextMiningCoordinator.getClass().getSimpleName());

      final Runnable stopActiveCoordinatorTask = () -> activeMiningCoordinator.stop();
      final Runnable startNextCoordinatorTask =
          () -> {
            activeMiningCoordinator = nextMiningCoordinator;
            startActiveMiningCoordinator();
            if (activeMiningCoordinator instanceof BlockAddedObserver) {
              ((BlockAddedObserver) activeMiningCoordinator).onBlockAdded(event);
            }
          };

      CompletableFuture.runAsync(stopActiveCoordinatorTask).thenRun(startNextCoordinatorTask);

    } else if (activeMiningCoordinator instanceof BlockAddedObserver) {
      ((BlockAddedObserver) activeMiningCoordinator).onBlockAdded(event);
    }
  }

  /**
   * Gets mining coordinator schedule.
   *
   * @return the mining coordinator schedule
   */
  @VisibleForTesting
  public ForksSchedule<MiningCoordinator> getMiningCoordinatorSchedule() {
    return this.miningCoordinatorSchedule;
  }
}
