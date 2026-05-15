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
package org.hyperledger.besu.ethereum.blockcreation;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MinedBlockObserver;
import org.hyperledger.besu.ethereum.chain.PoWObserver;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.AbstractGasLimitSpecification;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.util.Subscribers;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMinerExecutor<M extends BlockMiner<? extends AbstractBlockCreator>> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractMinerExecutor.class);

  private final ExecutorService executorService =
      Executors.newCachedThreadPool(r -> new Thread(r, "MinerExecutor"));
  protected final ProtocolContext protocolContext;
  protected final ProtocolSchedule protocolSchedule;
  protected final TransactionPool transactionPool;
  protected final DefaultBlockScheduler blockScheduler;
  protected final MiningConfiguration miningConfiguration;
  protected final EthScheduler ethScheduler;
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  protected AbstractMinerExecutor(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final TransactionPool transactionPool,
      final MiningConfiguration miningParams,
      final DefaultBlockScheduler blockScheduler,
      final EthScheduler ethScheduler) {
    this.protocolContext = protocolContext;
    this.protocolSchedule = protocolSchedule;
    this.transactionPool = transactionPool;
    this.blockScheduler = blockScheduler;
    this.miningConfiguration = miningParams;
    this.ethScheduler = ethScheduler;
  }

  public Optional<M> startAsyncMining(
      final Subscribers<MinedBlockObserver> observers,
      final Subscribers<PoWObserver> ethHashObservers,
      final BlockHeader parentHeader) {
    try {
      final M currentRunningMiner = createMiner(observers, ethHashObservers, parentHeader);
      executorService.execute(currentRunningMiner);
      return Optional.of(currentRunningMiner);
    } catch (RejectedExecutionException e) {
      LOG.warn("Unable to start mining.", e);
      return Optional.empty();
    }
  }

  public void shutDown() {
    if (stopped.compareAndSet(false, true)) {
      executorService.shutdownNow();
    }
  }

  public void awaitShutdown() throws InterruptedException {
    if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
      LOG.error("Failed to shutdown {}.", this.getClass().getSimpleName());
    }
  }

  public abstract M createMiner(
      final Subscribers<MinedBlockObserver> subscribers,
      final Subscribers<PoWObserver> ethHashObservers,
      final BlockHeader parentHeader);

  public void setMinTransactionGasPrice(final Wei minTransactionGasPrice) {
    miningConfiguration.setMinTransactionGasPrice(minTransactionGasPrice);
  }

  public Wei getMinTransactionGasPrice() {
    return miningConfiguration.getMinTransactionGasPrice();
  }

  public Wei getMinPriorityFeePerGas() {
    return miningConfiguration.getMinPriorityFeePerGas();
  }

  public abstract Optional<Address> getCoinbase();

  public void changeTargetGasLimit(final Long newTargetGasLimit) {
    if (AbstractGasLimitSpecification.isValidTargetGasLimit(newTargetGasLimit)) {
      miningConfiguration.setTargetGasLimit(newTargetGasLimit);
    } else {
      throw new UnsupportedOperationException("Specified target gas limit is invalid");
    }
  }
}
