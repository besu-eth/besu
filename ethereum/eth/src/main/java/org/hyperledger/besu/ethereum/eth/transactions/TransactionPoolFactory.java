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
package org.hyperledger.besu.ethereum.eth.transactions;

import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration.Implementation.LAYERED;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.transactions.layered.AbstractPrioritizedTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.BaseFeePrioritizedTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.EndLayer;
import org.hyperledger.besu.ethereum.eth.transactions.layered.GasPricePrioritizedTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredPendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.ReadyTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.SenderBalanceChecker;
import org.hyperledger.besu.ethereum.eth.transactions.layered.SparseTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.AbstractPendingTransactionsSorter;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.BaseFeePendingTransactionsSorter;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.GasPricePendingTransactionsSorter;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.plugin.services.BesuEvents;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.time.Clock;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionPoolFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionPoolFactory.class);

  public static TransactionPool createTransactionPool(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final Clock clock,
      final MetricsSystem metricsSystem,
      final SyncState syncState,
      final TransactionPoolConfiguration transactionPoolConfiguration,
      final EthProtocolConfiguration ethProtocolConfiguration,
      final BlobCache blobCache,
      final MiningConfiguration miningConfiguration) {

    final TransactionPoolMetrics metrics = new TransactionPoolMetrics(metricsSystem);

    final PeerTransactionTracker transactionTracker =
        new PeerTransactionTracker(
            transactionPoolConfiguration, ethContext.getEthPeers(), ethContext.getScheduler());
    final TransactionsMessageSender transactionsMessageSender =
        new TransactionsMessageSender(
            transactionTracker, ethProtocolConfiguration.getMaxTransactionsMessageSize());

    final NewPooledTransactionHashesMessageSender newPooledTransactionHashesMessageSender =
        new NewPooledTransactionHashesMessageSender(transactionTracker);

    return createTransactionPool(
        protocolSchedule,
        protocolContext,
        ethContext,
        clock,
        metrics,
        syncState,
        transactionPoolConfiguration,
        transactionTracker,
        transactionsMessageSender,
        newPooledTransactionHashesMessageSender,
        blobCache,
        miningConfiguration,
        ethProtocolConfiguration);
  }

  static TransactionPool createTransactionPool(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final Clock clock,
      final TransactionPoolMetrics metrics,
      final SyncState syncState,
      final TransactionPoolConfiguration transactionPoolConfiguration,
      final PeerTransactionTracker transactionTracker,
      final TransactionsMessageSender transactionsMessageSender,
      final NewPooledTransactionHashesMessageSender newPooledTransactionHashesMessageSender,
      final BlobCache blobCache,
      final MiningConfiguration miningConfiguration,
      final EthProtocolConfiguration ethProtocolConfiguration) {

    final TransactionPool transactionPool =
        new TransactionPool(
            () ->
                createPendingTransactions(
                    protocolSchedule,
                    protocolContext,
                    ethContext.getScheduler(),
                    clock,
                    metrics,
                    transactionPoolConfiguration,
                    blobCache,
                    miningConfiguration),
            protocolSchedule,
            protocolContext,
            new TransactionBroadcaster(
                ethContext,
                transactionTracker,
                transactionsMessageSender,
                newPooledTransactionHashesMessageSender),
            ethContext,
            metrics,
            transactionPoolConfiguration,
            blobCache);

    final TransactionsMessageHandler transactionsMessageHandler =
        new TransactionsMessageHandler(
            ethContext.getScheduler(),
            new TransactionsMessageProcessor(
                transactionTracker,
                transactionPool,
                metrics,
                ethProtocolConfiguration.getMaxTransactionsPerMessage()),
            transactionPoolConfiguration.getUnstable().getTxMessageKeepAliveSeconds(),
            ethProtocolConfiguration.getMaxMessageSize());

    final NewPooledTransactionHashesMessageHandler pooledTransactionsMessageHandler =
        new NewPooledTransactionHashesMessageHandler(
            ethContext.getScheduler(),
            new NewPooledTransactionHashesMessageProcessor(
                transactionTracker,
                transactionPool,
                transactionPoolConfiguration,
                ethContext,
                metrics,
                ethProtocolConfiguration.getMaxTransactionsMessageSize()),
            transactionPoolConfiguration.getUnstable().getTxMessageKeepAliveSeconds());

    subscribeTransactionHandlers(
        protocolContext,
        ethContext,
        transactionTracker,
        transactionPool,
        transactionsMessageHandler,
        pooledTransactionsMessageHandler);

    if (syncState.isInitialSyncPhaseDone()) {
      LOG.info("Enabling transaction pool");
      pooledTransactionsMessageHandler.setEnabled();
      transactionsMessageHandler.setEnabled();
      transactionPool.setEnabled();
    } else {
      LOG.info("Transaction pool disabled while initial sync in progress");
    }

    syncState.subscribeCompletionReached(
        new BesuEvents.InitialSyncCompletionListener() {
          @Override
          public void onInitialSyncCompleted() {
            LOG.info("Enabling transaction handling following initial sync");
            enableTransactionHandling(
                transactionTracker,
                transactionPool,
                transactionsMessageHandler,
                pooledTransactionsMessageHandler);
          }

          @Override
          public void onInitialSyncRestart() {
            LOG.info("Disabling transaction handling during re-sync");
            disableTransactionHandling(
                transactionPool, transactionsMessageHandler, pooledTransactionsMessageHandler);
          }
        });

    syncState.subscribeInSync(
        isInSync -> {
          if (isInSync != transactionPool.isEnabled()) {
            if (isInSync && syncState.isInitialSyncPhaseDone()) {
              LOG.info("Node is in sync, enabling transaction handling");
              enableTransactionHandling(
                  transactionTracker,
                  transactionPool,
                  transactionsMessageHandler,
                  pooledTransactionsMessageHandler);
            } else {
              if (transactionPool.isEnabled()) {
                LOG.info("Node out of sync, disabling transaction handling");
                disableTransactionHandling(
                    transactionPool, transactionsMessageHandler, pooledTransactionsMessageHandler);
              }
            }
          }
        });

    return transactionPool;
  }

  private static void enableTransactionHandling(
      final PeerTransactionTracker transactionTracker,
      final TransactionPool transactionPool,
      final TransactionsMessageHandler transactionsMessageHandler,
      final NewPooledTransactionHashesMessageHandler pooledTransactionsMessageHandler) {
    transactionTracker.reset();
    transactionPool.setEnabled();
    transactionsMessageHandler.setEnabled();
    pooledTransactionsMessageHandler.setEnabled();
  }

  private static void disableTransactionHandling(
      final TransactionPool transactionPool,
      final TransactionsMessageHandler transactionsMessageHandler,
      final NewPooledTransactionHashesMessageHandler pooledTransactionsMessageHandler) {
    transactionPool.setDisabled();
    transactionsMessageHandler.setDisabled();
    pooledTransactionsMessageHandler.setDisabled();
  }

  private static void subscribeTransactionHandlers(
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final PeerTransactionTracker transactionTracker,
      final TransactionPool transactionPool,
      final TransactionsMessageHandler transactionsMessageHandler,
      final NewPooledTransactionHashesMessageHandler pooledTransactionsMessageHandler) {
    ethContext.getEthPeers().subscribeConnect(transactionTracker);
    ethContext.getEthPeers().subscribeDisconnect(transactionTracker);
    protocolContext.getBlockchain().observeBlockAdded(transactionPool);
    protocolContext.getBlockchain().observeBlockAdded(transactionTracker);
    ethContext
        .getEthMessages()
        .subscribe(EthProtocolMessages.TRANSACTIONS, transactionsMessageHandler);
    ethContext
        .getEthMessages()
        .subscribe(
            EthProtocolMessages.NEW_POOLED_TRANSACTION_HASHES, pooledTransactionsMessageHandler);
  }

  private static PendingTransactions createPendingTransactions(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthScheduler ethScheduler,
      final Clock clock,
      final TransactionPoolMetrics metrics,
      final TransactionPoolConfiguration transactionPoolConfiguration,
      final BlobCache blobCache,
      final MiningConfiguration miningConfiguration) {

    boolean isFeeMarketImplementBaseFee =
        protocolSchedule.anyMatch(
            scheduledSpec -> scheduledSpec.spec().getFeeMarket().implementsBaseFee());

    if (transactionPoolConfiguration.getTxPoolImplementation().equals(LAYERED)) {
      return createLayeredPendingTransactions(
          protocolSchedule,
          protocolContext,
          ethScheduler,
          metrics,
          transactionPoolConfiguration,
          isFeeMarketImplementBaseFee,
          blobCache,
          miningConfiguration);
    } else {
      return createPendingTransactionSorter(
          protocolContext,
          clock,
          metrics.getMetricsSystem(),
          transactionPoolConfiguration,
          isFeeMarketImplementBaseFee);
    }
  }

  private static AbstractPendingTransactionsSorter createPendingTransactionSorter(
      final ProtocolContext protocolContext,
      final Clock clock,
      final MetricsSystem metricsSystem,
      final TransactionPoolConfiguration transactionPoolConfiguration,
      final boolean isFeeMarketImplementBaseFee) {
    if (isFeeMarketImplementBaseFee) {
      return new BaseFeePendingTransactionsSorter(
          transactionPoolConfiguration,
          clock,
          metricsSystem,
          protocolContext.getBlockchain()::getChainHeadHeader);
    } else {
      return new GasPricePendingTransactionsSorter(
          transactionPoolConfiguration,
          clock,
          metricsSystem,
          protocolContext.getBlockchain()::getChainHeadHeader);
    }
  }

  private static PendingTransactions createLayeredPendingTransactions(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthScheduler ethScheduler,
      final TransactionPoolMetrics metrics,
      final TransactionPoolConfiguration transactionPoolConfiguration,
      final boolean isFeeMarketImplementBaseFee,
      final BlobCache blobCache,
      final MiningConfiguration miningConfiguration) {

    final TransactionPoolReplacementHandler transactionReplacementHandler =
        new TransactionPoolReplacementHandler(
            transactionPoolConfiguration.getPriceBump(),
            transactionPoolConfiguration.getBlobPriceBump());

    final BiFunction<PendingTransaction, PendingTransaction, Boolean> transactionReplacementTester =
        (t1, t2) ->
            transactionReplacementHandler.shouldReplace(
                t1, t2, protocolContext.getBlockchain().getChainHeadHeader());

    final SenderBalanceChecker senderBalanceChecker =
        SenderBalanceChecker.create(
            protocolSchedule, protocolContext, transactionPoolConfiguration);

    final EndLayer endLayer = new EndLayer(metrics);

    final SparseTransactions sparseTransactions =
        new SparseTransactions(
            transactionPoolConfiguration,
            ethScheduler,
            endLayer,
            metrics,
            transactionReplacementTester,
            blobCache);

    final ReadyTransactions readyTransactions =
        new ReadyTransactions(
            transactionPoolConfiguration,
            ethScheduler,
            sparseTransactions,
            metrics,
            transactionReplacementTester,
            blobCache);

    final AbstractPrioritizedTransactions pendingTransactionsSorter;
    if (isFeeMarketImplementBaseFee) {
      final FeeMarket feeMarket =
          protocolSchedule
              .getByBlockHeader(protocolContext.getBlockchain().getChainHeadHeader())
              .getFeeMarket();

      pendingTransactionsSorter =
          new BaseFeePrioritizedTransactions(
              transactionPoolConfiguration,
              protocolContext.getBlockchain()::getChainHeadHeader,
              ethScheduler,
              readyTransactions,
              metrics,
              transactionReplacementTester,
              feeMarket,
              blobCache,
              miningConfiguration,
              senderBalanceChecker);
    } else {
      pendingTransactionsSorter =
          new GasPricePrioritizedTransactions(
              transactionPoolConfiguration,
              ethScheduler,
              readyTransactions,
              metrics,
              transactionReplacementTester,
              blobCache,
              miningConfiguration,
              senderBalanceChecker);
    }

    return new LayeredPendingTransactions(
        transactionPoolConfiguration, pendingTransactionsSorter, ethScheduler);
  }
}
