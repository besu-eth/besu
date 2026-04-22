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
package org.hyperledger.besu.ethereum.eth.transactions.layered;

import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolStructuredLogUtils.logSenderBalance;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.worldstate.WorldState;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks if the sender has enough balance to pay to a pending transaction. This is used by the
 * txpool to decided what to do in case the sender hasn't enough balance.
 */
public interface SenderBalanceChecker {
  boolean hasEnoughBalanceFor(final PendingTransaction pendingTransaction);

  void clear();

  static SenderBalanceChecker create(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final TransactionPoolConfiguration transactionPoolConfiguration) {
    return transactionPoolConfiguration.getEnableBalanceCheck()
        ? new SenderBalanceChecker.WorldStateChecker(protocolSchedule, protocolContext)
        : new SenderBalanceChecker.NoOpChecker();
  }

  /**
   * Does no check at all and always returns that sender has enough balance. It is used when the
   * check is disabled by configuration.
   */
  class NoOpChecker implements SenderBalanceChecker {
    @Override
    public boolean hasEnoughBalanceFor(final PendingTransaction pendingTransaction) {
      return true;
    }

    @Override
    public void clear() {}
  }

  /**
   * Does the check reading the balance from the chain head world state. It must be reset, calling
   * the clear method, on every new block imported, that because the class keeps a cache of the
   * balances, both for performance reasons, and to be able to check all the pending transactions
   * for a sender.
   *
   * <p>For simplicity and performance, the check of balance, takes the assumption that the sender
   * does not receive any new balance during the period of a block, so it is a pessimistic
   * approximation.
   *
   * <p>The algorithm to check a sequence of pending transactions from the same sender works like
   * that: for the first pending transaction of the sender, its account is read from the world
   * state, and put in the cache, the check is done and the upfront cost is subtracted from the
   * cached balance, so following pending transactions will found the updated balance in the cache
   * and will subtract their upfront cost, and so on.
   */
  class WorldStateChecker implements SenderBalanceChecker {
    private static final Logger LOG = LoggerFactory.getLogger(SenderBalanceChecker.class);
    private final ProtocolSchedule protocolSchedule;
    private final WorldStateArchive worldStateArchive;
    private final Blockchain blockchain;
    private final Map<Address, Wei> senderBalancesCache = new HashMap<>();

    public WorldStateChecker(
        final ProtocolSchedule protocolSchedule, final ProtocolContext protocolContext) {
      this.protocolSchedule = protocolSchedule;
      this.worldStateArchive = protocolContext.getWorldStateArchive();
      this.blockchain = protocolContext.getBlockchain();
    }

    @Override
    public boolean hasEnoughBalanceFor(final PendingTransaction pendingTransaction) {
      final var tx = pendingTransaction.getTransaction();
      final var sender = tx.getSender();

      final var senderBalance = senderBalancesCache.computeIfAbsent(sender, this::getSenderBalance);

      if (senderBalance.equals(Wei.ZERO)) {
        LOG.atTrace()
            .setMessage("Sender has zero balance for transaction {}")
            .addArgument(pendingTransaction::toTraceLog)
            .log();
        return false;
      }

      final var gasCalculator =
          protocolSchedule.getByBlockHeader(blockchain.getChainHeadHeader()).getGasCalculator();
      final var upfrontCost = tx.getUpfrontCost(gasCalculator.blobGasCost(tx.getBlobCount()));

      if (senderBalance.lessThan(upfrontCost)) {
        LOG.atTrace()
            .setMessage("Sender balance {} is not enough to pay upfront cost {} for transaction {}")
            .addArgument(senderBalance::toHumanReadableString)
            .addArgument(upfrontCost::toHumanReadableString)
            .addArgument(pendingTransaction::toTraceLog)
            .log();

        senderBalancesCache.put(sender, Wei.ZERO);
        return false;
      }

      senderBalancesCache.put(sender, senderBalance.subtract(upfrontCost));
      return true;
    }

    private Wei getSenderBalance(final Address sender) {
      final BlockHeader chainHead = blockchain.getChainHeadHeader();
      try (final WorldState headState =
          worldStateArchive
              .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(chainHead))
              .get()) {
        final Account maybeAccount = headState.get(sender);
        final Wei senderBalance = maybeAccount != null ? maybeAccount.getBalance() : Wei.ZERO;
        logSenderBalance(sender, senderBalance);
        return senderBalance;
      } catch (Exception e) {
        LOG.warn(
            "Failed to get balance for {} at chain header {}", sender, chainHead.toLogString(), e);
        return Wei.ZERO;
      }
    }

    @Override
    public void clear() {
      senderBalancesCache.clear();
    }
  }
}
