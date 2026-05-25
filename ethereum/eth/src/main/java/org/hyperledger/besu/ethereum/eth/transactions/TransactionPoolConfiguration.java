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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.plugin.services.TransactionPoolValidatorService;
import org.hyperledger.besu.plugin.services.txvalidator.PluginTransactionPoolValidator;
import org.hyperledger.besu.plugin.services.txvalidator.PluginTransactionPoolValidatorFactory;
import org.hyperledger.besu.util.number.Fraction;
import org.hyperledger.besu.util.number.Percentage;

import java.io.File;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import org.immutables.value.Value;

@Value.Immutable
@Value.Style(allParameters = true)
@Value.Enclosing
public interface TransactionPoolConfiguration {

  @Value.Immutable
  interface Unstable {
    Duration ETH65_TRX_ANNOUNCED_BUFFERING_PERIOD = Duration.ofMillis(500);
    int DEFAULT_TX_MSG_KEEP_ALIVE = 60;
    int DEFAULT_MAX_TRACKED_SEEN_TXS = 300_000;
    int DEFAULT_MAX_SEND_QUEUE_SIZE_PER_PEER = 100_000;
    boolean DEFAULT_PEER_TRACKER_FORGET_EVICTED_TXS = false;
    Duration DEFAULT_SAVE_RESTORE_TIMEOUT = Duration.ofMinutes(1);

    TransactionPoolConfiguration.Unstable DEFAULT =
        ImmutableTransactionPoolConfiguration.Unstable.builder().build();

    @Value.Default
    default Duration getEth65TrxAnnouncedBufferingPeriod() {
      return ETH65_TRX_ANNOUNCED_BUFFERING_PERIOD;
    }

    @Value.Default
    default int getTxMessageKeepAliveSeconds() {
      return DEFAULT_TX_MSG_KEEP_ALIVE;
    }

    @Value.Default
    default int getMaxTrackedSeenTxs() {
      return DEFAULT_MAX_TRACKED_SEEN_TXS;
    }

    @Value.Default
    default int getMaxSendQueueSizePerPeer() {
      return DEFAULT_MAX_SEND_QUEUE_SIZE_PER_PEER;
    }

    @Value.Default
    default boolean getPeerTrackerForgetEvictedTxs() {
      return DEFAULT_PEER_TRACKER_FORGET_EVICTED_TXS;
    }

    @Value.Default
    default Duration getSaveRestoreTimeout() {
      return DEFAULT_SAVE_RESTORE_TIMEOUT;
    }
  }

  enum Implementation {
    LEGACY, // Remove in future version
    LAYERED,
    SEQUENCED; // Synonym for LEGACY
  }

  String DEFAULT_SAVE_FILE_NAME = "txpool.dump";
  int DEFAULT_MAX_PENDING_TRANSACTIONS = 4096;
  Fraction DEFAULT_LIMIT_TX_POOL_BY_ACCOUNT_PERCENTAGE = Fraction.fromFloat(0.001f); // 0.1%
  int DEFAULT_TX_RETENTION_HOURS = 13;
  boolean DEFAULT_STRICT_TX_REPLAY_PROTECTION_ENABLED = false;
  Percentage DEFAULT_PRICE_BUMP = Percentage.fromInt(10);
  Percentage DEFAULT_BLOB_PRICE_BUMP = Percentage.fromInt(100);
  Wei DEFAULT_RPC_TX_FEE_CAP = Wei.fromEth(1);
  boolean DEFAULT_NO_LOCAL_PRIORITY = false;
  boolean DEFAULT_ENABLE_SAVE_RESTORE = false;
  File DEFAULT_SAVE_FILE = new File(DEFAULT_SAVE_FILE_NAME);
  // 50 MB expressed in decimal bytes (50 * 10^6), not 50 MiB
  long DEFAULT_PENDING_TRANSACTIONS_LAYER_MAX_CAPACITY_BYTES = 50L * 1_000_000L;
  int DEFAULT_MAX_PRIORITIZED_TRANSACTIONS = 5000;
  EnumMap<TransactionType, Integer> DEFAULT_MAX_PRIORITIZED_TRANSACTIONS_BY_TYPE =
      new EnumMap<>(Map.of(TransactionType.BLOB, 72));
  int DEFAULT_MAX_FUTURE_BY_SENDER = 200;
  Implementation DEFAULT_TX_POOL_IMPLEMENTATION = Implementation.LAYERED;
  Set<Address> DEFAULT_PRIORITY_SENDERS = Set.of();
  Wei DEFAULT_TX_POOL_MIN_GAS_PRICE = Wei.of(1000);
  byte DEFAULT_TX_POOL_MIN_SCORE = -128;
  boolean DEFAULT_TX_POOL_ENABLE_BALANCE_CHECK = true;

  TransactionPoolConfiguration DEFAULT = ImmutableTransactionPoolConfiguration.builder().build();

  @Value.Default
  default int getTxPoolMaxSize() {
    return DEFAULT_MAX_PENDING_TRANSACTIONS;
  }

  @Value.Default
  default Fraction getTxPoolLimitByAccountPercentage() {
    return DEFAULT_LIMIT_TX_POOL_BY_ACCOUNT_PERCENTAGE;
  }

  @Value.Derived
  default int getTxPoolMaxFutureTransactionByAccount() {
    return (int) Math.ceil(getTxPoolLimitByAccountPercentage().getValue() * getTxPoolMaxSize());
  }

  @Value.Default
  default int getPendingTxRetentionPeriod() {
    return DEFAULT_TX_RETENTION_HOURS;
  }

  @Value.Default
  default Percentage getPriceBump() {
    return DEFAULT_PRICE_BUMP;
  }

  @Value.Default
  default Percentage getBlobPriceBump() {
    return DEFAULT_BLOB_PRICE_BUMP;
  }

  @Value.Default
  default Wei getTxFeeCap() {
    return DEFAULT_RPC_TX_FEE_CAP;
  }

  @Value.Default
  default Boolean getStrictTransactionReplayProtectionEnabled() {
    return DEFAULT_STRICT_TX_REPLAY_PROTECTION_ENABLED;
  }

  @Value.Default
  default Boolean getNoLocalPriority() {
    return DEFAULT_NO_LOCAL_PRIORITY;
  }

  @Value.Default
  default Boolean getEnableSaveRestore() {
    return DEFAULT_ENABLE_SAVE_RESTORE;
  }

  @Value.Default
  default File getSaveFile() {
    return DEFAULT_SAVE_FILE;
  }

  @Value.Default
  default Implementation getTxPoolImplementation() {
    return DEFAULT_TX_POOL_IMPLEMENTATION;
  }

  @Value.Default
  default long getPendingTransactionsLayerMaxCapacityBytes() {
    return DEFAULT_PENDING_TRANSACTIONS_LAYER_MAX_CAPACITY_BYTES;
  }

  @Value.Default
  default int getMaxPrioritizedTransactions() {
    return DEFAULT_MAX_PRIORITIZED_TRANSACTIONS;
  }

  @Value.Default
  default Map<TransactionType, Integer> getMaxPrioritizedTransactionsByType() {
    return DEFAULT_MAX_PRIORITIZED_TRANSACTIONS_BY_TYPE;
  }

  @Value.Default
  default int getMaxFutureBySender() {
    return DEFAULT_MAX_FUTURE_BY_SENDER;
  }

  @Value.Default
  default Set<Address> getPrioritySenders() {
    return DEFAULT_PRIORITY_SENDERS;
  }

  @Value.Default
  default Wei getMinGasPrice() {
    return DEFAULT_TX_POOL_MIN_GAS_PRICE;
  }

  @Value.Default
  default byte getMinScore() {
    return DEFAULT_TX_POOL_MIN_SCORE;
  }

  @Value.Default
  default boolean getEnableBalanceCheck() {
    return DEFAULT_TX_POOL_ENABLE_BALANCE_CHECK;
  }

  @Value.Default
  default TransactionPoolValidatorService getTransactionPoolValidatorService() {
    return new TransactionPoolValidatorService() {
      @Override
      public PluginTransactionPoolValidator createTransactionValidator() {
        return PluginTransactionPoolValidator.VALIDATE_ALL;
      }

      @Override
      public void registerPluginTransactionValidatorFactory(
          final PluginTransactionPoolValidatorFactory pluginTransactionPoolValidatorFactory) {}
    };
  }

  @Value.Default
  default Unstable getUnstable() {
    return Unstable.DEFAULT;
  }
}
