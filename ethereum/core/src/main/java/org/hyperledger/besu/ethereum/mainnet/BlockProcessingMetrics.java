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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;

import java.util.concurrent.atomic.AtomicLong;

public class BlockProcessingMetrics {

  private final AtomicLong accountsCount = new AtomicLong();
  private final AtomicLong storageSlotsCount = new AtomicLong();
  private final AtomicLong codeCount = new AtomicLong();
  private final AtomicLong accountsUpdated = new AtomicLong();
  private final AtomicLong storageSlotsUpdated = new AtomicLong();
  private final Counter blocksTotal;
  private final OperationTimer stateRootCalculationTimer;

  public BlockProcessingMetrics(final MetricsSystem metricsSystem) {
    metricsSystem.createLongGauge(
        BesuMetricCategory.BAL,
        "accounts_count",
        "Number of unique accounts in a BAL (reads + writes)",
        accountsCount::get);
    metricsSystem.createLongGauge(
        BesuMetricCategory.BAL,
        "storage_slots_count",
        "Number of total storage slots in a BAL (reads + writes)",
        storageSlotsCount::get);
    metricsSystem.createLongGauge(
        BesuMetricCategory.BAL,
        "code_count",
        "Number of unique contracts with code in a BAL",
        codeCount::get);
    metricsSystem.createLongGauge(
        BesuMetricCategory.BAL,
        "accounts_updated",
        "Number of accounts with changes in a BAL (writes)",
        accountsUpdated::get);
    metricsSystem.createLongGauge(
        BesuMetricCategory.BAL,
        "storage_slots_updated",
        "Number of storage slots modified in a BAL (writes)",
        storageSlotsUpdated::get);
    blocksTotal =
        metricsSystem.createCounter(
            BesuMetricCategory.BAL, "blocks_total", "Number of BAL-enabled blocks processed");

    stateRootCalculationTimer =
        metricsSystem.createTimer(
            BesuMetricCategory.BLOCK_PROCESSING,
            "state_root_calculation_duration_seconds",
            "Time taken by state root calculation");
  }

  public OperationTimer stateRootCalculationTimer() {
    return stateRootCalculationTimer;
  }

  public void recordBlockAccessListMetrics(final BlockAccessList bal) {
    blocksTotal.inc();
    accountsCount.set(bal.accountChanges().size());
    storageSlotsCount.set(countStorageSlots(bal));
    codeCount.set(countCodeAccounts(bal));
    accountsUpdated.set(countUpdatedAccounts(bal));
    storageSlotsUpdated.set(countUpdatedStorageSlots(bal));
  }

  private long countStorageSlots(final BlockAccessList bal) {
    return bal.accountChanges().stream()
        .mapToLong(
            accountChanges ->
                accountChanges.storageChanges().size() + accountChanges.storageReads().size())
        .sum();
  }

  private long countCodeAccounts(final BlockAccessList bal) {
    return bal.accountChanges().stream()
        .filter(accountChanges -> !accountChanges.codeChanges().isEmpty())
        .count();
  }

  private long countUpdatedAccounts(final BlockAccessList bal) {
    return bal.accountChanges().stream().filter(this::hasAnyChange).count();
  }

  private long countUpdatedStorageSlots(final BlockAccessList bal) {
    return bal.accountChanges().stream()
        .mapToLong(accountChanges -> accountChanges.storageChanges().size())
        .sum();
  }

  private boolean hasAnyChange(final AccountChanges accountChanges) {
    return accountChanges.hasAnyChange();
  }
}
