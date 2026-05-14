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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static io.netty.util.internal.ObjectUtil.checkNonEmpty;
import static org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager.Step.HEAL_TRIE;

import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages the metrics related to the SnapSync process. */
public class SnapSyncMetricsManager {

  private static final Logger LOG = LoggerFactory.getLogger(SnapSyncMetricsManager.class);
  private static final long PRINT_DELAY = TimeUnit.MINUTES.toMillis(1);

  private final MetricsSystem metricsSystem;
  private final EthContext ethContext;

  /** Represents the progress status of the snapsync process. */
  private final AtomicReference<BigDecimal> percentageProgress;

  /**
   * Represents the number of accounts downloaded during the initial step of the snapsync process.
   */
  private final AtomicLong nbAccountsDownloaded;

  /** Represents the number of slots downloaded during the initial step of the snapsync process. */
  private final AtomicLong nbSlotsDownloaded;

  /** Represents the number of code entries downloaded. */
  private final AtomicLong nbCodes;

  /**
   * Represents the number of trie nodes generated during the initial step of the snapsync process.
   */
  private final AtomicLong nbTrieNodesGenerated;

  /** Represents the number of flat accounts healed during the healing process. */
  private final AtomicLong nbFlatAccountsHealed;

  /** Represents the number of flat slots healed during the healing process. */
  private final AtomicLong nbFlatSlotsHealed;

  /** Represents the number of trie nodes healed during the healing process. */
  private final AtomicLong nbTrieNodesHealed;

  private long startSyncTime;

  private final Map<Bytes32, BigInteger> lastRangeIndex = new HashMap<>();

  private long lastNotifyTimestamp;

  public SnapSyncMetricsManager(final MetricsSystem metricsSystem, final EthContext ethContext) {
    this.metricsSystem = metricsSystem;
    this.ethContext = ethContext;
    percentageProgress = new AtomicReference<>(new BigDecimal(0));
    nbAccountsDownloaded = new AtomicLong(0);
    nbSlotsDownloaded = new AtomicLong(0);
    nbCodes = new AtomicLong(0);
    nbTrieNodesGenerated = new AtomicLong(0);
    nbFlatAccountsHealed = new AtomicLong(0);
    nbFlatSlotsHealed = new AtomicLong(0);
    nbTrieNodesHealed = new AtomicLong(0);
    metricsSystem.createCounter(
        BesuMetricCategory.SYNCHRONIZER,
        "snap_world_state_generated_nodes_total",
        "Total number of data nodes generated as part of snap sync world state download",
        nbTrieNodesGenerated::get);
    metricsSystem.createCounter(
        BesuMetricCategory.SYNCHRONIZER,
        "snap_world_state_healed_nodes_total",
        "Total number of data nodes healed as part of snap sync world state heal process",
        nbTrieNodesHealed::get);
    metricsSystem.createCounter(
        BesuMetricCategory.SYNCHRONIZER,
        "snap_world_state_accounts_total",
        "Total number of accounts downloaded as part of snap sync world state",
        nbAccountsDownloaded::get);
    metricsSystem.createCounter(
        BesuMetricCategory.SYNCHRONIZER,
        "snap_world_state_slots_total",
        "Total number of slots downloaded as part of snap sync world state",
        nbSlotsDownloaded::get);
    metricsSystem.createCounter(
        BesuMetricCategory.SYNCHRONIZER,
        "snap_world_state_flat_accounts_healed_total",
        "Total number of accounts healed in the flat database as part of snap sync world state",
        nbFlatAccountsHealed::get);
    metricsSystem.createCounter(
        BesuMetricCategory.SYNCHRONIZER,
        "snap_world_state_flat_slots_healed_total",
        "Total number of slots healed in the flat database as part of snap sync world state",
        nbFlatSlotsHealed::get);
    metricsSystem.createCounter(
        BesuMetricCategory.SYNCHRONIZER,
        "snap_world_state_codes_total",
        "Total number of codes downloaded as part of snap sync world state",
        nbCodes::get);
  }

  public void initRange(final Map<Bytes32, Bytes32> ranges) {
    for (Map.Entry<Bytes32, Bytes32> entry : ranges.entrySet()) {
      this.lastRangeIndex.put(entry.getValue(), entry.getKey().toUnsignedBigInteger());
    }
    this.startSyncTime = System.currentTimeMillis();
    this.lastNotifyTimestamp = startSyncTime;
  }

  public void notifyRangeProgress(
      final Step step, final Bytes32 startKeyHash, final Bytes32 endKeyHash) {
    checkNonEmpty(lastRangeIndex, "snapsync range collection");
    if (lastRangeIndex.containsKey(endKeyHash)) {
      final BigInteger lastPos = lastRangeIndex.get(endKeyHash);
      final BigInteger newPos = startKeyHash.toUnsignedBigInteger();
      percentageProgress.getAndAccumulate(
          BigDecimal.valueOf(100)
              .multiply(new BigDecimal(newPos.subtract(lastPos)))
              .divide(
                  new BigDecimal(RangeManager.MAX_RANGE.toUnsignedBigInteger()),
                  MathContext.DECIMAL32),
          BigDecimal::add);
      lastRangeIndex.put(endKeyHash, newPos);
      print(step);
    }
  }

  public void notifyAccountsDownloaded(final long nbAccounts) {
    this.nbAccountsDownloaded.getAndAdd(nbAccounts);
  }

  public void notifySlotsDownloaded(final long nbSlots) {
    this.nbSlotsDownloaded.getAndAdd(nbSlots);
  }

  public void notifyCodeDownloaded() {
    this.nbCodes.getAndIncrement();
  }

  public void notifyNodesGenerated(final long nbNodes) {
    this.nbTrieNodesGenerated.getAndAdd(nbNodes);
  }

  public void notifyTrieNodesHealed(final long nbNodes) {
    this.nbTrieNodesHealed.getAndAdd(nbNodes);
    print(HEAL_TRIE);
  }

  private void print(final Step step) {
    final long now = System.currentTimeMillis();
    if (now - lastNotifyTimestamp >= PRINT_DELAY) {
      lastNotifyTimestamp = now;
      int peerCount = -1; // ethContext is not available in tests
      if (ethContext != null && ethContext.getEthPeers().peerCount() >= 0) {
        peerCount = ethContext.getEthPeers().peerCount();
      }
      switch (step) {
        case DOWNLOAD -> {
          LOG.debug(
              "Worldstate {} in progress accounts={}, slots={}, codes={}, nodes={}",
              step.message,
              nbAccountsDownloaded,
              nbSlotsDownloaded,
              nbCodes,
              nbTrieNodesGenerated);
          LOG.info(
              "Worldstate {} progress: {}%, Peer count: {}",
              step.message, percentageProgress.get().setScale(2, RoundingMode.HALF_UP), peerCount);
        }
        case HEAL_FLAT -> {
          LOG.debug(
              "Worldstate {} in progress accounts={}, slots={}",
              step.message,
              nbFlatAccountsHealed,
              nbFlatSlotsHealed);
          LOG.info(
              "Worldstate {} progress: {}%, Peer count: {}",
              step.message, percentageProgress.get().setScale(2, RoundingMode.HALF_UP), peerCount);
        }
        case HEAL_TRIE -> {
          LOG.info(
              "Healed {} world state trie nodes, Peer count: {}",
              nbTrieNodesHealed.get(),
              peerCount);
        }
      }
    }
  }

  public void notifySnapSyncCompleted() {
    final Duration duration = Duration.ofMillis(System.currentTimeMillis() - startSyncTime);
    LOG.info(
        "Finished worldstate snapsync with nodes {} (healed={}) duration {}{}:{},{}.",
        nbTrieNodesGenerated.get() + nbTrieNodesHealed.get(),
        nbTrieNodesHealed,
        duration.toHoursPart() > 0 ? (duration.toHoursPart() + ":") : "",
        duration.toMinutesPart(),
        duration.toSecondsPart(),
        duration.toMillisPart());
  }

  public MetricsSystem getMetricsSystem() {
    return metricsSystem;
  }

  public enum Step {
    DOWNLOAD("download"),
    HEAL_TRIE("trie node healing"),
    HEAL_FLAT("flat database healing");

    final String message;

    Step(final String message) {
      this.message = message;
    }
  }
}
