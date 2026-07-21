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
package org.hyperledger.besu.plugin.services.health;

import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.SyncStatus;
import org.hyperledger.besu.plugin.services.BesuEvents;
import org.hyperledger.besu.plugin.services.HealthCheckService;
import org.hyperledger.besu.plugin.services.p2p.P2PService;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/** The readiness check plugin. */
public class ReadinessCheckPlugin implements BesuPlugin {

  private static final String READINESS_ENDPOINT = "/readiness";
  private static final int DEFAULT_MIN_PEERS = 1;
  private static final long DEFAULT_MAX_BLOCKS_BEHIND = 2L;

  /** Instantiates a new readiness check plugin. */
  public ReadinessCheckPlugin() {}

  /**
   * Safely parses a string to a non-negative integer.
   *
   * @param value the string to parse
   * @param defaultValue the value to return when the input is null or unparseable
   * @return an Optional containing the parsed value if successful and non-negative, or the default
   *     otherwise
   */
  private static Optional<Integer> parseNonNegativeInt(final String value, final int defaultValue) {
    if (value == null) {
      return Optional.of(defaultValue);
    }
    try {
      int parsed = Integer.parseInt(value);
      return (parsed >= 0) ? Optional.of(parsed) : Optional.empty();
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  /**
   * Safely parses a string to a non-negative long.
   *
   * @param value the string to parse
   * @param defaultValue the value to return when the input is null
   * @return an Optional containing the parsed value if successful and non-negative, the default
   *     when null, or empty for malformed/negative input
   */
  private static Optional<Long> parseNonNegativeLong(final String value, final long defaultValue) {
    if (value == null) {
      return Optional.of(defaultValue);
    }
    try {
      long parsed = Long.parseLong(value);
      return (parsed >= 0) ? Optional.of(parsed) : Optional.empty();
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  private ServiceManager context;
  private P2PService p2pService;
  // Push model: sync status is fed by a BesuEvents listener registered in start().
  // Until the first callback, cachedSyncStatus is empty and the sync check is skipped
  // (node treated as sync-healthy) to avoid a false-negative at startup.
  private volatile Optional<SyncStatus> cachedSyncStatus = Optional.empty();
  private long syncListenerId = -1;

  @Override
  public void register(final ServiceManager context) {
    this.context = context;

    final HealthCheckService healthCheckService =
        context
            .getService(HealthCheckService.class)
            .orElseThrow(
                () -> new IllegalStateException("Required service missing: HealthCheckService"));

    healthCheckService.registerHealthCheck(READINESS_ENDPOINT, this::checkReadiness);
  }

  @Override
  public void start() {
    this.p2pService =
        context
            .getService(P2PService.class)
            .orElseThrow(() -> new IllegalStateException("Required service missing: P2PService"));
    final BesuEvents besuEvents =
        context
            .getService(BesuEvents.class)
            .orElseThrow(() -> new IllegalStateException("Required service missing: BesuEvents"));

    syncListenerId = besuEvents.addSyncStatusListener(status -> cachedSyncStatus = status);
  }

  private HealthCheckService.HealthCheckResult checkReadiness(
      final HealthCheckService.ParamSource params) {
    if (p2pService == null) {
      return HealthCheckService.HealthCheckResult.of(false);
    }

    final Map<String, Object> checks = new LinkedHashMap<>();
    boolean healthy = true;

    final String minPeersStr = params.getParam("minPeers");
    final Optional<Integer> minPeers = parseNonNegativeInt(minPeersStr, DEFAULT_MIN_PEERS);
    if (minPeers.isEmpty()) {
      healthy = false;
      if (p2pService.isP2pEnabled()) {
        final Map<String, Object> peersDetail = new LinkedHashMap<>();
        peersDetail.put("status", false);
        peersDetail.put("currentPeers", p2pService.getPeerCount());
        peersDetail.put("error", "invalid minPeers parameter: " + minPeersStr);
        checks.put("peers", peersDetail);
      }
    } else if (p2pService.isP2pEnabled()) {
      final int peerCount = p2pService.getPeerCount();
      final boolean peersOk = peerCount >= minPeers.get();
      final Map<String, Object> peersDetail = new LinkedHashMap<>();
      peersDetail.put("status", peersOk);
      peersDetail.put("currentPeers", peerCount);
      peersDetail.put("requiredPeers", minPeers.get());
      checks.put("peers", peersDetail);
      if (!peersOk) {
        healthy = false;
      }
    }
    final String maxBlocksStr = params.getParam("maxBlocksBehind");
    final Optional<Long> maxBlocksBehind =
        parseNonNegativeLong(maxBlocksStr, DEFAULT_MAX_BLOCKS_BEHIND);
    final Optional<SyncStatus> syncStatusOpt = cachedSyncStatus;
    if (syncStatusOpt.isPresent()) {
      final SyncStatus syncStatus = syncStatusOpt.get();
      final long highestBlock = syncStatus.getHighestBlock();
      final long currentBlock = syncStatus.getCurrentBlock();
      final long blocksBehind = highestBlock - currentBlock;
      final Map<String, Object> syncDetail = new LinkedHashMap<>();
      final boolean syncOk;
      if (maxBlocksBehind.isEmpty()) {
        syncOk = false;
        syncDetail.put("error", "invalid maxBlocksBehind parameter: " + maxBlocksStr);
      } else if (currentBlock > Long.MAX_VALUE - maxBlocksBehind.get()) {
        syncOk = true;
      } else {
        syncOk = highestBlock <= currentBlock + maxBlocksBehind.get();
      }
      syncDetail.put("status", syncOk);
      syncDetail.put("blocksBehind", blocksBehind);
      maxBlocksBehind.ifPresent(v -> syncDetail.put("maxBlocksBehind", v));
      checks.put("sync", syncDetail);
      if (!syncOk) {
        healthy = false;
      }
    } else if (maxBlocksBehind.isEmpty()) {
      healthy = false;
    }

    return new HealthCheckService.HealthCheckResult(healthy, checks);
  }

  @Override
  public void stop() {
    if (context != null && syncListenerId != -1) {
      context
          .getService(BesuEvents.class)
          .ifPresent(
              besuEvents -> {
                besuEvents.removeSyncStatusListener(syncListenerId);
                syncListenerId = -1;
              });
    }
  }
}
