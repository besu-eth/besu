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
package org.hyperledger.besu.ethereum.mainnet.slowblock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Renders a completed {@link SlowBlockMetrics} as the cross-client slow-block JSON document and
 * emits it on a dedicated "SlowBlock" logger, so operators can route slow-block output to its own
 * sink without touching the rest of Besu's logging.
 *
 * <p>The {@code bal} section is a Besu extension that reports the work BAL block processing does
 * off the import thread; it is not part of the cross-client specification.
 */
public final class SlowBlockJsonLogger {

  private static final Logger SLOW_BLOCK_LOG = LoggerFactory.getLogger("SlowBlock");
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private SlowBlockJsonLogger() {}

  /**
   * Emits the slow-block document at WARN.
   *
   * @param metrics the completed per-block metrics
   */
  public static void log(final SlowBlockMetrics metrics) {
    try {
      SLOW_BLOCK_LOG.warn(toJson(metrics));
    } catch (final JsonProcessingException e) {
      SLOW_BLOCK_LOG
          .atWarn()
          .setMessage("Slow block number={} hash={} total={}ms gas={} mgas/s={} txs={}")
          .addArgument(metrics.blockNumber())
          .addArgument(metrics.blockHash())
          .addArgument(millis(metrics.totalNanos()))
          .addArgument(metrics.gasUsed())
          .addArgument(round(metrics.mgasPerSecond(), 2))
          .addArgument(metrics.txCount())
          .log();
    }
  }

  static String toJson(final SlowBlockMetrics metrics) throws JsonProcessingException {
    final ObjectNode json = JSON_MAPPER.createObjectNode();
    json.put("level", "warn");
    json.put("msg", "Slow block");

    final ObjectNode block = json.putObject("block");
    block.put("number", metrics.blockNumber());
    block.put("hash", metrics.blockHash());
    block.put("gas_used", metrics.gasUsed());
    block.put("tx_count", metrics.txCount());

    final ObjectNode timing = json.putObject("timing");
    timing.put("execution_ms", millis(metrics.executionNanos()));
    timing.put("state_read_ms", millis(metrics.stateReadNanos()));
    timing.put("state_hash_ms", millis(metrics.stateHashWaitNanos()));
    timing.put("commit_ms", millis(metrics.commitNanos()));
    timing.put("total_ms", millis(metrics.totalNanos()));

    json.putObject("throughput").put("mgas_per_sec", round(metrics.mgasPerSecond(), 2));

    final ObjectNode stateReads = json.putObject("state_reads");
    stateReads.put("accounts", metrics.readAccounts());
    stateReads.put("storage_slots", metrics.readStorageSlots());
    stateReads.put("code", metrics.readCode());
    stateReads.put("code_bytes", metrics.readCodeBytes());

    final ObjectNode stateWrites = json.putObject("state_writes");
    stateWrites.put("accounts", metrics.writeAccounts());
    stateWrites.put("storage_slots", metrics.writeStorageSlots());
    stateWrites.put("code", metrics.writeCode());
    stateWrites.put("code_bytes", metrics.writeCodeBytes());
    stateWrites.put("accounts_deleted", metrics.accountsDeleted());
    stateWrites.put("storage_slots_deleted", metrics.storageSlotsDeleted());

    final ObjectNode cache = json.putObject("cache");
    putCache(cache.putObject("account"), metrics.hitAccounts(), metrics.missAccounts());
    putCache(cache.putObject("storage"), metrics.hitStorageSlots(), metrics.missStorageSlots());
    putCache(cache.putObject("code"), metrics.hitCode(), metrics.missCode());

    final ObjectNode unique = json.putObject("unique");
    unique.put("accounts", metrics.uniqueAccounts());
    unique.put("storage_slots", metrics.uniqueStorageSlots());
    unique.put("contracts", metrics.uniqueContracts());

    final ObjectNode evm = json.putObject("evm");
    evm.put("sload", metrics.sload());
    evm.put("sstore", metrics.sstore());
    evm.put("calls", metrics.calls());
    evm.put("creates", metrics.creates());

    final ObjectNode bal = json.putObject("bal");
    bal.put("state_hash_background_ms", millis(metrics.stateHashBackgroundNanos()));
    bal.put("tx_exec_background_ms", millis(metrics.txExecBackgroundNanos()));
    bal.put("tx_result_wait_ms", millis(metrics.txResultWaitNanos()));
    bal.put("replayed_txs", metrics.replayedTxs());
    final ObjectNode prefetch = bal.putObject("prefetch");
    prefetch.put("duration_ms", millis(metrics.prefetchNanos()));
    prefetch.put("accounts", metrics.prefetchAccounts());
    prefetch.put("storage_slots", metrics.prefetchStorageSlots());

    return JSON_MAPPER.writeValueAsString(json);
  }

  private static void putCache(final ObjectNode node, final long hits, final long misses) {
    node.put("hits", hits);
    node.put("misses", misses);
    node.put("hit_rate", hitRate(hits, misses));
  }

  private static double hitRate(final long hits, final long misses) {
    final long total = hits + misses;
    return total > 0L ? round((hits * 100.0) / total, 2) : 0.0;
  }

  private static double millis(final long nanos) {
    return round(nanos / 1_000_000.0, 3);
  }

  private static double round(final double value, final int places) {
    final double scale = Math.pow(10, places);
    return Math.round(value * scale) / scale;
  }
}
