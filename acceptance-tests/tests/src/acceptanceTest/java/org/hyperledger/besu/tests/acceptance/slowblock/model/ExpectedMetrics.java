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
package org.hyperledger.besu.tests.acceptance.slowblock.model;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

/**
 * Defines expected metric values for each test scenario. Used to validate that slow block metrics
 * are correctly populated for different operation types.
 *
 * <p>Each expectation can be:
 *
 * <ul>
 *   <li>"*" - any value is acceptable (field just needs to exist)
 *   <li>">= N" - value should be greater than or equal to N
 *   <li>"= N" - value should equal N exactly
 *   <li>"0-100" - value should be in range (for percentages)
 * </ul>
 */
public class ExpectedMetrics {

  // All 38 metric field paths
  public static final List<String> ALL_METRIC_PATHS =
      ImmutableList.of(
          // Top level
          "level",
          "msg",
          // Block
          "block/number",
          "block/hash",
          "block/gas_used",
          "block/tx_count",
          // Timing
          "timing/execution_ms",
          "timing/state_read_ms",
          "timing/state_hash_ms",
          "timing/commit_ms",
          "timing/total_ms",
          // Throughput
          "throughput/mgas_per_sec",
          // State reads
          "state_reads/accounts",
          "state_reads/storage_slots",
          "state_reads/code",
          "state_reads/code_bytes",
          // State writes
          "state_writes/accounts",
          "state_writes/storage_slots",
          "state_writes/code",
          "state_writes/code_bytes",
          "state_writes/eip7702_delegations_set",
          "state_writes/eip7702_delegations_cleared",
          // Cache
          "cache/account/hits",
          "cache/account/misses",
          "cache/account/hit_rate",
          "cache/storage/hits",
          "cache/storage/misses",
          "cache/storage/hit_rate",
          "cache/code/hits",
          "cache/code/misses",
          "cache/code/hit_rate",
          // Unique (Besu extra)
          "unique/accounts",
          "unique/storage_slots",
          "unique/contracts",
          // EVM (Besu extra)
          "evm/sload",
          "evm/sstore",
          "evm/calls",
          "evm/creates");

  // Category labels for report display
  public static final Map<String, String> METRIC_CATEGORIES = new HashMap<>();

  static {
    METRIC_CATEGORIES.put("level", "Top Level");
    METRIC_CATEGORIES.put("msg", "Top Level");
    METRIC_CATEGORIES.put("block/number", "Block");
    METRIC_CATEGORIES.put("block/hash", "Block");
    METRIC_CATEGORIES.put("block/gas_used", "Block");
    METRIC_CATEGORIES.put("block/tx_count", "Block");
    METRIC_CATEGORIES.put("timing/execution_ms", "Timing");
    METRIC_CATEGORIES.put("timing/state_read_ms", "Timing");
    METRIC_CATEGORIES.put("timing/state_hash_ms", "Timing");
    METRIC_CATEGORIES.put("timing/commit_ms", "Timing");
    METRIC_CATEGORIES.put("timing/total_ms", "Timing");
    METRIC_CATEGORIES.put("throughput/mgas_per_sec", "Throughput");
    METRIC_CATEGORIES.put("state_reads/accounts", "State Reads");
    METRIC_CATEGORIES.put("state_reads/storage_slots", "State Reads");
    METRIC_CATEGORIES.put("state_reads/code", "State Reads");
    METRIC_CATEGORIES.put("state_reads/code_bytes", "State Reads");
    METRIC_CATEGORIES.put("state_writes/accounts", "State Writes");
    METRIC_CATEGORIES.put("state_writes/storage_slots", "State Writes");
    METRIC_CATEGORIES.put("state_writes/code", "State Writes");
    METRIC_CATEGORIES.put("state_writes/code_bytes", "State Writes");
    METRIC_CATEGORIES.put("state_writes/eip7702_delegations_set", "State Writes");
    METRIC_CATEGORIES.put("state_writes/eip7702_delegations_cleared", "State Writes");
    METRIC_CATEGORIES.put("cache/account/hits", "Cache");
    METRIC_CATEGORIES.put("cache/account/misses", "Cache");
    METRIC_CATEGORIES.put("cache/account/hit_rate", "Cache");
    METRIC_CATEGORIES.put("cache/storage/hits", "Cache");
    METRIC_CATEGORIES.put("cache/storage/misses", "Cache");
    METRIC_CATEGORIES.put("cache/storage/hit_rate", "Cache");
    METRIC_CATEGORIES.put("cache/code/hits", "Cache");
    METRIC_CATEGORIES.put("cache/code/misses", "Cache");
    METRIC_CATEGORIES.put("cache/code/hit_rate", "Cache");
    METRIC_CATEGORIES.put("unique/accounts", "Unique");
    METRIC_CATEGORIES.put("unique/storage_slots", "Unique");
    METRIC_CATEGORIES.put("unique/contracts", "Unique");
    METRIC_CATEGORIES.put("evm/sload", "EVM");
    METRIC_CATEGORIES.put("evm/sstore", "EVM");
    METRIC_CATEGORIES.put("evm/calls", "EVM");
    METRIC_CATEGORIES.put("evm/creates", "EVM");
  }

  private static final EnumMap<MetricsTestScenario, Map<String, String>> EXPECTATIONS =
      new EnumMap<>(MetricsTestScenario.class);

  static {
    // Initialize base expectations that apply to all test scenarios
    Map<String, String> baseExpectations = createBaseExpectations();

    // Genesis block - minimal activity
    Map<String, String> genesis = new HashMap<>(baseExpectations);
    genesis.put("block/number", "= 0");
    genesis.put("block/tx_count", "= 0");
    genesis.put("block/gas_used", "= 0");
    EXPECTATIONS.put(MetricsTestScenario.GENESIS, genesis);

    // Empty block - consensus only, no user transactions
    Map<String, String> emptyBlock = new HashMap<>(baseExpectations);
    emptyBlock.put("block/tx_count", "= 0");
    emptyBlock.put("block/gas_used", "= 0");
    EXPECTATIONS.put(MetricsTestScenario.EMPTY_BLOCK, emptyBlock);

    // ETH Transfer - account reads/writes only
    Map<String, String> ethTransfer = new HashMap<>(baseExpectations);
    ethTransfer.put("block/tx_count", ">= 1");
    ethTransfer.put("block/gas_used", ">= 21000");
    ethTransfer.put("state_reads/accounts", ">= 2"); // sender + recipient
    ethTransfer.put("state_writes/accounts", ">= 2"); // balance changes
    ethTransfer.put("unique/accounts", ">= 2");
    // No storage/code/EVM activity for simple transfer
    ethTransfer.put("state_reads/storage_slots", "= 0");
    ethTransfer.put("state_reads/code", "= 0");
    ethTransfer.put("state_writes/storage_slots", "= 0");
    ethTransfer.put("state_writes/code", "= 0");
    ethTransfer.put("evm/sload", "= 0");
    ethTransfer.put("evm/sstore", "= 0");
    ethTransfer.put("evm/calls", "= 0");
    ethTransfer.put("evm/creates", "= 0");
    EXPECTATIONS.put(MetricsTestScenario.ETH_TRANSFER, ethTransfer);

    // Contract Deploy - code write + create
    Map<String, String> contractDeploy = new HashMap<>(baseExpectations);
    contractDeploy.put("block/tx_count", ">= 1");
    contractDeploy.put("block/gas_used", ">= 21000");
    contractDeploy.put("state_reads/accounts", ">= 1"); // sender
    contractDeploy.put("state_writes/accounts", ">= 2"); // sender + new contract
    contractDeploy.put("state_writes/code", ">= 1");
    contractDeploy.put("state_writes/code_bytes", ">= 1");
    contractDeploy.put("evm/creates", ">= 1");
    contractDeploy.put("unique/contracts", ">= 1");
    EXPECTATIONS.put(MetricsTestScenario.CONTRACT_DEPLOY, contractDeploy);

    // Storage Write (SSTORE) - storage slot write
    Map<String, String> storageWrite = new HashMap<>(baseExpectations);
    storageWrite.put("block/tx_count", ">= 1");
    storageWrite.put("block/gas_used", ">= 21000");
    storageWrite.put("state_reads/accounts", ">= 2"); // sender + contract
    storageWrite.put("state_reads/code", ">= 1"); // contract code
    storageWrite.put("state_writes/storage_slots", ">= 1");
    storageWrite.put("evm/sstore", ">= 1");
    storageWrite.put("unique/storage_slots", ">= 1");
    EXPECTATIONS.put(MetricsTestScenario.STORAGE_WRITE, storageWrite);

    // Storage Read (SLOAD) - storage slot read
    Map<String, String> storageRead = new HashMap<>(baseExpectations);
    storageRead.put("block/tx_count", ">= 1");
    storageRead.put("block/gas_used", ">= 21000");
    storageRead.put("state_reads/accounts", ">= 2");
    storageRead.put("state_reads/code", ">= 1");
    storageRead.put("state_reads/storage_slots", ">= 1");
    storageRead.put("evm/sload", ">= 1");
    EXPECTATIONS.put(MetricsTestScenario.STORAGE_READ, storageRead);

    // Contract Call (CALL opcode)
    Map<String, String> contractCall = new HashMap<>(baseExpectations);
    contractCall.put("block/tx_count", ">= 1");
    contractCall.put("block/gas_used", ">= 21000");
    contractCall.put("state_reads/accounts", ">= 3"); // sender + caller + callee
    contractCall.put("state_reads/code", ">= 2"); // caller + callee code
    contractCall.put("evm/calls", ">= 1");
    EXPECTATIONS.put(MetricsTestScenario.CONTRACT_CALL, contractCall);

    // Code Read (EXTCODESIZE)
    Map<String, String> codeRead = new HashMap<>(baseExpectations);
    codeRead.put("block/tx_count", ">= 1");
    codeRead.put("block/gas_used", ">= 21000");
    codeRead.put("state_reads/accounts", ">= 2");
    codeRead.put("state_reads/code", ">= 1");
    EXPECTATIONS.put(MetricsTestScenario.CODE_READ, codeRead);

    // EIP-7702 Delegation Set
    Map<String, String> eip7702Set = new HashMap<>(baseExpectations);
    eip7702Set.put("block/tx_count", ">= 1");
    eip7702Set.put("state_writes/eip7702_delegations_set", ">= 1");
    EXPECTATIONS.put(MetricsTestScenario.EIP7702_DELEGATION_SET, eip7702Set);

    // EIP-7702 Delegation Clear
    Map<String, String> eip7702Clear = new HashMap<>(baseExpectations);
    eip7702Clear.put("block/tx_count", ">= 1");
    eip7702Clear.put("state_writes/eip7702_delegations_cleared", ">= 1");
    EXPECTATIONS.put(MetricsTestScenario.EIP7702_DELEGATION_CLEAR, eip7702Clear);
  }

  private static Map<String, String> createBaseExpectations() {
    Map<String, String> base = new HashMap<>();

    // Top level - always present
    base.put("level", "= warn");
    base.put("msg", "= Slow block");

    // Block - always present, values vary
    base.put("block/number", ">= 0");
    base.put("block/hash", "*"); // non-empty
    base.put("block/gas_used", ">= 0");
    base.put("block/tx_count", ">= 0");

    // Timing - always present, values vary
    base.put("timing/execution_ms", ">= 0");
    base.put("timing/state_read_ms", ">= 0");
    base.put("timing/state_hash_ms", ">= 0");
    base.put("timing/commit_ms", ">= 0");
    base.put("timing/total_ms", ">= 0");

    // Throughput
    base.put("throughput/mgas_per_sec", ">= 0");

    // State reads - default to >= 0
    base.put("state_reads/accounts", ">= 0");
    base.put("state_reads/storage_slots", ">= 0");
    base.put("state_reads/code", ">= 0");
    base.put("state_reads/code_bytes", ">= 0");

    // State writes - default to >= 0
    base.put("state_writes/accounts", ">= 0");
    base.put("state_writes/storage_slots", ">= 0");
    base.put("state_writes/code", ">= 0");
    base.put("state_writes/code_bytes", ">= 0");
    base.put("state_writes/eip7702_delegations_set", ">= 0");
    base.put("state_writes/eip7702_delegations_cleared", ">= 0");

    // Cache - always present
    base.put("cache/account/hits", ">= 0");
    base.put("cache/account/misses", ">= 0");
    base.put("cache/account/hit_rate", "0-100");
    base.put("cache/storage/hits", ">= 0");
    base.put("cache/storage/misses", ">= 0");
    base.put("cache/storage/hit_rate", "0-100");
    base.put("cache/code/hits", ">= 0");
    base.put("cache/code/misses", ">= 0");
    base.put("cache/code/hit_rate", "0-100");

    // Unique
    base.put("unique/accounts", ">= 0");
    base.put("unique/storage_slots", ">= 0");
    base.put("unique/contracts", ">= 0");

    // EVM
    base.put("evm/sload", ">= 0");
    base.put("evm/sstore", ">= 0");
    base.put("evm/calls", ">= 0");
    base.put("evm/creates", ">= 0");

    return base;
  }

  /**
   * Get the expected metric values for a given test scenario.
   *
   * @param type the test scenario
   * @return map of metric path to expected value expression
   */
  public static Map<String, String> getExpectations(final MetricsTestScenario type) {
    return EXPECTATIONS.getOrDefault(type, createBaseExpectations());
  }

  /**
   * Get the category for a metric path.
   *
   * @param metricPath the metric path (e.g., "state_reads/accounts")
   * @return the category name (e.g., "State Reads")
   */
  public static String getCategory(final String metricPath) {
    return METRIC_CATEGORIES.getOrDefault(metricPath, "Unknown");
  }

  /**
   * Get a simple metric name from the full path.
   *
   * @param metricPath the full metric path (e.g., "state_reads/accounts")
   * @return the simple name (e.g., "accounts")
   */
  public static String getSimpleName(final String metricPath) {
    int lastSlash = metricPath.lastIndexOf('/');
    return lastSlash >= 0 ? metricPath.substring(lastSlash + 1) : metricPath;
  }
}
