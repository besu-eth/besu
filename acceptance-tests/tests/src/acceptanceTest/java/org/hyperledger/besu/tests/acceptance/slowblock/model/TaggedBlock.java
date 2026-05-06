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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Splitter;

/**
 * Wraps a slow block JSON with its test scenario and validation results. This class is used to
 * associate each captured slow block log with its expected metrics and track validation outcomes.
 */
public class TaggedBlock {

  private final JsonNode jsonNode;
  private final MetricsTestScenario scenario;
  private final Map<String, ValidationResult> validationResults;

  /**
   * Creates a new TaggedBlock.
   *
   * @param jsonNode the parsed slow block JSON
   * @param scenario the test scenario for this block
   */
  public TaggedBlock(final JsonNode jsonNode, final MetricsTestScenario scenario) {
    this.jsonNode = jsonNode;
    this.scenario = scenario;
    this.validationResults = new HashMap<>();
  }

  /** Result of validating a single metric. */
  public static class ValidationResult {
    private final String metricPath;
    private final String expected;
    private final String actual;
    private final boolean passed;
    private final String reason;

    public ValidationResult(
        final String metricPath,
        final String expected,
        final String actual,
        final boolean passed,
        final String reason) {
      this.metricPath = metricPath;
      this.expected = expected;
      this.actual = actual;
      this.passed = passed;
      this.reason = reason;
    }

    public String getMetricPath() {
      return metricPath;
    }

    public String getExpected() {
      return expected;
    }

    public String getActual() {
      return actual;
    }

    public boolean isPassed() {
      return passed;
    }

    public String getReason() {
      return reason;
    }

    public String getStatus() {
      return passed ? "PASS" : "FAIL";
    }
  }

  /**
   * Validate all metrics against expected values for this block's test scenario.
   *
   * @return true if all validations passed
   */
  public boolean validate() {
    validationResults.clear();
    Map<String, String> expectations = ExpectedMetrics.getExpectations(scenario);

    boolean allPassed = true;
    for (String metricPath : ExpectedMetrics.ALL_METRIC_PATHS) {
      String jsonPointer = "/" + metricPath.replace("/", "/");
      JsonNode valueNode = jsonNode.at(jsonPointer);

      String expected = expectations.getOrDefault(metricPath, ">= 0");
      String actual;
      boolean passed;
      String reason = "";

      if (valueNode.isMissingNode()) {
        actual = "MISSING";
        passed = false;
        reason = "Field not found in JSON";
      } else if (valueNode.isTextual()) {
        actual = valueNode.asText();
        passed = validateStringValue(actual, expected);
        if (!passed) {
          reason = "Value mismatch";
        }
      } else if (valueNode.isNumber()) {
        actual = valueNode.asText();
        passed = validateNumericValue(valueNode.asDouble(), expected);
        if (!passed) {
          reason = "Value out of expected range";
        }
      } else {
        actual = valueNode.toString();
        passed = true; // Accept any non-missing value for complex types
      }

      validationResults.put(
          metricPath, new ValidationResult(metricPath, expected, actual, passed, reason));
      if (!passed) {
        allPassed = false;
      }
    }

    return allPassed;
  }

  private boolean validateStringValue(final String actual, final String expected) {
    if ("*".equals(expected)) {
      return actual != null && !actual.isEmpty();
    }
    if (expected.startsWith("= ")) {
      return actual.equals(expected.substring(2));
    }
    return true; // Accept any value if no specific expectation
  }

  private boolean validateNumericValue(final double actual, final String expected) {
    if ("*".equals(expected)) {
      return true;
    }
    if (expected.startsWith(">= ")) {
      double threshold = Double.parseDouble(expected.substring(3));
      return actual >= threshold;
    }
    if (expected.startsWith("= ")) {
      double exactValue = Double.parseDouble(expected.substring(2));
      return Math.abs(actual - exactValue) < 0.001;
    }
    if (expected.contains("-")) {
      List<String> parts = Splitter.on('-').splitToList(expected);
      double min = Double.parseDouble(parts.get(0));
      double max = Double.parseDouble(parts.get(1));
      return actual >= min && actual <= max;
    }
    return true;
  }

  public JsonNode getJsonNode() {
    return jsonNode;
  }

  public MetricsTestScenario getMetricsTestScenario() {
    return scenario;
  }

  public Map<String, ValidationResult> getValidationResults() {
    return validationResults;
  }

  /**
   * Get the block number from the JSON.
   *
   * @return block number or -1 if not found
   */
  public long getBlockNumber() {
    JsonNode node = jsonNode.at("/block/number");
    return node.isMissingNode() ? -1 : node.asLong();
  }

  /**
   * Get the block hash from the JSON.
   *
   * @return block hash or empty string if not found
   */
  public String getBlockHash() {
    JsonNode node = jsonNode.at("/block/hash");
    return node.isMissingNode() ? "" : node.asText();
  }

  /**
   * Get gas used from the JSON.
   *
   * @return gas used or 0 if not found
   */
  public long getGasUsed() {
    JsonNode node = jsonNode.at("/block/gas_used");
    return node.isMissingNode() ? 0 : node.asLong();
  }

  /**
   * Get transaction count from the JSON.
   *
   * @return tx count or 0 if not found
   */
  public int getTxCount() {
    JsonNode node = jsonNode.at("/block/tx_count");
    return node.isMissingNode() ? 0 : node.asInt();
  }

  /**
   * Get a metric value as a number.
   *
   * @param metricPath the metric path
   * @return the value or 0 if not found
   */
  public long getMetricValue(final String metricPath) {
    JsonNode node = jsonNode.at("/" + metricPath);
    return node.isMissingNode() ? 0 : node.asLong();
  }

  /**
   * Get a metric value as a double.
   *
   * @param metricPath the metric path
   * @return the value or 0.0 if not found
   */
  public double getMetricValueAsDouble(final String metricPath) {
    JsonNode node = jsonNode.at("/" + metricPath);
    return node.isMissingNode() ? 0.0 : node.asDouble();
  }

  /**
   * Check if all validations passed.
   *
   * @return true if all validations passed
   */
  public boolean allPassed() {
    return validationResults.values().stream().allMatch(ValidationResult::isPassed);
  }

  /**
   * Count how many validations failed.
   *
   * @return count of failed validations
   */
  public long failedCount() {
    return validationResults.values().stream().filter(r -> !r.isPassed()).count();
  }

  /**
   * Convert the block to a human-readable key=value format (Reth style). This format is easier to
   * read than nested JSON and matches the output format used by other Ethereum clients.
   *
   * <p>Example output:
   *
   * <pre>
   * level=warn msg="Slow block" block_number=18 block_hash=0x... gas_used=21000 tx_count=1
   * timing_execution_ms=0.123 timing_state_read_ms=0.0 timing_total_ms=0.123
   * throughput_mgas_per_sec=170.73
   * state_reads_accounts=5 state_reads_storage=1 state_reads_code=0
   * state_writes_accounts=2 state_writes_storage=0 state_writes_code=0
   * cache_account_hits=2 cache_account_misses=3 cache_account_hit_rate=40.0
   * evm_sload=0 evm_sstore=1 evm_calls=0 evm_creates=0
   * </pre>
   *
   * @return the block in key=value format
   */
  public String toKeyValueFormat() {
    StringBuilder sb = new StringBuilder();

    // Line 1: Top-level fields and block info
    sb.append("level=warn msg=\"Slow block\" ");
    sb.append("block_number=").append(getBlockNumber()).append(" ");
    sb.append("block_hash=").append(truncateHash(getBlockHash())).append(" ");
    sb.append("gas_used=").append(getGasUsed()).append(" ");
    sb.append("tx_count=").append(getTxCount()).append("\n");

    // Line 2: Timing metrics
    sb.append("timing_execution_ms=")
        .append(formatDouble(getMetricValueAsDouble("timing/execution_ms")))
        .append(" ");
    sb.append("timing_state_read_ms=")
        .append(formatDouble(getMetricValueAsDouble("timing/state_read_ms")))
        .append(" ");
    sb.append("timing_state_hash_ms=")
        .append(formatDouble(getMetricValueAsDouble("timing/state_hash_ms")))
        .append(" ");
    sb.append("timing_commit_ms=")
        .append(formatDouble(getMetricValueAsDouble("timing/commit_ms")))
        .append(" ");
    sb.append("timing_total_ms=")
        .append(formatDouble(getMetricValueAsDouble("timing/total_ms")))
        .append("\n");

    // Line 3: Throughput
    sb.append("throughput_mgas_per_sec=")
        .append(formatDouble(getMetricValueAsDouble("throughput/mgas_per_sec")))
        .append("\n");

    // Line 4: State reads
    sb.append("state_reads_accounts=").append(getMetricValue("state_reads/accounts")).append(" ");
    sb.append("state_reads_storage=")
        .append(getMetricValue("state_reads/storage_slots"))
        .append(" ");
    sb.append("state_reads_code=").append(getMetricValue("state_reads/code")).append(" ");
    sb.append("state_reads_code_bytes=")
        .append(getMetricValue("state_reads/code_bytes"))
        .append("\n");

    // Line 5: State writes
    sb.append("state_writes_accounts=").append(getMetricValue("state_writes/accounts")).append(" ");
    sb.append("state_writes_storage=")
        .append(getMetricValue("state_writes/storage_slots"))
        .append(" ");
    sb.append("state_writes_code=").append(getMetricValue("state_writes/code")).append(" ");
    sb.append("state_writes_code_bytes=")
        .append(getMetricValue("state_writes/code_bytes"))
        .append("\n");

    // Line 6: EIP-7702 delegations
    sb.append("state_writes_eip7702_set=")
        .append(getMetricValue("state_writes/eip7702_delegations_set"))
        .append(" ");
    sb.append("state_writes_eip7702_cleared=")
        .append(getMetricValue("state_writes/eip7702_delegations_cleared"))
        .append("\n");

    // Line 7: Cache stats - accounts
    sb.append("cache_account_hits=").append(getMetricValue("cache/account/hits")).append(" ");
    sb.append("cache_account_misses=").append(getMetricValue("cache/account/misses")).append(" ");
    sb.append("cache_account_hit_rate=")
        .append(formatDouble(getMetricValueAsDouble("cache/account/hit_rate")))
        .append("\n");

    // Line 8: Cache stats - storage
    sb.append("cache_storage_hits=").append(getMetricValue("cache/storage/hits")).append(" ");
    sb.append("cache_storage_misses=").append(getMetricValue("cache/storage/misses")).append(" ");
    sb.append("cache_storage_hit_rate=")
        .append(formatDouble(getMetricValueAsDouble("cache/storage/hit_rate")))
        .append("\n");

    // Line 9: Cache stats - code
    sb.append("cache_code_hits=").append(getMetricValue("cache/code/hits")).append(" ");
    sb.append("cache_code_misses=").append(getMetricValue("cache/code/misses")).append(" ");
    sb.append("cache_code_hit_rate=")
        .append(formatDouble(getMetricValueAsDouble("cache/code/hit_rate")))
        .append("\n");

    // Line 10: Unique counts
    sb.append("unique_accounts=").append(getMetricValue("unique/accounts")).append(" ");
    sb.append("unique_storage_slots=").append(getMetricValue("unique/storage_slots")).append(" ");
    sb.append("unique_contracts=").append(getMetricValue("unique/contracts")).append("\n");

    // Line 11: EVM opcodes
    sb.append("evm_sload=").append(getMetricValue("evm/sload")).append(" ");
    sb.append("evm_sstore=").append(getMetricValue("evm/sstore")).append(" ");
    sb.append("evm_calls=").append(getMetricValue("evm/calls")).append(" ");
    sb.append("evm_creates=").append(getMetricValue("evm/creates"));

    return sb.toString();
  }

  /**
   * Truncate block hash for readability (first 10 + last 6 chars).
   *
   * @param hash the full hash
   * @return truncated hash like "0x5920486224...024210"
   */
  private String truncateHash(final String hash) {
    if (hash == null || hash.length() < 20) {
      return hash;
    }
    return hash.substring(0, 12) + "..." + hash.substring(hash.length() - 6);
  }

  /**
   * Format double values consistently with appropriate precision.
   *
   * @param value the value to format
   * @return formatted string
   */
  private String formatDouble(final double value) {
    if (value == 0.0) {
      return "0.0";
    } else if (value < 0.001) {
      return String.format("%.6f", value);
    } else if (value < 1.0) {
      return String.format("%.4f", value);
    } else {
      return String.format("%.2f", value);
    }
  }

  /**
   * Get a compact summary of key metrics for this block.
   *
   * @return compact summary string
   */
  public String getKeyMetricsSummary() {
    StringBuilder sb = new StringBuilder();
    long accountReads = getMetricValue("state_reads/accounts");
    long storageReads = getMetricValue("state_reads/storage_slots");
    long codeReads = getMetricValue("state_reads/code");
    long accountWrites = getMetricValue("state_writes/accounts");
    long storageWrites = getMetricValue("state_writes/storage_slots");
    long codeWrites = getMetricValue("state_writes/code");
    long sload = getMetricValue("evm/sload");
    long sstore = getMetricValue("evm/sstore");
    long calls = getMetricValue("evm/calls");
    long creates = getMetricValue("evm/creates");

    if (accountReads > 0 || accountWrites > 0) {
      sb.append("accounts_r/w=").append(accountReads).append("/").append(accountWrites);
    }
    if (storageReads > 0 || storageWrites > 0) {
      if (sb.length() > 0) sb.append(", ");
      sb.append("storage_r/w=").append(storageReads).append("/").append(storageWrites);
    }
    if (codeReads > 0 || codeWrites > 0) {
      if (sb.length() > 0) sb.append(", ");
      sb.append("code_r/w=").append(codeReads).append("/").append(codeWrites);
    }
    if (sload > 0) {
      if (sb.length() > 0) sb.append(", ");
      sb.append("sload=").append(sload);
    }
    if (sstore > 0) {
      if (sb.length() > 0) sb.append(", ");
      sb.append("sstore=").append(sstore);
    }
    if (calls > 0) {
      if (sb.length() > 0) sb.append(", ");
      sb.append("calls=").append(calls);
    }
    if (creates > 0) {
      if (sb.length() > 0) sb.append(", ");
      sb.append("creates=").append(creates);
    }

    return sb.length() > 0 ? sb.toString() : "(no significant metrics)";
  }
}
