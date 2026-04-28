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
package org.hyperledger.besu.tests.acceptance;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.BesuNodeConfigurationBuilder;
import org.hyperledger.besu.tests.acceptance.slowblock.model.ExpectedMetrics;
import org.hyperledger.besu.tests.acceptance.slowblock.model.MetricsTestScenario;
import org.hyperledger.besu.tests.acceptance.slowblock.model.TaggedBlock;
import org.hyperledger.besu.tests.acceptance.slowblock.report.SlowBlockMetricsReportGenerator;
import org.hyperledger.besu.tests.web3j.generated.SimpleStorage;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end acceptance test for validating slow block metrics. This test sends various transaction
 * types to a local Besu node, captures slow block logs, and validates that all JSON fields are
 * correctly populated.
 *
 * <p>The test uses a QBFT node (BFT consensus) with a threshold of 0ms to ensure ALL blocks are
 * logged as slow blocks. QBFT is used instead of dev mode because it automatically produces blocks,
 * which is required for contract deployment and transaction execution.
 *
 * <p><b>Note on EIP-7702:</b> Testing EIP-7702 delegation metrics requires a Prague-enabled genesis
 * and the Engine API for block production. See {@code CodeDelegationTransactionAcceptanceTest} for
 * comprehensive EIP-7702 testing. The EIP-7702 metrics (eip7702_delegations_set/cleared) are
 * validated to be present in the JSON structure but may be 0 in this test as the genesis doesn't
 * enable EIP-7702.
 */
public class SlowBlockMetricsValidationTest extends AcceptanceTestBase {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Pattern to match slow block JSON in console output
  // Matches from the start marker to the final closing braces on the same line,
  // without depending on any specific field order.
  private static final Pattern SLOW_BLOCK_PATTERN =
      Pattern.compile("(\\{\"level\":\"warn\",\"msg\":\"Slow block\"[^\\n]*\\}\\})");

  // All expected JSON field paths (38 fields - matching geth + Besu extras)
  // Now sourced from ExpectedMetrics for consistency
  private static final List<String> REQUIRED_FIELDS = ExpectedMetrics.ALL_METRIC_PATHS;

  // Report output path
  private static final Path REPORT_OUTPUT_PATH =
      Paths.get("build/reports/slow-block-metrics-analysis.md");

  private BesuNode devNode;

  @BeforeEach
  public void setUp() throws Exception {
    // Create a QBFT node with:
    // - BFT consensus that automatically produces blocks
    // - 0ms slow block threshold (log ALL blocks as slow)
    // We use a config modifier to add the environment variable for slow block threshold
    final UnaryOperator<BesuNodeConfigurationBuilder> configModifier =
        builder -> builder.extraCLIOptions(List.of("--slow-block-threshold=0"));

    devNode = besu.createQbftNode("qbft-metrics-node", configModifier);

    // Start console capture BEFORE starting the node
    cluster.startConsoleCapture();
    cluster.start(devNode);

    // Wait for blockchain to progress (QBFT produces blocks automatically)
    cluster.verify(blockchain.reachesHeight(devNode, 1, 30));
  }

  @Test
  public void shouldCaptureSlowBlockMetricsForVariousTransactions() throws Exception {
    // 1. Send ETH transfer
    final Account sender = accounts.getPrimaryBenefactor();
    final Account recipient = accounts.createAccount("recipient");
    devNode.execute(accountTransactions.createTransfer(sender, recipient, 1));

    // 2. Deploy contract
    final SimpleStorage contract =
        devNode.execute(contractTransactions.createSmartContract(SimpleStorage.class));

    // 3. Call contract to write storage (SSTORE)
    contract.set(BigInteger.valueOf(42)).send();

    // 4. Call contract again to read/write (SLOAD + SSTORE)
    contract.set(BigInteger.valueOf(100)).send();

    // 5. Read contract storage (triggers SLOAD without SSTORE)
    contract.get().send();

    // Wait a moment for blocks to be processed
    Thread.sleep(2000);

    // Get console output and parse slow block logs
    String consoleOutput = cluster.getConsoleContents();
    List<JsonNode> slowBlocks = parseSlowBlockLogs(consoleOutput);

    // Assertions
    assertThat(slowBlocks).as("Should capture at least one slow block log").isNotEmpty();

    // Validate fields in the last slow block
    JsonNode lastBlock = slowBlocks.get(slowBlocks.size() - 1);

    // Check all required fields are present
    List<String> missingFields = new ArrayList<>();
    for (String fieldPath : REQUIRED_FIELDS) {
      String jsonPointerPath = "/" + fieldPath.replace("/", "/");
      JsonNode fieldNode = lastBlock.at(jsonPointerPath);
      if (fieldNode.isMissingNode()) {
        missingFields.add(fieldPath);
      }
    }

    assertThat(missingFields)
        .as("All required fields should be present in slow block JSON. Missing: " + missingFields)
        .isEmpty();

    // Tag blocks with their transaction types and generate comprehensive report
    List<TaggedBlock> taggedBlocks = tagBlocksWithMetricsTestScenarios(slowBlocks);
    generateComprehensiveReport(taggedBlocks);

    // Also print legacy console report for quick verification
    printReport(slowBlocks, lastBlock, missingFields);
  }

  /**
   * Tag each captured slow block with its transaction type based on block metrics. Uses heuristics
   * based on EVM opcode counts, state read/write patterns, and transaction count to classify each
   * block.
   *
   * <p>Classification priority (first match wins):
   *
   * <ol>
   *   <li>Genesis block (block number 0)
   *   <li>Empty block (no transactions)
   *   <li>Contract deployment (CREATE opcode or code writes)
   *   <li>Storage write with read (SLOAD + SSTORE with code interaction)
   *   <li>Storage write only (SSTORE with code interaction)
   *   <li>Storage read only (SLOAD without SSTORE)
   *   <li>ETH transfer (transactions without contract code interaction)
   * </ol>
   */
  private List<TaggedBlock> tagBlocksWithMetricsTestScenarios(final List<JsonNode> slowBlocks) {
    List<TaggedBlock> taggedBlocks = new ArrayList<>();

    for (int i = 0; i < slowBlocks.size(); i++) {
      JsonNode block = slowBlocks.get(i);
      long blockNumber = block.at("/block/number").asLong();
      int txCount = block.at("/block/tx_count").asInt();
      long codeWrites = block.at("/state_writes/code").asLong();
      long codeReads = block.at("/state_reads/code").asLong();
      long creates = block.at("/evm/creates").asLong();
      long sstore = block.at("/evm/sstore").asLong();
      long sload = block.at("/evm/sload").asLong();
      long calls = block.at("/evm/calls").asLong();

      // Determine transaction type based on block metrics
      MetricsTestScenario txType;
      if (blockNumber == 0) {
        // Genesis block
        txType = MetricsTestScenario.GENESIS;
      } else if (txCount == 0) {
        // Empty consensus block (no user transactions)
        txType = MetricsTestScenario.EMPTY_BLOCK;
      } else if (creates > 0 || codeWrites > 0) {
        // Contract deployment: CREATE/CREATE2 opcode executed or code written to state
        txType = MetricsTestScenario.CONTRACT_DEPLOY;
      } else if (sload > 0 && sstore > 0 && codeReads > 0) {
        // Storage read-modify-write: contract reads then writes storage
        txType = MetricsTestScenario.STORAGE_WRITE;
      } else if (sstore > 0 && codeReads > 0) {
        // Storage write only: contract writes to storage slot
        txType = MetricsTestScenario.STORAGE_WRITE;
      } else if (sload > 0 && sstore == 0 && codeReads > 0) {
        // Storage read only: contract reads storage without writing
        txType = MetricsTestScenario.STORAGE_READ;
      } else if (calls > 0 && codeReads > 0) {
        // Contract call without storage access
        txType = MetricsTestScenario.CONTRACT_CALL;
      } else if (txCount > 0) {
        // Simple ETH transfer: has transactions but no contract code interaction
        txType = MetricsTestScenario.ETH_TRANSFER;
      } else {
        // Fallback for unidentified patterns
        txType = MetricsTestScenario.EMPTY_BLOCK;
      }

      taggedBlocks.add(new TaggedBlock(block, txType));
    }

    return taggedBlocks;
  }

  /** Generate a comprehensive markdown report with expected vs actual analysis for all metrics. */
  private void generateComprehensiveReport(final List<TaggedBlock> taggedBlocks)
      throws IOException {
    SlowBlockMetricsReportGenerator generator =
        new SlowBlockMetricsReportGenerator(taggedBlocks, "QBFT (BFT Consensus)");

    // Generate and write the report
    generator.generateReport(REPORT_OUTPUT_PATH);

    // Also print to console for immediate visibility in test output
    System.out.println("\n" + generator.generateReportString());
  }

  private List<JsonNode> parseSlowBlockLogs(final String consoleOutput) {
    List<JsonNode> allBlocks = new ArrayList<>();
    Matcher matcher = SLOW_BLOCK_PATTERN.matcher(consoleOutput);

    while (matcher.find()) {
      try {
        String json = matcher.group();
        JsonNode node = OBJECT_MAPPER.readTree(json);
        allBlocks.add(node);
      } catch (Exception e) {
        // Skip malformed JSON
      }
    }

    // Deduplicate by block hash to avoid duplicate entries
    // (blocks may be logged multiple times during validation/import)
    Set<String> seenHashes = new LinkedHashSet<>();
    return allBlocks.stream()
        .filter(block -> seenHashes.add(block.at("/block/hash").asText()))
        .collect(Collectors.toList());
  }

  private void printReport(
      final List<JsonNode> slowBlocks, final JsonNode lastBlock, final List<String> missingFields) {
    StringBuilder report = new StringBuilder();

    report.append("\n");
    report.append("═══════════════════════════════════════════════════════════════\n");
    report.append("           SLOW BLOCK METRICS VALIDATION REPORT\n");
    report.append("═══════════════════════════════════════════════════════════════\n");
    report.append("\n");

    // Summary
    report.append("SUMMARY\n");
    report.append("-------\n");
    report.append(String.format("Blocks Processed: %d%n", slowBlocks.size()));
    report.append(String.format("Missing Fields: %d%n", missingFields.size()));
    if (!missingFields.isEmpty()) {
      report.append(String.format("  -> %s%n", missingFields));
    }
    report.append("\n");

    // Sample metrics from last block
    if (lastBlock != null) {
      report.append("SAMPLE METRICS (last block)\n");
      report.append("---------------------------\n");
      report.append(String.format("Block number: %s%n", lastBlock.at("/block/number").asText()));
      report.append(String.format("Gas used: %s%n", lastBlock.at("/block/gas_used").asText()));
      report.append(String.format("Tx count: %s%n", lastBlock.at("/block/tx_count").asText()));
      report.append(
          String.format("Execution time: %s ms%n", lastBlock.at("/timing/execution_ms").asText()));
      report.append(
          String.format("Account reads: %s%n", lastBlock.at("/state_reads/accounts").asText()));
      report.append(
          String.format("Account writes: %s%n", lastBlock.at("/state_writes/accounts").asText()));
      report.append(
          String.format(
              "Storage reads: %s%n", lastBlock.at("/state_reads/storage_slots").asText()));
      report.append(
          String.format(
              "Storage writes: %s%n", lastBlock.at("/state_writes/storage_slots").asText()));
      report.append(String.format("Code reads: %s%n", lastBlock.at("/state_reads/code").asText()));
      report.append(
          String.format("Code writes: %s%n", lastBlock.at("/state_writes/code").asText()));
      report.append(String.format("SLOAD: %s%n", lastBlock.at("/evm/sload").asText()));
      report.append(String.format("SSTORE: %s%n", lastBlock.at("/evm/sstore").asText()));
      report.append(String.format("CALLS: %s%n", lastBlock.at("/evm/calls").asText()));
      report.append(String.format("CREATES: %s%n", lastBlock.at("/evm/creates").asText()));
    }

    report.append("\n");
    report.append("═══════════════════════════════════════════════════════════════\n");

    System.out.println(report);
  }
}
