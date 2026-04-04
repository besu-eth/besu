/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.evmtool;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hyperledger.besu.evmtool.EngineTestSubCommand.COMMAND_NAME;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.PostMergeContext;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineNewPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineNewPayloadV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineNewPayloadV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineNewPayloadV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineNewPayloadV5;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineForkchoiceUpdatedV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineForkchoiceUpdatedV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineForkchoiceUpdatedV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineForkchoiceUpdatedV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineCallListener;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EngineForkchoiceUpdatedParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineUpdateForkchoiceResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EnginePayloadStatusResult;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.referencetests.EngineTestCaseSpec;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestProtocolSchedules;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

/**
 * CLI subcommand that executes Ethereum Engine API test fixtures
 * (blockchain_test_engine format) through the real Engine API code path.
 *
 * <p>Routes payloads through AbstractEngineNewPayload.syncResponse() →
 * MergeCoordinator.rememberBlock(), exercising the same validation and
 * execution logic as consume engine via Hive.
 */
@Command(
    name = COMMAND_NAME,
    description = "Execute an Ethereum Engine Test.",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class EngineTestSubCommand implements Runnable {

  public static final String COMMAND_NAME = "engine-test";

  @Option(
      names = {"--test-name", "--run"},
      description =
          "Limit execution to tests whose name contains the given substring, or matches a glob pattern.")
  private String testName = null;

  @Option(
      names = {"--workers"},
      description = "Number of parallel workers for processing fixture files.",
      defaultValue = "1")
  private int workers = 1;

  @ParentCommand private final EvmToolCommand parentCommand;

  @Parameters private final List<Path> engineTestFiles = new ArrayList<>();

  private static class TestResults {
    private static final String SEPARATOR = "=".repeat(80);
    private final AtomicInteger passedTests = new AtomicInteger(0);
    private final AtomicInteger failedTests = new AtomicInteger(0);
    private final Map<String, String> failures =
        Collections.synchronizedMap(new LinkedHashMap<>());

    void recordPass() {
      passedTests.incrementAndGet();
    }

    void recordFailure(final String testName, final String reason) {
      failedTests.incrementAndGet();
      failures.put(testName, reason);
    }

    boolean hasTests() {
      return passedTests.get() + failedTests.get() > 0;
    }

    void printSummary(final java.io.PrintWriter out) {
      final int totalTests = passedTests.get() + failedTests.get();
      out.println();
      out.println(SEPARATOR);
      out.println("TEST SUMMARY");
      out.println(SEPARATOR);
      out.printf("Total tests:  %d%n", totalTests);
      out.printf("Passed:       %d%n", passedTests.get());
      out.printf("Failed:       %d%n", failedTests.get());
      if (!failures.isEmpty()) {
        out.println("\nFailed tests:");
        failures.forEach((name, reason) -> out.printf("  - %s: %s%n", name, reason));
      }
      out.println(SEPARATOR);
    }
  }

  @SuppressWarnings("unused")
  public EngineTestSubCommand() {
    this(null);
  }

  EngineTestSubCommand(final EvmToolCommand parentCommand) {
    this.parentCommand = parentCommand;
  }

  @Override
  public void run() {
    final ObjectMapper mapper = JsonUtils.createObjectMapper();
    final TestResults results = new TestResults();
    final JavaType javaType =
        mapper
            .getTypeFactory()
            .constructParametricType(Map.class, String.class, EngineTestCaseSpec.class);

    try {
      final List<Path> collectedFiles = BlockchainTestSubCommand.collectFiles(engineTestFiles);

      if (collectedFiles.isEmpty()) {
        final BufferedReader in =
            new BufferedReader(new InputStreamReader(parentCommand.in, UTF_8));
        while (true) {
          final String fileName = in.readLine();
          if (fileName == null) break;
          final File file = new File(fileName);
          if (file.isFile()) {
            executeEngineTests(mapper.readValue(file, javaType), results);
          }
        }
      } else if (workers > 1 && collectedFiles.size() > 1) {
        final var executor = java.util.concurrent.Executors.newFixedThreadPool(workers);
        final var futures = new ArrayList<java.util.concurrent.Future<?>>();
        for (final Path file : collectedFiles) {
          futures.add(
              executor.submit(
                  () -> {
                    try {
                      executeEngineTests(mapper.readValue(file.toFile(), javaType), results);
                    } catch (final Exception e) {
                      // Skip non-fixture files
                    }
                  }));
        }
        for (final var future : futures) {
          future.get();
        }
        executor.shutdown();
      } else {
        for (final Path file : collectedFiles) {
          try {
            executeEngineTests(mapper.readValue(file.toFile(), javaType), results);
          } catch (final Exception e) {
            // Skip non-fixture files
          }
        }
      }
    } catch (final JsonProcessingException jpe) {
      parentCommand.out.println("File content error: " + jpe);
    } catch (final IOException e) {
      System.err.println("Unable to read test file: " + e.getMessage());
    } catch (final Exception e) {
      System.err.println("Error: " + e.getMessage());
    } finally {
      if (results.hasTests()) {
        results.printSummary(parentCommand.out);
      }
    }
  }

  // Shared across all tests to avoid thread exhaustion
  private static final Vertx SHARED_VERTX = Vertx.vertx();
  private static final NoOpMetricsSystem SHARED_METRICS = new NoOpMetricsSystem();
  private static final org.hyperledger.besu.ethereum.eth.manager.EthScheduler SHARED_SCHEDULER =
      new org.hyperledger.besu.ethereum.eth.manager.EthScheduler(
          Runtime.getRuntime().availableProcessors(),
          1,
          Runtime.getRuntime().availableProcessors(),
          SHARED_METRICS);
  private static final org.hyperledger.besu.ethereum.eth.manager.EthPeers SHARED_PEERS =
      org.mockito.Mockito.mock(org.hyperledger.besu.ethereum.eth.manager.EthPeers.class);
  private volatile ReferenceTestProtocolSchedules cachedSchedules;

  private void executeEngineTests(
      final Map<String, EngineTestCaseSpec> tests, final TestResults results) {
    for (final Map.Entry<String, EngineTestCaseSpec> entry : tests.entrySet()) {
      if (testName != null && !matchesTestName(entry.getKey())) continue;
      runSingleEngineTest(entry.getKey(), entry.getValue(), results);
    }
  }

  private boolean matchesTestName(final String test) {
    if (testName.contains("*") || testName.contains("?")) {
      final String regex =
          "(?i)" + testName.replace(".", "\\.").replace("*", ".*").replace("?", ".");
      return test.matches(regex);
    }
    return test.toLowerCase(Locale.ROOT).contains(testName.toLowerCase(Locale.ROOT));
  }

  private void runSingleEngineTest(
      final String test, final EngineTestCaseSpec spec, final TestResults results) {
    parentCommand.out.println("Running " + test);

    // Build chain and protocol context (cache protocol schedules across tests)
    final MutableBlockchain blockchain = spec.buildBlockchain();
    if (cachedSchedules == null) {
      cachedSchedules = ReferenceTestProtocolSchedules.create(parentCommand.getEvmConfiguration());
    }
    final ProtocolSchedule schedule = cachedSchedules.getByName(spec.getNetwork());
    if (schedule == null) {
      results.recordFailure(test, "Unsupported fork: " + spec.getNetwork());
      return;
    }

    // Build engine-aware protocol context with MergeContext
    final ProtocolContext context = spec.buildProtocolContextForEngine(blockchain);

    // Use shared static instances to avoid thread exhaustion across tests
    final EvmToolMergeCoordinator coordinator =
        new EvmToolMergeCoordinator(context, schedule, SHARED_SCHEDULER);
    final EngineCallListener listener = new EngineCallListener() {
      @Override public void executionEngineCalled() {}
      @Override public void stop() {}
    };

    // Create engine newPayload methods (V1-V5)
    final Map<Integer, ExecutionEngineJsonRpcMethod> newPayloadMethods = new HashMap<>();
    newPayloadMethods.put(1, new EngineNewPayloadV1(SHARED_VERTX, schedule, context, coordinator, SHARED_PEERS, listener, SHARED_METRICS));
    newPayloadMethods.put(2, new EngineNewPayloadV2(SHARED_VERTX, schedule, context, coordinator, SHARED_PEERS, listener, SHARED_METRICS));
    newPayloadMethods.put(3, new EngineNewPayloadV3(SHARED_VERTX, schedule, context, coordinator, SHARED_PEERS, listener, SHARED_METRICS));
    newPayloadMethods.put(4, new EngineNewPayloadV4(SHARED_VERTX, schedule, context, coordinator, SHARED_PEERS, listener, SHARED_METRICS));
    newPayloadMethods.put(5, new EngineNewPayloadV5(SHARED_VERTX, schedule, context, coordinator, SHARED_PEERS, listener, SHARED_METRICS));

    // Create engine forkchoiceUpdated methods (V1-V4)
    final Map<Integer, ExecutionEngineJsonRpcMethod> fcuMethods = new HashMap<>();
    fcuMethods.put(1, new EngineForkchoiceUpdatedV1(SHARED_VERTX, schedule, context, coordinator, listener));
    fcuMethods.put(2, new EngineForkchoiceUpdatedV2(SHARED_VERTX, schedule, context, coordinator, listener));
    fcuMethods.put(3, new EngineForkchoiceUpdatedV3(SHARED_VERTX, schedule, context, coordinator, listener));
    fcuMethods.put(4, new EngineForkchoiceUpdatedV4(SHARED_VERTX, schedule, context, coordinator, listener));

    boolean testPassed = true;
    String failureReason = "";

    final EngineTestCaseSpec.EngineNewPayload[] payloads = spec.getEngineNewPayloads();
    if (payloads == null || payloads.length == 0) {
      results.recordFailure(test, "No engine payloads");
      return;
    }

    // Send initial forkchoiceUpdated to genesis (matching consume engine behavior)
    final int initialFcuVersion = payloads[0].getForkchoiceUpdatedVersion();
    final ExecutionEngineJsonRpcMethod initialFcu = fcuMethods.getOrDefault(initialFcuVersion, fcuMethods.get(1));
    try {
      final var fcuParam = new EngineForkchoiceUpdatedParameter(
          spec.getGenesisBlockHeader().getHash(),
          spec.getGenesisBlockHeader().getHash(),
          spec.getGenesisBlockHeader().getHash());
      final JsonRpcResponse fcuResponse = initialFcu.syncResponse(
          new JsonRpcRequestContext(
              new JsonRpcRequest("2.0", "engine_forkchoiceUpdatedV" + initialFcuVersion,
                  new Object[]{fcuParam, null})));
      if (fcuResponse instanceof JsonRpcErrorResponse err) {
        testPassed = false;
        failureReason = "Initial FCU error: " + err.getError().getCode() + " " + err.getError().getMessage();
      }
    } catch (final Exception e) {
      testPassed = false;
      failureReason = "Initial FCU exception: " + e.getMessage();
    }

    if (!testPassed) {
      parentCommand.out.println("FAIL: " + test + " - " + failureReason);
      results.recordFailure(test, failureReason);
      try {
        context.getWorldStateArchive().close();
      } catch (final Exception ignored) {
        // cleanup best-effort
      }
      return;
    }

    for (int i = 0; i < payloads.length; i++) {
      final EngineTestCaseSpec.EngineNewPayload payload = payloads[i];
      final int version = payload.getNewPayloadVersion();

      final ExecutionEngineJsonRpcMethod method = newPayloadMethods.get(version);
      if (method == null) {
        testPassed = false;
        failureReason = String.format("payload %d: unsupported newPayload version %d", i, version);
        break;
      }

      // Build JSON-RPC request params — convert JsonNode to types the engine methods expect
      final JsonNode[] fixtureParams = payload.getParams();
      final ObjectMapper paramsMapper = JsonUtils.createObjectMapper();
      final Object[] rpcParams = new Object[fixtureParams.length];
      try {
        // params[0] = ExecutionPayload (as EnginePayloadParameter)
        rpcParams[0] = paramsMapper.treeToValue(
            fixtureParams[0],
            org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadParameter.class);
        // params[1] = versioned hashes (V3+) as List<String>
        if (fixtureParams.length > 1) {
          rpcParams[1] = fixtureParams[1].isArray()
              ? paramsMapper.treeToValue(fixtureParams[1], List.class)
              : null;
        }
        // params[2] = beacon root (V3+) as String
        if (fixtureParams.length > 2) {
          rpcParams[2] = fixtureParams[2].isTextual() ? fixtureParams[2].asText() : null;
        }
        // params[3] = execution requests (V4+) as List<String>
        if (fixtureParams.length > 3) {
          rpcParams[3] = fixtureParams[3].isArray()
              ? paramsMapper.treeToValue(fixtureParams[3], List.class)
              : null;
        }
      } catch (final JsonProcessingException e) {
        if (payload.expectsValid()) {
          testPassed = false;
          failureReason = String.format("payload %d: param parse error: %s", i, e.getMessage());
          break;
        }
        continue;
      }

      try {
        // Call the real engine method directly
        final JsonRpcResponse response =
            method.syncResponse(
                new JsonRpcRequestContext(
                    new JsonRpcRequest("2.0", "engine_newPayloadV" + version, rpcParams)));

        // Handle RPC-level errors
        if (response instanceof JsonRpcErrorResponse errorResponse) {
          if (payload.getErrorCode() != null) {
            // Expected error code
            parentCommand.out.printf(
                "Payload %d: Expected error code %s, got %d%n",
                i, payload.getErrorCode(), errorResponse.getError().getCode());
            continue;
          }
          if (payload.getValidationError() != null) {
            // Expected to be invalid — RPC error is acceptable
            continue;
          }
          testPassed = false;
          failureReason =
              String.format(
                  "payload %d: unexpected RPC error: %d %s",
                  i, errorResponse.getError().getCode(), errorResponse.getError().getMessage());
          break;
        }

        // Get payload status from successful response
        final EnginePayloadStatusResult status =
            (EnginePayloadStatusResult) ((JsonRpcSuccessResponse) response).getResult();

        if (payload.expectsValid()) {
          if (!"VALID".equals(status.getStatusAsString())) {
            testPassed = false;
            failureReason =
                String.format(
                    "payload %d: expected VALID, got %s (err: %s)",
                    i, status.getStatusAsString(), status.getError());
            break;
          }
          parentCommand.out.printf(
              "Payload %d: VALID (block %s)%n",
              i, payload.getParams()[0].get("blockHash").asText());

          // Send real forkchoiceUpdated to advance head (matching consume engine)
          final String blockHash = payload.getParams()[0].get("blockHash").asText();
          final int fcuVersion = payload.getForkchoiceUpdatedVersion();
          final ExecutionEngineJsonRpcMethod fcuMethod = fcuMethods.getOrDefault(fcuVersion, fcuMethods.get(1));
          final var fcuParam = new EngineForkchoiceUpdatedParameter(
              org.hyperledger.besu.datatypes.Hash.fromHexString(blockHash),
              org.hyperledger.besu.datatypes.Hash.fromHexString(blockHash),
              org.hyperledger.besu.datatypes.Hash.fromHexString(blockHash));
          final JsonRpcResponse fcuResponse = fcuMethod.syncResponse(
              new JsonRpcRequestContext(
                  new JsonRpcRequest("2.0", "engine_forkchoiceUpdatedV" + fcuVersion,
                      new Object[]{fcuParam, null})));
          if (fcuResponse instanceof JsonRpcErrorResponse fcuErr) {
            testPassed = false;
            failureReason = String.format("payload %d: FCU error: %d %s",
                i, fcuErr.getError().getCode(), fcuErr.getError().getMessage());
            break;
          }
          if (fcuResponse instanceof JsonRpcSuccessResponse) {
            parentCommand.out.printf("Payload %d: FCU VALID%n", i);
          }
        } else {
          if ("VALID".equals(status.getStatusAsString())) {
            testPassed = false;
            failureReason =
                String.format(
                    "payload %d: expected INVALID status for validation error \"%s\", got VALID",
                    i, payload.getValidationError());
            break;
          }
          parentCommand.out.printf(
              "Payload %d: %s (expected: %s, got: %s)%n",
              i, status.getStatusAsString(), payload.getValidationError(), status.getError());
        }
      } catch (final Exception e) {
        if (payload.expectsValid()) {
          testPassed = false;
          failureReason = String.format("payload %d: exception: %s", i, e.getMessage());
          break;
        }
        parentCommand.out.printf("Payload %d: Rejected with exception: %s%n", i, e.getMessage());
      }
    }

    // Validate last block hash
    if (testPassed && !blockchain.getChainHeadHash().equals(spec.getLastBlockHash())) {
      testPassed = false;
      failureReason =
          String.format(
              "last block hash mismatch: have %s, want %s",
              blockchain.getChainHeadHash(), spec.getLastBlockHash());
    }

    if (testPassed) {
      parentCommand.out.println("PASS: " + test);
      results.recordPass();
    } else {
      parentCommand.out.println("FAIL: " + test + " - " + failureReason);
      results.recordFailure(test, failureReason);
    }

    // Cleanup resources to prevent thread/memory exhaustion across tests
    try {
      context.getWorldStateArchive().close();
    } catch (final Exception e) {
      // ignore cleanup errors
    }
  }
}
