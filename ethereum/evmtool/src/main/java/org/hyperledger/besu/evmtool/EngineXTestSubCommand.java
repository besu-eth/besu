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
import static org.hyperledger.besu.evmtool.EngineXTestSubCommand.COMMAND_NAME;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineCallListener;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineForkchoiceUpdatedV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineForkchoiceUpdatedV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineForkchoiceUpdatedV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineForkchoiceUpdatedV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineNewPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineNewPayloadV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineNewPayloadV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineNewPayloadV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineNewPayloadV5;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EngineForkchoiceUpdatedParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EnginePayloadStatusResult;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.referencetests.BlockchainReferenceTestCaseSpec;
import org.hyperledger.besu.ethereum.referencetests.EngineTestCaseSpec;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestProtocolSchedules;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestWorldState;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

/**
 * CLI subcommand that executes Ethereum Engine API test fixtures in the
 * "blockchain_tests_engine_x" format. This format separates shared genesis
 * state (pre-alloc files) from individual test payloads, allowing the
 * pre-alloc JSON to be parsed once and reused across all tests that
 * reference it.
 *
 * <p>Each test gets its own fresh blockchain and ProtocolContext (because
 * multiple tests reference block 1 on the same genesis), but the expensive
 * pre-alloc JSON deserialization is cached by preHash.
 */
@Command(
    name = COMMAND_NAME,
    description = "Execute Ethereum Engine X Test fixtures (shared pre-alloc format).",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class EngineXTestSubCommand implements Runnable {

  public static final String COMMAND_NAME = "engine-x-test";

  @Option(
      names = {"--test-name", "--run"},
      description =
          "Limit execution to tests whose name contains the given substring, "
              + "or matches a glob pattern.")
  private String testName = null;

  @Option(
      names = {"--workers"},
      description = "Number of parallel workers for processing fixture files.",
      defaultValue = "1")
  private int workers = 1;

  @Option(
      names = {"--pre-alloc-dir"},
      description = "Directory containing pre-alloc JSON files (pre_alloc/*.json).",
      required = true)
  private Path preAllocDir;

  @ParentCommand private final EvmToolCommand parentCommand;

  @Parameters private final List<Path> testFiles = new ArrayList<>();

  // ── Shared statics (same as EngineTestSubCommand) ──────────────────────
  private static final Vertx SHARED_VERTX = Vertx.vertx();
  private static final NoOpMetricsSystem SHARED_METRICS = new NoOpMetricsSystem();
  private static final EthScheduler SHARED_SCHEDULER =
      new EthScheduler(
          Runtime.getRuntime().availableProcessors(),
          1,
          Runtime.getRuntime().availableProcessors(),
          SHARED_METRICS);
  private static final EthPeers SHARED_PEERS =
      org.mockito.Mockito.mock(EthPeers.class);

  // ── Cached data ────────────────────────────────────────────────────────
  private volatile ReferenceTestProtocolSchedules cachedSchedules;

  /** Parsed pre-alloc data, keyed by preHash (e.g. "0x680be093b9b276d3"). */
  private final Map<String, PreAllocData> preAllocCache = new ConcurrentHashMap<>();

  // ── Test results tracking ──────────────────────────────────────────────
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
        failures.forEach(
            (name, reason) -> out.printf("  - %s: %s%n", name, reason));
      }
      out.println(SEPARATOR);
    }
  }

  // ── Pre-alloc JSON model ───────────────────────────────────────────────

  /** Parsed representation of a pre_alloc/{hash}.json file. */
  static class PreAllocData {
    final String network;
    final BlockchainReferenceTestCaseSpec.ReferenceTestBlockHeader genesisBlockHeader;
    final Map<String, ReferenceTestWorldState.AccountMock> pre;

    PreAllocData(
        final String network,
        final BlockchainReferenceTestCaseSpec.ReferenceTestBlockHeader genesisBlockHeader,
        final Map<String, ReferenceTestWorldState.AccountMock> pre) {
      this.network = network;
      this.genesisBlockHeader = genesisBlockHeader;
      this.pre = pre;
    }
  }

  /** Jackson model for pre_alloc JSON files. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  static class PreAllocJson {
    final String network;
    final BlockchainReferenceTestCaseSpec.ReferenceTestBlockHeader genesis;
    final Map<String, ReferenceTestWorldState.AccountMock> pre;

    @JsonCreator
    PreAllocJson(
        @JsonProperty("network") final String network,
        @JsonProperty("genesis")
            final BlockchainReferenceTestCaseSpec.ReferenceTestBlockHeader genesis,
        @JsonProperty("pre")
            final Map<String, ReferenceTestWorldState.AccountMock> pre) {
      this.network = network;
      this.genesis = genesis;
      this.pre = pre;
    }
  }

  /** Jackson model for individual engine-x test cases within a test file. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  static class EngineXTestCase {
    final String network;
    final String preHash;
    final Hash lastBlockHash;
    final EngineXPayload[] engineNewPayloads;

    @JsonCreator
    EngineXTestCase(
        @JsonProperty("network") final String network,
        @JsonProperty("preHash") final String preHash,
        @JsonProperty("lastblockhash") final String lastBlockHash,
        @JsonProperty("engineNewPayloads")
            final EngineXPayload[] engineNewPayloads) {
      this.network = network;
      this.preHash = preHash;
      this.lastBlockHash = Hash.fromHexString(lastBlockHash);
      this.engineNewPayloads = engineNewPayloads;
    }
  }

  /** A single engine_newPayload call within an engine-x test. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  static class EngineXPayload {
    final JsonNode[] params;
    final String newPayloadVersion;
    final String forkchoiceUpdatedVersion;
    final String validationError;
    final String errorCode;

    @JsonCreator
    EngineXPayload(
        @JsonProperty("params") final JsonNode[] params,
        @JsonProperty("newPayloadVersion") final String newPayloadVersion,
        @JsonProperty("forkchoiceUpdatedVersion")
            final String forkchoiceUpdatedVersion,
        @JsonProperty("validationError") final String validationError,
        @JsonProperty("errorCode") final String errorCode) {
      this.params = params;
      this.newPayloadVersion = newPayloadVersion;
      this.forkchoiceUpdatedVersion = forkchoiceUpdatedVersion;
      this.validationError = validationError;
      this.errorCode = errorCode;
    }

    int getNewPayloadVersion() {
      return Integer.parseInt(
          newPayloadVersion != null ? newPayloadVersion : "1");
    }

    int getForkchoiceUpdatedVersion() {
      return Integer.parseInt(
          forkchoiceUpdatedVersion != null ? forkchoiceUpdatedVersion : "1");
    }

    boolean expectsValid() {
      return validationError == null && errorCode == null;
    }
  }

  // ── Constructors ───────────────────────────────────────────────────────

  @SuppressWarnings("unused")
  public EngineXTestSubCommand() {
    this(null);
  }

  EngineXTestSubCommand(final EvmToolCommand parentCommand) {
    this.parentCommand = parentCommand;
  }

  // ── Entry point ────────────────────────────────────────────────────────

  @Override
  public void run() {
    final ObjectMapper mapper = JsonUtils.createObjectMapper();
    final TestResults results = new TestResults();

    try {
      // 1. Load all pre-alloc files into cache
      loadPreAllocFiles(mapper);

      // 2. Collect all test files
      final List<Path> collectedFiles =
          BlockchainTestSubCommand.collectFiles(testFiles);

      if (collectedFiles.isEmpty()) {
        final var in =
            new java.io.BufferedReader(
                new java.io.InputStreamReader(parentCommand.in, UTF_8));
        while (true) {
          final String fileName = in.readLine();
          if (fileName == null) break;
          final File file = new File(fileName);
          if (file.isFile()) {
            processTestFile(mapper, file, results);
          }
        }
      } else if (workers > 1 && collectedFiles.size() > 1) {
        final var executor =
            java.util.concurrent.Executors.newFixedThreadPool(workers);
        final var futures =
            new ArrayList<java.util.concurrent.Future<?>>();
        for (final Path file : collectedFiles) {
          futures.add(
              executor.submit(
                  () -> {
                    try {
                      processTestFile(mapper, file.toFile(), results);
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
            processTestFile(mapper, file.toFile(), results);
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

  // ── Pre-alloc loading ──────────────────────────────────────────────────

  private void loadPreAllocFiles(final ObjectMapper mapper)
      throws IOException {
    if (!Files.isDirectory(preAllocDir)) {
      throw new IOException(
          "Pre-alloc directory does not exist: " + preAllocDir);
    }
    parentCommand.out.println(
        "Loading pre-alloc files from " + preAllocDir + " ...");
    final AtomicInteger count = new AtomicInteger(0);
    try (var stream = Files.list(preAllocDir)) {
      stream
          .filter(p -> p.toString().endsWith(".json"))
          .forEach(
              p -> {
                try {
                  final String fileName = p.getFileName().toString();
                  // preHash is the filename without .json extension
                  final String preHash =
                      fileName.substring(0, fileName.length() - 5);
                  final PreAllocJson raw =
                      mapper.readValue(p.toFile(), PreAllocJson.class);
                  preAllocCache.put(
                      preHash,
                      new PreAllocData(raw.network, raw.genesis, raw.pre));
                  count.incrementAndGet();
                } catch (final IOException e) {
                  System.err.println(
                      "Warning: failed to parse pre-alloc "
                          + p + ": " + e.getMessage());
                }
              });
    }
    parentCommand.out.println("Loaded " + count.get() + " pre-alloc files.");
  }

  // ── Test file processing ───────────────────────────────────────────────

  private void processTestFile(
      final ObjectMapper mapper,
      final File file,
      final TestResults results)
      throws IOException {
    final var javaType =
        mapper
            .getTypeFactory()
            .constructParametricType(
                Map.class, String.class, EngineXTestCase.class);
    final Map<String, EngineXTestCase> tests = mapper.readValue(file, javaType);
    for (final Map.Entry<String, EngineXTestCase> entry : tests.entrySet()) {
      if (testName != null && !matchesTestName(entry.getKey())) continue;
      runSingleTest(entry.getKey(), entry.getValue(), results);
    }
  }

  private boolean matchesTestName(final String test) {
    if (testName.contains("*") || testName.contains("?")) {
      final String regex =
          "(?i)"
              + testName
                  .replace(".", "\\.")
                  .replace("*", ".*")
                  .replace("?", ".");
      return test.matches(regex);
    }
    return test.toLowerCase(Locale.ROOT)
        .contains(testName.toLowerCase(Locale.ROOT));
  }

  // ── Single test execution ──────────────────────────────────────────────

  private void runSingleTest(
      final String test,
      final EngineXTestCase testCase,
      final TestResults results) {
    parentCommand.out.println("Running " + test);

    // Resolve pre-alloc data
    final PreAllocData preAlloc = preAllocCache.get(testCase.preHash);
    if (preAlloc == null) {
      results.recordFailure(
          test, "Pre-alloc not found for preHash: " + testCase.preHash);
      return;
    }

    // Resolve protocol schedule
    if (cachedSchedules == null) {
      cachedSchedules =
          ReferenceTestProtocolSchedules.create(
              parentCommand.getEvmConfiguration());
    }
    final ProtocolSchedule schedule =
        cachedSchedules.getByName(testCase.network);
    if (schedule == null) {
      results.recordFailure(test, "Unsupported fork: " + testCase.network);
      return;
    }

    // Build fresh blockchain and context from cached pre-alloc data.
    // Uses static helpers on EngineTestCaseSpec which have access to the
    // test-support artifacts (InMemoryKeyValueStorageProvider etc.).
    final int payloadCount =
        testCase.engineNewPayloads != null
            ? testCase.engineNewPayloads.length
            : 0;

    final MutableBlockchain blockchain =
        EngineTestCaseSpec.buildBlockchainFrom(preAlloc.genesisBlockHeader);

    final ProtocolContext context =
        EngineTestCaseSpec.buildProtocolContextForEngineFrom(
            blockchain,
            preAlloc.genesisBlockHeader,
            preAlloc.pre,
            payloadCount);

    // Create coordinator and engine methods
    final EvmToolMergeCoordinator coordinator =
        new EvmToolMergeCoordinator(context, schedule, SHARED_SCHEDULER);
    final EngineCallListener listener =
        new EngineCallListener() {
          @Override
          public void executionEngineCalled() {}

          @Override
          public void stop() {}
        };

    final Map<Integer, ExecutionEngineJsonRpcMethod> newPayloadMethods =
        new HashMap<>();
    newPayloadMethods.put(
        1,
        new EngineNewPayloadV1(
            SHARED_VERTX, schedule, context, coordinator,
            SHARED_PEERS, listener, SHARED_METRICS));
    newPayloadMethods.put(
        2,
        new EngineNewPayloadV2(
            SHARED_VERTX, schedule, context, coordinator,
            SHARED_PEERS, listener, SHARED_METRICS));
    newPayloadMethods.put(
        3,
        new EngineNewPayloadV3(
            SHARED_VERTX, schedule, context, coordinator,
            SHARED_PEERS, listener, SHARED_METRICS));
    newPayloadMethods.put(
        4,
        new EngineNewPayloadV4(
            SHARED_VERTX, schedule, context, coordinator,
            SHARED_PEERS, listener, SHARED_METRICS));
    newPayloadMethods.put(
        5,
        new EngineNewPayloadV5(
            SHARED_VERTX, schedule, context, coordinator,
            SHARED_PEERS, listener, SHARED_METRICS));

    final Map<Integer, ExecutionEngineJsonRpcMethod> fcuMethods =
        new HashMap<>();
    fcuMethods.put(
        1,
        new EngineForkchoiceUpdatedV1(
            SHARED_VERTX, schedule, context, coordinator, listener));
    fcuMethods.put(
        2,
        new EngineForkchoiceUpdatedV2(
            SHARED_VERTX, schedule, context, coordinator, listener));
    fcuMethods.put(
        3,
        new EngineForkchoiceUpdatedV3(
            SHARED_VERTX, schedule, context, coordinator, listener));
    fcuMethods.put(
        4,
        new EngineForkchoiceUpdatedV4(
            SHARED_VERTX, schedule, context, coordinator, listener));

    boolean testPassed = true;
    String failureReason = "";

    final EngineXPayload[] payloads = testCase.engineNewPayloads;
    if (payloads == null || payloads.length == 0) {
      results.recordFailure(test, "No engine payloads");
      closeContext(context);
      return;
    }

    // Send initial forkchoiceUpdated to genesis
    final int initialFcuVersion = payloads[0].getForkchoiceUpdatedVersion();
    final ExecutionEngineJsonRpcMethod initialFcu =
        fcuMethods.getOrDefault(initialFcuVersion, fcuMethods.get(1));
    try {
      final var fcuParam =
          new EngineForkchoiceUpdatedParameter(
              preAlloc.genesisBlockHeader.getHash(),
              preAlloc.genesisBlockHeader.getHash(),
              preAlloc.genesisBlockHeader.getHash());
      final JsonRpcResponse fcuResponse =
          initialFcu.syncResponse(
              new JsonRpcRequestContext(
                  new JsonRpcRequest(
                      "2.0",
                      "engine_forkchoiceUpdatedV" + initialFcuVersion,
                      new Object[] {fcuParam, null})));
      if (fcuResponse instanceof JsonRpcErrorResponse err) {
        testPassed = false;
        failureReason =
            "Initial FCU error: "
                + err.getError().getCode()
                + " "
                + err.getError().getMessage();
      }
    } catch (final Exception e) {
      testPassed = false;
      failureReason = "Initial FCU exception: " + e.getMessage();
    }

    if (!testPassed) {
      parentCommand.out.println("FAIL: " + test + " - " + failureReason);
      results.recordFailure(test, failureReason);
      closeContext(context);
      return;
    }

    // Process each payload
    for (int i = 0; i < payloads.length; i++) {
      final EngineXPayload payload = payloads[i];
      final int version = payload.getNewPayloadVersion();

      final ExecutionEngineJsonRpcMethod method =
          newPayloadMethods.get(version);
      if (method == null) {
        testPassed = false;
        failureReason =
            String.format(
                "payload %d: unsupported newPayload version %d",
                i, version);
        break;
      }

      // Build JSON-RPC request params
      final JsonNode[] fixtureParams = payload.params;
      final ObjectMapper paramsMapper = JsonUtils.createObjectMapper();
      final Object[] rpcParams = new Object[fixtureParams.length];
      try {
        // params[0] = ExecutionPayload (as EnginePayloadParameter)
        rpcParams[0] =
            paramsMapper.treeToValue(
                fixtureParams[0],
                org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters
                    .EnginePayloadParameter.class);
        // params[1] = versioned hashes (V3+) as List<String>
        if (fixtureParams.length > 1) {
          rpcParams[1] =
              fixtureParams[1].isArray()
                  ? paramsMapper.treeToValue(fixtureParams[1], List.class)
                  : null;
        }
        // params[2] = beacon root (V3+) as String
        if (fixtureParams.length > 2) {
          rpcParams[2] =
              fixtureParams[2].isTextual()
                  ? fixtureParams[2].asText()
                  : null;
        }
        // params[3] = execution requests (V4+) as List<String>
        if (fixtureParams.length > 3) {
          rpcParams[3] =
              fixtureParams[3].isArray()
                  ? paramsMapper.treeToValue(fixtureParams[3], List.class)
                  : null;
        }
      } catch (final JsonProcessingException e) {
        if (payload.expectsValid()) {
          testPassed = false;
          failureReason =
              String.format(
                  "payload %d: param parse error: %s", i, e.getMessage());
          break;
        }
        continue;
      }

      try {
        final JsonRpcResponse response =
            method.syncResponse(
                new JsonRpcRequestContext(
                    new JsonRpcRequest(
                        "2.0",
                        "engine_newPayloadV" + version,
                        rpcParams)));

        // Handle RPC-level errors
        if (response instanceof JsonRpcErrorResponse errorResponse) {
          if (payload.errorCode != null) {
            parentCommand.out.printf(
                "Payload %d: Expected error code %s, got %d%n",
                i,
                payload.errorCode,
                errorResponse.getError().getCode());
            continue;
          }
          if (payload.validationError != null) {
            continue;
          }
          testPassed = false;
          failureReason =
              String.format(
                  "payload %d: unexpected RPC error: %d %s",
                  i,
                  errorResponse.getError().getCode(),
                  errorResponse.getError().getMessage());
          break;
        }

        // Get payload status
        final EnginePayloadStatusResult status =
            (EnginePayloadStatusResult)
                ((JsonRpcSuccessResponse) response).getResult();

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
              i, payload.params[0].get("blockHash").asText());

          // Send forkchoiceUpdated to advance head
          final String blockHash =
              payload.params[0].get("blockHash").asText();
          final int fcuVersion = payload.getForkchoiceUpdatedVersion();
          final ExecutionEngineJsonRpcMethod fcuMethod =
              fcuMethods.getOrDefault(fcuVersion, fcuMethods.get(1));
          final var fcuParam =
              new EngineForkchoiceUpdatedParameter(
                  Hash.fromHexString(blockHash),
                  Hash.fromHexString(blockHash),
                  Hash.fromHexString(blockHash));
          final JsonRpcResponse fcuResponse =
              fcuMethod.syncResponse(
                  new JsonRpcRequestContext(
                      new JsonRpcRequest(
                          "2.0",
                          "engine_forkchoiceUpdatedV" + fcuVersion,
                          new Object[] {fcuParam, null})));
          if (fcuResponse instanceof JsonRpcErrorResponse fcuErr) {
            testPassed = false;
            failureReason =
                String.format(
                    "payload %d: FCU error: %d %s",
                    i,
                    fcuErr.getError().getCode(),
                    fcuErr.getError().getMessage());
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
                    "payload %d: expected INVALID for \"%s\", got VALID",
                    i, payload.validationError);
            break;
          }
          parentCommand.out.printf(
              "Payload %d: %s (expected: %s, got: %s)%n",
              i,
              status.getStatusAsString(),
              payload.validationError,
              status.getError());
        }
      } catch (final Exception e) {
        if (payload.expectsValid()) {
          testPassed = false;
          failureReason =
              String.format("payload %d: exception: %s", i, e.getMessage());
          break;
        }
        parentCommand.out.printf(
            "Payload %d: Rejected with exception: %s%n",
            i, e.getMessage());
      }
    }

    // Validate last block hash
    if (testPassed
        && !blockchain.getChainHeadHash().equals(testCase.lastBlockHash)) {
      testPassed = false;
      failureReason =
          String.format(
              "last block hash mismatch: have %s, want %s",
              blockchain.getChainHeadHash(), testCase.lastBlockHash);
    }

    if (testPassed) {
      parentCommand.out.println("PASS: " + test);
      results.recordPass();
    } else {
      parentCommand.out.println("FAIL: " + test + " - " + failureReason);
      results.recordFailure(test, failureReason);
    }

    closeContext(context);
  }

  private static void closeContext(final ProtocolContext context) {
    try {
      context.getWorldStateArchive().close();
    } catch (final Exception ignored) {
      // cleanup best-effort
    }
  }
}
