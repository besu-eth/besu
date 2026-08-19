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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PARIS;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PRAGUE;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.SHANGHAI;
import static org.hyperledger.besu.evmtool.EngineTestSubCommand.COMMAND_NAME;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcRequestException;
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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ForkchoiceStateV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.PayloadAttributesV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.PayloadAttributesV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.PayloadAttributesV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.PayloadAttributesV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.PayloadStatusV1;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.referencetests.EngineTestCaseSpec;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestProtocolSchedules;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.IntFunction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes;
import picocli.CommandLine.Command;
import picocli.CommandLine.IExitCodeGenerator;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

/**
 * CLI subcommand that executes Ethereum Engine API test fixtures (blockchain_test_engine format)
 * through the real Engine API code path.
 *
 * <p>Routes payloads through AbstractEngineNewPayload.syncResponse() →
 * MergeCoordinator.rememberBlock(), exercising the same validation and execution logic as consume
 * engine via Hive.
 */
@Command(
    name = COMMAND_NAME,
    description = "Execute an Ethereum Engine Test.",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class EngineTestSubCommand implements Runnable, IExitCodeGenerator {

  /** The name users type to invoke this subcommand, and the value of the picocli command name. */
  public static final String COMMAND_NAME = "engine-test";

  /** Process exit code: 0 when all executed tests pass, 1 when any failed. */
  private volatile int exitCode = 0;

  @Option(
      names = {"--test-name", "--run"},
      description =
          "Limit execution to tests whose name contains the given substring, or matches the given"
              + " pattern (a regex, with * and ? as wildcards).")
  private String testName = null;

  // Compiled up front so a malformed expression fails before any fixture is read
  private TestNameFilter nameFilter;

  @Option(
      names = {"--workers"},
      description = "Number of parallel workers for processing fixture files.",
      defaultValue = "1")
  private int workers = 1;

  @Option(
      names = {"--json-array"},
      description =
          "Output results as a JSON array: name, pass, fork, lastBlockHash, lastPayloadStatus, error.")
  private boolean jsonArray = false;

  @Option(
      names = {"--verbose"},
      description =
          "Print per-test progress output. By default only failing tests and the final summary are"
              + " printed.")
  private boolean verbose = false;

  private final List<ObjectNode> jsonArrayResults = Collections.synchronizedList(new ArrayList<>());

  @ParentCommand private final EvmToolCommand parentCommand;

  @Parameters private final List<Path> engineTestFiles = new ArrayList<>();

  /** Required by PicoCLI, which instantiates subcommands reflectively with no arguments. */
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
    final FixtureRunner.TestResults results = new FixtureRunner.TestResults();
    final JavaType javaType =
        mapper
            .getTypeFactory()
            .constructParametricType(Map.class, String.class, EngineTestCaseSpec.class);

    if (testName != null) {
      try {
        nameFilter = TestNameFilter.compile(testName);
      } catch (final IllegalArgumentException e) {
        parentCommand.out.println(e.getMessage());
        exitCode = 1;
        return;
      }
    }

    boolean setupFailed = false;
    try {
      if (engineTestFiles.isEmpty()) {
        final BufferedReader in =
            new BufferedReader(new InputStreamReader(parentCommand.in, UTF_8));
        while (true) {
          final String fileName = in.readLine();
          if (fileName == null) break;
          final File file = new File(fileName);
          if (file.isFile()) {
            runFile(file.toPath(), mapper, javaType, results);
          } else {
            parentCommand.out.println("File not found: " + fileName);
          }
        }
      } else {
        FixtureRunner.runFiles(
            FixtureRunner.collectFiles(engineTestFiles),
            workers,
            file -> runFile(file, mapper, javaType, results));
      }
    } catch (final JsonProcessingException jpe) {
      setupFailed = true;
      parentCommand.out.println("File content error: " + jpe);
    } catch (final IOException e) {
      setupFailed = true;
      System.err.println("Unable to read test file: " + e.getMessage());
    } catch (final Exception e) {
      setupFailed = true;
      System.err.println("Error: " + e.getMessage());
    } finally {
      // An empty run is not a pass: a typo in --run, or a fixture tree that failed to
      // materialise, would otherwise be indistinguishable from a clean sweep.
      boolean ranNothing = false;
      if (!results.hasTests()) {
        ranNothing = true;
        parentCommand.out.printf(
            "No engine test was executed%s.%n",
            testName == null ? "" : " matching --run/--test-name '" + testName + "'");
      }
      if (jsonArray) {
        FixtureRunner.printJsonArray(parentCommand.out, jsonArrayResults);
      } else if (results.hasTests()) {
        results.printSummary(parentCommand.out);
      }
      exitCode = results.failed() > 0 || setupFailed || ranNothing ? 1 : 0;
    }
  }

  /**
   * Reads one fixture file and runs the tests in it. A file that cannot be read is reported in the
   * summary, but kept apart from the test failures: it says nothing about Besu, only that the
   * fixture has a shape this harness cannot build.
   */
  private void runFile(
      final Path file,
      final ObjectMapper mapper,
      final JavaType javaType,
      final FixtureRunner.TestResults results) {
    final Map<String, EngineTestCaseSpec> tests;
    try {
      tests = mapper.readValue(file.toFile(), javaType);
    } catch (final Exception e) {
      results.recordUnreadable(file.toString(), "not readable as an engine test fixture: " + e);
      return;
    }
    executeEngineTests(tests, results);
  }

  @Override
  public int getExitCode() {
    return exitCode;
  }

  // Shared across all tests to avoid thread exhaustion
  private static final Vertx SHARED_VERTX = Vertx.vertx();
  private static final NoOpMetricsSystem SHARED_METRICS = new NoOpMetricsSystem();
  private static final EthScheduler SHARED_SCHEDULER =
      new EthScheduler(
          Runtime.getRuntime().availableProcessors(),
          1,
          Runtime.getRuntime().availableProcessors(),
          SHARED_METRICS);
  private static final EthPeers SHARED_PEERS = new HarnessEthPeers();

  /**
   * Mirrors the production {@code GET_PAYLOAD_BODIES_MAX_REQUEST_SIZE}. Only the
   * engine_getPayloadBodiesBy* methods read it, which the fixtures never exercise, but the engine
   * methods share one constructor argument record.
   */
  private static final int GET_PAYLOAD_BODIES_MAX_REQUEST_SIZE = 1024;

  /**
   * The engine methods only ever call {@link EthPeers#peerCount()} on this, for the "N peers"
   * suffix of the imported-block log line, so everything else is left unwired. A stub rather than a
   * mock keeps a test framework off the shipped evmtool runtime classpath.
   */
  private static final class HarnessEthPeers extends EthPeers {
    private HarnessEthPeers() {
      super(
          () -> null,
          Clock.systemUTC(),
          SHARED_METRICS,
          0,
          List.of(),
          Bytes.EMPTY,
          0,
          0,
          false,
          SyncMode.FULL,
          null);
    }

    @Override
    public int peerCount() {
      return 0;
    }
  }

  // Shared no-op listener — avoids creating anonymous class per test
  private static final EngineCallListener SHARED_LISTENER =
      new EngineCallListener() {
        @Override
        public void executionEngineCalled() {}

        @Override
        public void stop() {}
      };
  // Cached ObjectMapper for params deserialization — ObjectMapper is thread-safe for reads
  private static final ObjectMapper SHARED_PARAMS_MAPPER = JsonUtils.createObjectMapper();
  // Protocol schedules are cached per distinct fixture blob schedule (key from
  // EngineTestCaseSpec.getBlobScheduleKey()), so devnets whose blob target/max differ from Besu
  // defaults are validated against the fixtures' own blob schedule rather than rebuilt per test.
  private final ConcurrentMap<String, ReferenceTestProtocolSchedules> cachedSchedules =
      new ConcurrentHashMap<>();

  private void executeEngineTests(
      final Map<String, EngineTestCaseSpec> tests, final FixtureRunner.TestResults results) {
    for (final Map.Entry<String, EngineTestCaseSpec> entry : tests.entrySet()) {
      if (nameFilter != null && !nameFilter.matches(entry.getKey())) continue;
      try {
        runSingleEngineTest(entry.getKey(), entry.getValue(), results);
      } catch (final RuntimeException e) {
        // Charge an execution error — failing to build the chain or schedule, say — to the test
        // that caused it, so one broken fixture cannot take the rest of the file down with it.
        results.recordFailure(entry.getKey(), "execution error: " + e);
      }
    }
  }

  /**
   * Validates an actual JSON-RPC error code against the fixture's expectation, mirroring the hive
   * consume-engine oracle.
   *
   * @param payloadIndex index of the payload, for messages
   * @param expectedErrorCode the fixture's expected errorCode, or {@code null} if none is expected
   * @param actualCode the error code the engine method returned
   * @return {@code null} when the outcome matches expectations, otherwise a failure reason
   */
  private static String checkExpectedErrorCode(
      final int payloadIndex, final String expectedErrorCode, final int actualCode) {
    if (expectedErrorCode == null) {
      return String.format(
          "payload %d: unexpected Engine API error code %d", payloadIndex, actualCode);
    }
    final int expected = Integer.parseInt(expectedErrorCode.trim());
    if (actualCode != expected) {
      return String.format(
          "payload %d: expected Engine API error code %d, got %d",
          payloadIndex, expected, actualCode);
    }
    return null;
  }

  private void runSingleEngineTest(
      final String test, final EngineTestCaseSpec spec, final FixtureRunner.TestResults results) {
    if (verbose) {
      parentCommand.out.println("Running " + test);
    }

    // Build chain and protocol context (cache protocol schedules per fixture blob schedule)
    final MutableBlockchain blockchain = spec.buildBlockchain();
    final ProtocolSchedule schedule;
    try {
      schedule =
          cachedSchedules
              .computeIfAbsent(
                  spec.getBlobScheduleKey(),
                  key ->
                      ReferenceTestProtocolSchedules.create(
                          parentCommand.getEvmConfiguration(),
                          spec.getBlobScheduleOptions().orElse(null)))
              .getByName(spec.getNetwork());
    } catch (final RuntimeException e) {
      results.recordFailure(test, "Failed to build protocol schedule: " + e);
      return;
    }
    if (schedule == null) {
      results.recordFailure(test, "Unsupported fork: " + spec.getNetwork());
      return;
    }

    // Build engine-aware protocol context with MergeContext
    final ProtocolContext context = spec.buildProtocolContextForEngine(blockchain);

    // Use shared static instances to avoid thread exhaustion across tests
    final EvmToolMergeCoordinator coordinator =
        new EvmToolMergeCoordinator(context, schedule, SHARED_SCHEDULER);

    // Lazily create engine methods — most tests use only 1-2 versions, not all 9
    final ExecutionEngineJsonRpcMethod.ConstructorArguments ctorArgs =
        new ExecutionEngineJsonRpcMethod.ConstructorArguments(
            schedule,
            context,
            SHARED_VERTX,
            SHARED_LISTENER,
            coordinator,
            SHARED_PEERS,
            SHARED_METRICS,
            GET_PAYLOAD_BODIES_MAX_REQUEST_SIZE);

    final Map<Integer, ExecutionEngineJsonRpcMethod> newPayloadMethods = new HashMap<>();
    final Map<Integer, ExecutionEngineJsonRpcMethod> fcuMethods = new HashMap<>();

    // Factory lambdas for lazy creation of engine methods
    final IntFunction<ExecutionEngineJsonRpcMethod> getNewPayload =
        v ->
            newPayloadMethods.computeIfAbsent(
                v,
                ver ->
                    switch (ver) {
                      case 1 ->
                          new EngineNewPayloadV1<
                              ExecutionPayloadV1,
                              NewPayloadRequestParametersV1<ExecutionPayloadV1>>(
                              ctorArgs, PARIS, SHANGHAI);
                      case 2 ->
                          new EngineNewPayloadV2<
                              ExecutionPayloadV2,
                              NewPayloadRequestParametersV1<ExecutionPayloadV2>>(
                              ctorArgs, PARIS, null);
                      case 3 ->
                          new EngineNewPayloadV3<
                              ExecutionPayloadV3,
                              NewPayloadRequestParametersV2<ExecutionPayloadV3>>(
                              ctorArgs, CANCUN, null);
                      case 4 ->
                          new EngineNewPayloadV4<
                              ExecutionPayloadV3,
                              NewPayloadRequestParametersV3<ExecutionPayloadV3>>(
                              ctorArgs, PRAGUE, null);
                      case 5 ->
                          new EngineNewPayloadV5<
                              ExecutionPayloadV4,
                              NewPayloadRequestParametersV3<ExecutionPayloadV4>>(
                              ctorArgs, AMSTERDAM, null);
                      default -> null;
                    });
    final IntFunction<ExecutionEngineJsonRpcMethod> getFcu =
        v ->
            fcuMethods.computeIfAbsent(
                v,
                ver ->
                    switch (ver) {
                      case 1 ->
                          new EngineForkchoiceUpdatedV1<PayloadAttributesV1>(
                              ctorArgs, PARIS, SHANGHAI);
                      case 2 ->
                          new EngineForkchoiceUpdatedV2<PayloadAttributesV2>(ctorArgs, PARIS, null);
                      case 3 ->
                          new EngineForkchoiceUpdatedV3<PayloadAttributesV3>(
                              ctorArgs, CANCUN, null);
                      case 4 ->
                          new EngineForkchoiceUpdatedV4<PayloadAttributesV4>(
                              ctorArgs, AMSTERDAM, null);
                      default -> null;
                    });

    boolean testPassed = true;
    String failureReason = "";
    String lastPayloadStatus = "";
    String lastValidationError = "";

    final EngineTestCaseSpec.EngineNewPayload[] payloads = spec.getEngineNewPayloads();
    if (payloads == null || payloads.length == 0) {
      results.recordFailure(test, "No engine payloads");
      return;
    }

    // Send initial forkchoiceUpdated to genesis (matching consume engine behavior)
    final int initialFcuVersion = payloads[0].getForkchoiceUpdatedVersion();
    final ExecutionEngineJsonRpcMethod initialFcu = getFcu.apply(initialFcuVersion);
    if (initialFcu == null) {
      final String reason =
          "unsupported forkchoiceUpdated version " + initialFcuVersion + " (initial FCU)";
      parentCommand.out.println("FAIL: " + test + " - " + reason);
      results.recordFailure(test, reason);
      closeQuietly(context);
      return;
    }
    try {
      final var fcuParam =
          new ForkchoiceStateV1(
              spec.getGenesisBlockHeader().getHash(),
              spec.getGenesisBlockHeader().getHash(),
              spec.getGenesisBlockHeader().getHash());
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
            "Initial FCU error: " + err.getError().getCode() + " " + err.getError().getMessage();
      }
    } catch (final Exception e) {
      testPassed = false;
      failureReason = "Initial FCU exception: " + e.getMessage();
    }

    if (!testPassed) {
      parentCommand.out.println("FAIL: " + test + " - " + failureReason);
      results.recordFailure(test, failureReason);
      closeQuietly(context);
      return;
    }

    for (int i = 0; i < payloads.length; i++) {
      final EngineTestCaseSpec.EngineNewPayload payload = payloads[i];
      final int version = payload.getNewPayloadVersion();

      final ExecutionEngineJsonRpcMethod method = getNewPayload.apply(version);
      if (method == null) {
        testPassed = false;
        failureReason = String.format("payload %d: unsupported newPayload version %d", i, version);
        break;
      }

      // Build JSON-RPC request params — convert JsonNode to types the engine methods expect
      final JsonNode[] fixtureParams = payload.getParams();
      final Object[] rpcParams = new Object[fixtureParams.length];
      try {
        // params[0] = ExecutionPayload: hand the raw JsonNode to the engine method so it
        // deserializes into its own version-specific ExecutionPayloadVn.
        rpcParams[0] = fixtureParams[0];
        // params[1] = versioned hashes (V3+) as List<String>
        if (fixtureParams.length > 1) {
          rpcParams[1] =
              fixtureParams[1].isArray()
                  ? SHARED_PARAMS_MAPPER.treeToValue(fixtureParams[1], List.class)
                  : null;
        }
        // params[2] = beacon root (V3+) as String
        if (fixtureParams.length > 2) {
          rpcParams[2] = fixtureParams[2].isTextual() ? fixtureParams[2].asText() : null;
        }
        // params[3] = execution requests (V4+) as List<String>
        if (fixtureParams.length > 3) {
          rpcParams[3] =
              fixtureParams[3].isArray()
                  ? SHARED_PARAMS_MAPPER.treeToValue(fixtureParams[3], List.class)
                  : null;
        }
      } catch (final JsonProcessingException e) {
        if (payload.expectsValid()) {
          testPassed = false;
          failureReason = String.format("payload %d: param parse error: %s", i, e.getMessage());
          break;
        }
        // Params that don't deserialize would be rejected by the engine with InvalidParams
        // (-32602), so verify that against the fixture's expectation rather than passing blindly.
        String mismatch = checkExpectedErrorCode(i, payload.getErrorCode(), -32602);
        if (mismatch != null) {
          mismatch = mismatch + " [HARNESS-PARAM-PREP: " + e.getMessage() + "]";
        }
        if (mismatch != null) {
          testPassed = false;
          failureReason = mismatch;
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

        // Handle RPC-level errors (the engine method returned a JSON-RPC error response).
        // Mirrors the hive consume-engine oracle: when the fixture sets an errorCode the
        // returned code must match exactly; when it does not, any RPC error is unexpected.
        if (response instanceof JsonRpcErrorResponse errorResponse) {
          String mismatch =
              checkExpectedErrorCode(i, payload.getErrorCode(), errorResponse.getError().getCode());
          if (mismatch != null) {
            mismatch = mismatch + " [ENGINE: " + errorResponse.getError().getMessage() + "]";
          }
          if (mismatch != null) {
            testPassed = false;
            failureReason = mismatch;
            break;
          }
          // Correctly rejected with the expected error code.
          continue;
        }

        // Get payload status from successful response
        final PayloadStatusV1 status =
            (PayloadStatusV1) ((JsonRpcSuccessResponse) response).getResult();
        lastPayloadStatus = status.getStatusAsString();
        if (status.getError() != null && !status.getError().isEmpty()) {
          lastValidationError = status.getError();
        }

        // A fixture that expects an Engine API errorCode requires an actual JSON-RPC error
        // response; a status response, even INVALID, does not satisfy it. This is the distinction
        // hive enforces.
        if (payload.getErrorCode() != null) {
          testPassed = false;
          failureReason =
              String.format(
                  "payload %d: expected Engine API error code %s, got success response with status %s",
                  i, payload.getErrorCode(), status.getStatusAsString());
          break;
        }

        if (payload.expectsValid()) {
          if (!"VALID".equals(status.getStatusAsString())) {
            testPassed = false;
            failureReason =
                String.format(
                    "payload %d: expected VALID, got %s (err: %s)",
                    i, status.getStatusAsString(), status.getError());
            break;
          }
          if (verbose) {
            parentCommand.out.printf(
                "Payload %d: VALID (block %s)%n",
                i, payload.getParams()[0].get("blockHash").asText());
          }

          // Send real forkchoiceUpdated to advance head (matching consume engine)
          final String blockHash = payload.getParams()[0].get("blockHash").asText();
          final int fcuVersion = payload.getForkchoiceUpdatedVersion();
          final ExecutionEngineJsonRpcMethod fcuMethod = getFcu.apply(fcuVersion);
          if (fcuMethod == null) {
            testPassed = false;
            failureReason =
                String.format(
                    "payload %d: unsupported forkchoiceUpdated version %d", i, fcuVersion);
            break;
          }
          final var fcuParam =
              new ForkchoiceStateV1(
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
                    i, fcuErr.getError().getCode(), fcuErr.getError().getMessage());
            break;
          }
          if (verbose && fcuResponse instanceof JsonRpcSuccessResponse) {
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
          // Strict exception matching (mirrors hive): the message Besu returns with INVALID must
          // map to the fixture's expected exception.
          if (payload.getValidationError() != null) {
            final String mismatch =
                EngineTestExceptionMapper.mismatch(payload.getValidationError(), status.getError());
            if (mismatch != null) {
              testPassed = false;
              failureReason = String.format("payload %d: %s", i, mismatch);
              break;
            }
          }
          if (verbose) {
            parentCommand.out.printf(
                "Payload %d: %s (expected: %s, got: %s)%n",
                i, status.getStatusAsString(), payload.getValidationError(), status.getError());
          }
        }
      } catch (final InvalidJsonRpcRequestException e) {
        // Calling syncResponse() directly throws this for param-level errors; over the wire it
        // would surface as a JSON-RPC error, so apply the same errorCode matching as above.
        final String mismatch =
            checkExpectedErrorCode(i, payload.getErrorCode(), e.getRpcErrorType().getCode());
        if (mismatch != null) {
          testPassed = false;
          failureReason = mismatch;
          break;
        }
      } catch (final Exception e) {
        // Anything else escaping syncResponse() is a defect, not a rejection: over the wire the
        // engine answers with a status or a JSON-RPC error, never a stack trace. Counting it as a
        // rejection would let harness NPEs and Besu internal errors past the oracles below.
        testPassed = false;
        failureReason = String.format("payload %d: %s: %s", i, e.getClass().getName(), e);
        break;
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
      if (verbose) {
        parentCommand.out.println("PASS: " + test);
      }
      results.recordPass();
    } else {
      parentCommand.out.println("FAIL: " + test + " - " + failureReason);
      results.recordFailure(test, failureReason);
    }

    if (jsonArray) {
      final ObjectNode result = SHARED_PARAMS_MAPPER.createObjectNode();
      result.put("name", test);
      result.put("pass", testPassed);
      result.put("fork", spec.getNetwork());
      result.put("lastBlockHash", blockchain.getChainHeadHash().toHexString());
      result.put("lastPayloadStatus", lastPayloadStatus);
      result.put("error", testPassed ? lastValidationError : failureReason);
      jsonArrayResults.add(result);
    }

    // Cleanup resources to prevent thread/memory exhaustion across tests
    closeQuietly(context);
  }

  private static void closeQuietly(final ProtocolContext context) {
    try {
      context.getWorldStateArchive().close();
    } catch (final Exception e) {
      // cleanup is best-effort
    }
  }
}
