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
package org.hyperledger.besu.evmtool;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ForkchoiceUpdatedResultV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.PayloadStatusV1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

// The first engine-test run in a JVM stops the harness's scheduler, after which a valid payload
// cannot run, so the one test that runs a valid payload goes first.
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class EngineTestSubCommandTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** An Amsterdam engine fixture with one self-transfer payload and its block access list. */
  private static final Path FIXTURE =
      Path.of(
          EngineTestSubCommandTest.class.getResource("bal-self-transfer-engine.json").getPath());

  /** An Amsterdam engine fixture whose payload is INVALID for an access list missing an entry. */
  private static final Path INVALID_ACCESS_LIST =
      Path.of(
          EngineTestSubCommandTest.class
              .getResource("bal-invalid-block-access-list-engine.json")
              .getPath());

  private static final String INVALID_BLOCK_HASH =
      "0xe07099693533f8c5b69f1030a1932f538ae19e0631dd9951d6d0a210950d8bbb";

  @TempDir Path tempDir;

  @ParameterizedTest(name = "--bal-sequential={0}")
  @ValueSource(booleans = {false, true})
  void invalidPayloadIsReportedWithBesusError(final boolean sequential) throws IOException {
    final JsonNode result = result(INVALID_ACCESS_LIST, sequential);

    assertThat(result.get("pass").asBoolean()).isTrue();
    assertThat(result.get("rejections")).hasSize(1);
    final JsonNode rejection = result.get("rejections").get(0);
    assertThat(rejection.get("index").asInt()).isZero();
    assertThat(rejection.get("hash").asText()).isEqualTo(INVALID_BLOCK_HASH);
    assertThat(rejection.get("error").asText()).contains("Block access list hash mismatch");
  }

  @Test
  void payloadRejectedForAnotherReasonPassesAndReportsTheError() throws IOException {
    final Path otherReason =
        withFirstPayload(
            payload ->
                payload.put("validationError", "TransactionException.INSUFFICIENT_ACCOUNT_FUNDS"));

    final JsonNode result = result(otherReason, false);

    assertThat(result.get("pass").asBoolean()).isTrue();
    assertThat(result.get("rejections").get(0).get("error").asText())
        .contains("Block access list hash mismatch");
  }

  @Test
  void payloadRejectedForAnotherReasonStillFailsEngineTestsOwnCheckWithoutJsonArray()
      throws IOException {
    final Path otherReason =
        withFirstPayload(
            payload ->
                payload.put("validationError", "TransactionException.INSUFFICIENT_ACCOUNT_FUNDS"));

    assertThat(exitStatus(System.in, otherReason.toString())).isEqualTo(1);
  }

  @Test
  void payloadRejectedWithAJsonRpcErrorIsReportedWithItsCodeMessageAndData() throws IOException {
    final Path wrongVersion =
        withFirstPayload(
            payload -> {
              payload.put("newPayloadVersion", "4");
              payload.remove("validationError");
              payload.put("errorCode", "-32602");
            });

    final JsonNode result = result(wrongVersion, false);

    assertThat(result.get("pass").asBoolean()).isTrue();
    assertThat(result.get("rejections")).hasSize(1);
    final JsonNode rejection = result.get("rejections").get(0);
    assertThat(rejection.get("index").asInt()).isZero();
    assertThat(rejection.has("hash")).isFalse();
    // Code, message, then the error's data, which names the parameter Besu could not decode.
    assertThat(rejection.get("error").asText())
        .startsWith("-32602: Invalid engine payload parameter: ")
        .contains("Failed to decode block parameter");
  }

  @Test
  void testEndedBeforeAnyPayloadHasNoRejections() throws IOException {
    final ObjectNode fixture = (ObjectNode) MAPPER.readTree(INVALID_ACCESS_LIST.toFile());
    ((ObjectNode) fixture.elements().next()).put("network", "NoSuchFork");
    final Path unsupported = tempDir.resolve("unsupported.json");
    Files.writeString(unsupported, MAPPER.writeValueAsString(fixture));

    final JsonNode result = result(unsupported, false);

    assertThat(result.get("pass").asBoolean()).isFalse();
    assertThat(result.get("rejections").isArray()).isTrue();
    assertThat(result.get("rejections")).isEmpty();
  }

  @Test
  @Order(1)
  void balReportPrintsTheDecisionLineOnStderr() throws IOException {
    final PrintStream originalErr = System.err;
    final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
    System.setErr(new PrintStream(stderr, true, UTF_8));
    try {
      run("--json-array", "--bal-report", FIXTURE.toString());
    } finally {
      System.setErr(originalErr);
    }

    final List<String> lines =
        stderr.toString(UTF_8).lines().filter(line -> line.startsWith("{\"event\":")).toList();
    assertThat(lines).hasSize(1);
    final JsonNode line = MAPPER.readTree(lines.get(0));
    assertThat(line.get("event").asText()).isEqualTo("balExecution");
    assertThat(line.get("path").asText()).isEqualTo("parallel");
  }

  @Test
  void validForkchoiceUpdateIsNotAFailure() {
    assertThat(forkchoiceFailure(forkchoiceStatus(EngineStatus.VALID))).isNull();
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(
      value = EngineStatus.class,
      names = {"SYNCING", "INVALID"})
  void forkchoiceUpdateThatIsNotValidFailsTheTest(final EngineStatus status) {
    assertThat(forkchoiceFailure(forkchoiceStatus(status)))
        .isEqualTo("status: expected VALID, got " + status + " (err: null)");
  }

  @Test
  void forkchoiceUpdateAnsweredWithAnErrorFailsTheTest() {
    assertThat(
            forkchoiceFailure(
                new JsonRpcErrorResponse(null, RpcErrorType.INVALID_FORKCHOICE_STATE)))
        .startsWith("error: -38002");
  }

  private static JsonRpcResponse forkchoiceStatus(final EngineStatus status) {
    return new JsonRpcSuccessResponse(
        null, new ForkchoiceUpdatedResultV1(new PayloadStatusV1(status)));
  }

  private static String forkchoiceFailure(final JsonRpcResponse response) {
    final ExecutionEngineJsonRpcMethod forkchoiceUpdated = mock(ExecutionEngineJsonRpcMethod.class);
    when(forkchoiceUpdated.syncResponse(any())).thenReturn(response);
    return EngineTestSubCommand.forkchoiceFailure(forkchoiceUpdated, 4, Hash.ZERO);
  }

  private Path withFirstPayload(final Consumer<ObjectNode> change) throws IOException {
    final ObjectNode fixture = (ObjectNode) MAPPER.readTree(INVALID_ACCESS_LIST.toFile());
    change.accept((ObjectNode) fixture.elements().next().get("engineNewPayloads").get(0));
    final Path changed = tempDir.resolve("changed.json");
    Files.writeString(changed, MAPPER.writeValueAsString(fixture));
    return changed;
  }

  @Test
  void exitStatusIsZeroWhenEveryTestPasses() {
    assertThat(exitStatus(System.in, "--json-array", INVALID_ACCESS_LIST.toString())).isZero();
  }

  @Test
  void exitStatusIsOneWhenATestFails() throws IOException {
    final Path expectedValid = withFirstPayload(payload -> payload.remove("validationError"));
    assertThat(exitStatus(System.in, "--json-array", expectedValid.toString())).isEqualTo(1);
  }

  @Test
  void exitStatusIsTwoWhenAPathDoesNotExist() {
    final Path missing = tempDir.resolve("missing.json");
    assertThat(
            exitStatus(
                System.in, "--json-array", INVALID_ACCESS_LIST.toString(), missing.toString()))
        .isEqualTo(2);
  }

  @Test
  void exitStatusIsTwoWhenAFixtureFileCannotBeParsedAndTheRestStillRun() throws IOException {
    final Path unparseable = Files.writeString(tempDir.resolve("unparseable.json"), "not json");
    final ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    final EngineTestSubCommand engineTest =
        new EngineTestSubCommand(
            new EvmToolCommand(System.in, new PrintWriter(stdout, true, UTF_8)));
    new CommandLine(engineTest)
        .parseArgs("--json-array", INVALID_ACCESS_LIST.toString(), unparseable.toString());
    final PrintStream originalErr = System.err;
    final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
    System.setErr(new PrintStream(stderr, true, UTF_8));
    try {
      engineTest.run();
    } finally {
      System.setErr(originalErr);
    }

    assertThat(engineTest.getExitCode()).isEqualTo(2);
    // The good file's results only: the bad file is named on stderr, never as a result.
    final JsonNode results = MAPPER.readTree(stdout.toString(UTF_8));
    assertThat(results).hasSize(1);
    assertThat(results.get(0).get("pass").asBoolean()).isTrue();
    assertThat(stderr.toString(UTF_8)).contains("Unreadable file " + unparseable);
  }

  @Test
  void exitStatusIsTwoWhenAListedFixtureDoesNotExist() {
    final String fileNames = tempDir.resolve("missing.json") + "\n" + INVALID_ACCESS_LIST + "\n";
    assertThat(exitStatus(new ByteArrayInputStream(fileNames.getBytes(UTF_8)))).isEqualTo(2);
  }

  @Test
  void exitStatusIsTwoWhenTheTestNameFilterIsInvalid() {
    assertThat(
            exitStatus(
                System.in,
                "--json-array",
                "--test-name-regex",
                "(",
                INVALID_ACCESS_LIST.toString()))
        .isEqualTo(2);
  }

  private static int exitStatus(final InputStream in, final String... args) {
    final EngineTestSubCommand engineTest =
        new EngineTestSubCommand(
            new EvmToolCommand(in, new PrintWriter(new ByteArrayOutputStream(), true, UTF_8)));
    new CommandLine(engineTest).parseArgs(args);
    engineTest.run();
    return engineTest.getExitCode();
  }

  private static JsonNode result(final Path fixture, final boolean sequential) throws IOException {
    final List<String> args = new ArrayList<>(List.of("--json-array"));
    if (sequential) {
      args.add("--bal-sequential");
    }
    args.add(fixture.toString());
    return MAPPER.readTree(run(args.toArray(String[]::new))).get(0);
  }

  private static String run(final String... args) {
    final ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    final EngineTestSubCommand engineTest =
        new EngineTestSubCommand(
            new EvmToolCommand(System.in, new PrintWriter(stdout, true, UTF_8)));
    new CommandLine(engineTest).parseArgs(args);
    engineTest.run();
    return stdout.toString(UTF_8);
  }
}
