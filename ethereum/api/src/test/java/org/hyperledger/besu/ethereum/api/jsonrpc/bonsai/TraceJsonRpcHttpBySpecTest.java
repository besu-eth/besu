/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.api.jsonrpc.bonsai;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.api.jsonrpc.AbstractJsonRpcHttpBySpecTest;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TraceJsonRpcHttpBySpecTest extends AbstractJsonRpcHttpBySpecTest {

  @Override
  protected void doSetup() throws Exception {
    setupBonsaiBlockchain();
    startService();
  }

  @Override
  protected BlockchainSetupUtil getBlockchainSetupUtil(final DataStorageFormat storageFormat) {
    return createBlockchainSetupUtil(
        "trace/chain-data/genesis.json", "trace/chain-data/blocks.bin", storageFormat);
  }

  @ParameterizedTest
  @CsvSource({"53086, false", "53089, true"})
  void rootOpcodeReportsEffectsOnlyWithSufficientGas(final long gas, final boolean succeeds)
      throws Exception {
    // Istanbul creation costs 53,080 intrinsic gas. Two PUSHes cost six more;
    // ADD needs another three, so only the second case can execute it.
    final JsonNode result = traceVm("6001600201", gas);
    final JsonNode ops = result.get("vmTrace").get("ops");
    final JsonNode add = ops.get(ops.size() - 1);
    assertThat(add.get("pc").asInt()).isEqualTo(4);
    if (succeeds) {
      assertThat(result.get("trace").get(0).has("error")).isFalse();
      assertThat(add.get("ex").get("push").get(0).asText()).isEqualTo("0x3");
      assertThat(add.get("ex").get("used").asLong()).isZero();
    } else {
      assertThat(result.get("trace").get(0).get("error").asText()).isEqualTo("Out of gas");
      assertThat(add.get("ex").isNull()).isTrue();
    }
  }

  // Pushes CALL(gas=1, to=0x06 ecAdd, no value/input/output), then STOPs. ecAdd needs 150 gas, so
  // the precompile halts while the CALL itself succeeds and pushes 0.
  private static final String UNDERFUNDED_EC_ADD_CALL = "6000600060006000600060066001f100";

  @Test
  void rootCallToUnderfundedPrecompileKeepsReport() throws Exception {
    assertUnderfundedPrecompileCallReport(
        traceVm(UNDERFUNDED_EC_ADD_CALL, 100_000).get("vmTrace").get("ops"));
  }

  @Test
  void nestedCallToUnderfundedPrecompileKeepsReport() throws Exception {
    final JsonNode result = traceVm(createChild(UNDERFUNDED_EC_ADD_CALL), 200_000);
    final JsonNode create = opAt(result.get("vmTrace").get("ops"), 26);
    assertThat(create.get("ex").get("push").get(0).asText()).isNotEqualTo("0x0");
    assertUnderfundedPrecompileCallReport(create.get("sub").get("ops"));
  }

  @Test
  void rootCodeDepositOutOfGasKeepsLastOpcodeReport() throws Exception {
    // PUSH1 0x20 PUSH1 0 RETURN: execution fits in the gas limit, the 6,400 gas code deposit does
    // not. The tracer stamps the deposit failure onto the second PUSH1, which itself succeeded.
    final JsonNode result = traceVm("60206000f3", 55_000);
    assertThat(result.get("trace").get(0).get("error").asText()).isEqualTo("Out of gas");
    final JsonNode push = opAt(result.get("vmTrace").get("ops"), 2);
    assertThat(push.get("ex").isNull()).isFalse();
    assertThat(push.get("ex").get("push").get(0).asText()).isEqualTo("0x0");
    assertThat(push.get("ex").get("used").asLong()).isPositive();
  }

  @Test
  void nestedOpcodeOutOfGasOmitsReport() throws Exception {
    // Child init code PUSH4 0xffffffff MLOAD cannot pay for the memory expansion.
    final JsonNode result = traceVm(createChild("63ffffffff51"), 200_000);
    assertThat(result.get("trace").get(0).has("error")).isFalse();
    final JsonNode create = opAt(result.get("vmTrace").get("ops"), 16);
    assertThat(create.get("ex").get("push").get(0).asText()).isEqualTo("0x0");
    final JsonNode mload = opAt(create.get("sub").get("ops"), 5);
    assertThat(mload.get("ex").isNull()).isTrue();
  }

  private static void assertUnderfundedPrecompileCallReport(final JsonNode ops) {
    final JsonNode call = opAt(ops, 14);
    final JsonNode stop = opAt(ops, 15);
    assertThat(call.get("ex").isNull()).isFalse();
    assertThat(call.get("ex").get("push").get(0).asText()).isEqualTo("0x0");
    assertThat(call.get("ex").get("used").asLong())
        .isEqualTo(stop.get("ex").get("used").asLong())
        .isPositive();
  }

  private static JsonNode opAt(final JsonNode ops, final int pc) {
    for (final JsonNode op : ops) {
      if (op.get("pc").asInt() == pc) {
        return op;
      }
    }
    throw new AssertionError("no op at pc " + pc + " in " + ops);
  }

  /**
   * Root init code that stores {@code childInitCode} in memory, CREATEs a child running it (at pc
   * {@code size + 10}), then STOPs.
   */
  private static String createChild(final String childInitCode) {
    final int size = childInitCode.length() / 2;
    return String.format(
        "%02x%s60005260%02x60%02x6000f000", 0x5f + size, childInitCode, size, 32 - size);
  }

  private JsonNode traceVm(final String code, final long gas) throws Exception {
    final String request =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"trace_call\",\"params\":[{"
            + "\"from\":\"0x627306090abab3a6e1400e9345bc60c78a8bef57\",\"gas\":\"0x"
            + Long.toHexString(gas)
            + "\",\"data\":\"0x"
            + code
            + "\"},[\"vmTrace\",\"trace\"],\"latest\"]}";
    try (Response response =
        client
            .newCall(
                new Request.Builder().url(baseUrl).post(RequestBody.create(request, JSON)).build())
            .execute()) {
      assertThat(response.code()).isEqualTo(200);
      final JsonNode body = new ObjectMapper().readTree(response.body().string());
      assertThat(body.has("error")).isFalse();
      assertThat(body.has("result")).isTrue();
      return body.get("result");
    }
  }

  public static Object[][] specs() {
    return AbstractJsonRpcHttpBySpecTest.findSpecFiles(
        new String[] {
          "trace/specs/trace-block",
          "trace/specs/trace-get",
          "trace/specs/trace-transaction",
          "trace/specs/replay-trace-transaction/flat",
          "trace/specs/replay-trace-transaction/vm-trace",
          "trace/specs/replay-trace-transaction/statediff",
          "trace/specs/replay-trace-transaction/all",
          "trace/specs/replay-trace-transaction/halt-cases",
          "trace/specs/trace-filter",
          "trace/specs/trace-call",
          "trace/specs/trace-callMany",
          "trace/specs/trace-raw-transaction"
        });
  }
}
