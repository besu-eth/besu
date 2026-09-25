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
import org.junit.jupiter.params.provider.ValueSource;

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
  @ValueSource(strings = {"fe", "50"})
  void parentResumesAfterImmediateChildHalt(final String childOpcode) throws Exception {
    // CREATE executes a single INVALID or stack-underflowing POP; the parent then returns 42.
    final JsonNode result =
        traceVm("60" + childOpcode + "600053600160006000f050602a60005260206000f3", 1_000_000);
    assertThat(result.get("output").asText())
        .isEqualTo("0x000000000000000000000000000000000000000000000000000000000000002a");
    final JsonNode ops = result.get("vmTrace").get("ops");
    final int[] expectedPcs = {0, 2, 4, 5, 7, 9, 11, 12, 13, 15, 17, 18, 20, 22};
    assertThat(ops.size()).isEqualTo(expectedPcs.length);
    for (int i = 0; i < expectedPcs.length; i++) {
      assertThat(ops.get(i).get("pc").asInt()).isEqualTo(expectedPcs[i]);
    }
    final JsonNode child = ops.get(6).get("sub");
    assertThat(child.get("ops").isEmpty()).isTrue();
    final JsonNode root = traceVm(childOpcode, 1_000_000).get("vmTrace");
    assertThat(root.get("ops").isEmpty()).isTrue();
    assertThat(root.get("code").asText()).isEqualTo("0x" + childOpcode);
    assertThat(child.get("code").asText()).isEqualTo(root.get("code").asText());
  }

  @Test
  void repeatedFailedCallsUseTheirOwnResumptionGas() throws Exception {
    // The genesis target executes PUSH1 1 followed by an invalid opcode. Repeated calls
    // at the same PC expose an index that drifts behind the raw frames after each halt.
    final String beforeCall =
        "60405b60006000600060006000" + "7300c00000000000000000000000000000000000006103e8";
    final JsonNode result = traceVm(beforeCall + "f150600190038060025700", 1_000_000);
    assertThat(result.get("trace").get(0).has("error")).isFalse();
    final JsonNode ops = result.get("vmTrace").get("ops");
    final int callPc = beforeCall.length() / 2;
    int calls = 0;
    for (int i = 0; i < ops.size(); i++) {
      final JsonNode op = ops.get(i);
      if (op.get("pc").asInt() == callPc) {
        calls++;
        final JsonNode pop = ops.get(i + 1);
        assertThat(pop.get("pc").asInt()).isEqualTo(callPc + 1);
        // The next parent instruction is POP, costing two gas.
        assertThat(op.get("ex").get("used").asLong())
            .as("call %s resumption gas", calls)
            .isEqualTo(pop.get("ex").get("used").asLong() + 2);
      }
    }
    assertThat(calls).isEqualTo(64);
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
