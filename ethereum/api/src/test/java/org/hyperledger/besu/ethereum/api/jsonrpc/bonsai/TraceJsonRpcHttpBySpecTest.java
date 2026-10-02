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
  @CsvSource({"f1,0", "f1,4", "f4,0", "f4,4"})
  void callMemoryDoesNotIncludeLaterParentReturn(final String opcode, final int outputSize)
      throws Exception {
    // Store input, call identity with output at offset 32, then return unrelated bytes at offset 0.
    final String beforeCall =
        "63deadbeef600052600"
            + outputSize
            + "60206004601c"
            + (opcode.equals("f1") ? "6000" : "")
            + "6004615000";
    final String code = beforeCall + opcode + "5063cafebabe6000526004601cf3";
    final String request =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"trace_call\",\"params\":[{"
            + "\"from\":\"0x627306090abab3a6e1400e9345bc60c78a8bef57\","
            + "\"gas\":\"0xfffff\",\"data\":\"0x"
            + code
            + "\"},[\"vmTrace\"],\"latest\"]}";
    try (Response response =
        client
            .newCall(
                new Request.Builder().url(baseUrl).post(RequestBody.create(request, JSON)).build())
            .execute()) {
      final JsonNode result = new ObjectMapper().readTree(response.body().string()).get("result");
      assertThat(result.get("output").asText()).isEqualTo("0xcafebabe");
      JsonNode call = null;
      for (final JsonNode op : result.get("vmTrace").get("ops")) {
        if (op.get("pc").asInt() == beforeCall.length() / 2) {
          call = op;
          break;
        }
      }
      assertThat(call).isNotNull();
      final JsonNode memory = call.get("ex").get("mem");
      if (outputSize == 0) {
        assertThat(memory.isNull()).isTrue();
      } else {
        assertThat(memory.get("off").asInt()).isEqualTo(32);
        assertThat(memory.get("data").asText()).isEqualTo("0xdeadbeef");
      }
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
