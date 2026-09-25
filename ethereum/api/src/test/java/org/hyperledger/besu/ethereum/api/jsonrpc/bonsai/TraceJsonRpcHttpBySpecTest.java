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

  @Test
  void callManyPricesEachCallFromItsOwnOriginalStorage() throws Exception {
    // 0x0090... increments storage slot 0 (3 at the chain head). Each trace_callMany entry is a
    // separate transaction, so the second call's SSTORE sees original == current == 4 and costs
    // the same EIP-2200 5,000 gas as the first, not the 800 of a slot already dirty in the call.
    final String call =
        "[{\"from\":\"0xfe3b557e8fb62b89f4916b721be55ceb828dbd73\","
            + "\"to\":\"0x0090000000000000000000000000000000000000\"},"
            + "[\"trace\",\"vmTrace\",\"stateDiff\"]]";
    final JsonNode results = traceCallMany(call + "," + call);
    for (int i = 0; i < 2; i++) {
      final JsonNode result = results.get(i);
      // 0x16c6 = SLOAD 800 + SSTORE 5,000 + MSTORE8 6 + eight 3-gas ops; ops[8] is the SSTORE.
      assertThat(result.get("trace").get(0).get("result").get("gasUsed").asText())
          .isEqualTo("0x16c6");
      assertThat(result.get("vmTrace").get("ops").get(8).get("cost").asLong()).isEqualTo(5000);
      final JsonNode slot =
          result
              .get("stateDiff")
              .get("0x0090000000000000000000000000000000000000")
              .get("storage")
              .get("0x0000000000000000000000000000000000000000000000000000000000000000")
              .get("*");
      assertThat(slot.get("from").asText()).isEqualTo(String.format("0x%064x", 3 + i));
      assertThat(slot.get("to").asText()).isEqualTo(String.format("0x%064x", 4 + i));
    }
  }

  private JsonNode traceCallMany(final String calls) throws Exception {
    final String request =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"trace_callMany\",\"params\":[["
            + calls
            + "],\"latest\"]}";
    try (Response response =
        client
            .newCall(
                new Request.Builder().url(baseUrl).post(RequestBody.create(request, JSON)).build())
            .execute()) {
      assertThat(response.code()).isEqualTo(200);
      final JsonNode body = new ObjectMapper().readTree(response.body().string());
      assertThat(body.has("error")).isFalse();
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
