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
package org.hyperledger.besu.tests.acceptance.jsonrpc;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.rpc.JsonRpcTestCase;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Sends a freshly started node a valid block whose only transaction makes compiled SDIV or SMOD
 * code fall back to the interpreter, and expects the node to accept it.
 *
 * <p>Each transaction is a contract creation with five rounds. A round runs 40,000 iterations of
 * the operation on 100 and 3 with every combination of signs, so that the JIT compiles it, and then
 * runs it once on operands of a kind the loop never used:
 *
 * <ul>
 *   <li>SDIV: 100 divided by -1, -2, -(2^64 + 1), -(2^128 + 1) and -(2^192 + 1)
 *   <li>SMOD: -(2^70 + 5), -(2^130 + 5) and -(2^250 + 7) modulo 7, then -100 modulo 2^192 + 1 and
 *       -(2^250 + 7) modulo 2^254 + 1
 * </ul>
 *
 * <p>SDIV runs on the default configuration. Parallel transaction processing executes a failed
 * block again, up to three times in total, which is why there are several rounds. SMOD can fail
 * only once per node, so its node runs with parallel transaction processing off.
 *
 * <p>Every case gets its own node, because the fallback happens only on a node that has not
 * executed such operands before.
 */
public class ExecutionEngineSignedDivisionAcceptanceTest extends AcceptanceTestBase {
  private static final String GENESIS_FILE = "/dev/dev_prague.json";
  private static final String TEST_CASE_PATH = "/jsonrpc/engine/signed-division/";
  private static final MediaType MEDIA_TYPE_JSON =
      MediaType.parse("application/json; charset=utf-8");

  @ParameterizedTest(name = "{0}, EVM v2 {1}")
  @CsvSource({"sdiv, false, true", "sdiv, true, true", "smod, false, false", "smod, true, false"})
  public void acceptsValidBlock(
      final String operation, final boolean evmV2, final boolean parallelTxProcessing)
      throws IOException {
    final BesuNode node =
        besu.createExecutionEngineGenesisNode(
            "executionEngine",
            GENESIS_FILE,
            true,
            List.of(
                "--Xevm-v2=" + evmV2,
                "--bonsai-parallel-tx-processing-enabled=" + parallelTxProcessing));
    cluster.start(node);

    final JsonRpcTestCase testCase =
        new ObjectMapper()
            .readValue(
                getClass().getResource(TEST_CASE_PATH + operation + "_newPayloadV4.json"),
                JsonRpcTestCase.class);

    final Request request =
        new Request.Builder()
            .url(node.engineRpcUrl().get())
            .post(RequestBody.create(testCase.getRequest().toString(), MEDIA_TYPE_JSON))
            .build();
    try (final Response response = new OkHttpClient().newCall(request).execute()) {
      assertThat(response.code()).isEqualTo(testCase.getStatusCode());
      final ObjectNode actualBody = JsonUtil.objectNodeFromString(response.body().string());
      final ObjectNode expectedBody =
          JsonUtil.objectNodeFromString(testCase.getResponse().toString());
      assertThat(actualBody)
          .withFailMessage(
              "%s\ndid not equal\n %s", actualBody.toPrettyString(), expectedBody.toPrettyString())
          .isEqualTo(expectedBody);
    }
  }
}
