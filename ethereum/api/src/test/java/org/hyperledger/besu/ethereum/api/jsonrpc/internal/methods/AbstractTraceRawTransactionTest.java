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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.api.jsonrpc.AbstractJsonRpcHttpServiceTest;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/** Shared setup for the trace_rawTransaction tests on the trace/signed-transaction fixtures. */
abstract class AbstractTraceRawTransactionTest extends AbstractJsonRpcHttpServiceTest {
  /** The marker contract stores and returns 42. */
  protected static final String MARKER = "0x0000000000000000000000000000000000001002";

  protected static final String WORD_42 = "0x" + "0".repeat(62) + "2a";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  protected JsonNode rpc(final String method, final List<?> params) throws Exception {
    final String body =
        MAPPER.writeValueAsString(
            Map.of("jsonrpc", "2.0", "id", 1, "method", method, "params", params));
    final Request request =
        new Request.Builder().url(baseUrl).post(RequestBody.create(body, JSON)).build();
    try (final Response response = client.newCall(request).execute()) {
      assertThat(response.code()).isEqualTo(200);
      return MAPPER.readTree(response.body().string());
    }
  }
}
