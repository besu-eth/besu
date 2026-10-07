/*
 * Copyright contributors to Hyperledger Besu.
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

import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.AbstractJsonRpcHttpServiceTest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TraceRawTransactionSignedTest extends AbstractJsonRpcHttpServiceTest {
  private static final String SENDER = "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf";
  private static final String MARKER = "0x0000000000000000000000000000000000001002";
  private static final String WORD_42 = "0x" + "0".repeat(62) + "2a";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  @BeforeEach
  public void setup() {
    startServiceWithEmptyChain(DataStorageFormat.BONSAI);
  }

  @Override
  protected BlockchainSetupUtil getBlockchainSetupUtil(final DataStorageFormat format) {
    return createBlockchainSetupUtil(
        "trace/signed-transaction/genesis.json", "trace/signed-transaction/blocks.bin", format);
  }

  @Override
  protected ApiConfiguration createApiConfiguration() {
    return ImmutableApiConfiguration.builder().gasCap(100_000L).build();
  }

  // Public fixture keys 1 (transaction) and 2 (authorization), chain ID 3503995874084926,
  // sender nonce 10. The marker stores and returns 42; each transaction runs against genesis.
  private record SignedCase(String name, String raw, RpcErrorType error, boolean halt) {}

  private static Stream<Arguments> transactions() {
    final List<SignedCase> cases =
        List.of(
            new SignedCase(
                "legacy-valid",
                "0xf86b0a842da282a9830186a094000000000000000000000000000000000000100201808718e5bb3abd109fa0a2f62f19fe621aee70421dbc7406655686d0cff2bedd6aaccbddf7f94b497168a068ee9aa988adb25c24c1dd48868f13ac72615bd55e2b471953c8ee9c824e66fe",
                null,
                false),
            new SignedCase(
                "legacy-high-s",
                "0xf86b0a842da282a9830186a094000000000000000000000000000000000000100201808718e5bb3abd10a0a0a2f62f19fe621aee70421dbc7406655686d0cff2bedd6aaccbddf7f94b497168a09711655677524da3db3e22b77970ec52484d8111511d59226c096ff04de7da43",
                RpcErrorType.INVALID_TRANSACTION_SIGNATURE,
                false),
            new SignedCase(
                "type-2-valid",
                "0x02f86e870c72dd9d5e883e0a01842da282a9830186a09400000000000000000000000000000000000010020180c080a0fa2eeb3f337ecbaa1012f43f625ede82b8ba76e9b4fd770e5fe0ccfc9f1dc291a071ea33c7ed408874b06ac069c1e31deb4a7ebb62e780e11d46f9b9a54bf73184",
                null,
                false),
            new SignedCase(
                "type-4-empty",
                "0x04f86f870c72dd9d5e883e0a01842da282a9830186a09400000000000000000000000000000000000010020180c0c080a01afd52c97780621c27da78bb2637b9607fbd74a1e1511a5c77a8271361c25e77a035c83bd0944394b6a9165b3d88787af70f8b593aba5f3fc1c8e0f7e98ace0281",
                RpcErrorType.INVALID_TRANSACTION_TYPE,
                false),
            new SignedCase(
                "type-4-valid",
                "0x04f8d3870c72dd9d5e883e0a01842da282a9830186a09400000000000000000000000000000000000010020180c0f863f861870c72dd9d5e883e9400000000000000000000000000000000000010028080a004f525e9d77f2cdbab13f140fed78835173e6573e7fe0ae1d2b271a76ef404b1a039858dc6e5d5cbb3472e07f754749a6358faaf2560cd9415b28b1d631965891d80a0c04d3fce953e01884c84aeb19de31fbee35ee83e88236c58245d52e68b7e6e1fa014c121437107b680a90e298ae714d36bd51341e70b9c02bb346a92bfe4da4454",
                null,
                false),
            new SignedCase(
                "signed-gas-over-cap",
                "0xf86b0a842da282a983030d4094000000000000000000000000000000000000100201808718e5bb3abd10a0a02bfe1071ab946fcf37c9c53b25e79519a9b3123276ce10d17eb998020befefb3a04fd7fb37cf8dff46b28ee7d6a1b23383c787987387d5ad116297dd09c407f949",
                RpcErrorType.EXCEEDS_TRANSACTION_GAS_LIMIT,
                false),
            new SignedCase(
                "execution-out-of-gas",
                "0xf86a0a842da282a982520894000000000000000000000000000000000000100201808718e5bb3abd109fa0438c25241c47cb30acced697295dbdc7efd1c51379f4d9a278676f0a234be35ba03a1865e41b9a3ae09cb8e900d0295dc60fa0d1412a2b6658dffc7498eb8ca34d",
                null,
                true));
    return cases.stream()
        .flatMap(
            tx ->
                Stream.of(
                        new String[] {"trace"},
                        new String[] {"stateDiff"},
                        new String[] {"vmTrace"},
                        new String[] {"trace", "stateDiff", "vmTrace"})
                    .map(types -> Arguments.of(tx, types)));
  }

  @ParameterizedTest(name = "{0}, {1}")
  @MethodSource("transactions")
  void shouldValidateAndExecuteTheOriginalSignedTransaction(
      final SignedCase transaction, final String[] traceTypes) throws Exception {
    final JsonNode response =
        rpc("trace_rawTransaction", List.of(transaction.raw(), List.of(traceTypes)));
    if (transaction.error() != null) {
      assertThat(response.has("result")).isFalse();
      assertThat(response.path("error").path("code").asInt())
          .isEqualTo(transaction.error().getCode());
    } else {
      assertThat(response.has("error")).isFalse();
      final JsonNode result = response.get("result");
      assertThat(result.path("output").asText()).isEqualTo(transaction.halt() ? "0x" : WORD_42);
      if (Arrays.asList(traceTypes).contains("trace")) {
        final JsonNode frame = result.path("trace").get(0);
        if (transaction.halt()) {
          assertThat(frame.path("error").asText()).isEqualTo("Out of gas");
        } else {
          assertThat(frame.has("error")).isFalse();
          assertThat(frame.path("action").path("gas").asText())
              .isEqualTo(transaction.name().equals("type-4-valid") ? "0xd2f0" : "0x13498");
        }
      }
    }
    assertThat(rpc("eth_getTransactionCount", List.of(SENDER, "latest")).path("result").asText())
        .isEqualTo("0xa");
    assertThat(rpc("eth_getStorageAt", List.of(MARKER, "0x0", "latest")).path("result").asText())
        .isEqualTo("0x" + "0".repeat(64));
  }

  private JsonNode rpc(final String method, final List<?> params) throws Exception {
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
