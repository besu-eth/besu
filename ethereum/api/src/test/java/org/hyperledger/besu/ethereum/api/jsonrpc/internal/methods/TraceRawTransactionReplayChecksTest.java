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

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.AbstractJsonRpcHttpServiceTest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** trace_rawTransaction applies the checks block processing applies to a signed transaction. */
class TraceRawTransactionReplayChecksTest extends AbstractJsonRpcHttpServiceTest {
  private static final BigInteger CHAIN_ID = BigInteger.valueOf(3503995874084926L);
  private static final Address MARKER = Address.fromHexString("0x1002");
  private static final String WORD_42 = "0x" + "0".repeat(62) + "2a";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  // Public fixture keys: key 1 is a plain account with nonce 10, key 3 an account with code.
  private static final KeyPair PLAIN_SENDER = keyPair(1);
  private static final KeyPair CODE_SENDER = keyPair(3);
  private String genesis;

  @Override
  @BeforeEach
  public void setup() {
    // Each case starts the service on its own genesis.
  }

  @Override
  protected BlockchainSetupUtil getBlockchainSetupUtil(final DataStorageFormat format) {
    return createBlockchainSetupUtil(
        "trace/signed-transaction/" + genesis, "trace/signed-transaction/blocks.bin", format);
  }

  private static Stream<Arguments> transactions() {
    return Stream.of(
        Arguments.of(
            "EIP-7825 cap, at the cap",
            "genesis-osaka.json",
            raw(PLAIN_SENDER, 10, 16_777_216L, Bytes.EMPTY),
            null,
            null),
        Arguments.of(
            "EIP-7825 cap, above the cap",
            "genesis-osaka.json",
            raw(PLAIN_SENDER, 10, 16_777_217L, Bytes.EMPTY),
            RpcErrorType.EXCEEDS_TRANSACTION_GAS_LIMIT,
            "Transaction gas limit must be at most 16777216"),
        Arguments.of(
            "EIP-3607 sender with code",
            "genesis-osaka.json",
            raw(CODE_SENDER, 0, 100_000L, Bytes.EMPTY),
            RpcErrorType.TX_SENDER_NOT_AUTHORIZED,
            "has deployed code and so is not authorized to send transactions"),
        // EIP-8037 raises the cap on the gas limit, and caps the larger of the intrinsic gas and
        // the
        // calldata floor at the EIP-7825 value instead. 262,144 bytes of calldata put the floor
        // above it, while the gas limit covers it.
        Arguments.of(
            "EIP-8037 intrinsic gas cap",
            "genesis-amsterdam.json",
            raw(PLAIN_SENDER, 10, 20_000_000L, Bytes.wrap(new byte[262_144])),
            RpcErrorType.INTRINSIC_GAS_EXCEEDS_LIMIT,
            "exceeds gas limit 16777216"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("transactions")
  void shouldApplyBlockProcessingChecks(
      final String name,
      final String genesis,
      final String raw,
      final RpcErrorType error,
      final String reason)
      throws Exception {
    this.genesis = genesis;
    startServiceWithEmptyChain(DataStorageFormat.BONSAI);
    final JsonNode response = rpc("trace_rawTransaction", List.of(raw, List.of("trace")));
    if (error == null) {
      assertThat(response.has("error")).isFalse();
      assertThat(response.path("result").path("output").asText()).isEqualTo(WORD_42);
    } else {
      assertThat(response.has("result")).isFalse();
      assertThat(response.path("error").path("code").asInt()).isEqualTo(error.getCode());
      assertThat(response.path("error").path("message").asText()).contains(reason);
    }
  }

  private static KeyPair keyPair(final int key) {
    final SignatureAlgorithm algorithm = SignatureAlgorithmFactory.getInstance();
    return algorithm.createKeyPair(algorithm.createPrivateKey(Bytes32.leftPad(Bytes.of(key))));
  }

  private static String raw(
      final KeyPair sender, final long nonce, final long gasLimit, final Bytes payload) {
    return Transaction.builder()
        .type(TransactionType.FRONTIER)
        .chainId(CHAIN_ID)
        .nonce(nonce)
        .gasPrice(Wei.of(1_000_000_000L))
        .gasLimit(gasLimit)
        .to(MARKER)
        .value(Wei.ZERO)
        .payload(payload)
        .signAndBuild(sender)
        .encoded()
        .toHexString();
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
