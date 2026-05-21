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
package org.hyperledger.besu.tests.acceptance.jsonrpc;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.WaitUtils;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.BesuNodeConfigurationBuilder;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.genesis.GenesisConfigurationFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.TransactionReceipt;

public class DebugGetModifiedAccountsAcceptanceTest extends AcceptanceTestBase {

  private static final MediaType MEDIA_TYPE_JSON =
      MediaType.parse("application/json; charset=utf-8");

  private final OkHttpClient httpClient = new OkHttpClient();
  private final ObjectMapper mapper = new ObjectMapper();

  private BesuNode node;
  private Account recipient;

  @BeforeEach
  public void setUpNode() throws Exception {
    node =
        besu.create(
            new BesuNodeConfigurationBuilder()
                .name("debugModifiedAccounts")
                .jsonRpcEnabled()
                .jsonRpcDebug()
                .miningEnabled()
                .devMode(false)
                .dataStorageConfiguration(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG)
                .genesisConfigProvider(GenesisConfigurationFactory::createQbftGenesisConfig)
                .build());
    cluster.start(node);
  }

  @Test
  public void shouldReturnModifiedAccountsByNumberAndHash() throws IOException {
    recipient = accounts.createAccount("modified-recipient");
    final Hash transactionHash = node.execute(accountTransactions.createTransfer(recipient, 1));

    final TransactionReceipt receipt = waitForTransactionReceipt(transactionHash);
    final BigInteger blockNumber = receipt.getBlockNumber();
    final BigInteger parentBlockNumber = blockNumber.subtract(BigInteger.ONE);
    final EthBlock.Block block =
        node.execute(ethTransactions.block(DefaultBlockParameter.valueOf(blockNumber)));
    final EthBlock.Block parentBlock =
        node.execute(ethTransactions.block(DefaultBlockParameter.valueOf(parentBlockNumber)));

    assertModifiedAccounts(
        callDebug("debug_getModifiedAccountsByNumber", List.of(quantity(blockNumber))));
    assertModifiedAccounts(
        callDebug(
            "debug_getModifiedAccountsByNumber",
            List.of(quantity(parentBlockNumber), quantity(blockNumber))));
    assertModifiedAccounts(callDebug("debug_getModifiedAccountsByHash", List.of(block.getHash())));
    assertModifiedAccounts(
        callDebug(
            "debug_getModifiedAccountsByHash", List.of(parentBlock.getHash(), block.getHash())));
  }

  private TransactionReceipt waitForTransactionReceipt(final Hash transactionHash) {
    final AtomicReference<Optional<TransactionReceipt>> maybeReceipt =
        new AtomicReference<>(Optional.empty());
    WaitUtils.waitFor(
        120,
        () -> {
          maybeReceipt.set(
              node.execute(ethTransactions.getTransactionReceipt(transactionHash.toHexString())));
          assertThat(maybeReceipt.get()).isPresent();
        });
    return maybeReceipt.get().orElseThrow();
  }

  private void assertModifiedAccounts(final JsonNode result) {
    assertThat(result.isArray()).isTrue();
    assertThat(result)
        .extracting(address -> address.asText().toLowerCase(Locale.ROOT))
        .containsExactlyInAnyOrder(
            recipientAddress().toLowerCase(Locale.ROOT),
            senderAddress().toLowerCase(Locale.ROOT),
            node.getAddress().toHexString().toLowerCase(Locale.ROOT));
  }

  private JsonNode callDebug(final String method, final List<String> params) throws IOException {
    final var requestBody = mapper.createObjectNode();
    requestBody.put("jsonrpc", "2.0");
    requestBody.put("method", method);
    final ArrayNode paramsNode = requestBody.putArray("params");
    params.forEach(paramsNode::add);
    requestBody.put("id", 1);

    try (final Response response =
        httpClient
            .newCall(
                new Request.Builder()
                    .url(node.jsonRpcBaseUrl().orElseThrow())
                    .post(RequestBody.create(requestBody.toString(), MEDIA_TYPE_JSON))
                    .build())
            .execute()) {
      assertThat(response.code()).isEqualTo(200);
      final JsonNode responseBody = mapper.readTree(response.body().string());
      assertThat(responseBody.has("error"))
          .withFailMessage("Unexpected JSON-RPC error: %s", responseBody)
          .isFalse();
      return responseBody.get("result");
    }
  }

  private String recipientAddress() {
    return recipient.getAddress();
  }

  private String senderAddress() {
    return accounts.getPrimaryBenefactor().getAddress();
  }

  private String quantity(final BigInteger value) {
    return "0x" + value.toString(16);
  }
}
