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
package org.hyperledger.besu.ethereum.api.jsonrpc.methods.fork.london;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StateOverride;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.BlockchainImporter;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcTestMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.CreateAccessListResult;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.testutil.BlockTestUtil;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import com.google.common.io.Resources;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EthCreateAccessListIntegrationTest {

  private static final String METHOD_NAME = "eth_createAccessList";
  private static final Address UNFUNDED_SENDER =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address RECIPIENT =
      Address.fromHexString("0x8888f1f195afa192cfee860698584c030f4c9db1");

  private static JsonRpcTestMethodsFactory BLOCKCHAIN;

  private JsonRpcMethod method;

  @BeforeAll
  public static void setUpOnce() throws Exception {
    final String genesisJson =
        Resources.toString(BlockTestUtil.getTestLondonGenesisUrl(), StandardCharsets.UTF_8);

    BLOCKCHAIN =
        new JsonRpcTestMethodsFactory(
            new BlockchainImporter(BlockTestUtil.getTestLondonBlockchainUrl(), genesisJson));
  }

  @BeforeEach
  public void setUp() {
    final Map<String, JsonRpcMethod> methods = BLOCKCHAIN.methods();
    method = methods.get(METHOD_NAME);
  }

  @Test
  public void shouldSucceedForUnfundedSenderWithoutFeeFields() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder().sender(UNFUNDED_SENDER).to(RECIPIENT).build();

    assertThat(method.response(requestWithParams(callParameter, "latest")))
        .usingRecursiveComparison()
        .isEqualTo(new JsonRpcSuccessResponse(null, new CreateAccessListResult(List.of(), 0x5208)));
  }

  @Test
  public void shouldSucceedWhenSenderCanPayValueButNotFeesWithoutFeeFields() {
    final Wei balance = Wei.fromEth(100);
    final StateOverrideMap stateOverrides = new StateOverrideMap();
    stateOverrides.put(UNFUNDED_SENDER, new StateOverride.Builder().withBalance(balance).build());
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(UNFUNDED_SENDER)
            .to(RECIPIENT)
            .value(balance)
            .build();

    assertThat(method.response(requestWithParams(callParameter, "latest", stateOverrides)))
        .usingRecursiveComparison()
        .isEqualTo(new JsonRpcSuccessResponse(null, new CreateAccessListResult(List.of(), 0x5208)));
  }

  @Test
  public void shouldSucceedWithoutSender() {
    final CallParameter callParameter = ImmutableCallParameter.builder().to(RECIPIENT).build();

    assertThat(method.response(requestWithParams(callParameter, "latest")))
        .usingRecursiveComparison()
        .isEqualTo(new JsonRpcSuccessResponse(null, new CreateAccessListResult(List.of(), 0x5208)));
  }

  @Test
  public void shouldReturnErrorForUnfundedSenderWithFeeFields() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(UNFUNDED_SENDER)
            .to(RECIPIENT)
            .maxFeePerGas(Wei.of(0x10))
            .build();

    final JsonRpcResponse response = method.response(requestWithParams(callParameter, "latest"));

    assertThat(response)
        .usingRecursiveComparison()
        .isEqualTo(
            new JsonRpcErrorResponse(
                null, RpcErrorType.TRANSACTION_UPFRONT_GAS_COST_EXCEEDS_BALANCE));
  }

  private JsonRpcRequestContext requestWithParams(final Object... params) {
    return new JsonRpcRequestContext(new JsonRpcRequest("2.0", METHOD_NAME, params));
  }
}
