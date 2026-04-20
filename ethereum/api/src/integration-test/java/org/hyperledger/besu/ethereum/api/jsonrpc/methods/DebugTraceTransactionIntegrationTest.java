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
package org.hyperledger.besu.ethereum.api.jsonrpc.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.BlockchainImporter;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcTestMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.CallTracerResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.OpCodeLoggerTracerResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.StructLog;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;
import org.hyperledger.besu.testutil.BlockTestUtil;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DebugTraceTransactionIntegrationTest {
  private static final String DEBUG_TRACE_TRANSACTION = "debug_traceTransaction";
  private static JsonRpcTestMethodsFactory blockchain;
  private JsonRpcMethod method;

  @BeforeAll
  public static void setUpOnce() throws Exception {
    final String genesisJson =
        Resources.toString(BlockTestUtil.getTestGenesisUrl(), StandardCharsets.UTF_8);

    blockchain =
        new JsonRpcTestMethodsFactory(
            new BlockchainImporter(BlockTestUtil.getTestBlockchainUrl(), genesisJson));
  }

  @BeforeEach
  public void setUp() {
    final Map<String, JsonRpcMethod> methods = blockchain.methods();
    method = methods.get(DEBUG_TRACE_TRANSACTION);
  }

  @Test
  public void debugTraceTransactionSuccessTest() {
    final Map<String, Boolean> map = Map.of("disableStorage", true);
    final Hash trxHash =
        Hash.fromHexString("0xcef53f2311d7c80e9086d661e69ac11a5f3d081e28e02a9ba9b66749407ac310");
    final Object[] params = new Object[] {trxHash, map};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", DEBUG_TRACE_TRANSACTION, params));

    final JsonRpcResponse response = method.response(request);
    assertThat(response.getType()).isEqualTo(RpcResponseType.SUCCESS);
    OpCodeLoggerTracerResult debugTraceTransactionDetails =
        (OpCodeLoggerTracerResult) ((JsonRpcSuccessResponse) response).getResult();
    assertThat(debugTraceTransactionDetails.getGas()).isEqualTo(23705L);
    assertThat(debugTraceTransactionDetails.getReturnValue()).isEmpty();
    assertThat(debugTraceTransactionDetails.failed()).isFalse();
    assertThat(debugTraceTransactionDetails.getStructLogs()).hasSize(106);
  }

  @Test
  public void debugTraceTransactionMissingTest() {
    final Map<String, Boolean> map = Map.of("disableStorage", true);
    final Object[] params =
        new Object[] {
          Hash.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000000"),
          map
        };
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", DEBUG_TRACE_TRANSACTION, params));
    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    final JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) response;
    assertThat(errorResponse.getErrorType())
        .isEqualByComparingTo(RpcErrorType.TRANSACTION_NOT_FOUND);
  }

  @Test
  public void debugTraceTransactionCallTracerSuccessTest() {
    final Map<String, String> map = Map.of("tracer", "callTracer");
    final Hash trxHash =
        Hash.fromHexString("0xcef53f2311d7c80e9086d661e69ac11a5f3d081e28e02a9ba9b66749407ac310");
    final Object[] params = new Object[] {trxHash, map};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", DEBUG_TRACE_TRANSACTION, params));

    final JsonRpcResponse response = method.response(request);
    assertThat(response.getType()).isEqualTo(RpcResponseType.SUCCESS);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);

    final CallTracerResult result =
        (CallTracerResult) ((JsonRpcSuccessResponse) response).getResult();
    assertThat(result.getGas()).isEqualTo("0x4cb2f"); // 314159
    assertThat(result.getGasUsed()).isEqualTo("0x5c99"); // 23705
    assertThat(result.getInput()).isEqualTo("0x9dc2c8f5");
  }

  @Test
  public void invalidTracerTypeErrorTest() {
    final Map<String, String> map = Map.of("tracer", "invalidTracerType");
    final Hash trxHash =
        Hash.fromHexString("0xcef53f2311d7c80e9086d661e69ac11a5f3d081e28e02a9ba9b66749407ac310");
    final Object[] params = new Object[] {trxHash, map};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", DEBUG_TRACE_TRANSACTION, params));

    assertThatExceptionOfType(InvalidJsonRpcParameters.class)
        .isThrownBy(() -> method.response(request))
        .withMessage("Invalid Tracer Type: invalidTracerType.");
  }

  @Test
  public void debugTraceTransactionSpecificOpcodes() {
    final ObjectMapper jsonMapper = new ObjectMapper();
    final Hash trxHash =
        Hash.fromHexString("0xcef53f2311d7c80e9086d661e69ac11a5f3d081e28e02a9ba9b66749407ac310");

    // Get response with defaults first to take expected JSON
    JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", DEBUG_TRACE_TRANSACTION, List.of(trxHash).toArray()));
    Object result = ((JsonRpcSuccessResponse) method.response(request)).getResult();
    final List<StructLog> defaultLogs = ((OpCodeLoggerTracerResult) result).getStructLogs();

    final AtomicReference<String> previousOpName = new AtomicReference<>();
    final List<JsonNode> expectedJson =
        defaultLogs.stream()
            .filter(
                log -> {
                  final boolean keep =
                      "EQ".equals(log.op())
                          || "EQ".equals(previousOpName.get())
                          || "DIV".equals(log.op())
                          || "DIV".equals(previousOpName.get());
                  previousOpName.set(log.op());
                  return keep;
                })
            .<JsonNode>map(jsonMapper::valueToTree)
            .toList();

    // Get response that we will assert on
    final Map<String, List<String>> map = Map.of("opcodes", Arrays.asList("EQ", "DIV"));
    final Object[] params = new Object[] {trxHash, map};
    request = new JsonRpcRequestContext(new JsonRpcRequest("2.0", DEBUG_TRACE_TRANSACTION, params));
    result = ((JsonRpcSuccessResponse) method.response(request)).getResult();
    final List<StructLog> logs = ((OpCodeLoggerTracerResult) result).getStructLogs();
    List<JsonNode> json = logs.stream().<JsonNode>map(jsonMapper::valueToTree).toList();

    assertThat(json).containsAll(expectedJson);
  }
}
