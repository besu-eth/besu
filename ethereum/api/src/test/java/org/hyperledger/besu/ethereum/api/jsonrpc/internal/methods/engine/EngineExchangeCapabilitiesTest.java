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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod.ENGINE_EXCHANGE_CAPABILITIES;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EngineExchangeCapabilitiesTest {
  private EngineExchangeCapabilities method;
  private static final Vertx vertx = Vertx.vertx();

  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolContext protocolContext;

  @Mock private EngineCallListener engineCallListener;

  @BeforeEach
  public void setUp() {
    this.method =
        new EngineExchangeCapabilities(
            protocolSchedule, protocolContext, vertx, engineCallListener);
  }

  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_exchangeCapabilities");
  }

  @Test
  public void shouldReturnAllSupportedEngineApiRpcNames() {
    var response = resp(List.of("engine_newPayloadV1", "engine_newPayloadV2", "nonsense"));

    var result = fromSuccessResp(response);
    assertThat(result).allMatch(name -> name.startsWith("engine_"));
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldNotReturnSelf() {
    var response = resp(Collections.emptyList());

    var result = fromSuccessResp(response);
    assertThat(result).allMatch(name -> !ENGINE_EXCHANGE_CAPABILITIES.getMethodName().equals(name));
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  private JsonRpcResponse resp(final List<String> capabilitiesParam) {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0",
                ENGINE_EXCHANGE_CAPABILITIES.getMethodName(),
                new Object[] {capabilitiesParam})));
  }

  @SuppressWarnings("unchecked")
  private List<String> fromSuccessResp(final JsonRpcResponse resp) {
    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    return Optional.of(resp)
        .map(JsonRpcSuccessResponse.class::cast)
        .map(JsonRpcSuccessResponse::getResult)
        .map(List.class::cast)
        .get();
  }
}
