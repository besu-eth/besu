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

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetClientVersionResultV1;

import java.util.List;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class EngineGetClientVersionV1Test {

  private static final String ENGINE_CLIENT_CODE = "BU";
  private static final String ENGINE_CLIENT_NAME = "Besu";

  private static final String CLIENT_VERSION = "v25.6.7-dev-abcdef12";
  private static final String COMMIT = "abcdef12";

  private EngineGetClientVersionV1 getClientVersion;

  @BeforeEach
  void before() {
    getClientVersion =
        new EngineGetClientVersionV1(
            null,
            Mockito.mock(ProtocolContext.class),
            Mockito.mock(Vertx.class),
            Mockito.mock(EngineCallListener.class),
            CLIENT_VERSION,
            COMMIT);
  }

  @Test
  void testGetName() {
    assertThat(getClientVersion.getName()).isEqualTo("engine_getClientVersionV1");
  }

  @Test
  void testSyncResponse() {
    JsonRpcRequestContext request = new JsonRpcRequestContext(new JsonRpcRequest("v", "m", null));
    JsonRpcResponse actualResult = getClientVersion.syncResponse(request);

    assertThat(actualResult).isInstanceOf(JsonRpcSuccessResponse.class);
    JsonRpcSuccessResponse successResponse = (JsonRpcSuccessResponse) actualResult;

    assertThat(successResponse.getResult()).isInstanceOf(List.class);

    List<?> resultList = (List<?>) successResponse.getResult();
    assertThat(resultList).hasSize(1);

    Object firstElement = resultList.get(0);
    assertThat(firstElement).isInstanceOf(EngineGetClientVersionResultV1.class);

    EngineGetClientVersionResultV1 actualEngineGetClientVersionResultV1 =
        (EngineGetClientVersionResultV1) firstElement;

    assertThat(actualEngineGetClientVersionResultV1.getName()).isEqualTo(ENGINE_CLIENT_NAME);
    assertThat(actualEngineGetClientVersionResultV1.getCode()).isEqualTo(ENGINE_CLIENT_CODE);
    assertThat(actualEngineGetClientVersionResultV1.getVersion()).isEqualTo(CLIENT_VERSION);
    assertThat(actualEngineGetClientVersionResultV1.getCommit()).isEqualTo(COMMIT);
  }
}
