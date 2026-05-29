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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.SHANGHAI;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcObjectMapperFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;

import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EngineGetPayloadV1Test extends AbstractEngineGetPayloadTest {
  private static final ObjectMapper OBJECT_MAPPER =
      JsonRpcObjectMapperFactory.createResponseMapper();

  public EngineGetPayloadV1Test() {
    super(
        (vertx,
            protocolSchedule,
            protocolContext,
            mergeMiningCoordinator,
            blockResultFactory,
            engineCallListener) ->
            new EngineGetPayloadV1(
                vertx,
                protocolSchedule,
                protocolContext,
                mergeMiningCoordinator,
                blockResultFactory,
                engineCallListener,
                null,
                SHANGHAI));
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getPayloadV1");
  }

  @Override
  @Test
  public void shouldReturnBlockForKnownPayloadId() {
    final var resp = resp(RpcMethod.ENGINE_GET_PAYLOAD_V1.getMethodName(), mockPid);
    assertThat(resp).isInstanceOf(JsonRpcSuccessResponse.class);
    Optional.of(resp)
        .map(JsonRpcSuccessResponse.class::cast)
        .ifPresent(
            r -> {
              assertThat(r.getResult()).isInstanceOf(ExecutionPayloadV1.class);
              final ExecutionPayloadV1 res = (ExecutionPayloadV1) r.getResult();
              assertThat(res.getBlockHash()).isEqualTo(mockHeader.getHash());
              assertThat(res.getPrevRandao()).isEqualTo(mockHeader.getPrevRandao().orElse(null));

              final Map<String, Object> wirePayload =
                  OBJECT_MAPPER.convertValue(res, new TypeReference<>() {});
              assertThat(wirePayload)
                  .containsEntry("blockHash", mockHeader.getHash().toString())
                  .containsEntry(
                      "prevRandao", mockHeader.getPrevRandao().orElseThrow().toHexString());
            });
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Override
  protected String getMethodName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_V1.getMethodName();
  }

  @Override
  protected long getValidPayloadTimestamp() {
    // V1 has no strict fork validation, use Paris era timestamp
    return 15L;
  }
}
