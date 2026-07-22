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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.SYNCING;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConstructorArgumentsBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePreparePayloadParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EnginePreparePayloadResult;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.Optional;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EnginePreparePayloadDebugTest {
  private static final Vertx vertx = Vertx.vertx();
  EnginePreparePayloadDebug method;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolContext protocolContext;
  @Mock private EngineCallListener engineCallListener;
  @Mock private MergeMiningCoordinator mergeCoordinator;
  @Mock private MergeContext mergeContext;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private JsonRpcRequestContext requestContext;

  @Mock private EnginePreparePayloadParameter param;

  @BeforeEach
  public void setUp() throws JsonRpcParameterException {
    when(protocolContext.safeConsensusContext(MergeContext.class))
        .thenReturn(Optional.of(mergeContext));
    when(requestContext.getOptionalParameter(0, EnginePreparePayloadParameter.class))
        .thenReturn(Optional.of(param));
    method =
        spy(
            new EnginePreparePayloadDebug(
                new ConstructorArgumentsBuilder()
                    .protocolSchedule(protocolSchedule)
                    .protocolContext(protocolContext)
                    .vertx(vertx)
                    .engineCallListener(engineCallListener)
                    .mergeCoordinator(mergeCoordinator)
                    .maxRequestBlocks(0)
                    .build()));
  }

  @Test
  public void shouldReturnSyncing() {
    when(mergeContext.isSyncing()).thenReturn(true);
    var resp = method.syncResponse(requestContext);
    assertThat(resp).isInstanceOf(JsonRpcSuccessResponse.class);
    var result = ((JsonRpcSuccessResponse) resp).getResult();
    assertThat(result).isInstanceOf(EnginePreparePayloadResult.class);
    var payloadResult = (EnginePreparePayloadResult) result;
    assertThat(payloadResult.getStatus()).isEqualTo(SYNCING.name());
  }

  @Test
  public void shouldReturnPayloadId() {
    checkForPayloadId();
  }

  @Test
  public void shouldReturnPayloadIdWhenNoParams() throws JsonRpcParameterException {
    when(requestContext.getOptionalParameter(0, EnginePreparePayloadParameter.class))
        .thenReturn(Optional.empty());
    checkForPayloadId();
  }

  private void checkForPayloadId() {
    doAnswer(__ -> Optional.of(new PayloadIdentifier(0xdeadbeefL)))
        .when(method)
        .generatePayload(any());
    var resp = method.syncResponse(requestContext);
    assertThat(resp).isInstanceOf(JsonRpcSuccessResponse.class);
    var result = ((JsonRpcSuccessResponse) resp).getResult();
    assertThat(result).isInstanceOf(EnginePreparePayloadResult.class);
    var payloadResult = (EnginePreparePayloadResult) result;
    assertThat(payloadResult.getStatus()).isEqualTo(VALID.name());
    assertThat(payloadResult.getPayloadId()).isEqualTo("0xdeadbeef");
  }
}
