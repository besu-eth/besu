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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

class DebugTraceCallTest {

  private static final Map<String, Object> CALL = Map.of("to", "0x" + "11".repeat(20));

  private final BlockchainQueries blockchainQueries = mock(BlockchainQueries.class);
  private final ProtocolSchedule protocolSchedule =
      mock(ProtocolSchedule.class, Answers.RETURNS_DEEP_STUBS);
  private final BlockHeader blockHeader = mock(BlockHeader.class, Answers.RETURNS_DEEP_STUBS);
  // An unstubbed simulator returns Optional.empty(), reported by DebugTraceCall as INTERNAL_ERROR.
  private final DebugTraceCall method =
      new DebugTraceCall(blockchainQueries, protocolSchedule, mock(TransactionSimulator.class));

  @BeforeEach
  void setUp() {
    when(blockchainQueries.headBlockNumber()).thenReturn(1L);
    when(blockchainQueries.getBlockHeaderByNumber(1L)).thenReturn(Optional.of(blockHeader));
  }

  @Test
  void rejectsTraceConfigWithBlockOverrides() {
    final JsonRpcRequestContext request =
        request(Map.of("blockOverrides", Map.of("time", "0x80000000")));

    assertThatThrownBy(() -> method.response(request))
        .isInstanceOfSatisfying(
            InvalidJsonRpcParameters.class,
            e -> {
              assertThat(e.getRpcErrorType())
                  .isEqualTo(RpcErrorType.INVALID_TRANSACTION_TRACE_PARAMS);
              assertThat(e.getRpcErrorType().getCode()).isEqualTo(-32602);
              assertThat(e).hasMessageContaining("Block overrides are not supported");
            });
  }

  @Test
  void doesNotRejectTraceConfigWithoutBlockOverrides() {
    final JsonRpcResponse response = method.response(request(Map.of("enableMemory", true)));

    // Reaching the (unstubbed) simulator means the trace config passed validation.
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.INTERNAL_ERROR);
  }

  private static JsonRpcRequestContext request(final Map<String, Object> traceConfig) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest("2.0", "debug_traceCall", new Object[] {CALL, "latest", traceConfig}));
  }
}
