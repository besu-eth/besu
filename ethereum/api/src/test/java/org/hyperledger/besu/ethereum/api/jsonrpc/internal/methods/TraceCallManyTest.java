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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulatorResult;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Optional;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TraceCallManyTest {

  private static final String CALL =
      "[{\"from\":\"0xfe3b557e8fb62b89f4916b721be55ceb828dbd73\","
          + "\"to\":\"0x0010000000000000000000000000000000000000\",\"gasPrice\":\"0x0\"},"
          + "[\"trace\"]]";

  @Mock private BlockchainQueries blockchainQueries;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ProtocolSchedule protocolSchedule;

  @Mock private TransactionSimulator transactionSimulator;
  @Mock private BlockHeader blockHeader;
  @Mock private MutableWorldState worldState;
  @Mock private WorldUpdater updater;
  @Mock private WorldUpdater callUpdater;

  private TraceCallMany method;

  @BeforeEach
  public void setUp() {
    method = new TraceCallMany(blockchainQueries, protocolSchedule, transactionSimulator);
    when(blockHeader.getBlockHash()).thenReturn(Hash.ZERO);
    when(blockchainQueries.getBlockHeaderByNumber(1L)).thenReturn(Optional.of(blockHeader));
    when(blockchainQueries.getAndMapWorldState(eq(Hash.ZERO), any()))
        .thenAnswer(
            invocation ->
                invocation
                    .<Function<MutableWorldState, Optional<?>>>getArgument(1)
                    .apply(worldState));
    when(transactionSimulator.getEffectiveWorldStateUpdater(worldState)).thenReturn(updater);
    when(updater.updater()).thenReturn(callUpdater);
  }

  @Test
  public void invalidCallReturnsErrorResponse() throws Exception {
    final TransactionSimulatorResult invalid =
        new TransactionSimulatorResult(
            null,
            TransactionProcessingResult.invalid(
                ValidationResult.invalid(
                    TransactionInvalidReason.GAS_PRICE_BELOW_CURRENT_BASE_FEE)));
    when(transactionSimulator.processWithWorldUpdater(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Optional.of(invalid));

    assertError(request(CALL + "," + CALL), RpcErrorType.INTERNAL_ERROR);
    // the bundle stops at the first invalid call
    verify(transactionSimulator, times(1))
        .processWithWorldUpdater(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void emptySimulatorResultReturnsErrorResponse() throws Exception {
    when(transactionSimulator.processWithWorldUpdater(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Optional.empty());

    assertError(request(CALL), RpcErrorType.INTERNAL_ERROR);
  }

  @Test
  public void unavailableWorldStateReturnsErrorResponse() throws Exception {
    when(blockchainQueries.getAndMapWorldState(eq(Hash.ZERO), any())).thenReturn(Optional.empty());

    assertError(request(CALL), RpcErrorType.WORLD_STATE_UNAVAILABLE);
  }

  private void assertError(final JsonRpcRequestContext request, final RpcErrorType errorType) {
    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    final JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) response;
    assertThat(errorResponse.getId()).isEqualTo(request.getRequest().getId());
    assertThat(errorResponse.getErrorType()).isEqualTo(errorType);
  }

  private static JsonRpcRequestContext request(final String calls) throws Exception {
    final String json =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"trace_callMany\",\"params\":[["
            + calls
            + "],\"0x1\"]}";
    return new JsonRpcRequestContext(new ObjectMapper().readValue(json, JsonRpcRequest.class));
  }
}
