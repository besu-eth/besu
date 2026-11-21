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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.RejectedParametersException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Answers;

class ExtraParametersRejectionTest {

  private static final String TOO_MANY_ARGUMENTS = "Too many arguments: expected at most 3";

  private static final BlockchainQueries BLOCKCHAIN_QUERIES = mock(BlockchainQueries.class);
  private static final ProtocolSchedule PROTOCOL_SCHEDULE =
      mock(ProtocolSchedule.class, Answers.RETURNS_DEEP_STUBS);
  private static final TransactionSimulator TRANSACTION_SIMULATOR =
      mock(TransactionSimulator.class);

  static {
    final BlockHeader blockHeader = mock(BlockHeader.class, Answers.RETURNS_DEEP_STUBS);
    final Blockchain blockchain = mock(Blockchain.class, Answers.RETURNS_DEEP_STUBS);
    when(blockchain.getChainHead().getBlockHeader()).thenReturn(blockHeader);
    when(BLOCKCHAIN_QUERIES.getBlockchain()).thenReturn(blockchain);
    when(BLOCKCHAIN_QUERIES.headBlockNumber()).thenReturn(1L);
    when(BLOCKCHAIN_QUERIES.getBlockHeaderByNumber(1L)).thenReturn(Optional.of(blockHeader));
  }

  @ParameterizedTest
  @MethodSource("callMethods")
  void methodReturnsInvalidParamsWithMessageForFourthParameter(final JsonRpcMethod method) {
    final JsonRpcResponse response = method.response(requestWithFourParams(method.getName()));

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    final JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) response;
    assertThat(errorResponse.getError().getCode()).isEqualTo(RpcErrorType.INVALID_PARAMS.getCode());
    assertThat(errorResponse.getError().getMessage()).isEqualTo(TOO_MANY_ARGUMENTS);
    verifyNoInteractions(TRANSACTION_SIMULATOR);
  }

  @ParameterizedTest
  @MethodSource("traceMethods")
  void traceMethodsRejectFourthParameterWhenReadingCallParams(final AbstractTraceCall method) {
    final JsonRpcRequestContext request = requestWithFourParams(method.getName());

    assertThatThrownBy(() -> method.getCallParams(request))
        .isInstanceOfSatisfying(
            RejectedParametersException.class,
            e -> {
              assertThat(e.getRpcErrorType()).isEqualTo(RpcErrorType.INVALID_PARAMS);
              assertThat(e).hasMessage(TOO_MANY_ARGUMENTS);
            });
  }

  @Test
  void validateAndGetCallParamsRejectsFourthParameter() {
    final JsonRpcRequestContext request = requestWithFourParams("eth_call");

    assertThatThrownBy(() -> CallParameterUtil.validateAndGetCallParams(request))
        .isInstanceOf(RejectedParametersException.class)
        .hasMessage(TOO_MANY_ARGUMENTS);
  }

  static Stream<JsonRpcMethod> callMethods() {
    return Stream.concat(
        Stream.of(
            new EthCall(BLOCKCHAIN_QUERIES, TRANSACTION_SIMULATOR, mock(MetricsSystem.class)),
            new EthEstimateGas(
                BLOCKCHAIN_QUERIES, TRANSACTION_SIMULATOR, mock(ApiConfiguration.class)),
            new EthCreateAccessList(BLOCKCHAIN_QUERIES, TRANSACTION_SIMULATOR)),
        traceMethods());
  }

  static Stream<AbstractTraceCall> traceMethods() {
    return Stream.of(
        new TraceCall(BLOCKCHAIN_QUERIES, PROTOCOL_SCHEDULE, TRANSACTION_SIMULATOR),
        new DebugTraceCall(BLOCKCHAIN_QUERIES, PROTOCOL_SCHEDULE, TRANSACTION_SIMULATOR));
  }

  private static JsonRpcRequestContext requestWithFourParams(final String methodName) {
    final Map<String, Object> call = Map.of("to", "0x" + "11".repeat(20));
    final Map<String, Object> blockOverrides = Map.of("time", "0x80000000");
    // trace_call takes (call, traceTypes, block); the other methods take (call, block, extra)
    final Object[] params =
        "trace_call".equals(methodName)
            ? new Object[] {call, new String[] {"trace"}, "latest", blockOverrides}
            : new Object[] {call, "latest", Map.of(), blockOverrides};
    return new JsonRpcRequestContext(new JsonRpcRequest("2.0", methodName, params));
  }
}
