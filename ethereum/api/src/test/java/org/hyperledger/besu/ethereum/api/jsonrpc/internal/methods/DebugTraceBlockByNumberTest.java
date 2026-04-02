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
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.OpCodeLoggerTracerResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class DebugTraceBlockByNumberTest {

  @Mock private BlockchainQueries blockchainQueries;
  @Mock private Blockchain blockchain;
  @Mock private Block block;
  @Mock private BlockHeader blockHeader;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ObservableMetricsSystem metricsSystem;
  private DebugTraceBlockByNumber debugTraceBlockByNumber;

  @BeforeEach
  public void setUp() {
    debugTraceBlockByNumber =
        new DebugTraceBlockByNumber(
            protocolSchedule, blockchainQueries, metricsSystem, new DeterministicEthScheduler());
  }

  @Test
  public void nameShouldBeDebugTraceBlockByNumber() {
    assertThat(debugTraceBlockByNumber.getName()).isEqualTo("debug_traceBlockByNumber");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldReturnCorrectResponse() {

    final long blockNumber = 1L;
    final Object[] params = new Object[] {Long.toHexString(blockNumber)};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_traceBlockByNumber", params));
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getBlockByNumber(blockNumber)).thenReturn(Optional.of(block));
    when(block.getHeader()).thenReturn(blockHeader);

    OpCodeLoggerTracerResult result1 = mock(OpCodeLoggerTracerResult.class);
    OpCodeLoggerTracerResult result2 = mock(OpCodeLoggerTracerResult.class);

    List<OpCodeLoggerTracerResult> resultList = Arrays.asList(result1, result2);

    try (MockedStatic<Tracer> mockedTracer = mockStatic(Tracer.class)) {
      mockedTracer
          .when(
              () ->
                  Tracer.processTracing(
                      eq(blockchainQueries), eq(Optional.of(blockHeader)), any(Function.class)))
          .thenReturn(Optional.of(resultList));

      final JsonRpcResponse jsonRpcResponse = debugTraceBlockByNumber.response(request);
      assertThat(jsonRpcResponse).isInstanceOf(JsonRpcSuccessResponse.class);
      JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) jsonRpcResponse;

      final Collection<OpCodeLoggerTracerResult> traceResult = getResult(response);
      assertThat(traceResult).isNotEmpty();
      assertThat(traceResult).isInstanceOf(Collection.class).hasSize(2);
      assertThat(traceResult).containsExactly(result1, result2);
    }
  }

  @SuppressWarnings("unchecked")
  private Collection<OpCodeLoggerTracerResult> getResult(final JsonRpcSuccessResponse response) {
    return (Collection<OpCodeLoggerTracerResult>) response.getResult();
  }

  @Test
  public void shouldReturnErrorForGenesisBlock() {
    final Object[] params = new Object[] {"0x0"};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_traceBlockByNumber", params));

    final JsonRpcResponse jsonRpcResponse = debugTraceBlockByNumber.response(request);
    assertThat(jsonRpcResponse).isInstanceOf(JsonRpcErrorResponse.class);
    final JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) jsonRpcResponse;
    assertThat(errorResponse.getErrorType())
        .isEqualByComparingTo(RpcErrorType.GENESIS_BLOCK_NOT_TRACEABLE);
  }

  @Test
  public void shouldHandleInvalidParametersGracefully() {
    final Object[] invalidParams = new Object[] {"invalid-block-number"};
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", "debug_traceBlockByNumber", invalidParams));

    assertThatThrownBy(() -> debugTraceBlockByNumber.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessageContaining("Invalid block parameter");
  }
}
