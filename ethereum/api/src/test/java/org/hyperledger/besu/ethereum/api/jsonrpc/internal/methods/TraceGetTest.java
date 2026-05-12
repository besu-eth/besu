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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockTracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.Arrays;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TraceGetTest {

  private static final String VALID_TX_HASH =
      "0x1234567890123456789012345678901234567890123456789012345678901234";

  private TraceGet method;

  @Mock Supplier<BlockTracer> blockTracerSupplier;
  @Mock ProtocolSchedule protocolSchedule;
  @Mock BlockchainQueries blockchainQueries;

  @BeforeEach
  public void setUp() {
    method = new TraceGet(blockTracerSupplier, blockchainQueries, protocolSchedule);
  }

  @Test
  public void shouldThrowOnInvalidHexDigitsTraceIndex() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0", "trace_get", new Object[] {VALID_TX_HASH, Arrays.asList("0xZZ")}));

    assertThatThrownBy(() -> method.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .asInstanceOf(
            org.assertj.core.api.InstanceOfAssertFactories.type(InvalidJsonRpcParameters.class))
        .extracting(InvalidJsonRpcParameters::getRpcErrorType)
        .isEqualTo(RpcErrorType.INVALID_TRACE_NUMBERS_PARAMS);
  }

  @Test
  public void shouldThrowOnTraceIndexMissingHexPrefix() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0", "trace_get", new Object[] {VALID_TX_HASH, Arrays.asList("1")}));

    assertThatThrownBy(() -> method.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .asInstanceOf(
            org.assertj.core.api.InstanceOfAssertFactories.type(InvalidJsonRpcParameters.class))
        .extracting(InvalidJsonRpcParameters::getRpcErrorType)
        .isEqualTo(RpcErrorType.INVALID_TRACE_NUMBERS_PARAMS);
  }

  @Test
  public void shouldThrowOnEmptyStringTraceIndex() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0", "trace_get", new Object[] {VALID_TX_HASH, Arrays.asList("")}));

    assertThatThrownBy(() -> method.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .asInstanceOf(
            org.assertj.core.api.InstanceOfAssertFactories.type(InvalidJsonRpcParameters.class))
        .extracting(InvalidJsonRpcParameters::getRpcErrorType)
        .isEqualTo(RpcErrorType.INVALID_TRACE_NUMBERS_PARAMS);
  }

  @Test
  public void shouldThrowOnNonStringTraceIndexElement() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0", "trace_get", new Object[] {VALID_TX_HASH, Arrays.asList(42)}));

    assertThatThrownBy(() -> method.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .asInstanceOf(
            org.assertj.core.api.InstanceOfAssertFactories.type(InvalidJsonRpcParameters.class))
        .extracting(InvalidJsonRpcParameters::getRpcErrorType)
        .isEqualTo(RpcErrorType.INVALID_TRACE_NUMBERS_PARAMS);
  }

  @Test
  public void shouldThrowOnTraceIndexMissingHexPrefixLongerThanTwo() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0", "trace_get", new Object[] {VALID_TX_HASH, Arrays.asList("deadbeef")}));

    assertThatThrownBy(() -> method.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .asInstanceOf(
            org.assertj.core.api.InstanceOfAssertFactories.type(InvalidJsonRpcParameters.class))
        .extracting(InvalidJsonRpcParameters::getRpcErrorType)
        .isEqualTo(RpcErrorType.INVALID_TRACE_NUMBERS_PARAMS);
  }

  @Test
  public void shouldThrowOnNullTraceIndexElement() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0", "trace_get", new Object[] {VALID_TX_HASH, Arrays.asList((Object) null)}));

    assertThatThrownBy(() -> method.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .asInstanceOf(
            org.assertj.core.api.InstanceOfAssertFactories.type(InvalidJsonRpcParameters.class))
        .extracting(InvalidJsonRpcParameters::getRpcErrorType)
        .isEqualTo(RpcErrorType.INVALID_TRACE_NUMBERS_PARAMS);
  }
}
