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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.LogsResult;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EthGetFilterLogsTest {

  private EthGetFilterLogs method;

  @Mock FilterManager filterManager;

  @BeforeEach
  public void setUp() {
    method = new EthGetFilterLogs(filterManager);
  }

  @Test
  public void shouldReturnCorrectMethodName() {
    assertThat(method.getName()).isEqualTo("eth_getFilterLogs");
  }

  @Test
  public void shouldReturnErrorWhenMissingParams() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "eth_getFilterLogs", new Object[] {}));

    final Throwable thrown = catchThrowable(() -> method.response(request));
    assertThat(thrown)
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessage("Invalid filter ID parameter (index 0)");

    verifyNoInteractions(filterManager);
  }

  @Test
  public void shouldReturnErrorWhenMissingFilterId() {
    final JsonRpcRequestContext request = requestWithFilterId();

    final Throwable thrown = catchThrowable(() -> method.response(request));
    assertThat(thrown)
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessage("Invalid filter ID parameter (index 0)");

    verifyNoInteractions(filterManager);
  }

  @Test
  public void shouldReturnFilterNotFoundWhenFilterManagerReturnsNull() {
    final JsonRpcRequestContext request = requestWithFilterId("NOT FOUND");
    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(null, RpcErrorType.LOGS_FILTER_NOT_FOUND);
    when(filterManager.logs(eq("NOT FOUND"), any())).thenReturn(null);

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnEmptyListWhenFilterManagerReturnsEmpty() {
    final JsonRpcRequestContext request = requestWithFilterId("0x1");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, new LogsResult(new ArrayList<>()));
    when(filterManager.logs(eq("0x1"), any())).thenReturn(new ArrayList<>());

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnExpectedLogsWhenFilterManagerReturnsLogs() {
    final JsonRpcRequestContext request = requestWithFilterId("0x1");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, new LogsResult(logs()));
    when(filterManager.logs(eq("0x1"), any())).thenReturn(logs());

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  private JsonRpcRequestContext requestWithFilterId(final Object... filterId) {
    return new JsonRpcRequestContext(new JsonRpcRequest("2.0", "eth_getFilterLogs", filterId));
  }

  private List<LogWithMetadata> logs() {
    return Collections.singletonList(
        new LogWithMetadata(
            0,
            100L,
            Hash.ZERO,
            0L,
            Hash.ZERO,
            0,
            Address.fromHexString("0x0"),
            Bytes.EMPTY,
            Lists.newArrayList(),
            false));
  }
}
