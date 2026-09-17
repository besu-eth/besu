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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.FilterParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.LogsQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EthNewFilterTest {

  @Mock private FilterManager filterManager;
  @Mock private BlockchainQueries blockchainQueries;
  private EthNewFilter method;
  private final String ETH_METHOD = "eth_newFilter";

  @BeforeEach
  public void setUp() {
    method = new EthNewFilter(filterManager, blockchainQueries, 0, 0);
  }

  @Test
  public void methodReturnsExpectedMethodName() {
    assertThat(method.getName()).isEqualTo(ETH_METHOD);
  }

  @Test
  public void newFilterWithoutFromBlockParamUsesLatestAsDefault() {
    final FilterParameter filterParameter =
        new FilterParameter(null, null, null, null, null, null, null, null, null);
    final JsonRpcRequestContext request = ethNewFilter(filterParameter);

    method.response(request);

    verify(filterManager).installLogFilter(refEq(BlockParameter.LATEST), any(), any());
  }

  @Test
  public void newFilterWithoutToBlockParamUsesLatestAsDefault() {
    final FilterParameter filterParameter =
        new FilterParameter(null, null, null, null, null, null, null, null, null);
    final JsonRpcRequestContext request = ethNewFilter(filterParameter);

    method.response(request);

    verify(filterManager).installLogFilter(any(), refEq(BlockParameter.LATEST), any());
  }

  @Test
  public void newFilterWithoutAddressAndTopicsParamsInstallsEmptyLogFilter() {
    final FilterParameter filterParameter =
        new FilterParameter(
            BlockParameter.LATEST, BlockParameter.LATEST, null, null, null, null, null, null, null);
    final JsonRpcRequestContext request = ethNewFilter(filterParameter);
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getRequest().getId(), "0x1");

    final LogsQuery expectedLogsQuery = new LogsQuery.Builder().build();
    when(filterManager.installLogFilter(any(), any(), eq(expectedLogsQuery))).thenReturn("0x1");

    final JsonRpcResponse actualResponse = method.response(request);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
    verify(filterManager)
        .installLogFilter(
            refEq(BlockParameter.LATEST), refEq(BlockParameter.LATEST), eq(expectedLogsQuery));
  }

  @Test
  public void newFilterWithTopicsOnlyParamInstallsExpectedLogFilter() {
    final FilterParameter filterParameter = filterParamWithAddressAndTopics(null, topics());
    final JsonRpcRequestContext request = ethNewFilter(filterParameter);
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getRequest().getId(), "0x1");

    final LogsQuery expectedLogsQuery =
        new LogsQuery.Builder().topics(filterParameter.getTopics()).build();
    when(filterManager.installLogFilter(any(), any(), eq(expectedLogsQuery))).thenReturn("0x1");

    final JsonRpcResponse actualResponse = method.response(request);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
    verify(filterManager)
        .installLogFilter(
            refEq(BlockParameter.LATEST), refEq(BlockParameter.LATEST), eq(expectedLogsQuery));
  }

  @Test
  public void newFilterWithAddressOnlyParamInstallsExpectedLogFilter() {
    final Address address = Address.fromHexString("0x0");
    final FilterParameter filterParameter = filterParamWithAddressAndTopics(address, null);
    final JsonRpcRequestContext request = ethNewFilter(filterParameter);
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getRequest().getId(), "0x1");

    final LogsQuery expectedLogsQuery = new LogsQuery.Builder().address(address).build();
    when(filterManager.installLogFilter(any(), any(), eq(expectedLogsQuery))).thenReturn("0x1");

    final JsonRpcResponse actualResponse = method.response(request);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
    verify(filterManager)
        .installLogFilter(
            refEq(BlockParameter.LATEST), refEq(BlockParameter.LATEST), eq(expectedLogsQuery));
  }

  @Test
  public void newFilterWithAddressAndTopicsParamInstallsExpectedLogFilter() {
    final Address address = Address.fromHexString("0x0");
    final List<List<LogTopic>> topics = topics();
    final FilterParameter filterParameter = filterParamWithAddressAndTopics(address, topics);
    final JsonRpcRequestContext request = ethNewFilter(filterParameter);
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(request.getRequest().getId(), "0x1");

    final LogsQuery expectedLogsQuery =
        new LogsQuery.Builder().address(address).topics(filterParameter.getTopics()).build();
    when(filterManager.installLogFilter(any(), any(), eq(expectedLogsQuery))).thenReturn("0x1");

    final JsonRpcResponse actualResponse = method.response(request);

    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(expectedResponse);
    verify(filterManager)
        .installLogFilter(
            refEq(BlockParameter.LATEST), refEq(BlockParameter.LATEST), eq(expectedLogsQuery));
  }

  @Test
  public void filterWithRangeExceedingMaxLogRangeReturnsError() {
    when(blockchainQueries.headBlockNumber()).thenReturn(10000L);
    final EthNewFilter methodWithLimit = new EthNewFilter(filterManager, blockchainQueries, 100, 0);
    final FilterParameter filterParameter =
        new FilterParameter(
            new BlockParameter(0L),
            new BlockParameter(5000L),
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    final JsonRpcRequestContext request = ethNewFilter(filterParameter);
    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(null, RpcErrorType.EXCEEDS_RPC_MAX_BLOCK_RANGE);

    final JsonRpcResponse response = methodWithLimit.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void filterWithAddressCountExceedingCapReturnsError() {
    final EthNewFilter methodWithCap = new EthNewFilter(filterManager, blockchainQueries, 0, 1000);
    final List<Address> addresses =
        IntStream.range(0, 1001)
            .mapToObj(i -> Address.fromHexString(String.format("0x%040x", i)))
            .collect(Collectors.toCollection(ArrayList::new));
    final FilterParameter filterParameter =
        new FilterParameter(
            BlockParameter.LATEST,
            BlockParameter.LATEST,
            null,
            null,
            addresses,
            null,
            null,
            null,
            null);
    final JsonRpcRequestContext request = ethNewFilter(filterParameter);
    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(null, RpcErrorType.EXCEEDS_RPC_MAX_FILTER_ADDRESSES);

    final JsonRpcResponse response = methodWithCap.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void filterWithAddressCountAtCapIsAccepted() {
    final EthNewFilter methodWithCap = new EthNewFilter(filterManager, blockchainQueries, 0, 1000);
    final List<Address> addresses =
        IntStream.range(0, 1000)
            .mapToObj(i -> Address.fromHexString(String.format("0x%040x", i)))
            .collect(Collectors.toCollection(ArrayList::new));
    final FilterParameter filterParameter =
        new FilterParameter(
            BlockParameter.LATEST,
            BlockParameter.LATEST,
            null,
            null,
            addresses,
            null,
            null,
            null,
            null);
    final JsonRpcRequestContext request = ethNewFilter(filterParameter);
    when(filterManager.installLogFilter(any(), any(), any())).thenReturn("0x1");

    final JsonRpcResponse response = methodWithCap.response(request);

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
  }

  @Test
  public void filterWithAddressesAndNoCap() {
    final List<Address> addresses =
        IntStream.range(0, 5000)
            .mapToObj(i -> Address.fromHexString(String.format("0x%040x", i)))
            .collect(Collectors.toCollection(ArrayList::new));
    final FilterParameter filterParameter =
        new FilterParameter(
            BlockParameter.LATEST,
            BlockParameter.LATEST,
            null,
            null,
            addresses,
            null,
            null,
            null,
            null);
    final JsonRpcRequestContext request = ethNewFilter(filterParameter);
    when(filterManager.installLogFilter(any(), any(), any())).thenReturn("0x1");

    // default method has maxFilterAddresses=0 (no limit)
    final JsonRpcResponse response = method.response(request);

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
  }

  @Test
  public void filterWithInvalidParameters() {
    final FilterParameter invalidFilter =
        new FilterParameter(
            BlockParameter.EARLIEST,
            BlockParameter.LATEST,
            null,
            null,
            Collections.emptyList(),
            Collections.emptyList(),
            Hash.ZERO,
            null,
            null);

    final JsonRpcRequestContext request = ethNewFilter(invalidFilter);

    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(null, RpcErrorType.INVALID_FILTER_PARAMS);

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  private List<List<LogTopic>> topics() {
    return singletonList(
        singletonList(
            LogTopic.fromHexString(
                "0x000000000000000000000000a94f5374fce5edbc8e2a8697c15331677e6ebf0b")));
  }

  private FilterParameter filterParamWithAddressAndTopics(
      final Address address, final List<List<LogTopic>> topics) {
    return new FilterParameter(
        BlockParameter.LATEST,
        BlockParameter.LATEST,
        null,
        null,
        Optional.ofNullable(address).map(Collections::singletonList).orElse(emptyList()),
        topics,
        null,
        null,
        null);
  }

  private JsonRpcRequestContext ethNewFilter(final FilterParameter filterParameter) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest("2.0", ETH_METHOD, new Object[] {filterParameter}));
  }
}
