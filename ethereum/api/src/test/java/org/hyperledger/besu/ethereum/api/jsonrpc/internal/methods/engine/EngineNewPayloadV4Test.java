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

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PRAGUE;
import static org.hyperledger.besu.ethereum.api.graphql.internal.response.GraphQLError.INVALID_PARAMS;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineTestSupport.fromErrorResp;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_EXECUTION_REQUESTS_PARAMS;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_PARAMS;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.RequestType;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EnginePayloadStatusResult;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.requests.MainnetRequestsValidator;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EngineNewPayloadV4Test extends EngineNewPayloadV3Test {

  protected static final List<Request> VALID_REQUESTS =
      List.of(
          new Request(RequestType.DEPOSIT, Bytes.of(1)),
          new Request(RequestType.WITHDRAWAL, Bytes.of(1)),
          new Request(RequestType.CONSOLIDATION, Bytes.of(1)));

  public EngineNewPayloadV4Test() {}

  @BeforeEach
  @Override
  public void before() {
    super.before();
    lenient().when(protocolSpec.getGasCalculator()).thenReturn(new PragueGasCalculator());
    mockAllowedRequestsValidator();
  }

  @Override
  protected EngineNewPayloadV1<?, ?> createMethodInstance() {
    return new EngineNewPayloadV4<>(
        vertx,
        protocolSchedule,
        protocolContext,
        mergeCoordinator,
        ethPeers,
        engineCallListener,
        new NoOpMetricsSystem(),
        PRAGUE,
        AMSTERDAM);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_newPayloadV4");
  }

  @Override
  protected long getMinSupportedTimestamp() {
    return pragueHardfork.milestone();
  }

  @Override
  protected long getMaxSupportedTimestamp() {
    return amsterdamHardfork.milestone() - 1;
  }

  @Test
  public void shouldReturnInvalidIfRequestsIsNull_WhenRequestsAllowed() {
    List<Request> requests = null;
    BlockHeader blockHeader =
        setupValidPayloadV4(
            getMinSupportedTimestamp(),
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))),
            requests);

    var resp = respV4(mockEnginePayloadParam(blockHeader, emptyList()), null);

    var res = fromErrorResp(resp);
    assertThat(res.getCode()).isEqualTo(INVALID_PARAMS.getCode());
    assertThat(res.getMessage()).isEqualTo("Invalid execution requests params");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnValidIfRequestsIsNotNull_WhenRequestsAllowed() {
    BlockHeader blockHeader =
        setupValidPayloadV4(
            getMinSupportedTimestamp(),
            new BlockProcessingResult(
                Optional.of(
                    new BlockProcessingOutputs(null, List.of(), Optional.of(VALID_REQUESTS)))),
            VALID_REQUESTS);

    var resp =
        respV4(mockEnginePayloadParam(blockHeader, emptyList()), requestsAsParam(VALID_REQUESTS));

    assertValidResponse(blockHeader, resp);
  }

  @Test
  public void shouldReturnInvalidStatusIfRequestsContainUnknownRequestType() {
      // An unknown request type byte is a block validity error, not an RPC parameter error.
      // The spec (execution-apis prague.md) only mandates -32602 for out-of-order, empty data,
      // duplicate type, or null. Unknown types must return INVALID payload status per EELS tests.
    RequestType unknowType = mock(RequestType.class);
    when(unknowType.getSerializedType()).thenReturn((byte) 0xff);
    Request unknownTypeRequest = mock(Request.class);
    when(unknownTypeRequest.getType()).thenReturn(unknowType);
    when(unknownTypeRequest.getData()).thenReturn(Bytes.of(1));
    when(unknownTypeRequest.getEncodedRequest()).thenReturn(Bytes.fromHexString("0xff01"));
    List<Request> unknownTypeRequests = List.of(unknownTypeRequest);

    BlockHeader blockHeader =
        setupValidPayloadV4(
            getMinSupportedTimestamp(),
            new BlockProcessingResult(
                Optional.of(
                    new BlockProcessingOutputs(null, List.of(), Optional.of(unknownTypeRequests)))),
            unknownTypeRequests);

    var resp =
        respV4(
            mockEnginePayloadParam(blockHeader, emptyList()), requestsAsParam(unknownTypeRequests));

    var result = fromSuccessResp(resp);
      assertThat(result.getStatusAsString()).isEqualTo(INVALID.name());
      assertThat(result.getLatestValidHash().get()).isEqualTo(mockHash);
      verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidParamsIfRequestsAreOutOfOrder() {
    // Requests must be in strictly ascending order by type; reverse order is invalid
    final List<Request> outOfOrderRequests =
        List.of(
            new Request(RequestType.CONSOLIDATION, Bytes.of(1)),
            new Request(RequestType.DEPOSIT, Bytes.of(1)));

    BlockHeader blockHeader =
        setupValidPayloadV4(
            getMinSupportedTimestamp(),
            new BlockProcessingResult(
                Optional.of(
                    new BlockProcessingOutputs(null, List.of(), Optional.of(outOfOrderRequests)))),
            outOfOrderRequests);

    var resp =
        respV4(
            mockEnginePayloadParam(blockHeader, emptyList()), requestsAsParam(outOfOrderRequests));

    assertThat(fromErrorResp(resp).getCode()).isEqualTo(INVALID_PARAMS.getCode());
    assertThat(fromErrorResp(resp).getMessage())
        .isEqualTo(INVALID_EXECUTION_REQUESTS_PARAMS.getMessage());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidIfRequestsIsNotNull_WhenRequestsProhibited() {
    BlockHeader blockHeader =
        setupValidPayloadV3(
            getMinSupportedTimestamp() - 1,
            new BlockProcessingResult(
                Optional.of(
                    new BlockProcessingOutputs(null, List.of(), Optional.of(VALID_REQUESTS)))),
            BlobGas.ZERO,
            0L);

    var methodV3 = super.createMethodInstance();

    var resp =
        methodV3.response(
            new JsonRpcRequestContext(
                new JsonRpcRequest(
                    "2.0",
                    methodV3.getName(),
                    new Object[] {
                      mockEnginePayloadParam(blockHeader, emptyList()),
                      emptyVersionedHashesParam(),
                      zeroParentBeaconBlockRootParam(),
                      requestsAsParam(VALID_REQUESTS)
                    })));

    final JsonRpcError jsonRpcError = fromErrorResp(resp);
    assertThat(jsonRpcError.getCode()).isEqualTo(INVALID_PARAMS.getCode());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void validateVersionedHash_whenListIsPresentAndEmpty() {
    BlockHeader blockHeader =
        setupValidPayloadV4(
            getMinSupportedTimestamp(),
            new BlockProcessingResult(
                Optional.of(new BlockProcessingOutputs(null, List.of(), Optional.of(emptyList())))),
            emptyList());

    var resp =
        respV4(mockEnginePayloadParam(blockHeader, emptyList()), requestsAsParam(emptyList()));

    assertValidResponse(blockHeader, resp);
  }

  @Override
  protected Object[] getVersionSpecificDefaultParams() {
    return ArrayUtils.addAll(super.getVersionSpecificDefaultParams(), emptyList());
  }

  protected BlockHeader setupValidPayloadV4(
      final long timestamp, final BlockProcessingResult value, final List<Request> requests) {
    return setupValidPayloadV4(timestamp, value, requests, UnaryOperator.identity());
  }

  protected BlockHeader setupValidPayloadV4(
      final long timestamp,
      final BlockProcessingResult value,
      final List<Request> requests,
      final UnaryOperator<BlockHeaderTestFixture> nextVersionSpecificModifier) {
    return setupValidPayloadV3(
        timestamp,
        value,
        BlobGas.ZERO,
        0L,
        fixture -> nextVersionSpecificModifier.apply(setRequestField(fixture, requests)));
  }

  @Override
  protected BlockHeaderTestFixture versionSpecificBlockHeaderFixture(final long timestamp) {
    BlockHeaderTestFixture baseFixture = super.versionSpecificBlockHeaderFixture(timestamp);

    return setRequestField(baseFixture, emptyList());
  }

  private BlockHeaderTestFixture setRequestField(
      final BlockHeaderTestFixture fixture, final List<Request> requests) {
    if (requests != null) {
      return fixture.requestsHash(BodyValidation.requestsHash(requests));
    }
    return fixture;
  }

  private void mockAllowedRequestsValidator() {
    var validator = new MainnetRequestsValidator();
    when(protocolSpec.getRequestsValidator()).thenReturn(validator);
  }

  protected JsonRpcResponse respV4(
      final Map<String, Object> payloadParam, final List<String> requestsParam) {
    return super.resp(
        payloadParam, emptyVersionedHashesParam(), zeroParentBeaconBlockRootParam(), requestsParam);
  }

  protected List<String> requestsAsParam(final List<Request> requests) {
    return requests.stream().map(Request::getEncodedRequest).map(Bytes::toHexString).toList();
  }
}
