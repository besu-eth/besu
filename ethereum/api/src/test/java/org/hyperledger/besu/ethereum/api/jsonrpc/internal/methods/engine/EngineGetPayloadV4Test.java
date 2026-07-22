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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.OSAKA;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PRAGUE;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.PayloadWrapper;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.consensus.merge.blockcreation.PreparePayloadArgsBuilder;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.RequestType;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConstructorArgumentsBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetPayloadResultV4;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockWithReceipts;
import org.hyperledger.besu.ethereum.core.Request;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class})
public class EngineGetPayloadV4Test extends EngineGetPayloadV3Test {

  @Override
  protected EngineGetPayloadV1 createMethodInstance() {
    return new EngineGetPayloadV4(
        new ConstructorArgumentsBuilder()
            .protocolSchedule(protocolSchedule)
            .protocolContext(protocolContext)
            .vertx(vertx)
            .engineCallListener(engineCallListener)
            .mergeCoordinator(mergeMiningCoordinator)
            .blockResultFactory(factory)
            .maxRequestBlocks(0)
            .build(),
        PRAGUE,
        OSAKA);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getPayloadV4");
  }

  @Override
  protected void assertPayloadResult(final Object result) {
    super.assertPayloadResult(result);
    assertThat(result).isInstanceOf(EngineGetPayloadResultV4.class);
    // default requests are already sorted by type and non-empty, so they are returned as is
    assertThat(getExecutionRequests(result)).isEqualTo(defaultRequests().orElseThrow());
  }

  @Test
  public void shouldExcludeEmptyRequestsInRequestsList() {

    final long validTimestamp = getValidPayloadTimestamp();
    BlockHeader header = blockHeaderTestFixture().timestamp(validTimestamp).buildHeader();
    PayloadIdentifier payloadIdentifier =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(new BlockHeaderTestFixture().buildHeader())
                .timestamp(validTimestamp)
                .prevRandao(Bytes32.random())
                .feeRecipient(Address.fromHexString("0x42"))
                .build());

    BlockWithReceipts block =
        new BlockWithReceipts(
            new Block(header, new BlockBody(emptyList(), emptyList(), Optional.of(emptyList()))),
            emptyList());
    final List<Request> unorderedRequests =
        List.of(
            new Request(RequestType.CONSOLIDATION, Bytes.of(1)),
            new Request(RequestType.DEPOSIT, Bytes.of(1)),
            new Request(RequestType.WITHDRAWAL, Bytes.EMPTY));
    PayloadWrapper payload =
        createPayload(payloadIdentifier, block, Optional.of(unorderedRequests));

    when(mergeContext.retrievePayloadById(payloadIdentifier)).thenReturn(Optional.of(payload));

    final var resp = resp(getMethodName(), payloadIdentifier);
    assertThat(resp).isInstanceOf(JsonRpcSuccessResponse.class);

    Optional.of(resp)
        .map(JsonRpcSuccessResponse.class::cast)
        .ifPresent(
            r -> {
              assertThat(getExecutionRequests(r.getResult()))
                  .containsExactly(
                      new Request(RequestType.DEPOSIT, Bytes.of(1)),
                      new Request(RequestType.CONSOLIDATION, Bytes.of(1)));
            });
  }

  protected List<Request> getExecutionRequests(final Object result) {
    assertThat(result).isInstanceOf(EngineGetPayloadResultV4.class);
    return ((EngineGetPayloadResultV4) result).getExecutionRequests();
  }

  @Override
  protected Optional<List<Request>> defaultRequests() {
    return Optional.of(
        List.of(
            new Request(RequestType.DEPOSIT, Bytes.of(1)),
            new Request(RequestType.WITHDRAWAL, Bytes.of(1)),
            new Request(RequestType.CONSOLIDATION, Bytes.of(1))));
  }

  @Override
  protected String getMethodName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_V4.getMethodName();
  }

  @Override
  protected long getMinSupportedTimestamp() {
    return pragueHardfork.milestone();
  }

  @Override
  protected OptionalLong getMaxSupportedTimestamp() {
    return OptionalLong.of(osakaHardfork.milestone() - 1);
  }
}
