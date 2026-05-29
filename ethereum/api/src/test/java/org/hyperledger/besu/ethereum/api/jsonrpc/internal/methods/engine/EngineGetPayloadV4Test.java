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
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.PayloadWrapper;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.consensus.merge.blockcreation.PreparePayloadArgsBuilder;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.RequestType;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetPayloadResultV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreationTiming;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockWithReceipts;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;

import java.math.BigInteger;
import java.util.Comparator;
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
        vertx,
        protocolSchedule,
        protocolContext,
        mergeMiningCoordinator,
        factory,
        engineCallListener,
        PRAGUE,
        OSAKA);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getPayloadV4");
  }

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsBeforeForkWindow() {
    assumeTrue(getMinSupportedTimestamp().isPresent());

    PayloadIdentifier cancunPayload = setupPayload(getMinSupportedTimestamp().getAsLong() - 1);
    final var resp = resp(getMethodName(), cancunPayload);
    assertThat(resp).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) resp).getErrorType())
        .isEqualTo(RpcErrorType.UNSUPPORTED_FORK);
  }

  @Override
  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsAtFirstUnsupportedForkMilestone() {
    assumeTrue(getFirstUnsupportedTimestamp().isPresent());

    PayloadIdentifier osakaPayload = setupPayload(getFirstUnsupportedTimestamp().getAsLong());
    final var resp = resp(getMethodName(), osakaPayload);
    assertThat(resp).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) resp).getErrorType())
        .isEqualTo(RpcErrorType.UNSUPPORTED_FORK);
  }

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsAfterFirstUnsupportedForkMilestone() {
    assumeTrue(getFirstUnsupportedTimestamp().isPresent());

    PayloadIdentifier osakaPayload = setupPayload(getFirstUnsupportedTimestamp().getAsLong() + 1);
    final var resp = resp(getMethodName(), osakaPayload);
    assertThat(resp).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) resp).getErrorType())
        .isEqualTo(RpcErrorType.UNSUPPORTED_FORK);
  }

  @Override
  @Test
  public void shouldReturnBlockForKnownPayloadId() {

    BlockHeader header =
        new BlockHeaderTestFixture()
            .prevRandao(Bytes32.random())
            .timestamp(pragueHardfork.milestone() + 1)
            .excessBlobGas(BlobGas.of(10L))
            .buildHeader();
    // should return withdrawals, deposits and excessGas for a post-6110 block
    PayloadIdentifier payloadIdentifier =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(new BlockHeaderTestFixture().buildHeader())
                .timestamp(pragueHardfork.milestone())
                .prevRandao(Bytes32.random())
                .feeRecipient(Address.fromHexString("0x42"))
                .build());

    BlobTestFixture blobTestFixture = new BlobTestFixture();
    BlobsWithCommitments bwc = blobTestFixture.createBlobsWithCommitments(1);
    Transaction blobTx =
        new TransactionTestFixture()
            .to(Optional.of(Address.fromHexString("0xDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF")))
            .type(TransactionType.BLOB)
            .chainId(Optional.of(BigInteger.ONE))
            .maxFeePerGas(Optional.of(Wei.of(15)))
            .maxFeePerBlobGas(Optional.of(Wei.of(128)))
            .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
            .blobsWithCommitments(Optional.of(bwc))
            .versionedHashes(Optional.of(bwc.getVersionedHashes()))
            .createTransaction(senderKeys);
    TransactionReceipt blobReceipt = mock(TransactionReceipt.class);
    when(blobReceipt.getCumulativeGasUsed()).thenReturn(100L);
    BlockWithReceipts block =
        new BlockWithReceipts(
            new Block(
                header, new BlockBody(List.of(blobTx), emptyList(), Optional.of(emptyList()))),
            List.of(blobReceipt));
    final List<Request> requests =
        List.of(
            new Request(RequestType.DEPOSIT, Bytes.of(1)),
            new Request(RequestType.WITHDRAWAL, Bytes.of(1)),
            new Request(RequestType.CONSOLIDATION, Bytes.of(1)));
    PayloadWrapper payload =
        new PayloadWrapper(
            payloadIdentifier,
            block,
            Optional.empty(),
            Optional.of(requests),
            BlockCreationTiming.EMPTY);

    when(mergeContext.retrievePayloadById(payloadIdentifier)).thenReturn(Optional.of(payload));

    final var resp = resp(getMethodName(), payloadIdentifier);
    assertThat(resp).isInstanceOf(JsonRpcSuccessResponse.class);
    final List<String> requestsWithoutRequestId =
        requests.stream()
            .sorted(Comparator.comparing(Request::getType))
            .map(Request::getEncodedRequest)
            .map(Bytes::toHexString)
            .toList();
    Optional.of(resp)
        .map(JsonRpcSuccessResponse.class::cast)
        .ifPresent(
            r -> {
              assertThat(r.getResult()).isInstanceOf(EngineGetPayloadResultV4.class);
              final EngineGetPayloadResultV4 res = (EngineGetPayloadResultV4) r.getResult();
              assertThat(res.getExecutionPayload()).isInstanceOf(ExecutionPayloadV3.class);
              assertThat(res.getExecutionPayload().getWithdrawals()).isNotNull();
              assertThat(res.getExecutionPayload().getBlockHash()).isEqualTo(header.getHash());
              assertThat(res.getBlockValue()).isEqualTo(Quantity.create(0));
              assertThat(res.getExecutionPayload().getPrevRandao())
                  .isEqualTo(header.getPrevRandao().orElse(null));
              assertThat(res.getExecutionPayload().getExcessBlobGas()).isEqualTo(BlobGas.of(10L));
              assertThat(res.getExecutionRequests()).isNotEmpty();
              assertThat(res.getExecutionRequests()).isEqualTo(requestsWithoutRequestId);
            });
    verify(engineCallListener, times(1)).executionEngineCalled();
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

    final List<String> expectedRequests =
        List.of(
            Bytes.concatenate(Bytes.of(RequestType.DEPOSIT.getSerializedType()), Bytes.of(1))
                .toHexString(),
            Bytes.concatenate(Bytes.of(RequestType.CONSOLIDATION.getSerializedType()), Bytes.of(1))
                .toHexString());
    Optional.of(resp)
        .map(JsonRpcSuccessResponse.class::cast)
        .ifPresent(
            r -> {
              assertThat(getExecutionRequests(r.getResult())).isEqualTo(expectedRequests);
            });
  }

  protected List<String> getExecutionRequests(final Object result) {
    assertThat(result).isInstanceOf(EngineGetPayloadResultV4.class);
    return ((EngineGetPayloadResultV4) result).getExecutionRequests();
  }

  @Override
  protected String getMethodName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_V4.getMethodName();
  }

  @Override
  protected long getValidPayloadTimestamp() {
    return 55L;
  }

  @Override
  protected OptionalLong getMinSupportedTimestamp() {
    return OptionalLong.of(pragueHardfork.milestone());
  }

  @Override
  protected OptionalLong getFirstUnsupportedTimestamp() {
    return OptionalLong.of(osakaHardfork.milestone());
  }
}
