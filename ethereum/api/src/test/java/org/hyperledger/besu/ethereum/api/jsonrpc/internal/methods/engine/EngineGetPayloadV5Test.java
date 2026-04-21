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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.PayloadWrapper;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.RequestType;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetPayloadResultV5;
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

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class})
public class EngineGetPayloadV5Test extends AbstractEngineGetPayloadTest {

  public EngineGetPayloadV5Test() {
    super();
  }

  @BeforeEach
  @Override
  public void before() {
    super.before();
    lenient()
        .when(mergeContext.retrievePayloadById(mockPid))
        .thenReturn(Optional.of(mockPayloadWithDepositRequests));
    when(protocolContext.safeConsensusContext(Mockito.any())).thenReturn(Optional.of(mergeContext));
    this.method =
        new EngineGetPayloadV5(
            vertx,
            protocolContext,
            mergeMiningCoordinator,
            factory,
            engineCallListener,
            protocolSchedule);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getPayloadV5");
  }

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsBeforeOsakaMilestone() {
    PayloadIdentifier cancunPayload = setupPayload(osakaHardfork.milestone() - 1);
    final var resp = resp(RpcMethod.ENGINE_GET_PAYLOAD_V5.getMethodName(), cancunPayload);
    assertThat(resp).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) resp).getErrorType())
        .isEqualTo(RpcErrorType.UNSUPPORTED_FORK);
  }

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsAtAmsterdamMilestone() {
    PayloadIdentifier amsterdamPayload = setupPayload(amsterdamHardfork.milestone());
    final var resp = resp(RpcMethod.ENGINE_GET_PAYLOAD_V5.getMethodName(), amsterdamPayload);
    assertThat(resp).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) resp).getErrorType())
        .isEqualTo(RpcErrorType.UNSUPPORTED_FORK);
  }

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsAfterAmsterdamMilestone() {
    PayloadIdentifier amsterdamPayload = setupPayload(amsterdamHardfork.milestone() + 1);
    final var resp = resp(RpcMethod.ENGINE_GET_PAYLOAD_V5.getMethodName(), amsterdamPayload);
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
            .timestamp(osakaHardfork.milestone() + 1)
            .excessBlobGas(BlobGas.of(10L))
            .buildHeader();
    // should return withdrawals, deposits and excessGas for a post-6110 block
    PayloadIdentifier payloadIdentifier =
        PayloadIdentifier.forPayloadParams(
            Hash.ZERO,
            osakaHardfork.milestone(),
            Bytes32.random(),
            Address.fromHexString("0x42"),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

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

    final var resp = resp(RpcMethod.ENGINE_GET_PAYLOAD_V5.getMethodName(), payloadIdentifier);
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
              assertThat(r.getResult()).isInstanceOf(EngineGetPayloadResultV5.class);
              final EngineGetPayloadResultV5 res = (EngineGetPayloadResultV5) r.getResult();
              assertThat(res.getExecutionPayload().getWithdrawals()).isNotNull();
              assertThat(res.getExecutionPayload().getHash())
                  .isEqualTo(header.getHash().toString());
              assertThat(res.getBlockValue()).isEqualTo(Quantity.create(0));
              assertThat(res.getExecutionPayload().getPrevRandao())
                  .isEqualTo(header.getPrevRandao().map(Bytes32::toString).orElse(""));
              // excessBlobGas: QUANTITY, 256 bits
              String expectedQuantityOf10 = Bytes32.leftPad(Bytes.of(10)).toQuantityHexString();
              assertThat(res.getExecutionPayload().getExcessBlobGas()).isNotEmpty();
              assertThat(res.getExecutionPayload().getExcessBlobGas())
                  .isEqualTo(expectedQuantityOf10);
              assertThat(res.getExecutionRequests()).isNotEmpty();
              assertThat(res.getExecutionRequests()).isEqualTo(requestsWithoutRequestId);
            });
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Override
  protected String getMethodName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_V5.getMethodName();
  }

  @Override
  protected long getValidPayloadTimestamp() {
    // V5 works with Osaka (>= 60) but must be before Amsterdam (< 70)
    return 65L;
  }
}
