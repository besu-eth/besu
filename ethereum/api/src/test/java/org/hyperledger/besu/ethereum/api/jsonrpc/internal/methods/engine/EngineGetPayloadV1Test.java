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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.SHANGHAI;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineTestSupport.fromErrorResp;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.UNSUPPORTED_FORK;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.PayloadWrapper;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.consensus.merge.blockcreation.PreparePayloadArgsBuilder;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcObjectMapperFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResultFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetPayloadResultV1;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreationTiming;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockWithReceipts;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EngineGetPayloadV1Test extends AbstractScheduledApiTest {
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  protected static final KeyPair senderKeys = SIGNATURE_ALGORITHM.generateKeyPair();
  private static final ObjectMapper OBJECT_MAPPER =
      JsonRpcObjectMapperFactory.createResponseMapper();

  protected EngineGetPayloadV1 method;

  protected static final Vertx vertx = Vertx.vertx();
  protected static final BlockResultFactory factory = new BlockResultFactory();
  protected static final PayloadIdentifier mockPid = new PayloadIdentifier(1337L);
  protected BlockHeader mockHeader =
      new BlockHeaderTestFixture().prevRandao(Bytes32.random()).buildHeader();

  @Mock protected ProtocolContext protocolContext;
  @Mock protected MergeContext mergeContext;
  @Mock protected MergeMiningCoordinator mergeMiningCoordinator;
  @Mock protected EngineCallListener engineCallListener;

  @BeforeEach
  @Override
  public void before() {
    super.before();
    when(mergeContext.retrievePayloadById(mockPid))
        .thenReturn(Optional.of(createDefaultPayloadWrapper()));
    when(protocolContext.safeConsensusContext(any())).thenReturn(Optional.of(mergeContext));
    createMethod();
  }

  protected long getMinSupportedTimestamp() {
    return parisHardfork.milestone();
  }

  protected OptionalLong getMaxSupportedTimestamp() {
    return OptionalLong.of(shanghaiHardfork.milestone() - 1);
  }

  protected EngineGetPayloadV1 createMethodInstance() {
    return new EngineGetPayloadV1(
        protocolSchedule,
        protocolContext,
        vertx,
        engineCallListener,
        mergeMiningCoordinator,
        factory,
        null,
        SHANGHAI);
  }

  protected final void createMethod() {
    this.method = createMethodInstance();
  }

  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getPayloadV1");
  }

  @Test
  public void shouldReturnBlockForKnownPayloadId() {
    final var resp = resp(getMethodName(), mockPid);
    assertThat(resp).isInstanceOf(JsonRpcSuccessResponse.class);
    assertPayloadResult(((JsonRpcSuccessResponse) resp).getResult());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  protected void assertPayloadResult(final Object result) {
    assertThat(result).isInstanceOf(EngineGetPayloadResultV1.class);
    final ExecutionPayloadV1 executionPayload =
        ((EngineGetPayloadResultV1) result).getExecutionPayload();

    final Map<String, Object> wirePayload =
        OBJECT_MAPPER.convertValue(executionPayload, new TypeReference<>() {});
    assertThat(wirePayload)
        .containsEntry("blockHash", mockHeader.getHash().toString())
        .containsEntry("prevRandao", mockHeader.getPrevRandao().orElseThrow().toHexString());
  }

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsBeforeMinSupportedForkMilestone() {
    assumeTrue(validatesMinSupportedFork());

    final PayloadIdentifier pid = setupPayload(getMinSupportedTimestamp() - 1);

    assertUnsupportedFork(resp(getMethodName(), pid));
  }

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsAtFirstUnsupportedForkMilestone() {
    assumeTrue(getMaxSupportedTimestamp().isPresent());

    final PayloadIdentifier pid = setupPayload(getMaxSupportedTimestamp().getAsLong() + 1);

    assertUnsupportedFork(resp(getMethodName(), pid));
  }

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsAfterFirstUnsupportedForkMilestone() {
    assumeTrue(getMaxSupportedTimestamp().isPresent());

    final PayloadIdentifier pid = setupPayload(getMaxSupportedTimestamp().getAsLong() + 2);

    assertUnsupportedFork(resp(getMethodName(), pid));
  }

  protected boolean validatesMinSupportedFork() {
    return false;
  }

  protected void assertUnsupportedFork(final JsonRpcResponse resp) {
    final JsonRpcError jsonRpcError = fromErrorResp(resp);
    assertThat(jsonRpcError.getCode()).isEqualTo(UNSUPPORTED_FORK.getCode());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldFailForUnknownPayloadId() {
    final var resp =
        resp(
            getMethodName(),
            PayloadIdentifier.forPayloadParams(
                new PreparePayloadArgsBuilder()
                    .parentHeader(new BlockHeaderTestFixture().buildHeader())
                    .timestamp(0L)
                    .prevRandao(Bytes32.random())
                    .feeRecipient(Address.fromHexString("0x42"))
                    .build()));
    assertThat(resp).isInstanceOf(JsonRpcErrorResponse.class);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  protected String getMethodName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_V1.getMethodName();
  }

  protected long getValidPayloadTimestamp() {
    return getMinSupportedTimestamp();
  }

  protected JsonRpcResponse resp(final String methodName, final PayloadIdentifier pid) {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", methodName, new Object[] {pid.serialize()})));
  }

  protected PayloadIdentifier setupPayload(final long timestamp) {
    PayloadIdentifier payloadIdentifier =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(new BlockHeaderTestFixture().buildHeader())
                .timestamp(timestamp)
                .prevRandao(Bytes32.random())
                .feeRecipient(Address.fromHexString("0x42"))
                .build());
    final BlockHeader mockHeader = blockHeaderTestFixture().timestamp(timestamp).buildHeader();
    final Block mockBlock =
        new Block(mockHeader, new BlockBody(Collections.emptyList(), Collections.emptyList()));
    BlockWithReceipts mockBlockWithReceipts =
        new BlockWithReceipts(mockBlock, Collections.emptyList());
    final PayloadWrapper mockPayload = createPayload(payloadIdentifier, mockBlockWithReceipts);
    when(mergeContext.retrievePayloadById(payloadIdentifier)).thenReturn(Optional.of(mockPayload));
    return payloadIdentifier;
  }

  protected PayloadWrapper createPayload(
      final PayloadIdentifier payloadIdentifier, final BlockWithReceipts blockWithReceipts) {
    return createPayload(payloadIdentifier, blockWithReceipts, Optional.empty());
  }

  protected PayloadWrapper createPayload(
      final PayloadIdentifier payloadIdentifier,
      final BlockWithReceipts blockWithReceipts,
      final Optional<List<Request>> requests) {
    return new PayloadWrapper(
        payloadIdentifier,
        blockWithReceipts,
        defaultBlockAccessList(),
        requests.or(this::defaultRequests),
        BlockCreationTiming.EMPTY);
  }

  protected Optional<BlockAccessList> defaultBlockAccessList() {
    return Optional.empty();
  }

  protected Optional<List<Request>> defaultRequests() {
    return Optional.empty();
  }

  protected BlockHeaderTestFixture blockHeaderTestFixture() {
    return new BlockHeaderTestFixture();
  }

  @Test
  public void shouldWaitForNonEmptyBlockWhenOnlyEmptyBlockAvailable() {
    final long validTimestamp = getValidPayloadTimestamp();
    final PayloadIdentifier testPid =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(new BlockHeaderTestFixture().buildHeader())
                .timestamp(validTimestamp)
                .prevRandao(Bytes32.random())
                .feeRecipient(Address.fromHexString("0x42"))
                .build());

    final BlockHeader testHeader =
        blockHeaderTestFixture()
            .prevRandao(Bytes32.random())
            .timestamp(validTimestamp)
            .buildHeader();
    final Block testBlock =
        new Block(testHeader, new BlockBody(Collections.emptyList(), Collections.emptyList()));
    final BlockWithReceipts testBlockWithReceipts =
        new BlockWithReceipts(testBlock, Collections.emptyList());
    final PayloadWrapper testPayload = createPayload(testPid, testBlockWithReceipts);

    final Block testBlockWithWithdrawals =
        new Block(
            testHeader,
            new BlockBody(
                Collections.emptyList(),
                Collections.emptyList(),
                Optional.of(Collections.emptyList())));
    final BlockWithReceipts testBlockWithReceiptsAndWithdrawals =
        new BlockWithReceipts(testBlockWithWithdrawals, Collections.emptyList());
    final PayloadWrapper testPayloadWithWithdrawals =
        createPayload(testPid, testBlockWithReceiptsAndWithdrawals);

    when(mergeContext.retrievePayloadById(testPid))
        .thenReturn(Optional.of(testPayload))
        .thenReturn(Optional.of(testPayloadWithWithdrawals));

    final JsonRpcResponse response = resp(getMethodName(), testPid);

    verify(mergeMiningCoordinator, times(1)).finalizeProposalById(testPid);
    verify(mergeMiningCoordinator, times(1)).awaitCurrentBuildCompletion(testPid);
    verify(mergeContext, times(2)).retrievePayloadById(testPid);

    InOrder inOrder = inOrder(mergeMiningCoordinator);
    inOrder.verify(mergeMiningCoordinator).finalizeProposalById(testPid);
    inOrder.verify(mergeMiningCoordinator).awaitCurrentBuildCompletion(testPid);

    assertThat(response).isNotInstanceOf(JsonRpcErrorResponse.class);
  }

  protected PayloadWrapper createDefaultPayloadWrapper() {
    this.mockHeader =
        blockHeaderTestFixture()
            .timestamp(getValidPayloadTimestamp())
            .prevRandao(Bytes32.random())
            .buildHeader();
    final Block mockBlock =
        new Block(
            mockHeader,
            new BlockBody(Collections.emptyList(), Collections.emptyList(), defaultWithdrawals()));
    return createPayload(mockPid, new BlockWithReceipts(mockBlock, Collections.emptyList()));
  }

  protected Optional<List<Withdrawal>> defaultWithdrawals() {
    return Optional.empty();
  }
}
