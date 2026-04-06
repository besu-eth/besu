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
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.PayloadWrapper;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResultFactory;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreationTiming;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockWithReceipts;

import java.util.Collections;
import java.util.Optional;

import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public abstract class AbstractEngineGetPayloadTest extends AbstractScheduledApiTest {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  protected static final KeyPair senderKeys = SIGNATURE_ALGORITHM.generateKeyPair();

  @FunctionalInterface
  interface MethodFactory {
    AbstractEngineGetPayload create(
        final Vertx vertx,
        final ProtocolContext protocolContext,
        final MergeMiningCoordinator mergeCoordinator,
        final BlockResultFactory ethPeers,
        final EngineCallListener engineCallListener);
  }

  private final Optional<MethodFactory> methodFactory;
  protected AbstractEngineGetPayload method;

  public AbstractEngineGetPayloadTest(final MethodFactory methodFactory) {
    this.methodFactory = Optional.of(methodFactory);
  }

  public AbstractEngineGetPayloadTest() {
    this.methodFactory = Optional.empty();
  }

  protected static final Vertx vertx = Vertx.vertx();
  protected static final BlockResultFactory factory = new BlockResultFactory();
  protected static final PayloadIdentifier mockPid =
      PayloadIdentifier.forPayloadParams(
          Hash.ZERO,
          1337L,
          Bytes32.random(),
          Address.fromHexString("0x42"),
          Optional.empty(),
          Optional.empty());
  protected static final BlockHeader mockHeader =
      new BlockHeaderTestFixture().prevRandao(Bytes32.random()).buildHeader();
  private static final Block mockBlock =
      new Block(mockHeader, new BlockBody(Collections.emptyList(), Collections.emptyList()));
  protected static final BlockWithReceipts mockBlockWithReceipts =
      new BlockWithReceipts(mockBlock, Collections.emptyList());
  protected static final PayloadWrapper mockPayload =
      new PayloadWrapper(
          mockPid,
          mockBlockWithReceipts,
          Optional.empty(),
          Optional.empty(),
          BlockCreationTiming.EMPTY);
  private static final Block mockBlockWithWithdrawals =
      new Block(
          mockHeader,
          new BlockBody(
              Collections.emptyList(),
              Collections.emptyList(),
              Optional.of(Collections.emptyList())));
  private static final Block mockBlockWithDepositRequests =
      new Block(
          mockHeader,
          new BlockBody(Collections.emptyList(), Collections.emptyList(), Optional.empty()));
  protected static final BlockWithReceipts mockBlockWithReceiptsAndWithdrawals =
      new BlockWithReceipts(mockBlockWithWithdrawals, Collections.emptyList());
  protected static final PayloadWrapper mockPayloadWithWithdrawals =
      new PayloadWrapper(
          mockPid,
          mockBlockWithReceiptsAndWithdrawals,
          Optional.empty(),
          Optional.empty(),
          BlockCreationTiming.EMPTY);

  protected static final BlockWithReceipts mockBlockWithReceiptsAndDepositRequests =
      new BlockWithReceipts(mockBlockWithDepositRequests, Collections.emptyList());
  protected static final PayloadWrapper mockPayloadWithDepositRequests =
      new PayloadWrapper(
          mockPid,
          mockBlockWithReceiptsAndDepositRequests,
          Optional.empty(),
          Optional.empty(),
          BlockCreationTiming.EMPTY);

  @Mock protected ProtocolContext protocolContext;

  @Mock protected MergeContext mergeContext;
  @Mock protected MergeMiningCoordinator mergeMiningCoordinator;

  @Mock protected EngineCallListener engineCallListener;

  @BeforeEach
  @Override
  public void before() {
    super.before();
    when(mergeContext.retrievePayloadById(mockPid)).thenReturn(Optional.of(mockPayload));
    when(protocolContext.safeConsensusContext(Mockito.any())).thenReturn(Optional.of(mergeContext));
    if (methodFactory.isPresent()) {
      this.method =
          methodFactory
              .get()
              .create(vertx, protocolContext, mergeMiningCoordinator, factory, engineCallListener);
    }
  }

  @Test
  public abstract void shouldReturnExpectedMethodName();

  @Test
  public abstract void shouldReturnBlockForKnownPayloadId();

  @Test
  public void shouldFailForUnknownPayloadId() {
    final var resp =
        resp(
            getMethodName(),
            PayloadIdentifier.forPayloadParams(
                Hash.ZERO,
                0L,
                Bytes32.random(),
                Address.fromHexString("0x42"),
                Optional.empty(),
                Optional.empty()));
    assertThat(resp).isInstanceOf(JsonRpcErrorResponse.class);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  abstract String getMethodName();

  /**
   * Returns a timestamp that is valid for the fork supported by the specific version being tested.
   * Each concrete test class must override this to return a timestamp that passes fork validation.
   */
  protected abstract long getValidPayloadTimestamp();

  protected JsonRpcResponse resp(final String methodName, final PayloadIdentifier pid) {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", methodName, new Object[] {pid.serialize()})));
  }

  protected PayloadIdentifier setupPayload(final long timestamp) {
    PayloadIdentifier payloadIdentifier =
        PayloadIdentifier.forPayloadParams(
            Hash.ZERO,
            timestamp,
            Bytes32.random(),
            Address.fromHexString("0x42"),
            Optional.empty(),
            Optional.empty());
    final BlockHeader mockHeader = new BlockHeaderTestFixture().timestamp(timestamp).buildHeader();
    final Block mockBlock =
        new Block(mockHeader, new BlockBody(Collections.emptyList(), Collections.emptyList()));
    BlockWithReceipts mockBlockWithReceipts =
        new BlockWithReceipts(mockBlock, Collections.emptyList());
    final PayloadWrapper mockPayload =
        new PayloadWrapper(
            payloadIdentifier,
            mockBlockWithReceipts,
            Optional.empty(),
            Optional.empty(),
            BlockCreationTiming.EMPTY);
    when(mergeContext.retrievePayloadById(payloadIdentifier)).thenReturn(Optional.of(mockPayload));
    return payloadIdentifier;
  }

  @Test
  public void shouldWaitForNonEmptyBlockWhenOnlyEmptyBlockAvailable() {
    // Setup: Create payloads with valid timestamp for the fork being tested
    // We simulate the scenario where:
    // 1. First retrieval returns empty block (payload without withdrawals)
    // 2. After waiting, we get a block that's been updated
    //    (payload with withdrawals - while both have 0 txs, this simulates the update)

    final long validTimestamp = getValidPayloadTimestamp();
    final PayloadIdentifier testPid =
        PayloadIdentifier.forPayloadParams(
            Hash.ZERO,
            validTimestamp,
            Bytes32.random(),
            Address.fromHexString("0x42"),
            Optional.empty(),
            Optional.empty());

    final BlockHeader testHeader =
        new BlockHeaderTestFixture()
            .prevRandao(Bytes32.random())
            .timestamp(validTimestamp)
            .buildHeader();
    final Block testBlock =
        new Block(testHeader, new BlockBody(Collections.emptyList(), Collections.emptyList()));
    final BlockWithReceipts testBlockWithReceipts =
        new BlockWithReceipts(testBlock, Collections.emptyList());
    final PayloadWrapper testPayload =
        new PayloadWrapper(
            testPid,
            testBlockWithReceipts,
            Optional.empty(),
            Optional.empty(),
            BlockCreationTiming.EMPTY);

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
        new PayloadWrapper(
            testPid,
            testBlockWithReceiptsAndWithdrawals,
            Optional.empty(),
            Optional.empty(),
            BlockCreationTiming.EMPTY);

    // Mock: First call returns the empty payload,
    // second call returns a different payload (simulating block building completion)
    when(mergeContext.retrievePayloadById(testPid))
        .thenReturn(Optional.of(testPayload))
        .thenReturn(Optional.of(testPayloadWithWithdrawals));

    // Execute: Call getPayload
    final JsonRpcResponse response = resp(getMethodName(), testPid);

    // Verify: finalizeProposalById was called (to signal loop to exit)
    verify(mergeMiningCoordinator, times(1)).finalizeProposalById(testPid);

    // Verify: awaitCurrentBuildCompletion was called
    verify(mergeMiningCoordinator, times(1)).awaitCurrentBuildCompletion(testPid);

    // Verify: retrievePayloadById was called twice (initial check + after waiting)
    verify(mergeContext, times(2)).retrievePayloadById(testPid);

    // Verify call order: finalize BEFORE wait (critical - finalize signals loop to exit)
    InOrder inOrder = inOrder(mergeMiningCoordinator);
    inOrder.verify(mergeMiningCoordinator).finalizeProposalById(testPid);
    inOrder.verify(mergeMiningCoordinator).awaitCurrentBuildCompletion(testPid);

    // Verify: response is successful (not an error)
    assertThat(response).isNotInstanceOf(JsonRpcErrorResponse.class);
  }
}
