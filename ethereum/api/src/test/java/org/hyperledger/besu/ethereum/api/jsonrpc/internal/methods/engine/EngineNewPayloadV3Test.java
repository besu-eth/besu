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
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineTestSupport.fromErrorResp;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_PARAMS;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.UNSUPPORTED_FORK;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EnginePayloadStatusResult;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.CancunTargetingGasLimitCalculator;
import org.hyperledger.besu.ethereum.mainnet.ScheduledProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class})
public class EngineNewPayloadV3Test extends EngineNewPayloadV2Test {
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  protected static final KeyPair senderKeys = SIGNATURE_ALGORITHM.generateKeyPair();

  public EngineNewPayloadV3Test() {}

  @Override
  protected Set<ScheduledProtocolSpec.Hardfork> supportedHardforks() {
    return Set.of(cancunHardfork);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_newPayloadV3");
  }

  @BeforeEach
  @Override
  public void before() {
    super.before();
    maybeParentBeaconBlockRoot = Optional.of(Bytes32.ZERO);
    this.method =
        new EngineNewPayloadV3(
            vertx,
            protocolSchedule,
            protocolContext,
            mergeCoordinator,
            ethPeers,
            engineCallListener,
            new NoOpMetricsSystem());
    lenient().when(protocolSpec.getGasCalculator()).thenReturn(new CancunGasCalculator());
    lenient()
        .when(protocolSpec.getGasLimitCalculator())
        .thenReturn(mock(CancunTargetingGasLimitCalculator.class));
  }

  @Override
  public void shouldReturnUnsupportedForkIfBlockTimestampIsAfterCancunMilestone() {
    // only relevant for v2
  }

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsAfterPragueMilestone() {
    final BlockHeader pragueHeader =
        createBlockHeaderFixture(Optional.of(emptyList()))
            .timestamp(pragueHardfork.milestone())
            .buildHeader();

    var resp = resp(mockEnginePayload(pragueHeader, emptyList(), null));
    final JsonRpcError jsonRpcError = fromErrorResp(resp);
    assertThat(jsonRpcError.getCode()).isEqualTo(UNSUPPORTED_FORK.getCode());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsBeforeCancunMilestone() {
    final BlockHeader shanghaiHeader =
        createBlockHeaderFixture(Optional.of(emptyList()))
            .timestamp(cancunHardfork.milestone() - 1)
            .buildHeader();

    var resp = resp(mockEnginePayload(shanghaiHeader, emptyList(), null));
    final JsonRpcError jsonRpcError = fromErrorResp(resp);
    assertThat(jsonRpcError.getCode()).isEqualTo(UNSUPPORTED_FORK.getCode());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldInvalidVersionedHash_whenShortVersionedHash() {
    final Bytes shortHash = Bytes.fromHexString("0x" + "69".repeat(31));

    final EnginePayloadParameter payload = mock(EnginePayloadParameter.class);
    when(payload.getTimestamp()).thenReturn(cancunHardfork.milestone());
    when(payload.getExcessBlobGas()).thenReturn("99");
    when(payload.getBlobGasUsed()).thenReturn(9l);

    // TODO locking this as V3 otherwise this breaks the EngineNewPayloadV4Test subclass when method
    // field is V4
    final EngineNewPayloadV3 methodV3 =
        new EngineNewPayloadV3(
            vertx,
            protocolSchedule,
            protocolContext,
            mergeCoordinator,
            ethPeers,
            engineCallListener,
            new NoOpMetricsSystem());
    final JsonRpcResponse badParam =
        methodV3.response(
            new JsonRpcRequestContext(
                new JsonRpcRequest(
                    "2.0",
                    RpcMethod.ENGINE_NEW_PAYLOAD_V3.getMethodName(),
                    new Object[] {
                      payload,
                      List.of(shortHash.toHexString()),
                      "0x0000000000000000000000000000000000000000000000000000000000000000"
                    })));
    final EnginePayloadStatusResult res = fromSuccessResp(badParam);
    assertThat(res.getStatusAsString()).isEqualTo(INVALID.name());
    assertThat(res.getError()).isEqualTo("Invalid versionedHash");
  }

  @Test
  public void validateVersionedHash_whenListIsPresentAndEmpty() {
    final BlockHeader mockHeader =
        setupValidPayload(
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))),
            Optional.empty());
    final EnginePayloadParameter payload = mockEnginePayload(mockHeader, emptyList(), null);

    ValidationResult<RpcErrorType> res =
        method.validateParameters(
            payload,
            Optional.of(List.of()),
            Optional.of("0x0000000000000000000000000000000000000000000000000000000000000000"),
            Optional.empty());
    assertThat(res.isValid()).isTrue();
  }

  @Test
  public void validateExecutionRequests_whenPresent() {
    final BlockHeader mockHeader =
        setupValidPayload(
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))),
            Optional.empty());
    final EnginePayloadParameter payload = mockEnginePayload(mockHeader, emptyList(), null);

    ValidationResult<RpcErrorType> res =
        method.validateParameters(
            payload,
            Optional.of(List.of()),
            Optional.of("0x0000000000000000000000000000000000000000000000000000000000000000"),
            Optional.of(emptyList()));
    assertThat(res.isValid()).isFalse();
    assertThat(res.getInvalidReason()).isEqualTo(RpcErrorType.INVALID_EXECUTION_REQUESTS_PARAMS);
  }

  @Override
  protected BlockHeader createBlockHeader(final Optional<List<Withdrawal>> maybeWithdrawals) {
    BlockHeader parentBlockHeader =
        new BlockHeaderTestFixture()
            .baseFeePerGas(Wei.ONE)
            .timestamp(super.cancunHardfork.milestone())
            .buildHeader();

    when(blockchain.getBlockHeader(parentBlockHeader.getBlockHash()))
        .thenReturn(Optional.of(parentBlockHeader));
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    BlockHeader mockHeader =
        new BlockHeaderTestFixture()
            .baseFeePerGas(Wei.ONE)
            .parentHash(parentBlockHeader.getParentHash())
            .number(parentBlockHeader.getNumber() + 1)
            .timestamp(parentBlockHeader.getTimestamp() + 12)
            .withdrawalsRoot(maybeWithdrawals.map(BodyValidation::withdrawalsRoot).orElse(null))
            .excessBlobGas(BlobGas.ZERO)
            .blobGasUsed(0L)
            .parentBeaconBlockRoot(
                maybeParentBeaconBlockRoot.isPresent() ? maybeParentBeaconBlockRoot : null)
            .buildHeader();
    return mockHeader;
  }

  @Override
  @Test
  public void shouldReturnValidIfProtocolScheduleIsEmpty() {
    // no longer the case, blob validation requires a protocol schedule
  }

  @Test
  @Override
  public void shouldValidateBlobGasUsedCorrectly() {
    // V3 must return error if null blobGasUsed
    BlockHeader blockHeader =
        createBlockHeaderFixture(Optional.of(emptyList()))
            .timestamp(getTimestampForSupportedFork())
            .excessBlobGas(BlobGas.MAX_BLOB_GAS)
            .blobGasUsed(null)
            .buildHeader();

    var resp = resp(mockEnginePayload(blockHeader, emptyList(), List.of()));

    final JsonRpcError jsonRpcError = fromErrorResp(resp);
    assertThat(jsonRpcError.getCode()).isEqualTo(INVALID_PARAMS.getCode());
    assertThat(jsonRpcError.getData()).isEqualTo("Missing blob gas used field");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  @Override
  public void shouldValidateExcessBlobGasCorrectly() {
    // V3 must return error if null excessBlobGas
    BlockHeader blockHeader =
        createBlockHeaderFixture(Optional.of(emptyList()))
            .timestamp(getTimestampForSupportedFork())
            .excessBlobGas(null)
            .blobGasUsed(100L)
            .buildHeader();

    var resp = resp(mockEnginePayload(blockHeader, emptyList(), List.of()));

    final JsonRpcError jsonRpcError = fromErrorResp(resp);
    assertThat(jsonRpcError.getCode()).isEqualTo(INVALID_PARAMS.getCode());
    assertThat(jsonRpcError.getData()).isEqualTo("Missing excess blob gas field");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldRejectTransactionsWithFullBlobs() {

    Bytes transactionWithBlobsBytes =
        TransactionEncoder.encodeOpaqueBytes(
            createTransactionWithBlobs(), EncodingContext.POOLED_TRANSACTION);

    List<String> transactions = List.of(transactionWithBlobsBytes.toString());

    BlockHeader mockHeader =
        setupValidPayload(
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))),
            Optional.empty());
    var resp = resp(mockEnginePayload(mockHeader, transactions));

    EnginePayloadStatusResult res = fromSuccessResp(resp);
    assertThat(res.getStatusAsString()).isEqualTo(INVALID.name());
    assertThat(res.getError()).isEqualTo("Failed to decode transactions from block parameter");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  private Transaction createTransactionWithBlobs() {
    BlobTestFixture blobTestFixture = new BlobTestFixture();
    BlobsWithCommitments bwc = blobTestFixture.createBlobsWithCommitments(1);

    return new TransactionTestFixture()
        .to(Optional.of(Address.fromHexString("0xDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF")))
        .type(TransactionType.BLOB)
        .chainId(Optional.of(BigInteger.ONE))
        .maxFeePerGas(Optional.of(Wei.of(15)))
        .maxFeePerBlobGas(Optional.of(Wei.of(128)))
        .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
        .blobsWithCommitments(Optional.of(bwc))
        .versionedHashes(Optional.of(bwc.getVersionedHashes()))
        .createTransaction(senderKeys);
  }

  @Override
  protected JsonRpcResponse resp(final EnginePayloadParameter payload) {
    Object[] params =
        maybeParentBeaconBlockRoot
            .map(bytes32 -> new Object[] {payload, emptyList(), bytes32.toHexString()})
            .orElseGet(() -> new Object[] {payload});
    return method.response(
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", this.method.getName(), params)));
  }

  protected long getTimestampForSupportedFork() {
    final int index = new Random().nextInt(supportedHardforks().size());
    return supportedHardforks().stream().toList().get(index).milestone();
  }
}
