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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PRAGUE;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineTestSupport.fromErrorResp;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_PARAMS;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.UNSUPPORTED_FORK;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.PayloadStatusV1;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.mainnet.CancunTargetingGasLimitCalculator;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

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
  protected static final Bytes32 DEFAULT_PARENT_BEACON_BLOCK_ROOT = Bytes32.ZERO;

  public EngineNewPayloadV3Test() {}

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_newPayloadV3");
  }

  @BeforeEach
  @Override
  public void before() {
    super.before();
    lenient().when(protocolSpec.getGasCalculator()).thenReturn(new CancunGasCalculator());
    lenient()
        .when(protocolSpec.getGasLimitCalculator())
        .thenReturn(mock(CancunTargetingGasLimitCalculator.class));
  }

  @Override
  protected EngineNewPayloadV1<?, ?> createMethodInstance() {
    return new EngineNewPayloadV3<>(
        vertx,
        protocolSchedule,
        protocolContext,
        mergeCoordinator,
        ethPeers,
        engineCallListener,
        new NoOpMetricsSystem(),
        CANCUN,
        PRAGUE);
  }

  @Override
  protected long getMinSupportedTimestamp() {
    return cancunHardfork.milestone();
  }

  @Override
  protected long getMaxSupportedTimestamp() {
    return pragueHardfork.milestone() - 1;
  }

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsBeforeForkWindow() {
    BlockHeader blockHeader =
        setupValidPayload(
            getMinSupportedTimestamp() - 1,
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))),
            BlobGas.ZERO,
            0L);

    var resp =
        resp(
            mockEnginePayloadParam(blockHeader, emptyList()),
            emptyVersionedHashesParam(),
            zeroParentBeaconBlockRootParam());
    final JsonRpcError jsonRpcError = fromErrorResp(resp);
    assertThat(jsonRpcError.getCode()).isEqualTo(UNSUPPORTED_FORK.getCode());
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldInvalidVersionedHash_whenShortVersionedHash() {
    final Bytes shortHash = Bytes.fromHexString("0x" + "69".repeat(31));

    BlockHeader blockHeader =
        setupValidPayload(
            getMinSupportedTimestamp(),
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))),
            BlobGas.ZERO,
            0L);

    var resp =
        resp(
            mockEnginePayloadParam(blockHeader, emptyList()),
            List.of(shortHash.toHexString()),
            zeroParentBeaconBlockRootParam());
    final JsonRpcError jsonRpcError = fromErrorResp(resp);
    assertThat(jsonRpcError.getCode()).isEqualTo(INVALID_PARAMS.getCode());
    assertThat(jsonRpcError.getMessage())
        .isEqualTo("Failed to decode blob versioned hashes parameter");
  }

  @Test
  public void shouldValidateBlobGasUsedCorrectly() {
    // V3 must return error if null blobGasUsed
    BlobGas excessBlobGas = BlobGas.MAX_BLOB_GAS;
    Long blobGasUsed = null;

    BlockHeader blockHeader =
        setupValidPayload(
            getMinSupportedTimestamp(),
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))),
            excessBlobGas,
            blobGasUsed);

    var resp =
        resp(
            mockEnginePayloadParam(blockHeader, emptyList(), excessBlobGas, blobGasUsed),
            emptyVersionedHashesParam(),
            zeroParentBeaconBlockRootParam());

    final JsonRpcError jsonRpcError = fromErrorResp(resp);
    assertThat(jsonRpcError.getCode()).isEqualTo(INVALID_PARAMS.getCode());
    assertThat(jsonRpcError.getData()).isEqualTo("Missing blob gas used field");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldValidateExcessBlobGasCorrectly() {
    // V3 must return error if null excessBlobGas
    BlobGas excessBlobGas = null;
    Long blobGasUsed = 100L;

    BlockHeader blockHeader =
        setupValidPayload(
            getMinSupportedTimestamp(),
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))),
            excessBlobGas,
            blobGasUsed);

    var resp =
        resp(
            mockEnginePayloadParam(blockHeader, emptyList(), excessBlobGas, blobGasUsed),
            emptyVersionedHashesParam(),
            zeroParentBeaconBlockRootParam());

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
            getMinSupportedTimestamp(),
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, List.of()))));
    var resp = resp(mockEnginePayloadParam(mockHeader, transactions));

    PayloadStatusV1 res = fromSuccessResp(resp);
    assertThat(res.getStatusAsString()).isEqualTo(INVALID.name());
    assertThat(res.getError()).startsWith("Failed to decode transactions from block parameter");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  @Override
  public void shouldReturnInvalidIfWithdrawalsIsNotNull_WhenWithdrawalsProhibited() {
    // not applicable for V3 and later
  }

  @Test
  @Override
  public void shouldReturnValidIfWithdrawalsIsNull_WhenWithdrawalsProhibited() {
    // not applicable for V3 and later
  }

  @Override
  protected Object[] getVersionSpecificParams(final Map<String, Object> payloadParams) {
    return new Object[] {
      payloadParams, emptyVersionedHashesParam(), zeroParentBeaconBlockRootParam()
    };
  }

  protected BlockHeader setupValidPayload(
      final long timestamp,
      final BlockProcessingResult value,
      final BlobGas excessBlobGas,
      final Long blobGasUsed) {
    return setupValidPayload(
        timestamp, value, excessBlobGas, blobGasUsed, UnaryOperator.identity());
  }

  protected BlockHeader setupValidPayload(
      final long timestamp,
      final BlockProcessingResult value,
      final BlobGas excessBlobGas,
      final Long blobGasUsed,
      final UnaryOperator<BlockHeaderTestFixture> nextVersionSpecificModifier) {
    return super.setupValidPayload(
        timestamp,
        value,
        List.of(),
        fixture ->
            nextVersionSpecificModifier.apply(
                setBlobGasFields(fixture, excessBlobGas, blobGasUsed)));
  }

  private BlockHeaderTestFixture setBlobGasFields(
      final BlockHeaderTestFixture fixture, final BlobGas excessBlobGas, final Long blobGasUsed) {
    return fixture.excessBlobGas(excessBlobGas).blobGasUsed(blobGasUsed);
  }

  protected Map<String, Object> mockEnginePayloadParam(
      final BlockHeader header,
      final List<String> txs,
      final BlobGas excessBlobGas,
      final Long blobGasUsed) {
    var param = super.mockEnginePayloadParam(header, txs, emptyList());
    if (blobGasUsed != null) {
      param.put("blobGasUsed", Bytes.ofUnsignedLong(blobGasUsed).toHexString());
    } else {
      param.remove("blobGasUsed");
    }
    if (excessBlobGas != null) {
      param.put("excessBlobGas", excessBlobGas.toHexString());
    } else {
      param.remove("excessBlobGas");
    }
    return param;
  }

  @Override
  protected void setDefaultExecutionPayloadFields(
      final Map<String, Object> payload, final BlockHeader header, final List<String> txs) {
    super.setDefaultExecutionPayloadFields(payload, header, txs);
    if (header.getTimestamp() >= cancunHardfork.milestone()) {
      payload.put("blobGasUsed", header.getBlobGasUsed().orElse(0L));
      payload.put("excessBlobGas", header.getExcessBlobGas().orElse(BlobGas.ZERO));
    }
  }

  @Override
  protected BlockHeaderTestFixture versionSpecificBlockHeaderFixture(final long timestamp) {
    BlockHeaderTestFixture baseFixture = super.versionSpecificBlockHeaderFixture(timestamp);
    return setBlobGasFields(baseFixture, BlobGas.ZERO, 0L)
        .parentBeaconBlockRoot(Optional.of(DEFAULT_PARENT_BEACON_BLOCK_ROOT));
  }

  protected String zeroParentBeaconBlockRootParam() {
    return DEFAULT_PARENT_BEACON_BLOCK_ROOT.toHexString();
  }

  protected List<String> emptyVersionedHashesParam() {
    return emptyList();
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
}
